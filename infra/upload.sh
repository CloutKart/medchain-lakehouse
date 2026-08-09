#!/usr/bin/env bash
#
# Upload the locally generated landing data to the ADLS landing container.
#
# Data is generated locally rather than on the cluster on purpose: generation is
# single-threaded pandas/numpy work that gains nothing from Spark, and doing it on a
# cluster would burn DBUs producing data a laptop makes in two minutes.
#
# Uses `az storage blob upload-batch`, which transfers through the Python SDK.
# The obvious alternative, `az storage fs directory upload`, shells out to AzCopy and
# downloads it on first use — and when that download fails (restricted network, a
# dead release URL) the command still exits 0 while transferring nothing. A silent
# no-op that reports success is worse than a failure, so this script verifies the
# remote file count afterwards rather than trusting the exit code.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
require_az
load_state

LOCAL_LANDING="${LOCAL_LANDING:-./data/landing}"
LOCAL_TRUTH="${LOCAL_TRUTH:-./data/_truth}"
[[ -d "${LOCAL_LANDING}" ]] || die "No local data at ${LOCAL_LANDING}. Run: make gen SCALE=1.0"

LOCAL_FILES=$(find "${LOCAL_LANDING}" -type f | wc -l)
SIZE=$(du -sh "${LOCAL_LANDING}" | cut -f1)
log "Uploading ${SIZE} (${LOCAL_FILES} files) to ${STORAGE_ACCOUNT}/landing"

# One batch per source directory. Uploading the whole tree in a single call gives no
# progress signal for several minutes and no way to resume a partial transfer; per
# source, a failure names the source that failed.
for source_dir in "${LOCAL_LANDING}"/*/; do
  source_name=$(basename "${source_dir}")
  count=$(find "${source_dir}" -type f | wc -l)
  printf '  %-24s %3d files ... ' "${source_name}" "${count}"
  if az storage blob upload-batch \
       --destination landing \
       --destination-path "${source_name}" \
       --source "${source_dir}" \
       --account-name "${STORAGE_ACCOUNT}" \
       --auth-mode login \
       --overwrite \
       --output none >/dev/null 2>&1; then
    echo "ok"
  else
    echo "FAILED"
    warn "  retrying ${source_name} with output shown"
    az storage blob upload-batch \
      --destination landing --destination-path "${source_name}" \
      --source "${source_dir}" --account-name "${STORAGE_ACCOUNT}" \
      --auth-mode login --overwrite --output none 2>&1 | tail -5
  fi
done

# ---------------------------------------------------------- reference data
#
# `_truth/` is not only the scorecard's answer key. Three of its files are genuine
# *inputs* that Gold cannot build without:
#
#   hospital_truth.parquet  the hospital register (names, cities, bed capacity)
#   ward_truth.parquet      ward capacities, the denominator of every occupancy rate
#   visit_truth.parquet     the visit spine — a HIS export that is simply not one of
#                           the seven files the source-list enumerates
#
# Omitting this directory is why the first cluster run failed at Gold with
# "Path does not exist: .../_truth/hospital_truth.parquet" — after Bronze and Silver
# had already run for 53 minutes. The measurement-only files (mpi_truth,
# claim_transitions_truth, tpa_truth) travel with them because the quality scorecard
# needs them to report recovery metrics rather than internal consistency.
if [[ -d "${LOCAL_TRUTH}" ]]; then
  truth_files=$(find "${LOCAL_TRUTH}" -type f | wc -l)
  printf '  %-24s %3d files ... ' "_truth" "${truth_files}"
  if az storage blob upload-batch \
       --destination landing --destination-path "_truth" \
       --source "${LOCAL_TRUTH}" --account-name "${STORAGE_ACCOUNT}" \
       --auth-mode login --overwrite --output none >/dev/null 2>&1; then
    echo "ok"
    LOCAL_FILES=$((LOCAL_FILES + truth_files))
  else
    echo "FAILED"
    die "Reference data upload failed — Gold cannot build without it."
  fi
else
  warn "No ${LOCAL_TRUTH}; Gold will fail without the hospital and ward registers."
fi

# ------------------------------------------------------------------- verify
# The count that matters is what is actually in the container, not what the CLI
# claimed. `fs file list` returns directories alongside files, so they have to be
# separated — and `isDirectory` is a JSON boolean. A JMESPath filter comparing it to
# the string 'false' matches nothing and reports an empty container after a perfectly
# good upload, so the counting happens in Python where the type is unambiguous.
read -r REMOTE_FILES REMOTE_MB < <(
  az storage fs file list -f landing \
    --account-name "${STORAGE_ACCOUNT}" --auth-mode login --output json 2>/dev/null \
  | python3 -c "
import json, sys
try:
    rows = json.load(sys.stdin)
except Exception:
    print(0, 0); sys.exit()
files = [r for r in rows if not r.get('isDirectory')]
print(len(files), round(sum(r.get('contentLength') or 0 for r in files) / 1e6, 1))
"
)

log "Local files: ${LOCAL_FILES}   Remote files: ${REMOTE_FILES} (${REMOTE_MB} MB)"

if [[ "${REMOTE_FILES}" -lt "${LOCAL_FILES}" ]]; then
  die "Upload incomplete: ${REMOTE_FILES} of ${LOCAL_FILES} files present in ADLS."
fi

log "Upload verified."
log "Inspect with:"
log "  az storage fs file list -f landing --account-name ${STORAGE_ACCOUNT} --auth-mode login -o table"
