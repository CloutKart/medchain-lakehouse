#!/usr/bin/env bash
#
# Upload the locally generated landing data to the ADLS landing container.
#
# Data is generated locally rather than on the cluster on purpose: generation is
# single-threaded pandas/numpy work that gains nothing from Spark, and doing it on a
# cluster would burn DBUs producing data that a laptop makes in two minutes.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
require_az
load_state

LOCAL_LANDING="${LOCAL_LANDING:-./data/landing}"
[[ -d "${LOCAL_LANDING}" ]] || die "No local data at ${LOCAL_LANDING}. Run: make gen SCALE=1.0"

SIZE=$(du -sh "${LOCAL_LANDING}" | cut -f1)
log "Uploading ${SIZE} from ${LOCAL_LANDING} to ${STORAGE_ACCOUNT}/landing"

# --recursive with the directory preserved keeps the source/initial_load|incremental
# layout that Bronze's ingest_date extraction depends on.
az storage fs directory upload \
  --account-name "${STORAGE_ACCOUNT}" \
  --file-system landing \
  --source "${LOCAL_LANDING}" \
  --recursive \
  --auth-mode login \
  --output none

log "Upload complete. Verify with:"
log "  az storage fs file list -f landing --account-name ${STORAGE_ACCOUNT} --auth-mode login -o table"
