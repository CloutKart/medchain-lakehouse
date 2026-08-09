#!/usr/bin/env bash
#
# Download the dashboard's JSON from ADLS after the cluster export has run.
#
# The export cannot run from a laptop against Azure: Databricks Runtime 15.4 enables
# deletion vectors on new Delta tables, and neither delta-rs nor DuckDB's Delta
# extension can read a table carrying that reader feature. So on Azure the export
# runs on the cluster (notebooks/40_web_export.py) and writes to gold/_web; this
# fetches the result.
#
# Local development does not need any of this — `make web-data` reads local Gold
# directly through DuckDB in about four seconds.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
require_az
load_state

TARGET="${TARGET:-./dashboards/web/public/data}"
mkdir -p "${TARGET}"

log "Fetching dashboard data from ${STORAGE_ACCOUNT}/gold/_web"
az storage blob download-batch \
  --source gold --pattern "_web/*" \
  --destination "${TARGET}" \
  --account-name "${STORAGE_ACCOUNT}" --auth-mode login --overwrite \
  --output none 2>/dev/null || die "Download failed. Has the export job run?"

# download-batch preserves the _web/ prefix as a directory; flatten it so the paths
# match what the frontend fetches.
if [[ -d "${TARGET}/_web" ]]; then
  mv "${TARGET}"/_web/*.json "${TARGET}/" 2>/dev/null || true
  rmdir "${TARGET}/_web" 2>/dev/null || true
fi

count=$(find "${TARGET}" -maxdepth 1 -name "*.json" | wc -l)
size=$(du -sh "${TARGET}" | cut -f1)
log "Fetched ${count} files (${size}) to ${TARGET}"

[[ "${count}" -ge 6 ]] || die "Expected 6 panel files, found ${count}."

python3 - "${TARGET}/headline.json" <<'PY'
import json, sys
h = json.load(open(sys.argv[1]))
c, f = h["clinical"], h["financial"]
print(f"  readmission gap : {c['readmission_gap_pp']:.2f} pp")
print(f"  misattributed   : {h['attribution']['misattributed']:,}")
print(f"  recoverable     : Rs {f['room_excess'] / 1e7:,.0f} Cr")
s = h["source"]
print(f"  computed by     : {s['engine']} on {s['store']} ({s['environment']})")
print(f"  exported at     : {s['generated_at']}")
PY
