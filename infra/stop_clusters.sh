#!/usr/bin/env bash
#
# Terminate every running Databricks cluster in the workspace.
#
# Autotermination is set to 10 minutes, but 10 minutes of an idle all-purpose
# cluster is still ~$0.10, and a cluster left running overnight by accident is most
# of a day's budget. Run this when you stop working.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
load_state

command -v databricks >/dev/null 2>&1 || die "Databricks CLI not found: uv tool install databricks-cli"

running=$(databricks clusters list --output json 2>/dev/null \
  | python3 -c "
import json,sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for c in (data.get('clusters') or data if isinstance(data, dict) else data) or []:
    if c.get('state') in ('RUNNING','PENDING','RESIZING'):
        print(c['cluster_id'], c.get('cluster_name',''))
")

if [[ -z "${running}" ]]; then
  log "No running clusters."
  exit 0
fi

echo "${running}" | while read -r cluster_id name; do
  [[ -z "${cluster_id}" ]] && continue
  log "Terminating ${name} (${cluster_id})"
  databricks clusters delete --cluster-id "${cluster_id}" 2>/dev/null \
    || warn "  could not terminate ${cluster_id}"
done
log "Done."
