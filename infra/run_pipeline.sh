#!/usr/bin/env bash
#
# Create (or update) the Databricks job from its template and trigger a run.
#
# databricks/job_medchain_pipeline.json is committed with ${VAR} placeholders rather
# than real values. The repository is public, and a workspace URL, storage account
# name or the single-user identity of a cluster are not credentials but are also not
# things to publish. Same reasoning as the ADF definitions.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
require_az
load_state

command -v databricks >/dev/null 2>&1 || die "Databricks CLI not found. See infra/unity_catalog.sh."

WORKSPACE_URL=$(az databricks workspace show \
  -n "${DATABRICKS_WORKSPACE}" -g "${RESOURCE_GROUP}" --query workspaceUrl -o tsv)
export DATABRICKS_HOST="https://${WORKSPACE_URL}"
export DATABRICKS_USER="${DATABRICKS_USER:-$(az account show --query user.name -o tsv)}"
export STORAGE_ACCOUNT CLUSTER_NODE_TYPE
export WHEEL_PATH="${WHEEL_PATH:-/Volumes/${CATALOG_NAME}/control/artifacts/medchain-0.1.0-py3-none-any.whl}"

rendered=$(mktemp /tmp/medchain-job-XXXXXX.json)
trap 'rm -f "${rendered}"' EXIT
envsubst < databricks/job_medchain_pipeline.json > "${rendered}"

JOB_NAME=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['name'])" "${rendered}")
JOB_ID=$(databricks jobs list --output json 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin); rows=d if isinstance(d,list) else d.get('jobs',[])
print(next((str(j['job_id']) for j in rows if j.get('settings',{}).get('name')==sys.argv[1]), ''))
" "${JOB_NAME}")

if [[ -z "${JOB_ID}" ]]; then
  log "Creating job ${JOB_NAME}"
  JOB_ID=$(databricks jobs create --json @"${rendered}" --output json 2>/dev/null \
    | python3 -c "import json,sys;print(json.load(sys.stdin)['job_id'])")
else
  log "Updating job ${JOB_NAME} (${JOB_ID})"
  python3 -c "
import json,sys
settings=json.load(open(sys.argv[1]))
print(json.dumps({'job_id': int(sys.argv[2]), 'new_settings': settings}))
" "${rendered}" "${JOB_ID}" | databricks jobs reset --json /dev/stdin >/dev/null 2>&1 \
    || warn "  could not update settings; running the existing definition"
fi

log "Job ${JOB_ID} — triggering a run"
RUN_ID=$(databricks jobs run-now "${JOB_ID}" --no-wait --output json 2>/dev/null \
  | python3 -c "import json,sys;print(json.load(sys.stdin)['run_id'])")

cat <<NEXT

  Run ${RUN_ID} started.

  Watch it:
    databricks jobs get-run ${RUN_ID}
    ${DATABRICKS_HOST}/#job/${JOB_ID}/run/${RUN_ID}

  Cluster time is the only thing here that costs money. When it finishes:
    make stop

NEXT
