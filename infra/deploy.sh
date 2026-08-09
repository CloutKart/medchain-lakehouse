#!/usr/bin/env bash
#
# Build the medchain wheel, publish it to a Unity Catalog volume, and deploy the
# notebooks and ADF pipelines.
#
# The wheel is what makes the notebooks thin: cluster code and locally tested code
# are the same artefact, so a green pytest run says something about what will
# actually execute on Databricks. The wheel also carries conf/ (see the
# force-include in pyproject.toml), so there is no second deployment step for
# configuration and no way for the two to drift apart.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
require_az
load_state

command -v databricks >/dev/null 2>&1 || die "Databricks CLI not found. See infra/unity_catalog.sh for install instructions."

WORKSPACE_URL=$(az databricks workspace show \
  -n "${DATABRICKS_WORKSPACE}" -g "${RESOURCE_GROUP}" --query workspaceUrl -o tsv)
export DATABRICKS_HOST="https://${WORKSPACE_URL}"

VOLUME_SCHEMA="${VOLUME_SCHEMA:-control}"
VOLUME_NAME="${VOLUME_NAME:-artifacts}"
VOLUME_PATH="/Volumes/${CATALOG_NAME}/${VOLUME_SCHEMA}/${VOLUME_NAME}"
NOTEBOOK_DIR="${NOTEBOOK_DIR:-/Shared/medchain}"

log "Workspace ${DATABRICKS_HOST}"

# ---------------------------------------------------------------- build wheel
log "Building the medchain wheel"
rm -rf dist
uv build --wheel >/dev/null 2>&1 || die "uv build failed"
WHEEL=$(ls -t dist/medchain-*.whl | head -1)
[[ -n "${WHEEL}" ]] || die "No wheel produced in dist/"

# The wheel is useless on a cluster without its configuration, and that failure only
# shows up minutes into a pipeline run. Check here instead.
python3 - "${WHEEL}" <<'PY' || die "Wheel is missing conf/ — check the force-include in pyproject.toml"
import sys, zipfile
names = set(zipfile.ZipFile(sys.argv[1]).namelist())
required = ["base.yaml", "sources.yaml", "quality.yaml", "seed/tpa_rules.csv"]
missing = [r for r in required if f"medchain/conf/{r}" not in names]
if missing:
    print("missing from wheel:", missing)
    sys.exit(1)
PY
log "  ${WHEEL} (conf/ included)"

# ------------------------------------------------- publish wheel to a volume
# A Unity Catalog volume rather than DBFS. DBFS root is deprecated, and on a
# UC-enabled workspace installing libraries from it is restricted depending on the
# cluster's access mode — which surfaces as a library-install failure at cluster
# start, not at deploy time.
if ! databricks volumes read "${CATALOG_NAME}.${VOLUME_SCHEMA}.${VOLUME_NAME}" >/dev/null 2>&1; then
  log "Creating volume ${VOLUME_PATH}"
  if error=$(databricks volumes create "${CATALOG_NAME}" "${VOLUME_SCHEMA}" "${VOLUME_NAME}" MANAGED 2>&1); then
    log "  created"
  else
    warn "Could not create volume:"
    printf '       %s\n' "${error}" | head -4 >&2
    die "Cannot publish the wheel without a volume."
  fi
fi

REMOTE_WHEEL="${VOLUME_PATH}/$(basename "${WHEEL}")"
log "Publishing wheel to ${REMOTE_WHEEL}"
databricks fs cp --overwrite "${WHEEL}" "dbfs:${REMOTE_WHEEL}" >/dev/null 2>&1 \
  || die "Failed to copy the wheel to ${REMOTE_WHEEL}"

# ------------------------------------------------------------ deploy notebooks
log "Deploying notebooks to ${NOTEBOOK_DIR}"
databricks workspace mkdirs "${NOTEBOOK_DIR}" >/dev/null 2>&1 || true
for notebook in notebooks/*.py; do
  name=$(basename "${notebook}" .py)
  if databricks workspace import "${NOTEBOOK_DIR}/${name}" \
       --file "${notebook}" --language PYTHON --format SOURCE --overwrite >/dev/null 2>&1; then
    log "  ${name}"
  else
    warn "  failed to import ${name}"
  fi
done

# ------------------------------------------------------------- ADF objects
#
# Order is not cosmetic — Data Factory validates references at create time:
#
#   linked services  ->  datasets  ->  pl_ingest_source  ->  pl_master
#
# Deploying a pipeline before the datasets it names fails with "invalid reference
# 'ds_landing_folder'", and pl_master fails on 'pl_ingest_source' for the same
# reason. There is no --force, and no eventual consistency to wait out.
if az account show >/dev/null 2>&1; then
  WORKSPACE_RESOURCE_ID=$(az databricks workspace show \
    -n "${DATABRICKS_WORKSPACE}" -g "${RESOURCE_GROUP}" --query id -o tsv)
  export STORAGE_ACCOUNT WORKSPACE_URL WORKSPACE_RESOURCE_ID CLUSTER_NODE_TYPE

  # Definitions carry ${VAR} placeholders so nothing environment-specific — and
  # nothing resembling a credential — is committed.
  render() { envsubst < "$1"; }

  log "Deploying ADF linked services"
  for file in adf/linked_services/*.json; do
    name=$(basename "${file}" .json)
    if error=$(az datafactory linked-service create \
         --resource-group "${RESOURCE_GROUP}" --factory-name "${DATA_FACTORY}" \
         --linked-service-name "${name}" \
         --properties "$(render "${file}")" --output none 2>&1); then
      log "  ${name}"
    else
      warn "  ${name} failed:"
      printf '       %s\n' "${error}" | head -3 >&2
    fi
  done

  log "Deploying ADF datasets"
  for file in adf/datasets/*.json; do
    name=$(basename "${file}" .json)
    if error=$(az datafactory dataset create \
         --resource-group "${RESOURCE_GROUP}" --factory-name "${DATA_FACTORY}" \
         --dataset-name "${name}" \
         --properties "$(render "${file}")" --output none 2>&1); then
      log "  ${name}"
    else
      warn "  ${name} failed:"
      printf '       %s\n' "${error}" | head -3 >&2
    fi
  done

  log "Deploying ADF pipelines"
  # pl_ingest_source first: pl_master executes it.
  for pipeline in adf/pipeline_ingest_source.json adf/pipeline_master.json; do
    [[ -f "${pipeline}" ]] || continue
    name=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['name'])" "${pipeline}")
    properties=$(python3 -c "import json,sys; print(json.dumps(json.load(open(sys.argv[1]))['properties']))" "${pipeline}")
    if error=$(az datafactory pipeline create \
         --resource-group "${RESOURCE_GROUP}" --factory-name "${DATA_FACTORY}" \
         --name "${name}" --pipeline "${properties}" --output none 2>&1); then
      log "  ${name}"
    else
      warn "  ${name} failed:"
      printf '       %s\n' "${error}" | head -4 >&2
    fi
  done
else
  warn "Skipping ADF deployment (az not logged in)"
fi

# ------------------------------------------------------------------- verify
log ""
log "Deployed:"
databricks fs ls "dbfs:${VOLUME_PATH}" 2>/dev/null | sed 's/^/  volume  /' || warn "  volume listing failed"
databricks workspace list "${NOTEBOOK_DIR}" 2>/dev/null | tail -n +2 | awk '{print "  notebook "$1}' || true

cat <<NEXT

  Attach the wheel to your cluster as a library:
    ${REMOTE_WHEEL}

  Then trigger the pipeline:
    az datafactory pipeline create-run \\
      --resource-group ${RESOURCE_GROUP} \\
      --factory-name ${DATA_FACTORY} \\
      --name pl_master

  Cluster time is the only thing here that costs real money. Stop it when done:
    make stop

NEXT
