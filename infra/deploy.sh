#!/usr/bin/env bash
#
# Build the medchain wheel, install it on the cluster, and deploy the notebooks
# and ADF pipelines.
#
# The wheel is what makes the notebooks thin: cluster code and locally tested code
# are the same artefact, so a green pytest run says something about what will
# actually execute on Databricks.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
load_state

command -v databricks >/dev/null 2>&1 || die "Databricks CLI not found: uv tool install databricks-cli"

# ---------------------------------------------------------------- build wheel
log "Building the medchain wheel"
uv build --wheel --out-dir dist
WHEEL=$(ls -t dist/medchain-*.whl | head -1)
log "  ${WHEEL}"

# --------------------------------------------------------------- upload wheel
DBFS_WHEEL="dbfs:/FileStore/medchain/$(basename "${WHEEL}")"
log "Uploading wheel to ${DBFS_WHEEL}"
databricks fs cp --overwrite "${WHEEL}" "${DBFS_WHEEL}"

# ------------------------------------------------------------ deploy notebooks
log "Deploying notebooks to /Shared/medchain"
databricks workspace mkdirs /Shared/medchain 2>/dev/null || true
for notebook in notebooks/*.py; do
  name=$(basename "${notebook}" .py)
  log "  ${name}"
  databricks workspace import --overwrite --language PYTHON --format SOURCE \
    --file "${notebook}" "/Shared/medchain/${name}"
done

# ---------------------------------------------------------------- config files
# conf/ is read at runtime by the wheel, so it has to travel with it.
log "Uploading conf/ to DBFS"
databricks fs cp --overwrite --recursive conf dbfs:/FileStore/medchain/conf

# ------------------------------------------------------------- ADF pipelines
if command -v az >/dev/null 2>&1 && az account show >/dev/null 2>&1; then
  log "Deploying ADF pipelines"
  for pipeline in adf/pipeline_*.json; do
    name=$(python3 -c "import json,sys; print(json.load(open('${pipeline}'))['name'])")
    log "  ${name}"
    az datafactory pipeline create \
      --resource-group "${RESOURCE_GROUP}" \
      --factory-name "${DATA_FACTORY}" \
      --name "${name}" \
      --pipeline @"${pipeline}" \
      --output none 2>/dev/null || warn "    failed — check linked services exist first"
  done
else
  warn "Skipping ADF deployment (az not logged in)"
fi

cat <<NEXT

  Deployment complete.

  Attach the wheel to your cluster as a library:
    ${DBFS_WHEEL}

  Then trigger the pipeline:
    az datafactory pipeline create-run \\
      --resource-group ${RESOURCE_GROUP} \\
      --factory-name ${DATA_FACTORY} \\
      --name pl_master

  And remember to stop the cluster when you are done:  make stop

NEXT
