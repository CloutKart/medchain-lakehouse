#!/usr/bin/env bash
#
# Create the Unity Catalog objects: storage credential, external locations, catalog
# and schemas.
#
# Run this AFTER provision.sh, and after the Access Connector has been registered as
# a storage credential in the Databricks *account* console. Storage credentials are
# account-level rather than workspace-level, so that one step cannot be scripted with
# the workspace CLI — the script prints exactly what to click.
#
# The result: notebooks address abfss:// paths and Databricks authenticates as the
# connector's managed identity. No secret exists anywhere in the system.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
require_az
load_state

command -v databricks >/dev/null 2>&1 || die "Databricks CLI not found: uv tool install databricks-cli"

CONNECTOR_ID=$(az databricks access-connector show \
  --name "${ACCESS_CONNECTOR}" --resource-group "${RESOURCE_GROUP}" --query id -o tsv)
CREDENTIAL_NAME="${PROJECT}-storage-credential"

if ! databricks unity-catalog storage-credentials get --name "${CREDENTIAL_NAME}" >/dev/null 2>&1; then
  cat <<MANUAL

  One manual step is required first.
  ---------------------------------------------------------------
  Storage credentials are account-level objects and cannot be created with the
  workspace CLI. In the Databricks account console:

    Catalog  ->  External Data  ->  Storage Credentials  ->  Create

      Name                     ${CREDENTIAL_NAME}
      Type                     Azure Managed Identity
      Access Connector ID      ${CONNECTOR_ID}

  Then re-run this script.

MANUAL
  exit 1
fi

log "Storage credential ${CREDENTIAL_NAME} found"

# External locations — one per container, each scoped to exactly that container so a
# compromised grant on `bronze` cannot reach `gold`.
for container in "${CONTAINERS[@]}"; do
  url="abfss://${container}@${STORAGE_ACCOUNT}.dfs.core.windows.net/"
  name="${PROJECT}-${container}"
  log "External location ${name} -> ${url}"
  databricks unity-catalog external-locations create \
    --name "${name}" --url "${url}" --storage-credential-name "${CREDENTIAL_NAME}" \
    2>/dev/null || log "  already exists"
done

# Catalog and schemas, one schema per medallion layer.
log "Catalog ${CATALOG_NAME}"
databricks unity-catalog catalogs create --name "${CATALOG_NAME}" \
  --comment "MedChain Analytics lakehouse" 2>/dev/null || log "  already exists"

for schema in bronze silver gold control quarantine; do
  log "Schema ${CATALOG_NAME}.${schema}"
  databricks unity-catalog schemas create \
    --catalog-name "${CATALOG_NAME}" --name "${schema}" 2>/dev/null || log "  already exists"
done

cat <<NEXT

  Unity Catalog is ready.

  Tables are registered as EXTERNAL tables over the same abfss:// paths the pipeline
  writes to, so the catalog is a view onto the storage layout rather than a second
  source of truth. That is what lets identical transformation code run locally
  against a directory and on the cluster against ADLS.

  Verify:
    databricks unity-catalog external-locations list
    databricks unity-catalog schemas list --catalog-name ${CATALOG_NAME}

NEXT
