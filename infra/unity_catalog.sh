#!/usr/bin/env bash
#
# Create the Unity Catalog objects: external locations, catalog and schemas.
#
# Run this AFTER provision.sh, and after the Access Connector has been registered as
# a storage credential in the Databricks *account* console. Storage credentials are
# account-level rather than workspace-level, so that one step cannot be scripted with
# the workspace CLI — the script prints exactly what to click.
#
# The result: notebooks address abfss:// paths and Databricks authenticates as the
# connector's managed identity. No secret exists anywhere in the system.
#
# Authentication here is the Databricks CLI's Azure CLI credential chain: it reuses
# the `az login` you already have, so there is no PAT to mint, store or rotate.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
require_az
load_state

command -v databricks >/dev/null 2>&1 || die "Databricks CLI not found.
       Install it (no sudo needed):
         VER=\$(curl -fsSL https://api.github.com/repos/databricks/cli/releases/latest \\
               | python3 -c \"import json,sys;print(json.load(sys.stdin)['tag_name'].lstrip('v'))\")
         curl -fsSL -o /tmp/db.zip \\
           https://github.com/databricks/cli/releases/download/v\${VER}/databricks_cli_\${VER}_linux_amd64.zip
         unzip -oq /tmp/db.zip -d /tmp/db && install -m 0755 /tmp/db/databricks ~/.local/bin/"

WORKSPACE_URL=$(az databricks workspace show \
  -n "${DATABRICKS_WORKSPACE}" -g "${RESOURCE_GROUP}" --query workspaceUrl -o tsv)
export DATABRICKS_HOST="https://${WORKSPACE_URL}"

CONNECTOR_ID=$(az databricks access-connector show \
  --name "${ACCESS_CONNECTOR}" --resource-group "${RESOURCE_GROUP}" --query id -o tsv)
CREDENTIAL_NAME="${CREDENTIAL_NAME:-${PROJECT}-storage-credential}"

log "Workspace ${DATABRICKS_HOST}"
databricks current-user me --output json >/dev/null 2>&1 \
  || die "Databricks CLI cannot authenticate to ${DATABRICKS_HOST}.
       It uses your Azure CLI login; confirm with:  az account show"

# ------------------------------------------------------ storage credential
if ! databricks storage-credentials get "${CREDENTIAL_NAME}" >/dev/null 2>&1; then
  cat <<MANUAL

  One manual step is required first.
  ---------------------------------------------------------------
  Storage credentials are account-level objects and cannot be created with the
  workspace CLI. In the Databricks account console:

    Catalog  ->  External Data  ->  Storage Credentials  ->  Create

      Credential name              ${CREDENTIAL_NAME}
      Credential type              Azure Managed Identity
      Access connector ID          ${CONNECTOR_ID}
      User assigned managed identity ID   leave BLANK

  The last field matters: provision.sh creates the connector with a *system*
  assigned identity, so naming a user-assigned one points Databricks at an
  identity that does not exist. The credential validates and then fails on read.

  Then re-run this script.

MANUAL
  exit 1
fi
log "Storage credential ${CREDENTIAL_NAME} found"

# --------------------------------------------------------- external locations
# One per container, each scoped to exactly that container, so a grant on `bronze`
# cannot reach `gold`. A single location over the whole account would make every
# per-layer grant decorative.
for container in "${CONTAINERS[@]}"; do
  url="abfss://${container}@${STORAGE_ACCOUNT}.dfs.core.windows.net/"
  name="${PROJECT}-${container}"
  if databricks external-locations get "${name}" >/dev/null 2>&1; then
    log "External location ${name} (exists)"
  else
    if error=$(databricks external-locations create "${name}" "${url}" "${CREDENTIAL_NAME}" \
         --comment "MedChain ${container} layer" 2>&1); then
      log "External location ${name} -> ${url}"
    else
      warn "Failed to create external location ${name}:"
      printf '       %s\n' "${error}" | head -3 >&2
    fi
  fi
done

# ---------------------------------------------------------- catalog + schemas
# A catalog needs a managed-table storage root whenever the metastore has none,
# which is the default on a new Azure Databricks workspace. Without --storage-root
# the create fails with "Metastore storage root URL does not exist" — a message that
# sounds like a broken metastore rather than a missing argument.
#
# Every table this project creates is external, so this location stays empty. It
# exists because Unity Catalog requires one.
MANAGED_ROOT="abfss://${MANAGED_CONTAINER}@${STORAGE_ACCOUNT}.dfs.core.windows.net/"

if databricks catalogs get "${CATALOG_NAME}" >/dev/null 2>&1; then
  log "Catalog ${CATALOG_NAME} (exists)"
else
  if error=$(databricks catalogs create "${CATALOG_NAME}" \
       --comment "MedChain Analytics lakehouse" \
       --storage-root "${MANAGED_ROOT}" 2>&1); then
    log "Catalog ${CATALOG_NAME} (managed root: ${MANAGED_ROOT})"
  else
    warn "Failed to create catalog ${CATALOG_NAME}:"
    printf '       %s\n' "${error}" | head -4 >&2
  fi
fi

# One schema per medallion layer. These names must match the `layer` argument that
# medchain.utils.tables.register_table passes, since it registers external tables as
# <catalog>.<layer>.<table>.
for schema in bronze silver gold control quarantine; do
  if databricks schemas get "${CATALOG_NAME}.${schema}" >/dev/null 2>&1; then
    log "Schema ${CATALOG_NAME}.${schema} (exists)"
  else
    if error=$(databricks schemas create "${schema}" "${CATALOG_NAME}" 2>&1); then
      log "Schema ${CATALOG_NAME}.${schema}"
    else
      warn "Failed to create schema ${CATALOG_NAME}.${schema}:"
      printf '       %s\n' "${error}" | head -3 >&2
    fi
  fi
done

# ------------------------------------------------------------------ verify
log ""
log "Verifying access by listing each external location..."
FAILED=0
for container in "${CONTAINERS[@]}"; do
  name="${PROJECT}-${container}"
  if databricks external-locations get "${name}" >/dev/null 2>&1; then
    printf '  %-28s ok\n' "${name}"
  else
    printf '  %-28s MISSING\n' "${name}"
    FAILED=1
  fi
done

cat <<NEXT

  Unity Catalog is ready.

  Tables are registered as EXTERNAL tables over the same abfss:// paths the
  pipeline writes to, so the catalog is a view onto the storage layout rather than
  a second source of truth. That is what lets identical transformation code run
  locally against a directory and on the cluster against ADLS.

  Next:
    export MEDCHAIN_ENV=azure
    export STORAGE_ACCOUNT=${STORAGE_ACCOUNT}
    make upload      # push the generated landing data to ADLS
    make deploy      # wheel + notebooks + ADF pipelines

NEXT

exit ${FAILED}
