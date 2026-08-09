#!/usr/bin/env bash
#
# Provision the MedChain lakehouse on Azure.
#
#   ./infra/provision.sh            # create everything
#   ./infra/provision.sh --dry-run  # print the plan and the cost estimate, create nothing
#
# THIS SPENDS REAL CREDIT. The script is idempotent — re-running it finds existing
# resources instead of duplicating them — but the first run creates a Databricks
# workspace, and Azure Databricks is not free on any subscription tier.
#
# What gets created:
#   - resource group
#   - ADLS Gen2 storage account (hierarchical namespace) + 7 containers
#   - Azure Databricks workspace (Premium — required for Unity Catalog)
#   - Access Connector (managed identity) granted Storage Blob Data Contributor
#   - Azure Data Factory with a system-assigned identity
#   - Key Vault (fallback auth path only)
#   - a budget with alerts, so credit exhaustion is not a surprise
#
# Authentication is Unity Catalog through the Access Connector's managed identity.
# No storage keys, no service principal secrets, nothing credential-bearing lands on
# disk or in a notebook.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

require_az
STORAGE_ACCOUNT=$(resolve_storage_account)
SUBSCRIPTION=$(subscription_id)

cat <<PLAN

  MedChain Azure provisioning plan
  ---------------------------------------------------------------
  subscription        ${SUBSCRIPTION}
  resource group      ${RESOURCE_GROUP}
  location            ${LOCATION}   (fallback: ${FALLBACK_LOCATION})
  storage account     ${STORAGE_ACCOUNT}   (ADLS Gen2, HNS enabled)
  containers          ${CONTAINERS[*]}
  databricks          ${DATABRICKS_WORKSPACE}   (sku: ${DATABRICKS_SKU})
  access connector    ${ACCESS_CONNECTOR}
  data factory        ${DATA_FACTORY}
  key vault           ${KEY_VAULT}
  budget              \$${BUDGET_AMOUNT} with alerts at ${BUDGET_ALERT_THRESHOLDS}%

  Indicative running cost
  ---------------------------------------------------------------
  ADLS Gen2 (~1 GB)                    ~\$0.03 / month
  Data Factory (activity runs)         ~\$1 per 1,000 runs
  Databricks job compute, single node  ~\$0.30-0.40 / hour
  Databricks all-purpose, single node  ~\$0.55-0.65 / hour

  Storage and ADF are rounding errors. Cluster time is the entire budget:
  roughly 150-180 hours of all-purpose compute on a \$100 grant. Develop
  locally (make run-local costs nothing), use the cluster for scaled runs.

PLAN

if [[ "${DRY_RUN}" == true ]]; then
  log "Dry run — nothing created."
  exit 0
fi

read -r -p "Create these resources? This spends credit. [y/N] " reply
[[ "${reply}" =~ ^[Yy]$ ]] || { log "Aborted."; exit 0; }

# --------------------------------------------------------------- region probe
# Azure for Students frequently has zero quota for Databricks-compatible VM SKUs in
# some regions. Finding that out *before* creating a resource group and a storage
# account is much better than failing three steps in with half a deployment.
log "Checking ${CLUSTER_NODE_TYPE} availability in ${LOCATION}..."
if ! az vm list-skus --location "${LOCATION}" --size "${CLUSTER_NODE_TYPE}" \
      --query "[?name=='${CLUSTER_NODE_TYPE}'] | [0].name" -o tsv 2>/dev/null | grep -q .; then
  warn "${CLUSTER_NODE_TYPE} is not offered in ${LOCATION}; falling back to ${FALLBACK_LOCATION}"
  LOCATION="${FALLBACK_LOCATION}"
fi
log "Using region ${LOCATION}"

# ------------------------------------------------------------ resource group
log "Resource group ${RESOURCE_GROUP}"
az group create --name "${RESOURCE_GROUP}" --location "${LOCATION}" \
  --tags project="${PROJECT}" environment="${ENVIRONMENT}" managed-by=provision.sh \
  --output none

# ------------------------------------------------------------------- storage
log "Storage account ${STORAGE_ACCOUNT} (ADLS Gen2)"
if ! az storage account show -n "${STORAGE_ACCOUNT}" -g "${RESOURCE_GROUP}" >/dev/null 2>&1; then
  az storage account create \
    --name "${STORAGE_ACCOUNT}" \
    --resource-group "${RESOURCE_GROUP}" \
    --location "${LOCATION}" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --enable-hierarchical-namespace true \
    --min-tls-version TLS1_2 \
    --allow-blob-public-access false \
    --output none
else
  log "  already exists"
fi

for container in "${CONTAINERS[@]}"; do
  az storage fs create --name "${container}" \
    --account-name "${STORAGE_ACCOUNT}" --auth-mode login --output none 2>/dev/null \
    && log "  container ${container}" || log "  container ${container} (exists)"
done

# ------------------------------------------------------- databricks workspace
log "Databricks workspace ${DATABRICKS_WORKSPACE} (${DATABRICKS_SKU})"
if ! az databricks workspace show -n "${DATABRICKS_WORKSPACE}" -g "${RESOURCE_GROUP}" >/dev/null 2>&1; then
  az databricks workspace create \
    --name "${DATABRICKS_WORKSPACE}" \
    --resource-group "${RESOURCE_GROUP}" \
    --location "${LOCATION}" \
    --sku "${DATABRICKS_SKU}" \
    --output none
else
  log "  already exists"
fi

# ------------------------------------------------- access connector + RBAC
# The Access Connector is a managed identity that Unity Catalog uses to reach the
# storage account. This is what removes credentials from the picture entirely:
# notebooks reference abfss:// paths and Databricks authenticates as the connector.
log "Access Connector ${ACCESS_CONNECTOR}"
az databricks access-connector create \
  --name "${ACCESS_CONNECTOR}" \
  --resource-group "${RESOURCE_GROUP}" \
  --location "${LOCATION}" \
  --identity-type SystemAssigned \
  --output none 2>/dev/null || log "  already exists"

CONNECTOR_PRINCIPAL=$(az databricks access-connector show \
  --name "${ACCESS_CONNECTOR}" --resource-group "${RESOURCE_GROUP}" \
  --query identity.principalId -o tsv)

log "Granting Storage Blob Data Contributor to the connector"
az role assignment create \
  --assignee-object-id "${CONNECTOR_PRINCIPAL}" \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/${SUBSCRIPTION}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT}" \
  --output none 2>/dev/null || log "  role assignment already present"

# ---------------------------------------------------------------- key vault
# Only used by the fallback service-principal auth path (ADR-004). Created now
# because creating it later, mid-incident, is worse.
log "Key Vault ${KEY_VAULT}"
az keyvault create \
  --name "${KEY_VAULT}" --resource-group "${RESOURCE_GROUP}" --location "${LOCATION}" \
  --enable-rbac-authorization true --output none 2>/dev/null || log "  already exists"

# -------------------------------------------------------------- data factory
log "Data Factory ${DATA_FACTORY}"
az datafactory create \
  --name "${DATA_FACTORY}" --resource-group "${RESOURCE_GROUP}" --location "${LOCATION}" \
  --output none 2>/dev/null || log "  already exists"

ADF_PRINCIPAL=$(az datafactory show --name "${DATA_FACTORY}" --resource-group "${RESOURCE_GROUP}" \
  --query identity.principalId -o tsv 2>/dev/null || echo "")

if [[ -n "${ADF_PRINCIPAL}" ]]; then
  log "Granting ADF access to storage"
  az role assignment create \
    --assignee-object-id "${ADF_PRINCIPAL}" \
    --assignee-principal-type ServicePrincipal \
    --role "Storage Blob Data Contributor" \
    --scope "/subscriptions/${SUBSCRIPTION}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT}" \
    --output none 2>/dev/null || log "  role assignment already present"
fi

# ------------------------------------------------------------------- budget
log "Budget: \$${BUDGET_AMOUNT} with alerts"
"$(dirname "${BASH_SOURCE[0]}")/budget.sh" || warn "Budget creation failed (non-fatal)"

save_state

WORKSPACE_URL=$(az databricks workspace show -n "${DATABRICKS_WORKSPACE}" -g "${RESOURCE_GROUP}" \
  --query workspaceUrl -o tsv)

cat <<NEXT

  Provisioning complete.
  ---------------------------------------------------------------
  Databricks workspace   https://${WORKSPACE_URL}
  Storage account        ${STORAGE_ACCOUNT}
  Access connector id    $(az databricks access-connector show -n "${ACCESS_CONNECTOR}" -g "${RESOURCE_GROUP}" --query id -o tsv)

  Next steps
  ---------------------------------------------------------------
  1. Export the storage account for the azure config profile:
       export STORAGE_ACCOUNT=${STORAGE_ACCOUNT}
       export MEDCHAIN_ENV=azure

  2. In the Databricks account console, create the Unity Catalog storage
     credential from the Access Connector above, then run:
       ./infra/unity_catalog.sh

  3. Upload the generated data and deploy the code:
       make gen SCALE=1.0
       make upload
       make deploy

  4. Watch the spend. Seriously:
       make cost

  Tear everything down with:  ./infra/teardown.sh

NEXT
