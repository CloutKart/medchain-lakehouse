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

# Everything before the plan is read-only. The plan therefore reports the region and
# node type that were actually *resolved* against this subscription, not the
# unvalidated defaults — a dry run that prints a node type which cannot be allocated
# is worse than no dry run, because it inspires confidence it has not earned.

# ------------------------------------------------------- resource providers
# A subscription only exposes a resource type once its provider is registered, and
# on a fresh student subscription several are not. Registering is free, idempotent
# and instant to request — but the create call fails outright without it, which is a
# poor way to discover the problem half way through a deployment.
log "Checking resource providers"
UNREGISTERED=()
for provider in Microsoft.Storage Microsoft.Databricks Microsoft.DataFactory Microsoft.KeyVault; do
  state=$(az provider show -n "${provider}" --query registrationState -o tsv 2>/dev/null || echo "Unknown")
  printf '  %-28s %s\n' "${provider}" "${state}"
  [[ "${state}" == "Registered" ]] || UNREGISTERED+=("${provider}")
done

if [[ ${#UNREGISTERED[@]} -gt 0 && "${DRY_RUN}" != true ]]; then
  for provider in "${UNREGISTERED[@]}"; do
    log "  registering ${provider}"
    az provider register -n "${provider}" --output none 2>/dev/null || warn "  could not register ${provider}"
  done
  # Registration is asynchronous and the first create call fails without it.
  for provider in "${UNREGISTERED[@]}"; do
    for _ in $(seq 1 36); do
      state=$(az provider show -n "${provider}" --query registrationState -o tsv 2>/dev/null || echo "Unknown")
      [[ "${state}" == "Registered" ]] && break
      sleep 5
    done
    log "  ${provider}: ${state}"
    [[ "${state}" == "Registered" ]] || warn "  ${provider} still ${state}; its resource may fail to create"
  done
fi

# ------------------------------------------------- region and node type probe
#
# Two traps here, both of which cost a half-finished deployment to discover:
#
# 1. `Standard_DS3_v2` is the node type every Databricks tutorial names, and it has
#    been retired from newer regions — it is simply absent from centralindia. Probing
#    for one hard-coded SKU and falling back to one hard-coded region can therefore
#    fail twice and still proceed with a node type that cannot be allocated.
#
# 2. A SKU's `restrictions` are not uniformly fatal. A restriction with
#    `type: Zone` means the SKU is unavailable *in availability zones* but usable for
#    a normal regional deployment, which is what Databricks creates here. Treating
#    those as fatal rejects every viable node type on this subscription.
#
# So: walk a candidate list across both regions, accept Zone-scoped restrictions,
# reject only Location-scoped ones, and pick the first that survives.
NODE_CANDIDATES=(
  "${CLUSTER_NODE_TYPE}"
  Standard_D4s_v3 Standard_D4s_v5 Standard_D4as_v5 Standard_D4ds_v5
  Standard_DS3_v2 Standard_F4s_v2
)

select_node_type() {
  local region="$1" skus candidate usable
  skus=$(az vm list-skus --location "${region}" --resource-type virtualMachines -o json 2>/dev/null)
  [[ -z "${skus}" ]] && return 1

  for candidate in "${NODE_CANDIDATES[@]}"; do
    usable=$(printf '%s' "${skus}" | python3 -c "
import json, sys
name = sys.argv[1]
for sku in json.load(sys.stdin):
    if sku.get('name') != name:
        continue
    # Location-scoped restrictions mean the SKU is unusable in the region at all.
    # Zone-scoped restrictions only rule out zonal deployments.
    if any(r.get('type') == 'Location' for r in (sku.get('restrictions') or [])):
        sys.exit(0)
    print(name)
    sys.exit(0)
" "${candidate}" 2>/dev/null)
    if [[ -n "${usable}" ]]; then
      echo "${candidate}"
      return 0
    fi
  done
  return 1
}

log "Probing usable Databricks node types (this takes a moment)..."
SELECTED_NODE=""
for region in "${LOCATION}" "${FALLBACK_LOCATION}"; do
  log "  checking ${region}..."
  if SELECTED_NODE=$(select_node_type "${region}"); then
    LOCATION="${region}"
    break
  fi
  warn "  no usable node type in ${region}"
  SELECTED_NODE=""
done

[[ -n "${SELECTED_NODE}" ]] || die "No usable Databricks node type in ${LOCATION} or ${FALLBACK_LOCATION}.
       Request a quota increase, or set CLUSTER_NODE_TYPE to a SKU your
       subscription can allocate:  az vm list-skus -l <region> -o table"

if [[ "${SELECTED_NODE}" != "${CLUSTER_NODE_TYPE}" ]]; then
  warn "${CLUSTER_NODE_TYPE} is unavailable; using ${SELECTED_NODE} instead"
fi
CLUSTER_NODE_TYPE="${SELECTED_NODE}"
log "Region ${LOCATION}, node type ${CLUSTER_NODE_TYPE}"

# ---------------------------------------------------------------- vCPU quota
# A single-node cluster is one VM. If the regional vCPU limit is smaller than the
# node, the cluster will never start — and Databricks reports that as an opaque
# timeout several minutes into the first pipeline run.
# Note the pipe placement: `[?name=='X'] | [0].capabilities[...]` selects the SKU
# first and then reads into it. Filtering and projecting in one expression
# (`[?name=='X'].capabilities[...]`) yields a nested list that flattens to empty,
# which silently skips the quota check rather than failing it.
NODE_VCPUS=$(az vm list-skus --location "${LOCATION}" --resource-type virtualMachines \
  --query "[?name=='${CLUSTER_NODE_TYPE}'] | [0].capabilities[?name=='vCPUs'].value | [0]" -o tsv 2>/dev/null)
VCPU_LIMIT=$(az vm list-usage --location "${LOCATION}" \
  --query "[?localName=='Total Regional vCPUs'].limit | [0]" -o tsv 2>/dev/null)

if [[ -n "${NODE_VCPUS}" && -n "${VCPU_LIMIT}" ]]; then
  log "Regional vCPU quota: ${VCPU_LIMIT}; ${CLUSTER_NODE_TYPE} needs ${NODE_VCPUS}"
  if (( NODE_VCPUS > VCPU_LIMIT )); then
    die "${CLUSTER_NODE_TYPE} needs ${NODE_VCPUS} vCPUs but the regional limit is ${VCPU_LIMIT}.
       The cluster cannot start. Request a quota increase in the portal
       (Subscriptions > Usage + quotas) before provisioning."
  fi
  if (( NODE_VCPUS == VCPU_LIMIT )); then
    warn "Quota allows exactly one ${CLUSTER_NODE_TYPE} node and nothing else."
    warn "Only one cluster can run at a time — an ADF job cluster will not start"
    warn "while an interactive cluster is up. Use 'make stop' between runs."
  fi
fi

# ------------------------------------------------------------------- the plan
cat <<PLAN

  MedChain Azure provisioning plan
  ---------------------------------------------------------------
  subscription        ${SUBSCRIPTION}
  resource group      ${RESOURCE_GROUP}
  location            ${LOCATION}   (resolved; fallback was ${FALLBACK_LOCATION})
  node type           ${CLUSTER_NODE_TYPE}   (resolved: ${NODE_VCPUS:-?} vCPU, quota ${VCPU_LIMIT:-?})
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
  if [[ ${#UNREGISTERED[@]} -gt 0 ]]; then
    warn "Would register these resource providers first: ${UNREGISTERED[*]}"
  fi
  log "Dry run — nothing created."
  exit 0
fi

read -r -p "Create these resources? This spends credit. [y/N] " reply
[[ "${reply}" =~ ^[Yy]$ ]] || { log "Aborted."; exit 0; }

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
