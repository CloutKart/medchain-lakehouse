#!/usr/bin/env bash
# Shared settings for every infra script. Source this, do not run it.
#
# Nothing secret belongs here — no keys, no connection strings, no service principal
# passwords. Authentication is Unity Catalog via a managed identity, so there is
# nothing to leak. Override any value by exporting it before running a script.

set -euo pipefail

# ---------------------------------------------------------------- identifiers
export PROJECT="${PROJECT:-medchain}"
export ENVIRONMENT="${ENVIRONMENT:-dev}"
export RESOURCE_GROUP="${RESOURCE_GROUP:-rg-${PROJECT}-${ENVIRONMENT}}"

# Central India keeps the data in-country, which is the right default for a
# healthcare workload. Azure for Students sometimes has no Databricks-compatible VM
# quota there; provision.sh probes and falls back rather than failing halfway.
export LOCATION="${LOCATION:-centralindia}"
export FALLBACK_LOCATION="${FALLBACK_LOCATION:-eastus}"

# Storage account names are globally unique, lowercase, 3-24 chars, no hyphens.
# The suffix is derived from the subscription id so re-running provision.sh finds
# the same account instead of creating a second one.
export STORAGE_ACCOUNT="${STORAGE_ACCOUNT:-}"
export DATABRICKS_WORKSPACE="${DATABRICKS_WORKSPACE:-dbw-${PROJECT}-${ENVIRONMENT}}"
export DATA_FACTORY="${DATA_FACTORY:-adf-${PROJECT}-${ENVIRONMENT}}"
export ACCESS_CONNECTOR="${ACCESS_CONNECTOR:-ac-${PROJECT}-${ENVIRONMENT}}"
export KEY_VAULT="${KEY_VAULT:-kv-${PROJECT}-${ENVIRONMENT}}"
export CATALOG_NAME="${CATALOG_NAME:-medchain}"

# The seven data-layer containers, plus `managed`. Every table this project creates
# is EXTERNAL — registered over an explicit abfss:// path — but a Unity Catalog
# catalog still requires a managed-table storage root. Giving it a dedicated
# container keeps anything accidentally created as a managed table out of the data
# layers, where it would be indistinguishable from pipeline output.
export CONTAINERS=(landing bronze silver gold control quarantine checkpoints managed)
export MANAGED_CONTAINER="${MANAGED_CONTAINER:-managed}"

# ---------------------------------------------------------------- cost control
# Azure for Students is a fixed grant with no overage: when it is gone, the
# resources stop. Every number below exists to make that harder to hit.
export BUDGET_AMOUNT="${BUDGET_AMOUNT:-100}"
export BUDGET_ALERT_THRESHOLDS="${BUDGET_ALERT_THRESHOLDS:-25 50 80 95}"
# Standard_DS3_v2 is the node type every Databricks tutorial names, and it has been
# retired from newer Azure regions — it does not exist in centralindia at all.
# Standard_D4s_v3 is the current-generation equivalent (4 vCPU, 16 GB) and fits the
# 4-vCPU regional quota that Azure for Students grants. provision.sh probes anyway
# and picks something else if this is unavailable.
export CLUSTER_NODE_TYPE="${CLUSTER_NODE_TYPE:-Standard_D4s_v3}"
export CLUSTER_AUTOTERMINATE_MINUTES="${CLUSTER_AUTOTERMINATE_MINUTES:-10}"
export DATABRICKS_SKU="${DATABRICKS_SKU:-premium}"   # premium is required for Unity Catalog

export STATE_FILE="${STATE_FILE:-$(dirname "${BASH_SOURCE[0]}")/.azure_state}"

# ---------------------------------------------------------------- helpers

log()  { printf '\033[0;36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
warn() { printf '\033[0;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[0;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

require_az() {
  command -v az >/dev/null 2>&1 || die "Azure CLI not found. Install it: https://aka.ms/azure-cli"
  az account show >/dev/null 2>&1 || die "Not logged in. Run:  az login"
}

subscription_id() { az account show --query id -o tsv; }

# Deterministic storage account name derived from the subscription, so re-running
# provision.sh is idempotent instead of creating a second account each time.
resolve_storage_account() {
  if [[ -n "${STORAGE_ACCOUNT}" ]]; then
    echo "${STORAGE_ACCOUNT}"
    return
  fi
  local suffix
  suffix=$(subscription_id | tr -d '-' | cut -c1-8)
  echo "st${PROJECT}${suffix}"
}

save_state() {
  {
    echo "# Written by infra/provision.sh on $(date -Iseconds). Safe to commit? No — gitignored."
    echo "export RESOURCE_GROUP='${RESOURCE_GROUP}'"
    echo "export LOCATION='${LOCATION}'"
    echo "export STORAGE_ACCOUNT='${STORAGE_ACCOUNT}'"
    echo "export DATABRICKS_WORKSPACE='${DATABRICKS_WORKSPACE}'"
    echo "export DATA_FACTORY='${DATA_FACTORY}'"
    echo "export ACCESS_CONNECTOR='${ACCESS_CONNECTOR}'"
    echo "export CATALOG_NAME='${CATALOG_NAME}'"
  } > "${STATE_FILE}"
  log "Wrote ${STATE_FILE}"
}

load_state() {
  [[ -f "${STATE_FILE}" ]] && source "${STATE_FILE}"
}
