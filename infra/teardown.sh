#!/usr/bin/env bash
#
# Delete the entire resource group.
#
# This is destructive and irreversible: the storage account, every Delta table in
# it, the Databricks workspace and all notebooks in it go away. Nothing is
# recoverable afterwards.
#
# It is also the single most effective cost control available. Deleting the group
# when you finish a session is what makes a fixed grant last, and everything here is
# reproducible from the repo: `make gen` regenerates the data byte-identically from
# the seed, and `make deploy` reinstalls the code.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
require_az
load_state

if ! az group show --name "${RESOURCE_GROUP}" >/dev/null 2>&1; then
  log "Resource group ${RESOURCE_GROUP} does not exist. Nothing to do."
  exit 0
fi

echo
warn "About to DELETE the resource group '${RESOURCE_GROUP}' and everything in it:"
az resource list --resource-group "${RESOURCE_GROUP}" \
  --query "[].{name:name, type:type}" -o table 2>/dev/null || true
echo
warn "This cannot be undone. Data in the storage account will be lost."
echo
read -r -p "Type the resource group name to confirm: " confirm

if [[ "${confirm}" != "${RESOURCE_GROUP}" ]]; then
  log "Names did not match. Aborted — nothing deleted."
  exit 0
fi

log "Deleting ${RESOURCE_GROUP} (this runs in the background on Azure)..."
az group delete --name "${RESOURCE_GROUP}" --yes --no-wait
rm -f "${STATE_FILE}"

log "Deletion started. Check progress with:"
log "  az group show --name ${RESOURCE_GROUP}"
log "Rebuild any time with: ./infra/provision.sh && make gen && make upload && make deploy"
