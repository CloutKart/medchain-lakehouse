#!/usr/bin/env bash
#
# Create (or update) the Azure budget and its alert thresholds.
#
# A budget does not cap spending — Azure will not stop a running cluster because a
# threshold was crossed. What it buys is warning: on a fixed student grant with no
# overage, the failure mode is resources silently stopping mid-run, and an email at
# 50% is what prevents that being the first you hear of it.
#
# The real cap is `make stop` and autotermination. This is the smoke alarm, not the
# sprinkler.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
require_az
load_state

SUBSCRIPTION=$(subscription_id)
BUDGET_NAME="budget-${PROJECT}-${ENVIRONMENT}"

# Budgets run on calendar months; start at the beginning of the current one.
START_DATE=$(date -u +%Y-%m-01)
END_DATE=$(date -u -d "${START_DATE} +3 years" +%Y-%m-01 2>/dev/null \
  || date -u -v+3y -j -f "%Y-%m-%d" "${START_DATE}" +%Y-%m-01)

CONTACT_EMAIL="${CONTACT_EMAIL:-$(az account show --query user.name -o tsv)}"

log "Budget ${BUDGET_NAME}: \$${BUDGET_AMOUNT}/month, alerts to ${CONTACT_EMAIL}"

# Build one notification block per threshold.
notifications="{"
first=true
for threshold in ${BUDGET_ALERT_THRESHOLDS}; do
  [[ "${first}" == true ]] || notifications+=","
  first=false
  notifications+="\"alert-${threshold}\":{
    \"enabled\":true,
    \"operator\":\"GreaterThanOrEqualTo\",
    \"threshold\":${threshold},
    \"contactEmails\":[\"${CONTACT_EMAIL}\"],
    \"thresholdType\":\"Actual\"
  }"
done
notifications+="}"

az consumption budget create-with-rg \
  --budget-name "${BUDGET_NAME}" \
  --resource-group "${RESOURCE_GROUP}" \
  --amount "${BUDGET_AMOUNT}" \
  --category Cost \
  --time-grain Monthly \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --notifications "${notifications}" \
  --output none 2>/dev/null \
  && log "Budget created" \
  || warn "Could not create the budget. Student subscriptions sometimes lack the
       Microsoft.Consumption permissions for this. Set a budget manually in the
       portal (Cost Management > Budgets) and keep using 'make cost'."
