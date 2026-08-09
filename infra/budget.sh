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
  # No `thresholdType` field: azure-cli 2.8x removed it from the notification model
  # ("Model 'AAZObjectArg' has no field named 'thresholdType'") and the API defaults
  # to Actual, which is what we want anyway — alert on money actually spent, not on
  # a forecast.
  notifications+="\"alert-${threshold}\":{
    \"enabled\":true,
    \"operator\":\"GreaterThanOrEqualTo\",
    \"threshold\":${threshold},
    \"contactEmails\":[\"${CONTACT_EMAIL}\"]
  }"
done
notifications+="}"

# azure-cli 2.8x takes the window as a single --time-period object. The older
# --start-date/--end-date pair was removed and now fails with "unrecognized
# arguments", so the error looks like a permissions problem when it is a syntax one.
# Full ISO 8601 datetimes, not bare dates. The API rejects "2026-08-01" with
# "Start date should be the first day of the month" — which it plainly is. The
# message is about the format it could not parse, not the day of the month.
time_period="{\"startDate\":\"${START_DATE}T00:00:00Z\",\"endDate\":\"${END_DATE}T00:00:00Z\"}"

error=$(az consumption budget create-with-rg \
  --budget-name "${BUDGET_NAME}" \
  --resource-group "${RESOURCE_GROUP}" \
  --amount "${BUDGET_AMOUNT}" \
  --category Cost \
  --time-grain Monthly \
  --time-period "${time_period}" \
  --notifications "${notifications}" \
  --output none 2>&1) \
  && log "Budget created: \$${BUDGET_AMOUNT}/month, alerts at ${BUDGET_ALERT_THRESHOLDS}%" \
  || {
    # Print the real reason rather than guessing at it. Student subscriptions do
    # sometimes lack Microsoft.Consumption write access, but so far every failure
    # here has been a CLI contract change, and swallowing the message hid that.
    warn "Could not create the budget:"
    printf '       %s\n' "${error}" | head -5 >&2
    warn "Set one manually in the portal (Cost Management > Budgets)."
    warn "The real cost controls are 'make stop' and autotermination regardless."
  }
