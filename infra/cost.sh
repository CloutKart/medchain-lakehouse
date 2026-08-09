#!/usr/bin/env bash
#
# Month-to-date spend for the resource group, broken down by service.
#
# Run it at the start and end of every session. The number that matters is
# Databricks; storage and ADF are rounding errors at this scale.

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
require_az
load_state

SUBSCRIPTION=$(subscription_id)
START=$(date -u +%Y-%m-01)
END=$(date -u +%Y-%m-%d)

log "Spend for ${RESOURCE_GROUP}, ${START} to ${END}"
echo

az consumption usage list \
  --start-date "${START}" --end-date "${END}" \
  --query "[?contains(instanceName, '${RESOURCE_GROUP}') || contains(instanceId, '${RESOURCE_GROUP}')].{service:meterCategory, cost:pretaxCost, currency:currency}" \
  -o json 2>/dev/null \
  | python3 -c "
import json, sys
from collections import defaultdict
try:
    rows = json.load(sys.stdin)
except Exception:
    print('  Could not read usage data. Student subscriptions often restrict the')
    print('  Consumption API — check Cost Management in the portal instead.')
    sys.exit(0)
if not rows:
    print('  No usage recorded yet (billing data lags by up to 24 hours).')
    sys.exit(0)
totals = defaultdict(float)
currency = 'USD'
for r in rows:
    totals[r['service']] += float(r['cost'] or 0)
    currency = r.get('currency') or currency
width = max(len(s) for s in totals)
for service, cost in sorted(totals.items(), key=lambda kv: -kv[1]):
    print(f'  {service:<{width}}  {cost:>10.2f} {currency}')
print(f'  {\"TOTAL\":<{width}}  {sum(totals.values()):>10.2f} {currency}')
"
echo
log "Budget: \$${BUDGET_AMOUNT}. Log this in docs/cost_log.md."
