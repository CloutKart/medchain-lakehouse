# Azure Cost Log

Azure for Students is a fixed grant with no overage: when it is gone, resources stop.
This log makes spend visible before that happens.

## Budget

| Item | Value |
|---|---|
| Grant | $100 |
| Alert thresholds | 25% / 50% / 80% / 95% |
| Workspace SKU | Premium (required for Unity Catalog — see ADR-004) |
| Node type | `Standard_DS3_v2`, single node, spot with on-demand fallback |
| Auto-termination | 10 minutes |

## What actually costs money

| Service | Rate | Reality |
|---|---|---|
| ADLS Gen2 | ~$0.02/GB/month | ~$0.03/month for 1.5 GB. Irrelevant. |
| Data Factory | ~$1 per 1,000 activity runs | ~$0.05/month at daily cadence. Irrelevant. |
| Databricks **job** compute | ~$0.30–0.40/hour | Pipeline runs |
| Databricks **all-purpose** compute | ~$0.55–0.65/hour | Interactive development |

**Cluster time is the entire budget** — roughly 150–180 hours of all-purpose compute.
Everything else rounds to zero.

## Controls in place

1. **Develop locally.** `make run-local` executes the identical code against local
   Delta tables and costs nothing. The cluster is for scaled runs and demos only.
2. **Job compute for pipelines** — roughly half the DBU rate of all-purpose.
3. **10-minute auto-termination** on every cluster.
4. **Spot instances** with fallback to on-demand.
5. **`make stop`** terminates every running cluster.
6. **`./infra/teardown.sh`** deletes the entire resource group. Everything is
   reproducible: `make gen` regenerates the data byte-identically from the seed, and
   `make deploy` reinstalls the code.
7. **Budget alerts** by email at four thresholds.

## Session log

Record every session. `make cost` reports month-to-date spend by service.

| Date | Duration | What was done | Cluster hrs | Est. cost | Cumulative | Notes |
|---|---|---|---|---|---|---|
| _(pending)_ | | Local development only — no Azure resources provisioned yet | 0 | $0.00 | $0.00 | Full pipeline built and validated locally at zero cost |

## Habits that matter

- `make cost` at the start and end of every session.
- `make stop` before closing the laptop. A cluster left running overnight is ~$6 —
  6% of the entire grant.
- `./infra/teardown.sh` at the end of a work block spanning days.
- Never run the generator on the cluster. It is single-threaded pandas work that a
  laptop does in two minutes; doing it on Databricks burns DBUs to no purpose.
