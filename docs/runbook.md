# Pipeline Runbook

Operational procedures for the MedChain lakehouse. Written to be usable at 2am by
someone who did not build it.

---

## 1. Normal operation

The `pl_master` ADF pipeline runs daily at 02:00 IST. It ingests all seven sources
into Bronze, then runs Silver, Gold and the quality scorecard as Databricks notebook
activities with proper dependencies — a Silver failure never lets a stale Gold build
publish.

| Source | Cadence | Trigger |
|---|---|---|
| patient_registrations, billing_transactions, bed_occupancy_log, claim_line_items | Daily | `tr_daily_0200_ist` |
| insurance_claims | Weekly | `tr_weekly_sunday` |
| doctor_assignments, procedure_master | Weekly | `tr_weekly_sunday` |

**Expected runtime** (single-node `Standard_DS3_v2`, full 3-year dataset):

| Layer | Duration | Rows |
|---|---|---|
| Bronze | ~2 min | 5.6M |
| Silver | ~7 min | — |
| Gold | ~4 min | 1.79M |
| Quality | ~2 min | 49 checks |

Silver is dominated by the MPI (~2 min: 1.8M candidate pairs scored, then connected
components).

### Running a layer by hand

```bash
export MEDCHAIN_ENV=azure            # or local
export STORAGE_ACCOUNT=stmedchain...

medchain-run bronze  --date 2025-03-31
medchain-run silver  --date 2025-03-31
medchain-run gold    --date 2025-03-31
medchain-run quality --date 2025-03-31
medchain-run all     --date 2025-03-31
```

Restrict scope while debugging:

```bash
medchain-run bronze --date 2025-03-31 --sources insurance_claims
medchain-run silver --date 2025-03-31 --steps mpi claim_history
```

---

## 2. Is the pipeline healthy?

Three tables answer this. Check them in order.

```sql
-- 1. Did every batch finish?
SELECT layer, source, status, row_count, started_at, finished_at, error_message
FROM medchain.control.batch_registry
WHERE ingest_date = '2025-03-31'
ORDER BY layer, source;

-- 2. How far has each source been processed?
SELECT source, layer, last_processed_date, updated_at
FROM medchain.control.watermark
ORDER BY layer, source;

-- 3. Did the warehouse pass its own checks?
SELECT check_name, severity, passed, actual_value, threshold, detail
FROM medchain.gold.dq_scorecard
WHERE run_ts = (SELECT MAX(run_ts) FROM medchain.gold.dq_scorecard)
  AND NOT passed
ORDER BY severity, check_name;
```

A healthy run: every batch `SUCCEEDED`, watermarks at the expected date, zero
blocking failures.

---

## 3. Failure playbook

### Bronze — "Header mismatch for source X"

**Meaning.** The source file's header no longer matches `conf/sources.yaml`. This is
a hard failure by design.

**Why it is a hard failure.** Spark binds CSV columns to an explicit schema **by
position**, not by name — the header is skipped, not verified. A source that drops
one column shifts every subsequent column by one, and because everything is read as a
string, nothing errors. The data looks fine and is silently wrong. This happened
during development: a missing `procedure_name` column put room types into
`item_category`, no ROOM line item was ever found, and the entire TPA calculation
produced garbage while every row count still reconciled.

**Fix.** Read the error — it prints declared vs found columns and the difference.
Either the upstream export changed (update `conf/sources.yaml` and rebuild Bronze for
that source) or the file is corrupt (re-request it). Never work around it by
disabling the check.

### Bronze — "No landing files found for source X"

The upstream export did not arrive. Check the landing container. Bronze skips the
source with a warning rather than failing, so the rest of the run proceeds; the
watermark for that source will not advance.

### Silver — MPI runs unusually long

The candidate-pair count is the thing to look at. Normal is ~1.8M for 220k
registrations.

```sql
SELECT COUNT(*) FROM medchain.silver.patient_crosswalk;
```

If pair counts have exploded, a blocking key has degenerated — usually a source
sending a constant or null value into a blocked field. `mpi.max_block_size` in
`conf/base.yaml` (default 120) caps the damage; blocks larger than it are skipped and
counted rather than expanded.

### Silver — SCD2 invariant violated

Log line: `SCD2 invariant violated: N open versions for M doctors`.

Every doctor must have exactly one open (`is_current`) version. More than one means
close-out failed and every point-in-time join downstream will fan out, silently
inflating counts.

```sql
SELECT doctor_id, COUNT(*) AS open_versions
FROM medchain.silver.dim_doctor_scd2
WHERE is_current GROUP BY doctor_id HAVING COUNT(*) > 1;
```

Usually caused by two roster exports claiming the same `effective_date` with different
departments. Rebuild: `medchain-run silver --steps doctors`.

### Quality — blocking check failed

The pipeline fails and Gold is not published. This is intended: a warehouse failing
its own integrity checks must not reach the dashboard.

Triage by category:
- **Uniqueness on a surrogate key** — a dimension is fanning out. Check for
  overlapping SCD2 ranges.
- **Referential** — a fact has orphaned foreign keys, meaning a dimension was rebuilt
  with different keys or a Silver table is stale. Rebuild dimensions before facts.
- **`reimbursement_within_billed`** — the deduction cascade has an ordering bug.
  Compare against `data/_truth/tpa_truth.parquet` component by component.

Override only to unblock, never as a fix:
```bash
medchain-run quality --date 2025-03-31 --no-fail-on-quality
```

### Everything fails with `UnsupportedClassVersionError` or a JVM crash

Wrong JDK. Spark 3.5 needs 8/11/17; Fedora ships 25/26 as system Java.

```bash
export JAVA_HOME=$HOME/.local/jdks/jdk-17
make doctor
```

---

## 4. Reprocessing

### Re-run a single date

Safe by construction. Every layer is MERGE-based or `replaceWhere`-scoped, and the
integration suite asserts that replaying a batch produces byte-identical checksums
across every Silver and Gold table.

```bash
medchain-run all --date 2025-03-31
```

Batches already marked `SUCCEEDED` are skipped. To force:

```bash
medchain-run bronze --date 2025-03-31 --force
```

### Backfill a date range

```bash
for d in $(seq 0 29); do
  date=$(date -d "2025-03-01 +${d} days" +%F)
  medchain-run bronze --date "$date"
done
medchain-run silver --date 2025-03-31   # Silver and Gold rebuild from all of Bronze
medchain-run gold   --date 2025-03-31
medchain-run quality --date 2025-03-31
```

Bronze is per-date; Silver and Gold read the whole of Bronze and only need one final
run.

### Reprocess a bad batch

```sql
-- 1. Identify it
SELECT * FROM medchain.control.batch_registry WHERE status = 'FAILED';

-- 2. Inspect what it wrote
SELECT COUNT(*) FROM medchain.bronze.insurance_claims WHERE batch_id = '<batch_id>';
```

```bash
# 3. Re-run. replaceWhere on batch_id replaces the rows rather than appending beside them.
medchain-run bronze --date <date> --sources <source> --force
medchain-run silver --date <date>
medchain-run gold --date <date>
```

### Rebuild from scratch

```bash
make clean && make gen SCALE=1.0 && make run-local
```

### Recover from a bad deploy — Delta time travel

```sql
-- What did this table look like before?
DESCRIBE HISTORY medchain.gold.fact_patient_visit;

SELECT * FROM medchain.gold.fact_patient_visit VERSION AS OF 12;

RESTORE TABLE medchain.gold.fact_patient_visit TO VERSION AS OF 12;
```

Retention is 7 days (`VACUUM ... RETAIN 168 HOURS`). Beyond that, rebuild from Bronze,
which is why Bronze never filters or repairs anything.

---

## 5. Cost control

Cluster time is the entire Azure budget. Storage and ADF are rounding errors.

| Action | When |
|---|---|
| `make cost` | Start and end of every session |
| `make stop` | Every time you stop working |
| `./infra/teardown.sh` | End of a work block spanning days |
| Check budget alert emails | 25 / 50 / 80 / 95% thresholds |

Clusters auto-terminate after 10 minutes idle, use spot instances, and pipeline runs
use job compute (roughly half the DBU rate of all-purpose). Develop locally —
`make run-local` costs nothing and exercises identical code.

Record every session in [cost_log.md](cost_log.md).

---

## 6. Escalation

| Symptom | Likely cause | First action |
|---|---|---|
| ADF activity times out | Cluster failed to start (quota/spot eviction) | Databricks event log; retry on on-demand |
| Databricks `LIBRARY_INSTALL_FAILED` | Wheel not uploaded or wrong Python version | `make deploy`; confirm DBR 15.4 LTS |
| `AnalysisException: Path does not exist` | Upstream layer never ran | Check `batch_registry` for the prior layer |
| Quality warnings rising over weeks | Genuine source degradation | Trend `dq_scorecard` by `run_ts`, not just the latest row |
| Spend rising with no runs | Cluster left running | `make stop` |
