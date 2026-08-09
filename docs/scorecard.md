# Data Quality Scorecard

Results from the full 3-year dataset: **50 checks · 0 blocking failures · 2 warnings**.

## Two kinds of measurement

The distinction shapes everything else in this document.

**Structural checks** (`conf/quality.yaml`) verify the warehouse is internally
consistent — keys unique, foreign keys resolving, amounts non-negative, grains
holding. **They would all pass even if every matching decision the platform made were
wrong.** Internal consistency is not correctness.

**Recovery metrics** (`quality/scorecard.py`) compare output against the ground truth
in `data/_truth/` and answer the question that matters: how much of what was destroyed
did we get back?

Ground truth exists because the source data is synthetic. In a real deployment these
become sampled manual audits — a stewardship team reviewing 200 linkages a month — but
the metric definitions and the scorecard table are unchanged.

## Severity

| Severity | Effect | Used for |
|---|---|---|
| `blocking` | Fails the run; Gold is not published | Defects that would publish wrong numbers: duplicated dimension keys, orphaned foreign keys, negative deductions |
| `warn` | Recorded and surfaced; run continues | Quality *metrics*. An MPI match rate drifting 93% → 91% is worth seeing and not worth halting a nightly load over |

## Recovery metrics

### Master Patient Index

| Metric | Value | Target |
|---|---|---|
| Precision | **0.9988** | ≥ 0.98 |
| Recall | **0.9119** | ≥ 0.85 |
| F1 | **0.9534** | ≥ 0.95 |
| Duplicate collapse | 219,600 → 183,469 (16.5%) | — |

Pairwise rather than cluster-exact: the useful question is "of the record pairs we said
are the same person, how many are?", not "did we reproduce every cluster perfectly".

Recall by defect type — the breakdown that drove the tuning:

| Injected defect | Share of duplicates | Linked |
|---|---|---|
| Identical details, different `patient_id` | 40% | 100% |
| Format-only difference (date/phone rendering) | 14% | 100% |
| Name spelling variant | 26% | 100% |
| Phone missing or mistyped | 12% | ~90% |
| Name + phone + DOB all corrupted | 8% | ~25% |

The last row is by design. Those records land in the review queue rather than being
auto-linked, because silently merging two people's medical histories is a clinical
safety issue and leaving a duplicate is an inconvenience.

### Claim lifecycle reconstruction

| Metric | Value | Target |
|---|---|---|
| Reconstruction coverage | **92.6%** (706,612 of 762,848 true transitions) | ≥ 0.90 |
| Reconstruction fidelity | **100%** — zero invented transitions | 1.0 (blocking) |
| Claims reaching a terminal state | 90.0% | — |
| Illegal transitions | **0** | 0 |

Coverage is capped below 100% by export cadence: a state that begins and ends between
two weekly snapshots was never observed and cannot be recovered. Fidelity is the
blocking check — recovering a transition that never happened is invention.

9,580 transitions are classified `GAP` (a legal path exists but intermediate states
were unobserved). Reporting these as "illegal" would bury the zero genuine anomalies
under thousands of sampling artefacts.

### TPA deduction engine

Every component matched against ground truth, not just the bottom line — a net figure
that lands close while the components are individually wrong is a coincidence, not a
working rules engine.

| Component | Within ₹1 of truth |
|---|---|
| `excluded_amount` | **100%** (208,456 / 208,456) |
| `room_rent_excess` | **100%** |
| `eligible_amount` | **100%** |
| `copay_amount` | **100%** |
| `net_reimbursement` | **100%** |

Against what the insurer actually paid:

| Metric | Value |
|---|---|
| Rule-predictable claims explained to within ₹1 | **96.1%** (101,526 of 105,664) |
| All adjudicated claims (context only) | 53.1% |

The blended figure is reported but is not the headline, because some claims are not
predictable from rules by construction: a rejected claim pays zero regardless of the
arithmetic, and a partially approved claim carries a medical officer's discretionary
reduction no rule encodes. Averaging those in makes a working engine look broken.

### ICD-10 inference

| Tier | Count | Share |
|---|---|---|
| `SOURCE` — supplied by the source | 368 | 92.0% |
| `EXACT_NAME` — exact catalogue match after normalisation | 21 | 5.2% |
| `FUZZY_NAME` — unambiguous near-match | 6 | 1.5% |
| `SPECIALTY` — specialty modal code, a placeholder | 5 | 1.2% |
| `UNMAPPED` | 0 | 0.0% |
| **Overall fill rate** | **400 / 400** | **100%** |

Reported per tier deliberately. A single "100% filled" would be misleading, because a
specialty-level default is not the same fact as a coded diagnosis. `icd10_source` and
`icd10_confidence` travel all the way into `dim_procedure` so the analyst filtering on
a diagnosis code can see which they are looking at.

### Bill-to-claim linkage

| Metric | Value |
|---|---|
| Claims linked to a bill | **99.9%** (208,281 of 208,456) |
| Corroborated by both matching routes | 36.6% |
| Ambiguous, sent for review | 175 |

## Structural checks

27 checks across all 10 Gold tables. All passing.

| Category | Checks | Notable |
|---|---|---|
| Uniqueness | 7 | Every surrogate key; `(ward_id, occupancy_date)` for the stated grain |
| Referential | 4 | Zero orphans across 619,601 visits |
| Not-null | 5 | `mpi_id` resolved on 100% of visits |
| Range | 4 | Age 0–120, length of stay 0–180 |
| Expression | 7 | `discharge >= admission`; `net_reimbursement <= billed`; deductions never negative |

## Current warnings

| Check | Value | Assessment |
|---|---|---|
| `fact_bed_occupancy.occupancy_rate_plausible` | 99.81% | 132 ward-days of 70,624 above 150% capacity. |
| `fact_bed_occupancy.occupied_within_capacity` | 99.81% | Same 132 rows. |

**On the occupancy warning.** This check found a real defect and is retained because
it did. The first run flagged 555 ward-days — traced to a ward mix that gave the
smallest hospital a 4-bed paediatric ward, into which ordinary admission variance put
10 patients. That is a 250% occupancy rate produced by a modelling artefact, not by a
hospital under pressure. Enforcing a minimum ward size at the source cut it to 132.

What remains, 0.19% of ward-days, is the genuine tail: surge days when a ward runs
well over its nominal capacity, which happens in Indian hospitals during monsoon
dengue peaks. The check stays at its threshold rather than being relaxed to make the
dashboard green, because a warning that describes something true is doing its job.

## Querying the scorecard

```sql
-- Latest run
SELECT check_name, severity, passed, actual_value, threshold, detail
FROM medchain.gold.dq_scorecard
WHERE run_ts = (SELECT MAX(run_ts) FROM medchain.gold.dq_scorecard)
ORDER BY passed, severity, check_name;

-- Trend a metric over time. This is the question a scorecard exists to answer:
-- not "did it pass today" but "when did it start degrading".
SELECT logical_date, actual_value
FROM medchain.gold.dq_scorecard
WHERE check_name = 'mpi.f1'
ORDER BY run_ts;
```
