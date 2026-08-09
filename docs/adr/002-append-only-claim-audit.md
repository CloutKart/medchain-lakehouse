# ADR-002: Append-only claim audit rather than SCD Type 2

**Status:** Accepted · **Date:** 2026-08-08

## Context

Claims move through six states. The insurer portal exports only the current status,
weekly. History must be reconstructed by accumulating snapshots.

SCD Type 2 is the obvious reach — it is what we use for patients and doctors.

## Decision

An append-only fact keyed on `(claim_id, status_code, status_date)`, populated with
`MERGE ... WHEN NOT MATCHED THEN INSERT`. Two tables:

- `silver.claim_transitions` — immutable, append-only, never widened
- `silver.claim_lifecycle` — derived; sequence, dwell time and classification
  recomputed in full each run

## Why

**A state transition is an event, not a version.** SCD2 models "this entity's
attributes changed"; a claim's lifecycle is a sequence of discrete events, each of
which is independently interesting. Approved-then-Settled is two facts, not one row
superseding another.

**Append-only makes replay free.** The MERGE only inserts when no match exists, so
replaying an export — or every export ever — converges to the same table. SCD2 would
need close-out logic on every replay, and closing an already-closed version is a
classic source of drift.

**Two tables because deriving in place breaks the MERGE.** The append target holds
base columns; the derived table holds base plus sequence and dwell columns. Merging a
base-column source into a widened target fails to resolve the INSERT clause on the
second run — which is exactly how this was discovered. The derived columns are
position-dependent regardless: a late-arriving export inserts a transition mid-history
and renumbers everything after it, which no incremental update can express.

**Transitions are classified, not just validated.** `DIRECT` / `GAP` / `ILLEGAL`,
using the transitive closure of the state machine. A weekly export legitimately misses
states that begin and end between snapshots — 9,580 transitions here. Labelling those
"illegal" buries the genuine anomalies (of which there are zero) under thousands of
sampling artefacts.

## Alternatives rejected

- **SCD2 on claim status.** Reconstructs the same information with more machinery and
  worse replay semantics.
- **Delta Change Data Feed.** Captures changes to *our* table, not to the source. The
  source has no CDC to consume.
- **Storing only the latest status.** What the portal already does, and the reason the
  problem exists.

## Consequences

- **92.6% reconstruction coverage, 100% fidelity** — no transition is ever recovered
  that did not occur.
- Coverage is capped below 100% by export cadence. This is reported as a metric rather
  than presented as completeness.
- The table grows monotonically. Partitioned by `status_date`; 706k rows over 3 years
  is unremarkable.
