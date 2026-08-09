# ADR-005: Hand-rolled quality framework over Great Expectations

**Status:** Accepted · **Date:** 2026-08-08

## Context

The platform needs data quality checks with severity levels, persisted history, and
the ability to fail a pipeline. Great Expectations, Soda and dbt tests all cover this.

## Decision

A ~200-line declarative framework: checks defined in `conf/quality.yaml`, results
written to `gold.dq_scorecard` as one row per (run, table, check), with
`blocking` / `warn` severity.

## Why

**The checks needed are mostly single aggregate queries.** not-null, unique, range,
referential, row-count and arbitrary SQL expressions cover every structural check in
this project. That is a few hundred lines, not a dependency.

**Results as a table, not a report.** The interesting question is never "did it pass
today" but "when did this start degrading". A Delta table with run history answers
that; an HTML report per run does not.

**Cluster dependencies have a cost.** Great Expectations pulls a substantial
dependency tree that must be installed on every cluster, version-matched to the
runtime, and re-verified at each DBR upgrade. For six check types that is a poor trade.

**The metrics that matter here are not expressible as generic expectations anyway.**
Pairwise MPI precision and recall against ground truth, claim reconstruction coverage
against a true transition log, per-component TPA agreement — these are bespoke
measurements. They live in `quality/scorecard.py` as ordinary Python and would need
custom expectations in any framework.

## The distinction that shaped the design

Structural checks verify the warehouse is *internally consistent*: keys unique,
foreign keys resolving, grains holding. **They would all pass even if every matching
decision the platform made were wrong.**

Recovery metrics compare output against ground truth and answer the question that
actually matters: how much of what was destroyed did we get back. Both are reported,
and the scorecard distinguishes them by `check_type`, because conflating them lets a
green dashboard hide a broken matcher.

## Alternatives rejected

- **Great Expectations** — the right choice for an organisation standardising across
  many teams, or where a data steward authors checks through a UI. Overweight for one
  pipeline with six check types.
- **Soda Core** — lighter, YAML-native, close to what was built. Reasonable
  alternative; rejected mainly to avoid a cluster dependency for something small.
- **dbt tests** — wrong shape. This is PySpark, not a dbt project.
- **Delta constraints** (`ALTER TABLE ... ADD CONSTRAINT`) — enforced at write time and
  used where appropriate, but cannot express thresholds, severities or history.

## Consequences

- Zero extra cluster dependencies.
- Adding a check type means writing an evaluator branch (~10 lines) rather than
  configuring one.
- No community-maintained expectation library, no profiling UI, no data docs site.
- 49 checks currently run: 27 structural, 22 recovery metrics.
- Revisit if check authorship needs to move to non-engineers.
