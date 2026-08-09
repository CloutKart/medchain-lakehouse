# ADR-001: Deterministic-first MPI with blocked probabilistic fallback

**Status:** Accepted · **Date:** 2026-08-08

## Context

The same person is registered at up to three hospitals under different `patient_id`
values, with name spelling variants, per-hospital date formats, and mistyped phone
numbers. 219,600 registrations represent 180,000 real people. Nothing links them.

A naive all-pairs comparison is 219,600² / 2 ≈ 2.4 × 10¹⁰ comparisons. It does not
finish.

## Decision

Three stages, in order of decreasing confidence and increasing cost:

1. **Normalise**, parsing dates with the *source hospital's* format from
   `conf/seed/source_date_formats.csv`.
2. **Deterministic key** — SHA-256 over (normalised name, DOB, phone last 4).
   Resolves the majority at near-zero cost.
3. **Blocked probabilistic matching** — four complementary blocking keys, then
   Jaro-Winkler on name plus graded DOB, phone and city agreement. Weights are
   renormalised over the fields actually present.

Outcomes are three-way: ≥0.90 auto-link, 0.75–0.90 quarantine for human review,
below that distinct.

## Why

**Deterministic first** because it is cheap, exact, and explainable to a clinician.
Running probabilistic matching over everything would spend a great deal of compute
rediscovering links a hash already found.

**Four blocking keys, not one**, because each key depends on a field that some defect
corrupts. Blocking on phone misses everyone whose number was mistyped; blocking on
name-and-date misses everyone whose birth year was keyed wrong. Adding the
name-and-city key (no date, no phone) lifted recall from 0.896 to 0.913.

**Weights renormalised over present fields.** A record with no phone has the phone
component removed from both numerator and denominator, rather than scored zero.
Treating "missing" as "disagrees" penalises a record for what it does not claim and
was the single largest avoidable source of false negatives.

**Transposition-aware phone comparison.** Plain Levenshtein scores a digit
transposition as distance 2 — the same as two unrelated wrong digits. Swapping
adjacent digits is one keying slip and by far the most common phone entry error.
Detecting it explicitly (equal length, identical digit multiset) recovered roughly a
third of the recall on phone-corrupted records.

**A quarantine band, not a lower threshold.** Silently merging two people's medical
histories is a clinical safety issue. Leaving a duplicate is an inconvenience. The
asymmetry justifies 3,584 records queued for review rather than auto-linked.

## Alternatives rejected

- **Splink / probabilistic record linkage libraries.** Better calibrated (Fellegi-Sunter
  with EM-estimated weights) but an extra cluster dependency, and the tunable weights
  here are legible to a data steward in a way EM output is not. Worth revisiting if
  recall needs to exceed ~0.95.
- **Graph database for clustering.** Iterative min-label propagation converges in 3–4
  iterations at this scale. A graph engine is not justified.
- **Blocking on phone alone.** Simple and fast; recall collapses on exactly the
  records that need matching most.

## Consequences

- **F1 0.953** (precision 0.999, recall 0.912) against ground truth.
- Recall is bounded by design: heavily corrupted records (name *and* phone *and* DOB
  all wrong) land in quarantine, correctly.
- Blocking cost is quadratic within a block. `mpi.max_block_size` (120) caps it; skipped
  blocks are counted and reported rather than hidden.
- `mpi_id` comes from a persisted registry, so identifiers survive re-runs. Anything
  derived from row order would renumber the population on every execution.
