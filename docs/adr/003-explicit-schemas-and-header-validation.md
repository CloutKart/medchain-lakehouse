# ADR-003: Explicit schemas with header validation at Bronze

**Status:** Accepted · **Date:** 2026-08-08

## Context

Seven CSV/JSON sources land daily. Spark can infer schemas, and it is tempting to let
it.

## Decision

Every source declares its columns and types in `conf/sources.yaml`. Bronze reads with
that schema, **every column as a string**, and validates the file header against the
declared column list before reading, failing hard on any mismatch.

## Why

**No inference**, because inference is non-deterministic across batches. A column that
is all-integer on Monday and has one null on Tuesday changes type between runs, and
the Bronze archive stops being a stable contract.

**Everything as a string**, because casting at Bronze destroys evidence. A date
recorded as `31/02/2024` or an amount written `1,25,000` becomes null on cast, and the
original text is gone. Reading as text and casting in Silver lets a failed cast be
quarantined *with its original value attached*.

**Header validation**, because an explicit schema binds CSV columns **by position, not
by name**. The header row is skipped, not checked. A source that drops one column
shifts every subsequent column by one — and since everything is read as a string,
nothing errors.

This is not hypothetical. It happened during development. The claim line-item export
omitted `procedure_name`, so `item_category` filled with room types, no ROOM line was
ever found, room days defaulted to 1, and the entire TPA deduction calculation
produced garbage. Every row count reconciled. Every referential check passed. The
defect surfaced only when computed reimbursement was compared against ground truth and
matched on 16% of claims instead of 100%.

One cheap header read per source converts that class of bug from a silent months-long
wrong number into an immediate, legible failure naming the missing column.

## Alternatives rejected

- **`inferSchema`** — non-deterministic; costs a full extra pass anyway.
- **Reading by column name** (`.select()` after a headered read) — solves ordering but
  not typing, and silently tolerates missing columns as nulls.
- **Validating in Silver instead** — too late. Bronze would already have archived
  shifted data, and Bronze is meant to be the thing you rebuild *from*.

## Consequences

- A source contract change is a deliberate act: update `conf/sources.yaml`, rebuild
  that Bronze table.
- One extra lightweight Spark job per source per run (~1s).
- Type coercion moves to Silver, where quarantine is available.
