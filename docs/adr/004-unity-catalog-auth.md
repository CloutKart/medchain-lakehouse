# ADR-004: Unity Catalog managed identity over service principal secrets

**Status:** Accepted · **Date:** 2026-08-08

## Context

Databricks must read and write ADLS Gen2. The options are a storage account key, a
service principal with a client secret in Key Vault, or Unity Catalog with an Access
Connector managed identity.

## Decision

Unity Catalog with an Access Connector (system-assigned managed identity) granted
`Storage Blob Data Contributor`, external locations per container, and a `medchain`
catalog with `bronze` / `silver` / `gold` / `control` schemas.

The service principal + Key Vault path is retained as a documented fallback for
subscriptions that will not permit an Access Connector.

## Why

**No credential exists to leak.** There is no key, no secret and no rotation schedule.
Nothing credential-bearing appears in a notebook, in config, or in git history.

**Governance is where auditors look for it.** Table-level grants, lineage and audit
logs come free. For healthcare data — even synthetic healthcare data — being able to
answer "who can read patient records" with a `SHOW GRANTS` is the difference between a
demo and something defensible.

**Tables stay path-addressed anyway.** `cfg.table_path()` returns a path on every
environment; UC registration creates *external* tables over those same paths. The
catalog is a view onto the storage layout, never a competing source of truth, so local
and cluster runs touch byte-identical layouts and the same transformation code runs in
both.

## Cost

Unity Catalog requires the **Premium** workspace SKU, at a higher DBU rate than
Standard — roughly $0.55/DBU against $0.40. On a $100 student grant that is real money:
about 40 hours of runway given up.

Accepted, because UC is the thing the industry has standardised on and the thing
interviewers ask about, and because the alternative puts a rotatable secret in a
student subscription's Key Vault where it will not be rotated.

## Alternatives rejected

- **Storage account keys** — full access, no expiry, no per-table grants. Compromise is
  total.
- **Service principal + Key Vault secret scope** — workable and cheaper (Standard SKU),
  but a secret with an expiry nobody tracks. Kept as the documented fallback only.
- **Credential passthrough** — deprecated in favour of UC.

## Consequences

- `infra/provision.sh` creates the Access Connector and role assignment; the UC storage
  credential and external locations are a one-time manual step in the account console
  (they are account-level, not workspace-level).
- Premium SKU is required and is the largest single cost decision in the project.
- `conf/azure.yaml` contains no secrets and is safe to commit.
