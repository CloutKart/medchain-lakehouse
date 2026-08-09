# Databricks job definition

`job_medchain_pipeline.json` runs the full pipeline as four sequential tasks on one
shared job cluster. Create it once, then trigger runs:

```bash
export DATABRICKS_HOST="https://$(az databricks workspace show \
  -n dbw-medchain-dev -g rg-medchain-dev --query workspaceUrl -o tsv)"

JOB_ID=$(databricks jobs create --json @databricks/job_medchain_pipeline.json \
         --output json | python3 -c "import json,sys;print(json.load(sys.stdin)['job_id'])")
databricks jobs run-now "$JOB_ID"
```

## Why the cluster is specified the way it is

**`num_workers: 0` with the `singleNode` profile and `ResourceClass: SingleNode` tag.**
These three together are how Databricks expresses a single-node cluster; setting a
worker count alongside the profile is contradictory. It also has to be zero here:
the regional quota is 4 vCPUs and `Standard_D4s_v3` is 4, so asking for a worker puts
the cluster over quota and it never starts — reported as an opaque timeout several
minutes in, not as a quota error.

**`data_security_mode: SINGLE_USER`.** This is what makes the cluster Unity Catalog
enabled, and UC is the only thing granting it access to ADLS — through the Access
Connector's managed identity. There is no storage key and no service principal
anywhere in this deployment, so a cluster without UC has no credential at all and
every `abfss://` read fails on authorization.

**One shared `job_cluster` across four tasks.** One cluster start (about five
minutes) instead of four. `depends_on` chains the tasks, so a Bronze failure stops
the run rather than paying for Silver, Gold and Quality behind it.

**The wheel comes from a Unity Catalog volume**, not DBFS. DBFS root is deprecated,
and library installs from it are restricted under UC depending on access mode — a
failure that surfaces at cluster start rather than at deploy time.

## Why this exists alongside ADF

`pl_master` in `adf/` is the production orchestrator and is deployed. Its Databricks
linked service, however, has no field for `data_security_mode`, so the job cluster it
creates is not Unity Catalog enabled and cannot reach ADLS through the Access
Connector. Two ways round it, neither free:

- point the linked service at an `existingClusterId` — a UC-enabled cluster created
  separately, billed at the higher all-purpose rate and running between activities;
- have ADF call the Jobs API through a Web activity, which keeps job-compute pricing
  and full control of the cluster spec.

This file is the second option's payload, and the direct way to run the pipeline
while that is decided.

## Cost

Single-node `Standard_D4s_v3` on job compute is roughly $0.30–0.40/hour, of which
about five minutes is cluster start. Stop anything left running with `make stop`.
