# Databricks notebook source
# MAGIC %md
# MAGIC # MedChain — web dashboard export
# MAGIC
# MAGIC Runs the dashboard's panel queries against the Gold layer and writes the JSON to
# MAGIC the `gold/_web` prefix in ADLS, from where it can be downloaded or served.
# MAGIC
# MAGIC This exists because the export cannot run from a laptop against Azure. Databricks
# MAGIC Runtime 15.4 enables **deletion vectors** on new Delta tables by default, and
# MAGIC neither delta-rs nor DuckDB's Delta extension can read a table carrying that
# MAGIC reader feature. Spark reads them natively, so on Azure the export runs here.
# MAGIC
# MAGIC The SQL is the same either way — only the backend that executes it differs, so
# MAGIC the local and cluster exports cannot report different numbers.

# COMMAND ----------

dbutils.widgets.text("env", "azure")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("run_id", "")

env = dbutils.widgets.get("env") or "azure"
storage_account = dbutils.widgets.get("storage_account")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------

import json
import os
from pathlib import Path

os.environ["MEDCHAIN_ENV"] = env
os.environ["MEDCHAIN_RUN_ID"] = run_id
if storage_account:
    os.environ["STORAGE_ACCOUNT"] = storage_account

from medchain.config import load_config
from medchain.utils.logging import setup_logging
from medchain.web import export as web_export

setup_logging()
cfg = load_config(env)
print(f"env={cfg.env} gold={cfg.path('gold')}")

# COMMAND ----------

# Write to the driver's local disk first, then copy into ADLS. Writing many small
# files straight to object storage through the local filesystem API is slower and
# offers no benefit for six files totalling well under a megabyte.
local_out = Path("/tmp/medchain_web")
sizes = web_export.export(cfg, local_out, spark=spark)  # noqa: F821 - notebook global

# COMMAND ----------

target = f"{cfg.path('gold').rstrip('/')}/_web"
dbutils.fs.mkdirs(target)
for path in sorted(local_out.glob("*.json")):
    dbutils.fs.cp(f"file://{path}", f"{target}/{path.name}", recurse=False)
    print(f"  {path.name:<18} {path.stat().st_size / 1024:7.1f} KB -> {target}/{path.name}")

# COMMAND ----------

# Echo the headline numbers so a run's output shows what it published, rather than
# only that it published something.
headline = json.loads((local_out / "headline.json").read_text())
clinical = headline["clinical"]
print(f"readmission gap : {clinical['readmission_gap_pp']:.2f} pp")
print(f"misattributed   : {headline['attribution']['misattributed']:,}")
print(f"recoverable     : Rs {headline['financial']['room_excess'] / 1e7:,.0f} Cr")

dbutils.notebook.exit(
    json.dumps({"status": "SUCCEEDED", "target": target, "files": {k: v for k, v in sizes.items()}})
)
