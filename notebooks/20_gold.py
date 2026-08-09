# Databricks notebook source
# MAGIC %md
# MAGIC # MedChain — Gold star schema
# MAGIC
# MAGIC This notebook is deliberately thin. All logic lives in the `medchain` wheel
# MAGIC installed on the cluster, which means the code running here is the same code the
# MAGIC pytest suite exercises locally. A notebook that reimplements transformation logic
# MAGIC cannot be unit tested, and drifts from the local version the moment either is edited.
# MAGIC
# MAGIC Parameters are supplied by the ADF `pl_master` pipeline.

# COMMAND ----------

dbutils.widgets.text("logical_date", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("env", "azure")

logical_date = dbutils.widgets.get("logical_date")
run_id = dbutils.widgets.get("run_id")
env = dbutils.widgets.get("env") or "azure"

# COMMAND ----------

import os

os.environ["MEDCHAIN_ENV"] = env
os.environ["MEDCHAIN_RUN_ID"] = run_id          # ties Databricks logs back to the ADF run
os.environ["MEDCHAIN_TRIGGER"] = "adf"

from medchain.config import load_config
from medchain.utils.logging import setup_logging

setup_logging()
cfg = load_config(env)
print(f"env={cfg.env} logical_date={logical_date} run_id={run_id}")

# COMMAND ----------

from medchain.gold import pipeline

results = pipeline.run(spark, cfg, logical_date)

# COMMAND ----------

# Return the results to ADF so the run is visible in the pipeline monitor without
# opening the notebook output.
import json

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "results": {k: str(v) for k, v in results.items()}}))
