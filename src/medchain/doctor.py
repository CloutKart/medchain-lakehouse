"""Toolchain self-check: prove Spark + Delta actually work before building on them.

``make doctor`` runs this. It writes a Delta table, reads it back, performs a MERGE
and checks time travel — the four capabilities the entire platform depends on. When
this passes, a failure later is a logic problem, not an environment problem.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

from medchain.config import load_config
from medchain.utils.logging import get_logger

log = get_logger("medchain.doctor")


def main() -> int:
    cfg = load_config()
    log.info("Environment      : %s", cfg.env)
    log.info("Bronze path      : %s", cfg.path("bronze"))
    log.info("Catalog          : %s", cfg.catalog or "(none — path-addressed)")
    log.info("Sources declared : %s", ", ".join(cfg.source_names))

    import os

    from medchain.spark import get_spark

    log.info("JAVA_HOME        : %s", os.environ.get("JAVA_HOME", "(unset)"))

    spark = get_spark(cfg)
    log.info("Spark version    : %s", spark.version)

    from importlib.metadata import version as pkg_version

    log.info("Delta version    : %s", pkg_version("delta-spark"))

    tmp = Path(tempfile.mkdtemp(prefix="medchain-doctor-"))
    table = str(tmp / "probe")
    try:
        # 1. write
        spark.createDataFrame([(1, "alpha"), (2, "beta")], "id INT, name STRING").write.format(
            "delta"
        ).save(table)

        # 2. read
        assert spark.read.format("delta").load(table).count() == 2, "readback count mismatch"

        # 3. merge — the operation SCD2 and the claim audit are built on
        from delta.tables import DeltaTable

        updates = spark.createDataFrame([(2, "beta-v2"), (3, "gamma")], "id INT, name STRING")
        (
            DeltaTable.forPath(spark, table)
            .alias("t")
            .merge(updates.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        rows = {r["id"]: r["name"] for r in spark.read.format("delta").load(table).collect()}
        assert rows == {1: "alpha", 2: "beta-v2", 3: "gamma"}, f"merge produced {rows}"

        # 4. time travel — the audit story depends on version history existing
        v0 = spark.read.format("delta").option("versionAsOf", 0).load(table).count()
        assert v0 == 2, f"time travel to v0 returned {v0} rows"

        log.info("")
        log.info("All checks passed: write, read, MERGE, and time travel are working.")
        return 0
    except Exception as exc:  # noqa: BLE001
        log.error("Self-check FAILED: %s", exc, exc_info=True)
        return 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
