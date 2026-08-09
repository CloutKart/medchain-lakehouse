"""Delta table helpers shared by every layer.

Two things worth knowing about this module:

1. Tables are addressed by **path**, not by catalog name, on every environment.
   Unity Catalog registration happens through :func:`register_table`, which creates
   an *external* table over the same path. The catalog is therefore a view onto the
   storage layout rather than an alternative source of truth, and a local run and a
   cluster run touch byte-identical layouts.

2. Writes go through :func:`overwrite` or :func:`upsert`, never bare
   ``df.write.save()``. This keeps schema handling, partitioning and clustering
   decisions in one place instead of scattered across twenty notebooks.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medchain.config import Config

log = logging.getLogger(__name__)


def table_exists(spark: SparkSession, path: str) -> bool:
    """True if a Delta table is present at ``path``."""
    from delta.tables import DeltaTable

    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:  # noqa: BLE001 - path may not exist at all
        return False


def read(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def read_or_empty(spark: SparkSession, path: str, schema) -> DataFrame:
    """Read a Delta table, or return an empty frame with ``schema`` if absent.

    Lets first-run and steady-state code paths be the same code path.
    """
    if table_exists(spark, path):
        return read(spark, path)
    return spark.createDataFrame([], schema)


def overwrite(
    df: DataFrame,
    path: str,
    *,
    partition_by: Sequence[str] | None = None,
    cluster_by: Sequence[str] | None = None,
    replace_where: str | None = None,
    schema_evolution: bool = False,
) -> None:
    """Full or partition-scoped overwrite.

    ``replace_where`` rewrites only the matching partitions, which is what makes a
    single-day re-run cheap instead of a full-table rewrite.
    """
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if cluster_by:
        writer = writer.option("clusterBy", ",".join(cluster_by))
    if replace_where:
        writer = writer.option("replaceWhere", replace_where)
    if schema_evolution:
        writer = writer.option("overwriteSchema", "true")
    writer.save(path)


def append(df: DataFrame, path: str, *, partition_by: Sequence[str] | None = None) -> None:
    writer = df.write.format("delta").mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def upsert(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: Sequence[str],
    *,
    partition_by: Sequence[str] | None = None,
    update: bool = True,
    partition_filter: str | None = None,
) -> None:
    """MERGE ``df`` into the Delta table at ``path`` on ``keys``.

    With ``update=False`` this becomes insert-if-absent, which is exactly the
    append-only semantic the claim lifecycle audit needs: replaying a batch inserts
    nothing and overwrites nothing.

    ``partition_filter`` is pushed into the merge condition so Delta can prune files
    instead of scanning the whole target — important once fact tables reach tens of
    millions of rows.
    """
    from delta.tables import DeltaTable

    if not table_exists(spark, path):
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return

    target = DeltaTable.forPath(spark, path)
    conditions = [f"t.{k} <=> s.{k}" for k in keys]
    if partition_filter:
        conditions.append(f"({partition_filter})")
    condition = " AND ".join(conditions)

    merge = target.alias("t").merge(df.alias("s"), condition)
    if update:
        merge = merge.whenMatchedUpdateAll()
    merge.whenNotMatchedInsertAll().execute()


def register_table(spark: SparkSession, cfg: Config, layer: str, table: str) -> None:
    """Register a path-based Delta table in Unity Catalog as an external table.

    A no-op when no catalog is configured (i.e. local runs), which is why the same
    pipeline code runs unchanged in both places.
    """
    fqn = cfg.table_fqn(layer, table)
    if not fqn:
        return
    location = cfg.table_path(layer, table)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{layer}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn} USING DELTA LOCATION '{location}'")
    log.info("Registered %s -> %s", fqn, location)


def optimize(
    spark: SparkSession,
    path: str,
    *,
    zorder_by: Sequence[str] | None = None,
    where: str | None = None,
) -> None:
    """Run OPTIMIZE, optionally Z-ordered and partition-scoped.

    Silently skipped on open-source Delta, which does not implement OPTIMIZE — the
    maintenance job is a no-op locally and effective on Databricks.
    """
    sql = f"OPTIMIZE delta.`{path}`"
    if where:
        sql += f" WHERE {where}"
    if zorder_by:
        sql += f" ZORDER BY ({', '.join(zorder_by)})"
    try:
        spark.sql(sql)
    except Exception as exc:  # noqa: BLE001
        log.info("OPTIMIZE unavailable for %s (%s); skipping", path, type(exc).__name__)


def vacuum(spark: SparkSession, path: str, retain_hours: int = 168) -> None:
    """Remove files no longer referenced, keeping ``retain_hours`` of time travel."""
    try:
        spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
    except Exception as exc:  # noqa: BLE001
        log.info("VACUUM unavailable for %s (%s); skipping", path, type(exc).__name__)


def row_count(spark: SparkSession, path: str) -> int:
    return read(spark, path).count() if table_exists(spark, path) else 0


def checksum(spark: SparkSession, path: str, keys: Sequence[str]) -> str:
    """Order-independent checksum over a table's key columns.

    Used by the idempotency tests: run a batch twice and assert the checksum is
    unchanged. Summing per-row hashes makes the result insensitive to row order and
    partitioning, which vary run to run even when the data does not.
    """
    if not table_exists(spark, path):
        return "MISSING"
    df = read(spark, path)
    row_hash = F.xxhash64(
        F.concat_ws("|", *[F.coalesce(F.col(k).cast("string"), F.lit("~")) for k in keys])
    )
    agg = df.select(
        F.count(F.lit(1)).alias("n"),
        F.sum(row_hash.cast("decimal(38,0)")).alias("h"),
    ).collect()[0]
    return f"{agg['n']}:{agg['h']}"
