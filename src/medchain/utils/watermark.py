"""Watermark and batch registry — the state that makes re-runs safe.

Two control tables live in the ``control`` layer:

``batch_registry``
    One row per (source, layer, ingest_date) unit of work, with its status and row
    count. Bronze consults it before ingesting so a completed batch is skipped
    rather than duplicated.

``watermark``
    The high-water mark per source. Silver reads only Bronze partitions newer than
    this, and it is advanced *only* after the downstream write succeeds. Advancing it
    optimistically is the classic way to silently lose a day of data when a job
    fails halfway.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from medchain.config import Config
from medchain.utils.tables import read, table_exists, upsert

BATCH_REGISTRY_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("run_id", StringType(), True),
        StructField("layer", StringType(), False),
        StructField("source", StringType(), False),
        StructField("ingest_date", StringType(), False),
        StructField("status", StringType(), False),  # RUNNING | SUCCEEDED | FAILED
        StructField("row_count", LongType(), True),
        StructField("file_count", IntegerType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("finished_at", TimestampType(), True),
        StructField("error_message", StringType(), True),
    ]
)

WATERMARK_SCHEMA = StructType(
    [
        StructField("source", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("last_processed_date", StringType(), True),
        StructField("last_run_id", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


def _registry_path(cfg: Config) -> str:
    return cfg.table_path("control", "batch_registry")


def _watermark_path(cfg: Config) -> str:
    return cfg.table_path("control", "watermark")


# ------------------------------------------------------------- batch registry


def batch_status(spark: SparkSession, cfg: Config, batch_id: str) -> str | None:
    """Current status of a batch, or ``None`` if it has never run."""
    path = _registry_path(cfg)
    if not table_exists(spark, path):
        return None
    rows = read(spark, path).filter(F.col("batch_id") == batch_id).select("status").collect()
    return rows[0]["status"] if rows else None


def is_batch_complete(spark: SparkSession, cfg: Config, batch_id: str) -> bool:
    return batch_status(spark, cfg, batch_id) == "SUCCEEDED"


def record_batch(
    spark: SparkSession,
    cfg: Config,
    *,
    batch_id: str,
    run_id: str,
    layer: str,
    source: str,
    ingest_date: str,
    status: str,
    row_count: int | None = None,
    file_count: int | None = None,
    error_message: str | None = None,
) -> None:
    """Upsert a batch's state. Called once at start (RUNNING) and once at end."""
    now = datetime.now(UTC)
    row = {
        "batch_id": batch_id,
        "run_id": run_id,
        "layer": layer,
        "source": source,
        "ingest_date": ingest_date,
        "status": status,
        "row_count": int(row_count) if row_count is not None else None,
        "file_count": int(file_count) if file_count is not None else None,
        "started_at": now if status == "RUNNING" else None,
        "finished_at": now if status != "RUNNING" else None,
        "error_message": error_message,
    }
    df = spark.createDataFrame([row], schema=BATCH_REGISTRY_SCHEMA)

    path = _registry_path(cfg)
    if status == "RUNNING" or not table_exists(spark, path):
        upsert(spark, df, path, ["batch_id"])
        return

    # On completion, preserve the started_at recorded by the RUNNING write so the
    # registry shows a real duration rather than a zero-length window.
    from delta.tables import DeltaTable

    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), "t.batch_id = s.batch_id")
        .whenMatchedUpdate(
            set={
                "status": "s.status",
                "row_count": "s.row_count",
                "file_count": "s.file_count",
                "finished_at": "s.finished_at",
                "error_message": "s.error_message",
                "run_id": "s.run_id",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


# ------------------------------------------------------------------ watermark


def get_watermark(spark: SparkSession, cfg: Config, source: str, layer: str) -> date | None:
    """Last successfully processed date for a source, or ``None`` on first run."""
    path = _watermark_path(cfg)
    if not table_exists(spark, path):
        return None
    rows = (
        read(spark, path)
        .filter((F.col("source") == source) & (F.col("layer") == layer))
        .select("last_processed_date")
        .collect()
    )
    if not rows or rows[0]["last_processed_date"] is None:
        return None
    return date.fromisoformat(rows[0]["last_processed_date"])


def set_watermark(
    spark: SparkSession,
    cfg: Config,
    source: str,
    layer: str,
    processed_date: date | str,
    run_id: str,
) -> None:
    """Advance the watermark. Call only after the downstream write has committed."""
    if isinstance(processed_date, date):
        processed_date = processed_date.isoformat()
    df = spark.createDataFrame(
        [
            {
                "source": source,
                "layer": layer,
                "last_processed_date": processed_date,
                "last_run_id": run_id,
                "updated_at": datetime.now(UTC),
            }
        ],
        schema=WATERMARK_SCHEMA,
    )
    upsert(spark, df, _watermark_path(cfg), ["source", "layer"])
