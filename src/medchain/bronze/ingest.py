"""Bronze ingestion — a faithful, replayable archive of what the sources sent.

Three rules define this layer, and every one of them exists because breaking it
causes a class of bug that is invisible until much later:

1. **Explicit schemas, read as strings.** Nothing is inferred and nothing is cast.
   A birth date recorded as ``31/02/2024`` and an amount written ``1,25,000`` are
   preserved verbatim. Casting here would null them silently; casting in Silver lets
   us quarantine the row *with its original value attached*.

2. **No filtering, no deduplication, no repair.** If we later discover the Silver
   logic was wrong, Bronze still holds everything needed to rebuild. A Bronze layer
   that has already "cleaned" the data cannot do that.

3. **Every row is traceable and every batch is replayable.** ``batch_id``,
   ``source_file`` and ``ingestion_ts`` go on every row, and the batch registry makes
   re-running a completed date a no-op instead of a duplicate.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medchain.config import Config
from medchain.utils.audit import RunContext
from medchain.utils.logging import banner, get_logger
from medchain.utils.schemas import column_names, source_schema
from medchain.utils.tables import register_table, row_count, table_exists
from medchain.utils.watermark import is_batch_complete, record_batch, set_watermark

log = get_logger("medchain.bronze")

# Landing layout produced by the generator (see generate/writer.py).
INITIAL_LOAD_DIR = "initial_load"
INCREMENTAL_DIR = "incremental"


def _is_local(uri: str) -> bool:
    return urlparse(uri).scheme in ("", "file")


def list_source_files(cfg: Config, source: str, *, include_initial: bool = True) -> list[str]:
    """Enumerate landing files for a source, initial load first.

    Only the local filesystem is enumerated directly. On Azure the directory listing
    is left to Spark's own globbing, which reads ADLS far more efficiently than
    round-tripping every path through the driver.
    """
    root = f"{cfg.path('landing').rstrip('/')}/{source}"
    if not _is_local(root):
        parts = [f"{root}/{INCREMENTAL_DIR}/*"]
        if include_initial:
            parts.insert(0, f"{root}/{INITIAL_LOAD_DIR}/*")
        return parts

    base = Path(urlparse(root).path if root.startswith("file:") else root)
    files: list[str] = []
    if include_initial and (base / INITIAL_LOAD_DIR).exists():
        files.extend(sorted(str(p) for p in (base / INITIAL_LOAD_DIR).iterdir() if p.is_file()))
    if (base / INCREMENTAL_DIR).exists():
        files.extend(sorted(str(p) for p in (base / INCREMENTAL_DIR).iterdir() if p.is_file()))
    return files


def validate_header(spark: SparkSession, cfg: Config, source: str, paths: list[str]) -> None:
    """Fail loudly when a CSV's header disagrees with the declared contract.

    Supplying an explicit schema to Spark's CSV reader binds columns **by position,
    not by name** — the header row is skipped, not checked. So a source that drops
    or reorders one column does not error: every column after it shifts by one and
    lands in the wrong field, still perfectly typed because everything is read as a
    string. The data looks fine and is silently wrong.

    That is not hypothetical. It happened in this project: the claim line-item
    export omitted ``procedure_name``, so ``item_category`` filled with room types,
    no ROOM line was ever found, and the entire TPA deduction calculation quietly
    produced garbage while every row count still reconciled.

    One cheap header read per source turns that class of bug into an immediate,
    legible failure.
    """
    source_cfg = cfg.source(source)
    if source_cfg.get("format", "csv") != "csv" or not paths:
        return

    expected = column_names(source_cfg)
    actual = spark.read.option("header", "true").csv(paths[0]).columns

    if actual == expected:
        return

    missing = [c for c in expected if c not in actual]
    unexpected = [c for c in actual if c not in expected]
    raise ValueError(
        f"Header mismatch for source {source!r} in {paths[0]}.\n"
        f"  declared in conf/sources.yaml : {expected}\n"
        f"  found in file                 : {actual}\n"
        f"  missing from file             : {missing or 'none'}\n"
        f"  not declared                  : {unexpected or 'none'}\n"
        "Reading with a positional schema would shift columns silently, so this is "
        "a hard failure. Fix the contract or the export before re-running."
    )


def _read_raw(spark: SparkSession, cfg: Config, source: str, paths: list[str]) -> DataFrame:
    """Read landing files with the declared schema, every column as a string."""
    source_cfg = cfg.source(source)
    fmt = source_cfg.get("format", "csv")
    schema = source_schema(source_cfg, all_strings=True)

    reader = spark.read.schema(schema)
    if fmt == "csv":
        reader = (
            reader.option("header", "true")
            .option("mode", "PERMISSIVE")
            # Malformed rows are kept, not dropped. Bronze records what arrived.
            .option("columnNameOfCorruptRecord", "_corrupt_record")
        )
    elif fmt == "json":
        reader = reader.option("multiLine", "false")
    else:
        raise ValueError(f"Unsupported format {fmt!r} for source {source!r}")

    return reader.format(fmt).load(paths)


def _derive_ingest_date(df: DataFrame) -> DataFrame:
    """Extract the export date from the file name into a partition column.

    The generator names files ``<source>_<YYYY-MM-DD>.<ext>``, mirroring how source
    systems actually stamp their drops. Partitioning Bronze on this makes a
    single-day reprocess a partition-scoped operation instead of a full rewrite.
    """
    filename = F.element_at(F.split(F.col("source_file"), "/"), -1)
    extracted = F.regexp_extract(filename, r"(\d{4}-\d{2}-\d{2})", 1)
    return df.withColumn(
        "ingest_date",
        F.when(extracted != "", extracted).otherwise(F.lit(None).cast("string")),
    )


def ingest_source(
    spark: SparkSession,
    cfg: Config,
    source: str,
    ctx: RunContext,
    *,
    include_initial: bool = True,
    force: bool = False,
) -> int:
    """Land one source into Bronze. Returns the number of rows written."""
    source_ctx = ctx.for_source(source)
    batch_id = source_ctx.batch_id

    if not force and is_batch_complete(spark, cfg, batch_id):
        log.info("Batch %s already SUCCEEDED; skipping (use --force to reprocess)", batch_id)
        return 0

    paths = list_source_files(cfg, source, include_initial=include_initial)
    if not paths:
        log.warning("No landing files found for source %r", source)
        return 0

    target = cfg.table_path("bronze", source)
    record_batch(
        spark,
        cfg,
        batch_id=batch_id,
        run_id=source_ctx.run_id,
        layer="bronze",
        source=source,
        ingest_date=source_ctx.logical_date.isoformat(),
        status="RUNNING",
    )

    try:
        validate_header(spark, cfg, source, paths)
        raw = _read_raw(spark, cfg, source, paths)
        enriched = (
            raw.withColumn("source_file", F.input_file_name())
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("run_id", F.lit(source_ctx.run_id))
            .withColumn("ingestion_ts", F.current_timestamp())
            .withColumn("source_system", F.lit(cfg.source(source).get("system", "UNKNOWN")))
        )
        enriched = _derive_ingest_date(enriched)
        # Files with no date in the name are the historical backfill; date them at
        # the start of the simulation window so the partition column is never null.
        enriched = enriched.withColumn(
            "ingest_date",
            F.coalesce(F.col("ingest_date"), F.lit(cfg.window_start.isoformat())),
        )

        writer = enriched.write.format("delta").mode("overwrite").partitionBy("ingest_date")
        if table_exists(spark, target):
            # replaceWhere scoped to this batch makes a re-run idempotent even when
            # the registry check is bypassed with --force: the batch's rows are
            # replaced, never appended alongside their previous copy.
            writer = writer.option("replaceWhere", f"batch_id = '{batch_id}'")
        else:
            # First write only. On subsequent runs a schema change in
            # conf/sources.yaml is a deliberate contract change that should require
            # an explicit rebuild, not a silent in-place overwrite of the archive.
            writer = writer.option("overwriteSchema", "true")
        writer.save(target)

        written = (
            spark.read.format("delta").load(target).filter(F.col("batch_id") == batch_id).count()
        )
        register_table(spark, cfg, "bronze", source)
        record_batch(
            spark,
            cfg,
            batch_id=batch_id,
            run_id=source_ctx.run_id,
            layer="bronze",
            source=source,
            ingest_date=source_ctx.logical_date.isoformat(),
            status="SUCCEEDED",
            row_count=written,
            file_count=len(paths),
        )
        set_watermark(spark, cfg, source, "bronze", source_ctx.logical_date, source_ctx.run_id)
        log.info("  %-24s %9d rows from %d path(s)", source, written, len(paths))
        return written

    except Exception as exc:  # noqa: BLE001 - recorded then re-raised
        record_batch(
            spark,
            cfg,
            batch_id=batch_id,
            run_id=source_ctx.run_id,
            layer="bronze",
            source=source,
            ingest_date=source_ctx.logical_date.isoformat(),
            status="FAILED",
            error_message=str(exc)[:1000],
        )
        log.error("Bronze ingestion failed for %s: %s", source, exc)
        raise


def run(
    spark: SparkSession,
    cfg: Config,
    logical_date: date | str | None = None,
    *,
    sources: list[str] | None = None,
    include_initial: bool = True,
    force: bool = False,
) -> dict[str, int]:
    """Ingest every configured source into Bronze."""
    ctx = RunContext.create(logical_date or date.today(), layer="bronze")
    banner(
        log,
        "BRONZE INGESTION",
        run_id=ctx.run_id,
        logical_date=ctx.logical_date,
        environment=cfg.env,
        target=cfg.path("bronze"),
    )

    results: dict[str, int] = {}
    for source in sources or cfg.source_names:
        results[source] = ingest_source(
            spark, cfg, source, ctx, include_initial=include_initial, force=force
        )

    total = sum(results.values())
    log.info("Bronze complete: %d rows across %d sources", total, len(results))
    return results


def bronze_summary(spark: SparkSession, cfg: Config) -> list[dict]:
    """Row counts per Bronze table, for the runbook and the quality scorecard."""
    return [
        {"source": source, "rows": row_count(spark, cfg.table_path("bronze", source))}
        for source in cfg.source_names
    ]
