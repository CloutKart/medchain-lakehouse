"""Gold layer orchestration: dimensions first, then facts, then maintenance."""

from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession

from medchain.config import Config
from medchain.gold import date_dim, dimensions, facts
from medchain.utils.audit import RunContext
from medchain.utils.logging import banner, get_logger
from medchain.utils.tables import optimize, table_exists
from medchain.utils.watermark import record_batch, set_watermark

log = get_logger("medchain.gold")

# Columns worth clustering each fact on: the ones analytical queries filter and join
# by most. On Databricks these become ZORDER/liquid clustering keys; locally the
# OPTIMIZE is a no-op and this is simply documentation of intent.
CLUSTER_KEYS = {
    "fact_patient_visit": ["mpi_id", "hospital_id", "doctor_id"],
    "fact_claim_lifecycle": ["claim_id", "insurer_id"],
    "fact_billing_reconciliation": ["claim_id", "hospital_id", "insurer_id"],
    "fact_bed_occupancy": ["ward_id", "hospital_id"],
}


def run(
    spark: SparkSession,
    cfg: Config,
    logical_date: date | str | None = None,
    *,
    maintenance: bool = True,
) -> dict[str, int]:
    """Build the Gold star schema."""
    ctx = RunContext.create(logical_date or date.today(), layer="gold")
    banner(
        log,
        "GOLD LAYER",
        run_id=ctx.run_id,
        logical_date=ctx.logical_date,
        environment=cfg.env,
        target=cfg.path("gold"),
    )

    results: dict[str, int] = {}
    record_batch(
        spark,
        cfg,
        batch_id=ctx.batch_id,
        run_id=ctx.run_id,
        layer="gold",
        source="all",
        ingest_date=ctx.logical_date.isoformat(),
        status="RUNNING",
    )

    try:
        log.info("")
        log.info("--- dimensions ---")
        results["dim_date"] = date_dim.run(spark, cfg)
        results.update(dimensions.run(spark, cfg, ctx))

        log.info("")
        log.info("--- facts ---")
        results.update(facts.run(spark, cfg, ctx))

        if maintenance:
            log.info("")
            log.info("--- maintenance ---")
            for table, keys in CLUSTER_KEYS.items():
                path = cfg.table_path("gold", table)
                if table_exists(spark, path):
                    optimize(spark, path, zorder_by=keys)

        record_batch(
            spark,
            cfg,
            batch_id=ctx.batch_id,
            run_id=ctx.run_id,
            layer="gold",
            source="all",
            ingest_date=ctx.logical_date.isoformat(),
            status="SUCCEEDED",
            row_count=sum(results.values()),
        )
        set_watermark(spark, cfg, "all", "gold", ctx.logical_date, ctx.run_id)
    except Exception as exc:  # noqa: BLE001 - recorded then re-raised
        record_batch(
            spark,
            cfg,
            batch_id=ctx.batch_id,
            run_id=ctx.run_id,
            layer="gold",
            source="all",
            ingest_date=ctx.logical_date.isoformat(),
            status="FAILED",
            error_message=str(exc)[:1000],
        )
        raise

    log.info("")
    log.info("Gold complete: %d tables, %d rows total", len(results), sum(results.values()))
    return results
