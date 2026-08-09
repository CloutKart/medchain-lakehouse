"""Silver layer orchestration.

Step order is a real dependency graph, not a preference:

    procedures ──┐
    mpi ─────────┤
    doctors ─────┤
    claim_history ┼──> tpa_rules      (needs claim state + procedure categories)
                  └──> bill_claim_link (needs claim state)
    bed_gapfill  (independent)

Each step is individually re-runnable and each records its own batch in the control
tables, so a failure part-way through is resumed rather than restarted.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession

from medchain.config import Config
from medchain.silver import (
    bed_gapfill,
    bill_claim_link,
    claim_history,
    doctors,
    mpi,
    procedures,
    tpa_rules,
)
from medchain.utils.audit import RunContext
from medchain.utils.logging import banner, get_logger
from medchain.utils.watermark import record_batch, set_watermark

log = get_logger("medchain.silver")

# (name, module) in dependency order.
STEPS = [
    ("procedures", procedures),
    ("mpi", mpi),
    ("doctors", doctors),
    ("claim_history", claim_history),
    ("tpa_rules", tpa_rules),
    ("bill_claim_link", bill_claim_link),
    ("bed_gapfill", bed_gapfill),
]


def run(
    spark: SparkSession,
    cfg: Config,
    logical_date: date | str | None = None,
    *,
    steps: list[str] | None = None,
) -> dict[str, dict]:
    """Execute the Silver layer."""
    ctx = RunContext.create(logical_date or date.today(), layer="silver")
    banner(
        log,
        "SILVER LAYER",
        run_id=ctx.run_id,
        logical_date=ctx.logical_date,
        environment=cfg.env,
        target=cfg.path("silver"),
    )

    selected = [(name, mod) for name, mod in STEPS if steps is None or name in steps]
    results: dict[str, dict] = {}

    for name, module in selected:
        step_ctx = ctx.for_source(name)
        log.info("")
        log.info("--- silver.%s ---", name)
        record_batch(
            spark,
            cfg,
            batch_id=step_ctx.batch_id,
            run_id=step_ctx.run_id,
            layer="silver",
            source=name,
            ingest_date=step_ctx.logical_date.isoformat(),
            status="RUNNING",
        )
        try:
            results[name] = module.run(spark, cfg, step_ctx)
            record_batch(
                spark,
                cfg,
                batch_id=step_ctx.batch_id,
                run_id=step_ctx.run_id,
                layer="silver",
                source=name,
                ingest_date=step_ctx.logical_date.isoformat(),
                status="SUCCEEDED",
                row_count=int(list(results[name].values())[0] or 0),
            )
            set_watermark(spark, cfg, name, "silver", step_ctx.logical_date, step_ctx.run_id)
        except Exception as exc:  # noqa: BLE001 - recorded then re-raised
            record_batch(
                spark,
                cfg,
                batch_id=step_ctx.batch_id,
                run_id=step_ctx.run_id,
                layer="silver",
                source=name,
                ingest_date=step_ctx.logical_date.isoformat(),
                status="FAILED",
                error_message=str(exc)[:1000],
            )
            log.error("silver.%s failed: %s", name, exc)
            raise

    log.info("")
    log.info("Silver complete: %d steps", len(results))
    return results
