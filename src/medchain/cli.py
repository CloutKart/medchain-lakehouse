"""``medchain-run`` — execute pipeline layers.

    medchain-run bronze  --date 2025-03-31
    medchain-run silver  --date 2025-03-31
    medchain-run gold    --date 2025-03-31
    medchain-run quality --date 2025-03-31
    medchain-run all     --date 2025-03-31

Databricks notebooks call the same functions, so what runs on the cluster is the
code that was tested locally — the notebook is a thin wrapper, never a second
implementation.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date

from medchain.config import load_config
from medchain.utils.logging import get_logger, setup_logging

log = get_logger("medchain.cli")

LAYERS = ("bronze", "silver", "gold", "quality", "all")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run MedChain pipeline layers")
    parser.add_argument("layer", choices=LAYERS, help="Which layer to run")
    parser.add_argument(
        "--date",
        default=None,
        help="Logical date (YYYY-MM-DD). Defaults to the end of the configured window.",
    )
    parser.add_argument("--env", default=None, help="Config environment (default: $MEDCHAIN_ENV)")
    parser.add_argument(
        "--sources", nargs="*", default=None, help="Restrict Bronze to these sources"
    )
    parser.add_argument("--steps", nargs="*", default=None, help="Restrict Silver to these steps")
    parser.add_argument(
        "--force", action="store_true", help="Reprocess batches already marked SUCCEEDED"
    )
    parser.add_argument(
        "--no-fail-on-quality",
        action="store_true",
        help="Record blocking quality failures without failing the run",
    )
    parser.add_argument(
        "--skip-maintenance", action="store_true", help="Skip OPTIMIZE after the Gold build"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    args = build_parser().parse_args(argv)
    cfg = load_config(args.env)

    logical_date = date.fromisoformat(args.date) if args.date else cfg.window_end

    from medchain.spark import get_spark

    spark = get_spark(cfg)
    started = time.time()
    layers = ["bronze", "silver", "gold", "quality"] if args.layer == "all" else [args.layer]

    try:
        for layer in layers:
            if layer == "bronze":
                from medchain.bronze import ingest

                ingest.run(spark, cfg, logical_date, sources=args.sources, force=args.force)
            elif layer == "silver":
                from medchain.silver import pipeline as silver_pipeline

                silver_pipeline.run(spark, cfg, logical_date, steps=args.steps)
            elif layer == "gold":
                from medchain.gold import pipeline as gold_pipeline

                gold_pipeline.run(spark, cfg, logical_date, maintenance=not args.skip_maintenance)
            elif layer == "quality":
                from medchain.quality import scorecard

                scorecard.run(
                    spark, cfg, logical_date, fail_on_blocking=not args.no_fail_on_quality
                )

        log.info("")
        log.info("Completed %s in %.1fs", " -> ".join(layers), time.time() - started)
        return 0
    except Exception as exc:  # noqa: BLE001 - top-level entry point
        log.error("Pipeline failed: %s", exc, exc_info=True)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
