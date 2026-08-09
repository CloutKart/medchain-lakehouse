"""A small declarative data-quality framework.

Checks are declared as data in ``conf/quality.yaml``, evaluated against the built
tables, and written to ``gold.dq_scorecard`` as rows — one per (run, table, check).
Results are a table rather than log output because the interesting question is never
"did it pass today" but "when did this start degrading", and only a table answers
that.

Severity decides what a failure *does*:

``blocking``  fails the pipeline. Reserved for checks where continuing would publish
              wrong numbers — a duplicated dimension key, a fact with orphaned
              foreign keys.
``warn``      recorded and surfaced, but the run proceeds. Most quality metrics
              belong here: an MPI match rate of 91% instead of 93% is worth seeing
              and not worth halting a nightly load over.

Hand-rolled rather than Great Expectations or Soda: the whole framework is under two
hundred lines, needs no extra cluster library, and the checks it has to express are
mostly single aggregate queries. See docs/adr/ADR-005 for the trade-off.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from medchain.config import CONF_DIR, Config
from medchain.utils.logging import get_logger
from medchain.utils.tables import read, register_table, table_exists

log = get_logger("medchain.quality")

SCORECARD_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("run_ts", TimestampType(), False),
        StructField("logical_date", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("check_name", StringType(), False),
        StructField("check_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("passed", BooleanType(), True),
        StructField("actual_value", DoubleType(), True),
        StructField("threshold", DoubleType(), True),
        StructField("comparison", StringType(), True),
        StructField("detail", StringType(), True),
    ]
)


@dataclass
class CheckResult:
    check_name: str
    check_type: str
    layer: str
    table_name: str
    severity: str
    passed: bool
    actual_value: float | None = None
    threshold: float | None = None
    comparison: str | None = None
    detail: str | None = None


@dataclass
class CheckSuite:
    """Loaded check definitions plus the custom metrics registered in code."""

    definitions: list[dict[str, Any]] = field(default_factory=list)
    custom: dict[str, Callable[[SparkSession, Config], list[CheckResult]]] = field(
        default_factory=dict
    )

    def register(self, name: str):
        """Decorator registering a metric that needs real code, not a YAML rule."""

        def wrapper(fn: Callable[[SparkSession, Config], list[CheckResult]]):
            self.custom[name] = fn
            return fn

        return wrapper


SUITE = CheckSuite()


def load_definitions(path=None) -> list[dict[str, Any]]:
    path = path or (CONF_DIR / "quality.yaml")
    doc = yaml.safe_load(path.read_text()) or {}
    return doc.get("checks", [])


# --------------------------------------------------------------- evaluators


def _compare(actual: float, threshold: float | None, comparison: str) -> bool:
    if threshold is None:
        return True
    if comparison == "gte":
        return actual >= threshold
    if comparison == "lte":
        return actual <= threshold
    if comparison == "eq":
        return abs(actual - threshold) < 1e-9
    if comparison == "gt":
        return actual > threshold
    if comparison == "lt":
        return actual < threshold
    raise ValueError(f"Unknown comparison {comparison!r}")


def evaluate_check(spark: SparkSession, cfg: Config, spec: dict[str, Any]) -> CheckResult:
    """Run one declared check against its table."""
    layer = spec.get("layer", "gold")
    table = spec["table"]
    check_type = spec["type"]
    severity = spec.get("severity", "warn")
    threshold = spec.get("threshold")
    comparison = spec.get("comparison", "gte")
    name = spec.get("name") or f"{table}.{check_type}.{spec.get('column', '')}".rstrip(".")

    path = cfg.table_path(layer, table)
    if not table_exists(spark, path):
        return CheckResult(
            check_name=name,
            check_type=check_type,
            layer=layer,
            table_name=table,
            severity=severity,
            passed=False,
            detail="table does not exist",
        )

    df = read(spark, path)
    total = df.count()
    actual: float
    detail = None

    if check_type == "row_count":
        actual = float(total)

    elif check_type == "not_null":
        column = spec["column"]
        nulls = df.filter(F.col(column).isNull()).count()
        actual = 1.0 - (nulls / total if total else 0.0)
        detail = f"{nulls} null of {total}"

    elif check_type == "unique":
        columns = spec.get("columns") or [spec["column"]]
        distinct = df.select(*columns).distinct().count()
        actual = distinct / total if total else 1.0
        detail = f"{total - distinct} duplicate rows on {columns}"

    elif check_type == "range":
        column = spec["column"]
        lo, hi = spec.get("min"), spec.get("max")
        condition = F.lit(True)
        if lo is not None:
            condition = condition & (F.col(column) >= F.lit(lo))
        if hi is not None:
            condition = condition & (F.col(column) <= F.lit(hi))
        in_range = df.filter(F.col(column).isNull() | condition).count()
        actual = in_range / total if total else 1.0
        detail = f"{total - in_range} outside [{lo}, {hi}]"

    elif check_type == "referential":
        column = spec["column"]
        ref_layer = spec.get("ref_layer", "gold")
        ref_path = cfg.table_path(ref_layer, spec["ref_table"])
        if not table_exists(spark, ref_path):
            return CheckResult(
                check_name=name,
                check_type=check_type,
                layer=layer,
                table_name=table,
                severity=severity,
                passed=False,
                detail=f"reference {spec['ref_table']} missing",
            )
        ref = read(spark, ref_path).select(F.col(spec["ref_column"]).alias("_ref")).distinct()
        present = df.filter(F.col(column).isNotNull())
        n_present = present.count()
        orphans = present.join(ref, present[column] == F.col("_ref"), "left_anti").count()
        actual = 1.0 - (orphans / n_present if n_present else 0.0)
        detail = f"{orphans} orphaned of {n_present}"

    elif check_type == "expression":
        # Share of rows satisfying an arbitrary boolean SQL expression.
        matching = df.filter(F.expr(spec["expression"])).count()
        actual = matching / total if total else 1.0
        detail = f"{total - matching} of {total} failed: {spec['expression']}"

    else:
        raise ValueError(f"Unknown check type {check_type!r} in conf/quality.yaml")

    return CheckResult(
        check_name=name,
        check_type=check_type,
        layer=layer,
        table_name=table,
        severity=severity,
        passed=_compare(actual, threshold, comparison),
        actual_value=float(actual),
        threshold=float(threshold) if threshold is not None else None,
        comparison=comparison,
        detail=detail,
    )


def results_to_dataframe(
    spark: SparkSession, results: list[CheckResult], run_id: str, logical_date: str
) -> DataFrame:
    now = datetime.now(UTC)
    rows = [
        {
            "run_id": run_id,
            "run_ts": now,
            "logical_date": logical_date,
            "layer": r.layer,
            "table_name": r.table_name,
            "check_name": r.check_name,
            "check_type": r.check_type,
            "severity": r.severity,
            "passed": r.passed,
            "actual_value": r.actual_value,
            "threshold": r.threshold,
            "comparison": r.comparison,
            "detail": r.detail,
        }
        for r in results
    ]
    return spark.createDataFrame(rows, schema=SCORECARD_SCHEMA)


def persist(spark: SparkSession, cfg: Config, df: DataFrame) -> None:
    """Append this run's results to the scorecard history."""
    target = cfg.table_path("gold", "dq_scorecard")
    df.write.format("delta").mode("append").save(target)
    register_table(spark, cfg, "gold", "dq_scorecard")
