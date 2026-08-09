"""The data quality scorecard.

Two kinds of measurement live here, and the distinction matters.

**Structural checks** (``conf/quality.yaml``) verify the warehouse is internally
consistent: keys are unique, foreign keys resolve, amounts are non-negative, grains
hold. These would pass even if every matching decision the platform made were wrong,
because internal consistency is not correctness.

**Recovery metrics** (this module) compare the platform's output against the ground
truth in ``data/_truth`` and answer the question that actually matters: *how much of
what was destroyed did we get back?* Measured MPI precision and recall against known
identities. Measured claim-history coverage against the true transition log. Measured
TPA accuracy against the true deduction breakdown.

Ground truth exists here because the source data is synthetic. In a real deployment
these become sampled manual audits — a stewardship team reviewing 200 linkages a
month — but the metric definitions and the scorecard table are unchanged.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from medchain.config import Config
from medchain.quality.expectations import (
    CheckResult,
    evaluate_check,
    load_definitions,
    persist,
    results_to_dataframe,
)
from medchain.utils.audit import RunContext
from medchain.utils.logging import banner, get_logger
from medchain.utils.tables import read, table_exists

log = get_logger("medchain.quality.scorecard")


def _truth(spark: SparkSession, cfg: Config, name: str):
    """Load a ground-truth table, or ``None`` when unavailable."""
    try:
        return spark.read.parquet(f"{cfg.path('truth')}/{name}.parquet")
    except Exception:  # noqa: BLE001 - truth is optional outside the simulation
        return None


def measure_mpi(spark: SparkSession, cfg: Config) -> list[CheckResult]:
    """Pairwise precision, recall and F1 of the Master Patient Index.

    Pairwise rather than cluster-exact: the useful question is "of the record pairs
    we said are the same person, how many are?", not "did we reproduce every cluster
    perfectly". A cluster that is right except for one missing member is mostly
    valuable, and an exact-match metric would score it zero.
    """
    truth = _truth(spark, cfg, "mpi_truth")
    crosswalk_path = cfg.table_path("silver", "patient_crosswalk")
    if truth is None or not table_exists(spark, crosswalk_path):
        return []

    crosswalk = read(spark, crosswalk_path).select("hospital_id", "patient_id", "mpi_id")
    joined = crosswalk.join(
        truth.select("hospital_id", "patient_id", "person_id"),
        ["hospital_id", "patient_id"],
        "inner",
    ).cache()

    def pair_count(group_cols: list[str]) -> float:
        grouped = joined.groupBy(*group_cols).agg(F.count(F.lit(1)).alias("n"))
        total = grouped.select(F.sum(F.col("n") * (F.col("n") - 1) / 2).alias("pairs")).collect()[
            0
        ]["pairs"]
        return float(total or 0)

    predicted = pair_count(["mpi_id"])
    actual = pair_count(["person_id"])
    correct = pair_count(["mpi_id", "person_id"])

    precision = correct / predicted if predicted else 0.0
    recall = correct / actual if actual else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    n_records = joined.count()
    n_clusters = joined.select("mpi_id").distinct().count()
    collapse = 1 - (n_clusters / n_records) if n_records else 0.0

    joined.unpersist()

    return [
        CheckResult(
            "mpi.precision",
            "recovery",
            "silver",
            "patient_crosswalk",
            "warn",
            precision >= 0.98,
            precision,
            0.98,
            "gte",
            f"{int(correct)} correct of {int(predicted)} predicted pairs",
        ),
        CheckResult(
            "mpi.recall",
            "recovery",
            "silver",
            "patient_crosswalk",
            "warn",
            recall >= 0.85,
            recall,
            0.85,
            "gte",
            f"{int(correct)} found of {int(actual)} true pairs",
        ),
        CheckResult(
            "mpi.f1",
            "recovery",
            "silver",
            "patient_crosswalk",
            "warn",
            f1 >= 0.95,
            f1,
            0.95,
            "gte",
            "harmonic mean of precision and recall",
        ),
        CheckResult(
            "mpi.duplicate_collapse_rate",
            "recovery",
            "silver",
            "patient_crosswalk",
            "warn",
            True,
            collapse,
            None,
            "gte",
            f"{n_records} registrations -> {n_clusters} people",
        ),
    ]


def measure_claim_reconstruction(spark: SparkSession, cfg: Config) -> list[CheckResult]:
    """How much of the true claim lifecycle the snapshots let us recover.

    The ceiling is below 100% by construction: a state that begins and ends between
    two weekly exports was never observed and cannot be recovered. Reporting coverage
    against the truth is honest; asserting completeness would not be.
    """
    truth = _truth(spark, cfg, "claim_transitions_truth")
    path = cfg.table_path("silver", "claim_lifecycle")
    if truth is None or not table_exists(spark, path):
        return []

    recovered = (
        read(spark, path).select("claim_id", F.col("status_code"), F.col("status_date")).distinct()
    )
    expected = truth.select(
        "claim_id", F.col("status_code"), F.to_date(F.col("status_date")).alias("status_date")
    ).distinct()

    n_expected = expected.count()
    n_recovered = recovered.count()
    matched = expected.join(recovered, ["claim_id", "status_code", "status_date"], "inner").count()

    coverage = matched / n_expected if n_expected else 0.0
    # Anything recovered that was never true would mean the reconstruction invented
    # a transition — a correctness failure, not a sampling limitation.
    spurious = n_recovered - matched
    fidelity = matched / n_recovered if n_recovered else 0.0

    claims = read(spark, path)
    n_claims = claims.select("claim_id").distinct().count()
    terminal = claims.filter(F.col("is_terminal")).select("claim_id").distinct().count()
    illegal = claims.filter(F.col("transition_class") == "ILLEGAL").count()

    return [
        CheckResult(
            "claims.reconstruction_coverage",
            "recovery",
            "silver",
            "claim_lifecycle",
            "warn",
            coverage >= 0.90,
            coverage,
            0.90,
            "gte",
            f"{matched} of {n_expected} true transitions recovered",
        ),
        CheckResult(
            "claims.reconstruction_fidelity",
            "recovery",
            "silver",
            "claim_lifecycle",
            "blocking",
            spurious == 0,
            fidelity,
            1.0,
            "gte",
            f"{spurious} recovered transitions with no counterpart in truth",
        ),
        CheckResult(
            "claims.terminal_state_rate",
            "recovery",
            "silver",
            "claim_lifecycle",
            "warn",
            True,
            terminal / n_claims if n_claims else 0.0,
            None,
            "gte",
            f"{terminal} of {n_claims} claims reached a terminal state",
        ),
        CheckResult(
            "claims.illegal_transitions",
            "recovery",
            "silver",
            "claim_lifecycle",
            "warn",
            illegal == 0,
            float(illegal),
            0.0,
            "lte",
            "transitions to an unreachable state",
        ),
    ]


def measure_tpa_accuracy(spark: SparkSession, cfg: Config) -> list[CheckResult]:
    """How closely the rules engine reproduces the true deduction breakdown.

    Reported per component, not just on the bottom line. A net figure that happens
    to land close while the exclusion and room-rent components are individually
    wrong is a coincidence, not a working rules engine.
    """
    truth = _truth(spark, cfg, "tpa_truth")
    path = cfg.table_path("silver", "claim_adjudication")
    if truth is None or not table_exists(spark, path):
        return []

    adj = read(spark, path)
    joined = adj.alias("a").join(truth.alias("t"), "claim_id", "inner")
    n = joined.count()
    if n == 0:
        return []

    results: list[CheckResult] = []
    for column, threshold in (
        ("excluded_amount", 0.99),
        ("room_rent_excess", 0.99),
        ("eligible_amount", 0.99),
        ("copay_amount", 0.99),
        ("net_reimbursement", 0.99),
    ):
        diff = F.abs(F.col(f"a.{column}").cast("double") - F.col(f"t.{column}").cast("double"))
        agree = joined.filter(diff <= F.lit(1.0)).count()
        rate = agree / n
        results.append(
            CheckResult(
                f"tpa.{column}_matches_truth",
                "recovery",
                "silver",
                "claim_adjudication",
                "warn",
                rate >= threshold,
                rate,
                threshold,
                "gte",
                f"{agree} of {n} within Rs.1",
            )
        )

    # Reconciliation against what the insurer actually paid — the number finance
    # cares about. Reported two ways, because a single blended figure is misleading.
    #
    # Some claims are *not predictable from rules by construction*: a rejected claim
    # pays zero regardless of the arithmetic, and a partially approved one carries a
    # medical officer's discretionary reduction that no rule encodes. Averaging those
    # in produces ~53% and makes a working engine look broken. The meaningful metric
    # is the rate among claims the rules genuinely should explain; the blended rate
    # is kept alongside it as context, not as the headline.
    adjudicated = adj.filter(F.col("insurer_approved_amount").isNotNull())
    n_adj = adjudicated.count()
    explained_all = adjudicated.filter(F.col("is_reconciled")).count()

    predictable = adjudicated.filter(
        ~F.coalesce(F.col("ever_rejected"), F.lit(False))
        & ~F.coalesce(F.col("ever_partially_approved"), F.lit(False))
    )
    n_predictable = predictable.count()
    explained_predictable = predictable.filter(F.col("is_reconciled")).count()
    rate_predictable = explained_predictable / n_predictable if n_predictable else 0.0

    results.extend(
        [
            CheckResult(
                "tpa.deduction_explained_rate",
                "recovery",
                "silver",
                "claim_adjudication",
                "warn",
                rate_predictable >= 0.90,
                rate_predictable,
                0.90,
                "gte",
                f"{explained_predictable} of {n_predictable} rule-predictable claims "
                "explained to within Rs.1 (excludes rejections and discretionary "
                "partial approvals)",
            ),
            CheckResult(
                "tpa.reconciliation_rate_all_claims",
                "recovery",
                "silver",
                "claim_adjudication",
                "warn",
                True,
                explained_all / n_adj if n_adj else 0.0,
                None,
                "gte",
                f"{explained_all} of {n_adj} adjudicated claims, including those with "
                "outcomes no rule can predict — context only",
            ),
        ]
    )
    return results


def measure_icd_fill(spark: SparkSession, cfg: Config) -> list[CheckResult]:
    """ICD-10 fill rate, broken down by the tier that produced each code."""
    path = cfg.table_path("silver", "procedure_catalog")
    if not table_exists(spark, path):
        return []

    df = read(spark, path)
    total = df.count()
    if total == 0:
        return []

    tiers = {
        row["icd10_source"]: row["n"]
        for row in df.groupBy("icd10_source").agg(F.count(F.lit(1)).alias("n")).collect()
    }
    filled = total - tiers.get("UNMAPPED", 0)

    results = [
        CheckResult(
            "icd10.overall_fill_rate",
            "recovery",
            "silver",
            "procedure_catalog",
            "warn",
            filled / total >= 0.99,
            filled / total,
            0.99,
            "gte",
            f"{filled} of {total} procedures carry a code",
        ),
    ]
    # Per tier, so "99% filled" cannot hide that most of it is a specialty default.
    for tier in ("SOURCE", "EXACT_NAME", "FUZZY_NAME", "SPECIALTY", "UNMAPPED"):
        n = tiers.get(tier, 0)
        results.append(
            CheckResult(
                f"icd10.tier_{tier.lower()}",
                "recovery",
                "silver",
                "procedure_catalog",
                "warn",
                True,
                n / total,
                None,
                "gte",
                f"{n} procedures",
            )
        )
    return results


def measure_linkage(spark: SparkSession, cfg: Config) -> list[CheckResult]:
    """Bill-to-claim link rate and the split between matching routes."""
    path = cfg.table_path("silver", "bill_claim_link")
    lifecycle = cfg.table_path("silver", "claim_lifecycle")
    if not table_exists(spark, path) or not table_exists(spark, lifecycle):
        return []

    links = read(spark, path)
    n_claims = read(spark, lifecycle).select("claim_id").distinct().count()
    n_linked = links.select("claim_id").distinct().count()
    corroborated = links.filter(F.col("methods_agreeing") > 1).count()

    quarantine = cfg.table_path("quarantine", "bill_claim_ambiguous")
    n_ambiguous = read(spark, quarantine).count() if table_exists(spark, quarantine) else 0

    return [
        CheckResult(
            "linkage.bill_claim_rate",
            "recovery",
            "silver",
            "bill_claim_link",
            "warn",
            (n_linked / n_claims if n_claims else 0) >= 0.95,
            n_linked / n_claims if n_claims else 0.0,
            0.95,
            "gte",
            f"{n_linked} of {n_claims} claims linked to a bill",
        ),
        CheckResult(
            "linkage.corroborated_by_both_routes",
            "recovery",
            "silver",
            "bill_claim_link",
            "warn",
            True,
            corroborated / n_linked if n_linked else 0.0,
            None,
            "gte",
            f"{corroborated} links confirmed by both reference and attribute matching",
        ),
        CheckResult(
            "linkage.ambiguous_queue",
            "recovery",
            "quarantine",
            "bill_claim_ambiguous",
            "warn",
            n_ambiguous < n_claims * 0.05,
            float(n_ambiguous),
            n_claims * 0.05,
            "lte",
            "candidate pairs a human must resolve",
        ),
    ]


RECOVERY_METRICS = (
    measure_mpi,
    measure_claim_reconstruction,
    measure_tpa_accuracy,
    measure_icd_fill,
    measure_linkage,
)


def run(
    spark: SparkSession,
    cfg: Config,
    logical_date: date | str | None = None,
    *,
    fail_on_blocking: bool = True,
) -> dict[str, int]:
    """Evaluate every check and write the scorecard."""
    ctx = RunContext.create(logical_date or date.today(), layer="quality")
    banner(log, "DATA QUALITY SCORECARD", run_id=ctx.run_id, logical_date=ctx.logical_date)

    results: list[CheckResult] = []

    log.info("")
    log.info("--- structural checks ---")
    for spec in load_definitions():
        result = evaluate_check(spark, cfg, spec)
        results.append(result)
        marker = "PASS" if result.passed else ("FAIL" if result.severity == "blocking" else "WARN")
        value = f"{result.actual_value:.4f}" if result.actual_value is not None else "-"
        log.info("  [%s] %-48s %10s  %s", marker, result.check_name, value, result.detail or "")

    log.info("")
    log.info("--- recovery metrics (measured against ground truth) ---")
    for metric in RECOVERY_METRICS:
        for result in metric(spark, cfg):
            results.append(result)
            marker = "PASS" if result.passed else "WARN"
            value = f"{result.actual_value:.4f}" if result.actual_value is not None else "-"
            log.info("  [%s] %-48s %10s  %s", marker, result.check_name, value, result.detail or "")

    df = results_to_dataframe(spark, results, ctx.run_id, ctx.logical_date.isoformat())
    persist(spark, cfg, df)

    blocking_failures = [r for r in results if r.severity == "blocking" and not r.passed]
    warnings = [r for r in results if r.severity != "blocking" and not r.passed]

    log.info("")
    log.info(
        "Scorecard: %d checks, %d blocking failures, %d warnings",
        len(results),
        len(blocking_failures),
        len(warnings),
    )

    if blocking_failures:
        for failure in blocking_failures:
            log.error("  BLOCKING: %s — %s", failure.check_name, failure.detail)
        if fail_on_blocking:
            raise RuntimeError(
                f"{len(blocking_failures)} blocking data quality check(s) failed: "
                + ", ".join(f.check_name for f in blocking_failures)
            )

    return {
        "checks": len(results),
        "blocking_failures": len(blocking_failures),
        "warnings": len(warnings),
        "passed": len(results) - len(blocking_failures) - len(warnings),
    }
