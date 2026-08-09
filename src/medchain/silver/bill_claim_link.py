"""Link hospital bills to insurance claims across two systems with no shared key.

Finance raises ``MC-DEL-2024-000123``. The insurer books ``NCI/2024/0004521``.
Neither system knows the other's identifier, and no foreign key exists anywhere —
which is why reconciling settlement against billing takes the finance team 7-10 days
of manual work every month.

Two independent matching routes are used, and their agreement is itself evidence:

``REFERENCE``  about 40% of claims carry the hospital's bill reference in a free-text
               field, re-keyed and mangled — the city segment dropped, separators
               changed, leading zeros lost. A normalised comparison of the numeric
               tail recovers these with near-certainty.
``ATTRIBUTE``  match on (patient, hospital) with the claimed amount equal to the
               billed amount within tolerance and the admission date within a few
               days. Strong, but not free: a patient admitted twice in one week for
               a similar amount is genuinely ambiguous.

Ambiguity is resolved by requiring the best candidate to be strictly better than the
runner-up. Anything left over goes to quarantine rather than being linked on a
coin-flip — a wrong link puts one patient's settlement against another's bill, which
is worse than an unlinked row that a human can resolve.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from medchain.config import Config
from medchain.utils.audit import RunContext
from medchain.utils.logging import get_logger
from medchain.utils.tables import read, register_table, table_exists

log = get_logger("medchain.silver.linkage")


def normalize_reference(col: F.Column) -> F.Column:
    """Reduce a bill reference to its comparable numeric tail.

    ``MC-DEL-2024-000123``, ``2024000123``, ``DEL/2024/123`` and ``REF 000123`` all
    reduce to a digit string ending ``123``. Comparing the last 6 digits is what
    survives every mangling observed in the source, without matching everything.
    """
    digits = F.regexp_replace(F.coalesce(col, F.lit("")), r"\D", "")
    return F.when(F.length(digits) >= 4, F.substring(digits, -6, 6))


def match_by_reference(bills: DataFrame, claims: DataFrame) -> DataFrame:
    """Route 1: the hospital reference carried on the claim."""
    b = bills.withColumn("bill_ref_key", normalize_reference(F.col("bill_id"))).alias("b")
    c = claims.withColumn("claim_ref_key", normalize_reference(F.col("hospital_ref_no"))).alias("c")

    return (
        c.filter(F.col("claim_ref_key").isNotNull())
        .join(
            b.filter(F.col("bill_ref_key").isNotNull()),
            (F.col("c.claim_ref_key") == F.col("b.bill_ref_key"))
            # Still require the same hospital: reference tails collide across sites.
            & (F.col("c.hospital_id") == F.col("b.hospital_id")),
            "inner",
        )
        .select(
            F.col("c.claim_id").alias("claim_id"),
            F.col("b.bill_id").alias("bill_id"),
            F.lit("REFERENCE").alias("match_method"),
            F.lit(0.98).alias("match_confidence"),
            F.abs(
                F.col("c.claim_amount").cast("double") - F.col("b.net_payable").cast("double")
            ).alias("amount_diff"),
            F.abs(F.datediff(F.col("c.admission_date"), F.col("b.admission_date"))).alias(
                "date_diff"
            ),
        )
    )


def match_by_attributes(
    bills: DataFrame, claims: DataFrame, *, date_tolerance: int, amount_tolerance_pct: float
) -> DataFrame:
    """Route 2: patient, hospital, amount and admission date together."""
    b = bills.alias("b")
    c = claims.alias("c")

    joined = c.join(
        b,
        (F.col("c.patient_id") == F.col("b.patient_id"))
        & (F.col("c.hospital_id") == F.col("b.hospital_id"))
        & (
            F.abs(F.datediff(F.col("c.admission_date"), F.col("b.admission_date")))
            <= F.lit(date_tolerance)
        ),
        "inner",
    )

    joined = (
        joined.withColumn(
            "amount_diff",
            F.abs(F.col("c.claim_amount").cast("double") - F.col("b.net_payable").cast("double")),
        )
        .withColumn(
            "amount_ratio",
            F.col("amount_diff") / F.greatest(F.col("b.net_payable").cast("double"), F.lit(1.0)),
        )
        .withColumn(
            "date_diff", F.abs(F.datediff(F.col("c.admission_date"), F.col("b.admission_date")))
        )
    )

    joined = joined.filter(F.col("amount_ratio") <= F.lit(amount_tolerance_pct))

    # Confidence falls off with both amount and date disagreement.
    confidence = (
        F.lit(0.95)
        - F.col("amount_ratio") * F.lit(2.0)
        - (F.col("date_diff") / F.lit(max(date_tolerance, 1))) * F.lit(0.10)
    )
    return joined.select(
        F.col("c.claim_id").alias("claim_id"),
        F.col("b.bill_id").alias("bill_id"),
        F.lit("ATTRIBUTE").alias("match_method"),
        F.round(F.greatest(confidence, F.lit(0.50)), 3).alias("match_confidence"),
        "amount_diff",
        "date_diff",
    )


def resolve_best_match(candidates: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Pick one bill per claim, quarantining anything genuinely ambiguous.

    Returns ``(accepted, ambiguous)``. A claim is ambiguous when its top two
    candidates are equally good on amount and date — which happens for real, when a
    patient is admitted twice in a week for the same procedure.
    """
    # Collapse the two routes first. Both can propose the *same* (claim, bill) pair,
    # and if that duplicate survives into the ranking the pair is compared against
    # itself, scores identically on every tie-break, and is declared ambiguous — the
    # correct answer thrown out for looking like a conflict. Agreement between the
    # routes is corroboration, not contention, so it is recorded as such.
    candidates = candidates.groupBy("claim_id", "bill_id").agg(
        F.max("match_confidence").alias("match_confidence"),
        F.min("amount_diff").alias("amount_diff"),
        F.min("date_diff").alias("date_diff"),
        F.countDistinct("match_method").alias("methods_agreeing"),
        # REFERENCE is the stronger evidence, so it names the pair when both fire.
        F.min("match_method").alias("match_method"),
    )

    ranked = Window.partitionBy("claim_id").orderBy(
        F.col("match_confidence").desc(),
        F.col("amount_diff").asc(),
        F.col("date_diff").asc(),
        F.col("bill_id").asc(),
    )
    scored = (
        candidates.withColumn("_rn", F.row_number().over(ranked))
        .withColumn("_n_candidates", F.count(F.lit(1)).over(Window.partitionBy("claim_id")))
        .withColumn("_next_amount_diff", F.lead("amount_diff").over(ranked))
        .withColumn("_next_date_diff", F.lead("date_diff").over(ranked))
    )
    top = scored.filter(F.col("_rn") == 1)

    is_ambiguous = (
        (F.col("_n_candidates") > 1)
        & (F.col("_next_amount_diff") == F.col("amount_diff"))
        & (F.col("_next_date_diff") == F.col("date_diff"))
    )

    accepted = top.filter(~is_ambiguous).drop(
        "_rn", "_n_candidates", "_next_amount_diff", "_next_date_diff"
    )
    ambiguous = top.filter(is_ambiguous).drop("_rn", "_next_amount_diff", "_next_date_diff")

    # A bill must not be claimed twice either. Where two claims resolve to the same
    # bill, keep the more confident and release the other for review.
    bill_ranked = Window.partitionBy("bill_id").orderBy(
        F.col("match_confidence").desc(), F.col("amount_diff").asc(), F.col("claim_id").asc()
    )
    accepted = accepted.withColumn("_brn", F.row_number().over(bill_ranked))
    contested = accepted.filter(F.col("_brn") > 1).drop("_brn")
    accepted = accepted.filter(F.col("_brn") == 1).drop("_brn")

    ambiguous = ambiguous.unionByName(
        contested.withColumn("_n_candidates", F.lit(-1)), allowMissingColumns=True
    )
    return accepted, ambiguous


def run(spark: SparkSession, cfg: Config, ctx: RunContext) -> dict[str, float]:
    """Build ``silver.bill_claim_link``."""
    bills_path = cfg.table_path("bronze", "billing_transactions")
    lifecycle_path = cfg.table_path("silver", "claim_lifecycle")
    for path, name in ((bills_path, "billing_transactions"), (lifecycle_path, "claim_lifecycle")):
        if not table_exists(spark, path):
            raise FileNotFoundError(f"Required table {name} missing at {path}")

    bills = (
        read(spark, bills_path)
        .dropDuplicates(["bill_id"])
        .select(
            "bill_id",
            "hospital_id",
            "patient_id",
            F.to_date(F.col("admission_date")).alias("admission_date"),
            F.to_date(F.col("discharge_date")).alias("discharge_date"),
            F.col("net_payable").cast("decimal(18,2)").alias("net_payable"),
            F.col("gross_amount").cast("decimal(18,2)").alias("gross_amount"),
        )
    )

    # One row per claim, from its most recent observed state.
    latest = Window.partitionBy("claim_id").orderBy(F.col("status_date").desc())
    claims = (
        read(spark, lifecycle_path)
        .withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .select(
            "claim_id",
            "hospital_id",
            "patient_id",
            "insurer_id",
            "hospital_ref_no",
            F.col("claim_amount").cast("decimal(18,2)").alias("claim_amount"),
            F.to_date(F.col("admission_date")).alias("admission_date"),
        )
    )

    n_bills = bills.count()
    n_claims = claims.count()

    date_tolerance = int(cfg.get("linkage", "admission_date_tolerance_days", default=2))
    amount_tolerance = float(cfg.get("linkage", "amount_tolerance_pct", default=0.02))

    by_ref = match_by_reference(bills, claims)
    by_attr = match_by_attributes(
        bills, claims, date_tolerance=date_tolerance, amount_tolerance_pct=amount_tolerance
    )
    candidates = by_ref.unionByName(by_attr)

    accepted, ambiguous = resolve_best_match(candidates)

    # Where both routes independently produced the same pair, that is the strongest
    # evidence available — promote its confidence accordingly.
    accepted = accepted.withColumn(
        "match_confidence",
        F.when(F.col("methods_agreeing") > 1, F.lit(0.99)).otherwise(F.col("match_confidence")),
    )

    output = (
        accepted.withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
        .select(
            "claim_id",
            "bill_id",
            "match_method",
            "match_confidence",
            "methods_agreeing",
            "amount_diff",
            "date_diff",
            "batch_id",
            "dw_updated_at",
        )
    )

    target = cfg.table_path("silver", "bill_claim_link")
    output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)
    register_table(spark, cfg, "silver", "bill_claim_link")

    quarantine_path = cfg.table_path("quarantine", "bill_claim_ambiguous")
    ambiguous.withColumn("batch_id", F.lit(ctx.batch_id)).write.format("delta").mode(
        "overwrite"
    ).option("overwriteSchema", "true").save(quarantine_path)

    stored = read(spark, target)
    n_linked = stored.count()
    n_ambiguous = read(spark, quarantine_path).count()
    by_method = {
        row["match_method"]: row["n"]
        for row in stored.groupBy("match_method").agg(F.count(F.lit(1)).alias("n")).collect()
    }
    corroborated = stored.filter(F.col("methods_agreeing") > 1).count()

    log.info(
        "  %d claims, %d bills -> %d linked (%.1f%% of claims)",
        n_claims,
        n_bills,
        n_linked,
        100 * n_linked / n_claims if n_claims else 0,
    )
    for method, n in sorted(by_method.items()):
        log.info("    %-12s %8d", method, n)
    log.info("    corroborated by both routes: %d", corroborated)
    if n_ambiguous:
        log.warning("  ambiguous, sent for review: %d", n_ambiguous)

    return {
        "claims": n_claims,
        "bills": n_bills,
        "linked": n_linked,
        "link_rate": n_linked / n_claims if n_claims else 0.0,
        "ambiguous": n_ambiguous,
        "corroborated": corroborated,
    }
