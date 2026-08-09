"""TPA deduction rules engine — codify what the insurer never explains.

The portal reports one number, ``approved_amount``, and no working. Finance is left
to reverse-engineer why a ₹4,20,000 bill was reimbursed at ₹2,68,000, by hand, for
every claim. That is the spec's ">12% manual reconciliation errors".

This module makes the adjudication explicit and repeatable. The order of operations
is the part that matters, and it mirrors how a TPA actually assesses a claim:

1. **Exclusions first.** Registration, admin, attendant and non-medical items come
   off entirely; pharmacy and dietary are partially disallowed. These are removed
   before anything is calculated as a percentage.
2. **Room-rent cap next.** Rent above the policy's per-day limit is deducted from
   the *room line*, not the whole bill.
3. **Co-pay on the remainder.** The patient's share of what is actually eligible.
4. **Residual percentage deduction.**

Getting this order wrong — applying co-pay to the gross, or the room cap to the bill
total — produces a number that looks plausible and is wrong by tens of thousands of
rupees. Both mistakes are exactly what manual reconciliation drifts into.

The computed ``net_reimbursement`` is then compared against the insurer's
``approved_amount``, and the variance is reported rather than hidden. Where they
agree, the deduction is fully explained; where they differ, the claim carries a
genuine ad-hoc adjustment that no rule can predict, and it is surfaced for review.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from medchain.config import Config
from medchain.utils.audit import RunContext
from medchain.utils.logging import get_logger
from medchain.utils.tables import read, register_table, table_exists

log = get_logger("medchain.silver.tpa")

# Item categories whose amounts form the room-rent line.
ROOM_CATEGORY = "ROOM"

# Tolerance for declaring the computed reimbursement to match the insurer's figure.
# One rupee, not zero: both sides round to paise at different points, and an exact
# float equality test would report spurious mismatches.
MATCH_TOLERANCE = 1.00


def load_reference_tables(spark: SparkSession, cfg: Config) -> tuple[DataFrame, DataFrame]:
    """Materialise the TPA rule and exclusion seeds as Silver Delta tables.

    They live in Delta rather than being read from CSV at runtime so that a rule
    change is a versioned, time-travellable event. "Why did this claim adjudicate
    differently in March?" is answerable by reading the table as of March.
    """
    rules_pd = pd.read_csv(cfg.seed_dir / "tpa_rules.csv")
    excl_pd = pd.read_csv(cfg.seed_dir / "tpa_exclusions.csv")

    rules = (
        spark.createDataFrame(rules_pd)
        .withColumn("effective_from", F.to_date(F.col("effective_from")))
        .withColumn("effective_to", F.to_date(F.col("effective_to")))
    )
    exclusions = (
        spark.createDataFrame(excl_pd)
        .withColumn("effective_from", F.to_date(F.col("effective_from")))
        .withColumn("effective_to", F.to_date(F.col("effective_to")))
    )

    rules.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        cfg.table_path("silver", "tpa_rules")
    )
    exclusions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        cfg.table_path("silver", "tpa_exclusions")
    )
    register_table(spark, cfg, "silver", "tpa_rules")
    register_table(spark, cfg, "silver", "tpa_exclusions")
    return rules, exclusions


def resolve_rules(claims: DataFrame, rules: DataFrame) -> DataFrame:
    """Attach the most specific applicable rule to each claim.

    Wildcards make this a ranked join rather than an equi-join: a claim can match
    the exact (insurer, category, room) rule *and* the insurer catch-all, and must
    take the former. ``rule_priority`` encodes specificity and ``rule_id`` breaks
    ties, so the result does not depend on join order or partitioning.
    """
    joined = claims.alias("c").join(
        rules.alias("r"),
        (F.col("c.insurer_id") == F.col("r.insurer_id"))
        & (
            (F.col("r.procedure_category") == F.col("c.procedure_category"))
            | (F.col("r.procedure_category") == F.lit("*"))
        )
        & ((F.col("r.room_type") == F.col("c.room_type")) | (F.col("r.room_type") == F.lit("*")))
        & (F.col("c.claim_date") >= F.col("r.effective_from"))
        & (F.col("c.claim_date") <= F.col("r.effective_to")),
        "left",
    )

    ranked = Window.partitionBy("c.claim_id").orderBy(
        F.col("r.rule_priority").asc_nulls_last(), F.col("r.rule_id").asc_nulls_last()
    )
    return (
        joined.withColumn("_rn", F.row_number().over(ranked))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "c.*",
            F.col("r.rule_id").alias("rule_id"),
            F.coalesce(F.col("r.copay_pct"), F.lit(0.0)).alias("copay_pct"),
            F.coalesce(F.col("r.room_rent_cap_per_day"), F.lit(0.0)).alias("room_rent_cap_per_day"),
            F.coalesce(F.col("r.deduction_pct"), F.lit(0.0)).alias("deduction_pct"),
        )
    )


def compute_exclusions(line_items: DataFrame, exclusions: DataFrame) -> DataFrame:
    """Sum the disallowed portion of each claim's line items.

    A left join, not inner: categories with no exclusion rule are fully payable and
    must contribute zero, not vanish from the claim's billed total.
    """
    joined = line_items.alias("li").join(
        exclusions.alias("ex"),
        (F.col("li.insurer_id") == F.col("ex.insurer_id"))
        & (F.col("li.item_category") == F.col("ex.item_category")),
        "left",
    )
    return (
        joined.withColumn(
            "excluded_amount",
            F.col("li.line_amount") * F.coalesce(F.col("ex.excluded_pct"), F.lit(0.0)),
        )
        .groupBy("li.claim_id")
        .agg(
            F.sum("li.line_amount").alias("billed_amount"),
            F.sum("excluded_amount").alias("excluded_amount"),
            F.sum(
                F.when(
                    F.col("li.item_category") == ROOM_CATEGORY, F.col("li.line_amount")
                ).otherwise(F.lit(0.0))
            ).alias("room_charge"),
            F.max(
                F.when(F.col("li.item_category") == ROOM_CATEGORY, F.col("li.quantity")).otherwise(
                    F.lit(0)
                )
            ).alias("room_days"),
        )
        .withColumnRenamed("claim_id", "claim_id")
    )


def adjudicate(claims: DataFrame) -> DataFrame:
    """Apply the four-step deduction cascade and reconcile against the insurer."""
    # Room days come from the ROOM line's quantity. A claim with no room line
    # (day-care, outpatient) gets 1 so the cap still has a denominator.
    df = claims.withColumn(
        "room_days", F.greatest(F.coalesce(F.col("room_days"), F.lit(1)), F.lit(1))
    )

    # Step 2: rent above the per-day cap. A cap of 0 means the rule imposes no room
    # limit at all (diagnostic and consultation categories), not a limit of zero.
    df = df.withColumn(
        "allowed_room",
        F.when(
            F.col("room_rent_cap_per_day") > 0,
            F.col("room_rent_cap_per_day") * F.col("room_days"),
        ).otherwise(F.col("room_charge")),
    )
    df = df.withColumn(
        "room_rent_excess", F.greatest(F.lit(0.0), F.col("room_charge") - F.col("allowed_room"))
    )

    # Step 3 and 4, on what remains eligible after exclusions and the room cap.
    df = df.withColumn(
        "eligible_amount",
        F.greatest(
            F.lit(0.0),
            F.col("billed_amount") - F.col("excluded_amount") - F.col("room_rent_excess"),
        ),
    )
    df = df.withColumn("copay_amount", F.col("eligible_amount") * F.col("copay_pct"))
    df = df.withColumn("other_deduction", F.col("eligible_amount") * F.col("deduction_pct"))
    df = df.withColumn(
        "net_reimbursement",
        F.greatest(
            F.lit(0.0),
            F.col("eligible_amount") - F.col("copay_amount") - F.col("other_deduction"),
        ),
    )
    df = df.withColumn("reimbursement_gap", F.col("billed_amount") - F.col("net_reimbursement"))

    # Reconciliation against what the insurer actually paid.
    df = df.withColumn(
        "reconciliation_variance",
        F.round(F.col("net_reimbursement") - F.col("approved_amount").cast("double"), 2),
    )
    df = df.withColumn(
        "is_reconciled",
        F.col("approved_amount").isNotNull()
        & (F.abs(F.col("reconciliation_variance")) <= F.lit(MATCH_TOLERANCE)),
    )
    # Classify the residual so the scorecard can separate "our rules are wrong" from
    # "the TPA made a judgement call our rules cannot see".
    # Classification uses the claim's *history*, not just its latest state.
    #
    # A partially approved claim goes on to Settle, so by the time we look at it
    # `latest_status` reads "Settled" and every trace of the discretionary reduction
    # is gone from the current state. Judging by latest status alone dumps ~55k
    # legitimately-discretionary claims into UNEXPLAINED and makes the rules engine
    # look far worse than it is. `ever_partially_approved` comes from the
    # reconstructed lifecycle — which is exactly what that audit table is for.
    df = df.withColumn(
        "variance_class",
        F.when(F.col("approved_amount").isNull(), F.lit("NOT_ADJUDICATED"))
        .when(F.col("is_reconciled"), F.lit("EXPLAINED"))
        .when(F.col("ever_rejected"), F.lit("REJECTED_CLAIM"))
        .when(F.col("ever_partially_approved"), F.lit("PARTIAL_APPROVAL_DISCRETION"))
        .otherwise(F.lit("UNEXPLAINED")),
    )
    return df.drop("allowed_room")


def run(spark: SparkSession, cfg: Config, ctx: RunContext) -> dict[str, float]:
    """Build ``silver.claim_adjudication`` from line items and the rules tables."""
    lifecycle_path = cfg.table_path("silver", "claim_lifecycle")
    line_items_path = cfg.table_path("bronze", "claim_line_items")
    procedures_path = cfg.table_path("silver", "procedure_catalog")

    for path, name in ((lifecycle_path, "claim_lifecycle"), (line_items_path, "claim_line_items")):
        if not table_exists(spark, path):
            raise FileNotFoundError(f"Required table {name} missing at {path}")

    rules, exclusions = load_reference_tables(spark, cfg)

    # Latest observed state per claim, plus the insurer's adjudicated amount.
    lifecycle = read(spark, lifecycle_path)
    latest = Window.partitionBy("claim_id").orderBy(
        F.col("status_date").desc(), F.col("transition_seq").desc()
    )
    claim_state = (
        lifecycle.withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .select(
            "claim_id",
            "insurer_id",
            "hospital_id",
            "patient_id",
            F.col("status_code").alias("latest_status"),
            "claim_amount",
            "approved_amount",
            "submitted_date",
            "admission_date",
            "discharge_date",
            F.coalesce(F.col("status_date"), F.col("submitted_date")).alias("claim_date"),
        )
    )

    # Whether the claim *ever* passed through each outcome state. A partial approval
    # that later settles is invisible in the latest state, so these come from the
    # full reconstructed history.
    history_flags = lifecycle.groupBy("claim_id").agg(
        F.max((F.col("status_code") == "Partially Approved").cast("int"))
        .cast("boolean")
        .alias("ever_partially_approved"),
        F.max((F.col("status_code") == "Rejected").cast("int"))
        .cast("boolean")
        .alias("ever_rejected"),
        F.max((F.col("status_code") == "Approved").cast("int"))
        .cast("boolean")
        .alias("ever_approved"),
    )
    claim_state = claim_state.join(history_flags, on="claim_id", how="left")

    # Line items carry room_type; the procedure category comes from the catalogue.
    line_items = read(spark, line_items_path).select(
        "claim_id",
        "procedure_code",
        "item_category",
        "room_type",
        F.col("quantity").cast("int").alias("quantity"),
        F.col("line_amount").cast("double").alias("line_amount"),
    )
    line_items = line_items.join(
        claim_state.select("claim_id", "insurer_id"), on="claim_id", how="inner"
    )

    aggregates = compute_exclusions(line_items, exclusions)

    # Room type for the claim = the room type on its ROOM line.
    room_type = (
        line_items.filter(F.col("item_category") == ROOM_CATEGORY)
        .groupBy("claim_id")
        .agg(F.max("room_type").alias("room_type"))
    )
    # Procedure category from the PROCEDURE line, resolved through the catalogue.
    if table_exists(spark, procedures_path):
        catalog = read(spark, procedures_path).select(
            "procedure_code", F.col("procedure_category").alias("cat_from_catalog")
        )
    else:
        catalog = (
            read(spark, cfg.table_path("bronze", "procedure_master"))
            .select("procedure_code", F.col("procedure_category").alias("cat_from_catalog"))
            .dropDuplicates(["procedure_code"])
        )

    procedure_category = (
        line_items.filter(F.col("procedure_code").isNotNull())
        .join(catalog, on="procedure_code", how="left")
        .groupBy("claim_id")
        .agg(F.max("cat_from_catalog").alias("procedure_category"))
    )

    claims = (
        claim_state.join(aggregates, on="claim_id", how="inner")
        .join(room_type, on="claim_id", how="left")
        .join(procedure_category, on="claim_id", how="left")
        .withColumn("room_type", F.coalesce(F.col("room_type"), F.lit("GENERAL")))
        .withColumn("procedure_category", F.coalesce(F.col("procedure_category"), F.lit("*")))
    )

    resolved = resolve_rules(claims, rules)
    adjudicated = adjudicate(resolved)

    output = (
        adjudicated.select(
            "claim_id",
            "insurer_id",
            "hospital_id",
            "patient_id",
            "rule_id",
            "procedure_category",
            "room_type",
            "room_days",
            F.round("billed_amount", 2).alias("billed_amount"),
            F.round("excluded_amount", 2).alias("excluded_amount"),
            F.round("room_charge", 2).alias("room_charge"),
            F.round("room_rent_excess", 2).alias("room_rent_excess"),
            F.round("eligible_amount", 2).alias("eligible_amount"),
            "copay_pct",
            F.round("copay_amount", 2).alias("copay_amount"),
            "deduction_pct",
            F.round("other_deduction", 2).alias("other_deduction"),
            F.round("net_reimbursement", 2).alias("net_reimbursement"),
            F.round("reimbursement_gap", 2).alias("reimbursement_gap"),
            F.col("approved_amount").cast("double").alias("insurer_approved_amount"),
            "reconciliation_variance",
            "is_reconciled",
            "variance_class",
            "latest_status",
            "ever_approved",
            "ever_partially_approved",
            "ever_rejected",
            "claim_amount",
            "submitted_date",
            "admission_date",
            "discharge_date",
        )
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
    )

    target = cfg.table_path("silver", "claim_adjudication")
    output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)
    register_table(spark, cfg, "silver", "claim_adjudication")

    stored = read(spark, target)
    total = stored.count()
    adjudged = stored.filter(F.col("insurer_approved_amount").isNotNull()).count()
    reconciled = stored.filter(F.col("is_reconciled")).count()
    unexplained = stored.filter(F.col("variance_class") == "UNEXPLAINED").count()

    log.info("  adjudicated %d claims (%d with an insurer decision)", total, adjudged)
    log.info(
        "  reconciled to within Rs.%.2f: %d (%.1f%% of adjudicated)",
        MATCH_TOLERANCE,
        reconciled,
        100 * reconciled / adjudged if adjudged else 0,
    )
    for row in stored.groupBy("variance_class").agg(F.count(F.lit(1)).alias("n")).collect():
        log.info("    %-30s %8d", row["variance_class"], row["n"])

    return {
        "claims": total,
        "adjudicated": adjudged,
        "reconciled": reconciled,
        "reconciliation_rate": reconciled / adjudged if adjudged else 0.0,
        "unexplained": unexplained,
    }
