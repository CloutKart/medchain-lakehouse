"""Gold fact tables.

Four facts, each at a stated grain:

``fact_patient_visit``          one row per visit
``fact_claim_lifecycle``        one row per claim state transition
``fact_billing_reconciliation`` one row per bill-to-claim linkage
``fact_bed_occupancy``          one row per ward per day

Two techniques carry most of the analytical value here.

**Point-in-time dimension joins.** Facts join the SCD2 dimensions on
``fact_date BETWEEN effective_from AND effective_to``, never on ``is_current``.
A consultation from March 2023 is credited to the department that doctor worked in
during March 2023. Joining on ``is_current`` instead would re-attribute years of
history to wherever each doctor happens to sit today — the misattribution the spec
calls out, and the one Business Question 4 measures.

**Network-wide readmission.** ``readmit_30d_network`` looks across all eight
hospitals through the MPI, alongside ``readmit_30d_same_hospital``. The difference
between the two is the readmission rate that no single hospital can see, and it is
the clearest quantification of what identity resolution actually bought.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from medchain.config import Config
from medchain.utils.audit import RunContext
from medchain.utils.keys import surrogate_key
from medchain.utils.logging import get_logger
from medchain.utils.tables import read, register_table, table_exists

log = get_logger("medchain.gold.facts")


def _write(
    df: DataFrame,
    spark: SparkSession,
    cfg: Config,
    name: str,
    *,
    partition_by: list[str] | None = None,
) -> int:
    target = cfg.table_path("gold", name)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(target)
    register_table(spark, cfg, "gold", name)
    n = read(spark, target).count()
    log.info("  %-30s %9d rows", name, n)
    return n


def _point_in_time_doctor(visits: DataFrame, dim_doctor: DataFrame) -> DataFrame:
    """Attach the doctor's department *as at the visit date*.

    Both sides carry ``hospital_id`` and ``specialty``, so both frames are aliased
    and every output column is named explicitly. Relying on ``select(*visits.columns)``
    after a join with overlapping names resolves ambiguously — and the failure mode
    when it does not raise is worse than when it does: the wrong side's column
    silently wins.
    """
    v = visits.alias("v")
    d = dim_doctor.alias("d")
    joined = v.join(
        d,
        (F.col("v.doctor_id") == F.col("d.doctor_id"))
        & (F.col("v.admission_date") >= F.col("d.effective_from"))
        & (F.col("v.admission_date") <= F.col("d.effective_to")),
        "left",
    )
    return joined.select(
        *[F.col(f"v.{c}").alias(c) for c in visits.columns],
        F.col("d.doctor_sk").alias("doctor_sk"),
        F.col("d.department").alias("department_at_visit"),
        F.col("d.specialty").alias("doctor_specialty"),
        F.col("d.designation").alias("doctor_designation"),
    )


def build_fact_patient_visit(spark: SparkSession, cfg: Config, ctx: RunContext) -> int:
    """One row per visit, with readmission flags computed at two scopes."""
    # Visits are reconstructed from the hospital's own record. The HIS visit feed is
    # not one of the seven source exports, so the visit spine comes from the truth
    # file — in a real deployment this is simply another Bronze source and nothing
    # downstream changes.
    visits = spark.read.parquet(f"{cfg.path('truth')}/visit_truth.parquet").select(
        "visit_id",
        "patient_id",
        "hospital_id",
        "doctor_id",
        F.col("department").alias("department_recorded"),
        "admission_type",
        F.to_date("admission_date").alias("admission_date"),
        F.to_date("discharge_date").alias("discharge_date"),
        F.col("length_of_stay").cast("int").alias("length_of_stay"),
    )

    crosswalk = read(spark, cfg.table_path("silver", "patient_crosswalk")).select(
        "hospital_id", "patient_id", "mpi_id"
    )
    visits = visits.join(crosswalk, on=["hospital_id", "patient_id"], how="left")

    # Procedure comes from the visit's own record via the catalogue.
    procedures = read(spark, cfg.table_path("gold", "dim_procedure")).select(
        "procedure_sk", "procedure_code", "procedure_category", "specialty", "icd10_code"
    )
    visit_procedures = spark.read.parquet(f"{cfg.path('truth')}/visit_truth.parquet")
    if "procedure_code" in visit_procedures.columns:
        visits = visits.join(
            visit_procedures.select("visit_id", "procedure_code"), on="visit_id", how="left"
        ).join(procedures, on="procedure_code", how="left")
    else:
        # The visit spine has no procedure reference. Keep the columns present and
        # null so the star schema shape is stable and the quality check reports the
        # gap, rather than the build failing on a missing column.
        visits = visits.withColumn("procedure_code", F.lit(None).cast("string")).withColumn(
            "procedure_sk", F.lit(None).cast("long")
        )

    dim_doctor = read(spark, cfg.table_path("gold", "dim_doctor"))
    visits = _point_in_time_doctor(visits, dim_doctor)

    # The naive alternative, kept side by side so the difference is measurable
    # rather than asserted. Business Question 4 reports the gap between the two.
    current_dept = dim_doctor.filter(F.col("is_current")).select(
        F.col("doctor_id").alias("_did"), F.col("department").alias("department_current")
    )
    visits = visits.join(current_dept, visits["doctor_id"] == current_dept["_did"], "left").drop(
        "_did"
    )

    # --- readmission, at two scopes -----------------------------------------
    window_days = int(cfg.get("clinical", "readmission_window_days", default=30))
    inpatient = F.col("admission_type").isin(["IPD", "EMERGENCY"])

    # Network scope: ordered by the resolved person, across all hospitals.
    net_window = Window.partitionBy("mpi_id").orderBy("admission_date")
    hosp_window = Window.partitionBy("mpi_id", "hospital_id").orderBy("admission_date")

    visits = (
        visits.withColumn("_prev_discharge_net", F.lag("discharge_date").over(net_window))
        .withColumn("_prev_discharge_hosp", F.lag("discharge_date").over(hosp_window))
        .withColumn("_prev_hospital", F.lag("hospital_id").over(net_window))
    )
    visits = (
        visits.withColumn(
            "days_since_prev_discharge",
            F.datediff(F.col("admission_date"), F.col("_prev_discharge_net")),
        )
        .withColumn(
            "readmit_30d_network",
            inpatient
            & F.col("_prev_discharge_net").isNotNull()
            & (F.col("days_since_prev_discharge") >= 0)
            & (F.col("days_since_prev_discharge") <= window_days),
        )
        .withColumn(
            "readmit_30d_same_hospital",
            inpatient
            & F.col("_prev_discharge_hosp").isNotNull()
            & (F.datediff(F.col("admission_date"), F.col("_prev_discharge_hosp")) >= 0)
            & (F.datediff(F.col("admission_date"), F.col("_prev_discharge_hosp")) <= window_days),
        )
    )
    # The cohort that only exists because identities were resolved.
    visits = visits.withColumn(
        "readmit_cross_hospital_only",
        F.col("readmit_30d_network") & ~F.col("readmit_30d_same_hospital"),
    )

    out = (
        visits.withColumn("visit_sk", surrogate_key(F.col("visit_id")))
        .withColumn("patient_sk", surrogate_key(F.col("mpi_id")))
        .withColumn("hospital_sk", surrogate_key(F.col("hospital_id")))
        .withColumn("admission_date_sk", F.date_format("admission_date", "yyyyMMdd").cast("int"))
        .withColumn("discharge_date_sk", F.date_format("discharge_date", "yyyyMMdd").cast("int"))
        .withColumn("is_inpatient", inpatient)
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
        .select(
            "visit_sk",
            "visit_id",
            "patient_sk",
            "mpi_id",
            "doctor_sk",
            "doctor_id",
            "hospital_sk",
            "hospital_id",
            "procedure_sk",
            "procedure_code",
            "admission_date_sk",
            "discharge_date_sk",
            "admission_date",
            "discharge_date",
            "admission_type",
            "is_inpatient",
            "length_of_stay",
            # Both attributions, deliberately side by side.
            "department_at_visit",
            "department_current",
            "department_recorded",
            "doctor_specialty",
            "doctor_designation",
            "days_since_prev_discharge",
            "readmit_30d_network",
            "readmit_30d_same_hospital",
            "readmit_cross_hospital_only",
            "_prev_hospital",
            "batch_id",
            "dw_updated_at",
        )
        .withColumnRenamed("_prev_hospital", "previous_hospital_id")
    )
    return _write(out, spark, cfg, "fact_patient_visit", partition_by=["admission_date"])


def build_fact_claim_lifecycle(spark: SparkSession, cfg: Config, ctx: RunContext) -> int:
    """One row per claim state transition — the full audit trail."""
    lifecycle = read(spark, cfg.table_path("silver", "claim_lifecycle"))

    crosswalk = read(spark, cfg.table_path("silver", "patient_crosswalk")).select(
        "hospital_id", "patient_id", "mpi_id"
    )
    df = lifecycle.join(crosswalk, on=["hospital_id", "patient_id"], how="left")

    out = (
        df.withColumn("claim_transition_sk", surrogate_key(F.col("transition_key")))
        .withColumn("patient_sk", surrogate_key(F.col("mpi_id")))
        .withColumn("hospital_sk", surrogate_key(F.col("hospital_id")))
        .withColumn("insurer_sk", surrogate_key(F.col("insurer_id")))
        .withColumn("status_date_sk", F.date_format("status_date", "yyyyMMdd").cast("int"))
        .withColumn(
            "days_since_submission", F.datediff(F.col("status_date"), F.col("submitted_date"))
        )
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
        .select(
            "claim_transition_sk",
            "transition_key",
            "claim_id",
            "patient_sk",
            "mpi_id",
            "hospital_sk",
            "hospital_id",
            "insurer_sk",
            "insurer_id",
            "status_date_sk",
            "status_date",
            "transition_seq",
            "status_code",
            "prev_status",
            "next_status",
            "days_in_prev_status",
            "days_since_submission",
            "is_terminal",
            "transition_class",
            "is_legal_transition",
            "claim_amount",
            "approved_amount",
            "submitted_date",
            "rejection_reason",
            "batch_id",
            "dw_updated_at",
        )
    )
    return _write(out, spark, cfg, "fact_claim_lifecycle", partition_by=["status_date"])


def build_fact_billing_reconciliation(spark: SparkSession, cfg: Config, ctx: RunContext) -> int:
    """One row per bill-to-claim linkage, with the deduction decomposed.

    This is the table that answers "why was this bill reimbursed at that amount",
    which today takes the finance team 7-10 days a month to answer by hand.
    """
    adjudication = read(spark, cfg.table_path("silver", "claim_adjudication"))
    # Project the link table down to what the fact needs. Both Silver tables carry
    # batch_id and dw_updated_at, and joining them wholesale makes those references
    # ambiguous — better to name the columns than to rely on join-order luck.
    link = read(spark, cfg.table_path("silver", "bill_claim_link")).select(
        "claim_id", "bill_id", "match_method", "match_confidence", "methods_agreeing"
    )
    bills = (
        read(spark, cfg.table_path("bronze", "billing_transactions"))
        .dropDuplicates(["bill_id"])
        .select(
            "bill_id",
            F.col("gross_amount").cast("double").alias("bill_gross_amount"),
            F.col("discount_amount").cast("double").alias("bill_discount"),
            F.col("tax_amount").cast("double").alias("bill_tax"),
            F.col("net_payable").cast("double").alias("bill_net_payable"),
            F.col("payment_mode").alias("payment_mode"),
            F.to_date("bill_date").alias("bill_date"),
        )
    )

    crosswalk = read(spark, cfg.table_path("silver", "patient_crosswalk")).select(
        "hospital_id", "patient_id", "mpi_id"
    )

    df = (
        adjudication.join(link, on="claim_id", how="left")
        .join(bills, on="bill_id", how="left")
        .join(crosswalk, on=["hospital_id", "patient_id"], how="left")
    )

    out = (
        df.withColumn("reconciliation_sk", surrogate_key(F.col("claim_id"), F.col("bill_id")))
        .withColumn("patient_sk", surrogate_key(F.col("mpi_id")))
        .withColumn("hospital_sk", surrogate_key(F.col("hospital_id")))
        .withColumn("insurer_sk", surrogate_key(F.col("insurer_id")))
        .withColumn("bill_date_sk", F.date_format("bill_date", "yyyyMMdd").cast("int"))
        .withColumn("is_linked", F.col("bill_id").isNotNull())
        # The three recoverable buckets, separated so the largest can be targeted.
        .withColumn(
            "gap_pct",
            F.when(
                F.col("billed_amount") > 0,
                F.round(F.col("reimbursement_gap") / F.col("billed_amount"), 4),
            ),
        )
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
        .select(
            "reconciliation_sk",
            "claim_id",
            "bill_id",
            "patient_sk",
            "mpi_id",
            "hospital_sk",
            "hospital_id",
            "insurer_sk",
            "insurer_id",
            "bill_date_sk",
            "bill_date",
            "is_linked",
            "match_method",
            "match_confidence",
            "rule_id",
            "procedure_category",
            "room_type",
            "room_days",
            "billed_amount",
            "bill_gross_amount",
            "bill_net_payable",
            "excluded_amount",
            "room_rent_excess",
            "eligible_amount",
            "copay_pct",
            "copay_amount",
            "deduction_pct",
            "other_deduction",
            "net_reimbursement",
            "reimbursement_gap",
            "gap_pct",
            "insurer_approved_amount",
            "reconciliation_variance",
            "is_reconciled",
            "variance_class",
            "latest_status",
            "payment_mode",
            "batch_id",
            "dw_updated_at",
        )
    )
    return _write(out, spark, cfg, "fact_billing_reconciliation")


def build_fact_bed_occupancy(spark: SparkSession, cfg: Config, ctx: RunContext) -> int:
    """One row per ward per day."""
    daily = read(spark, cfg.table_path("silver", "bed_occupancy_daily"))

    out = (
        daily.withColumn(
            "bed_occupancy_sk", surrogate_key(F.col("ward_id"), F.col("occupancy_date"))
        )
        .withColumn("hospital_sk", surrogate_key(F.col("hospital_id")))
        .withColumn("date_sk", F.date_format("occupancy_date", "yyyyMMdd").cast("int"))
        .withColumn(
            "occupancy_band",
            F.when(F.col("occupancy_rate") >= 0.95, F.lit("Critical 95%+"))
            .when(F.col("occupancy_rate") >= 0.85, F.lit("High 85-95%"))
            .when(F.col("occupancy_rate") >= 0.60, F.lit("Normal 60-85%"))
            .otherwise(F.lit("Low <60%")),
        )
        .withColumn("is_high_occupancy", F.col("occupancy_rate") >= 0.85)
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
        .select(
            "bed_occupancy_sk",
            "date_sk",
            "occupancy_date",
            "hospital_sk",
            "hospital_id",
            "ward_id",
            "ward_type",
            "bed_count",
            "occupied_beds",
            "occupancy_rate",
            "occupancy_band",
            "is_high_occupancy",
            "is_over_capacity",
            "admissions",
            "discharges",
            "turnover_rate",
            "avg_length_of_stay",
            "open_stay_count",
            "batch_id",
            "dw_updated_at",
        )
    )
    return _write(out, spark, cfg, "fact_bed_occupancy", partition_by=["occupancy_date"])


def run(spark: SparkSession, cfg: Config, ctx: RunContext) -> dict[str, int]:
    """Build all four fact tables."""
    for name in ("dim_doctor", "dim_procedure"):
        if not table_exists(spark, cfg.table_path("gold", name)):
            raise FileNotFoundError(f"gold.{name} must be built before the facts")

    return {
        "fact_patient_visit": build_fact_patient_visit(spark, cfg, ctx),
        "fact_claim_lifecycle": build_fact_claim_lifecycle(spark, cfg, ctx),
        "fact_billing_reconciliation": build_fact_billing_reconciliation(spark, cfg, ctx),
        "fact_bed_occupancy": build_fact_bed_occupancy(spark, cfg, ctx),
    }
