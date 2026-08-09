"""Gold dimensions: patient, doctor, hospital, insurer, procedure.

Six dimensions in total; ``dim_date`` lives in :mod:`medchain.gold.date_dim`.

Two are SCD Type 2 (``dim_patient``, ``dim_doctor``) and carry
``effective_from`` / ``effective_to`` / ``is_current``. Facts join them on the date
range, never on ``is_current`` — see :func:`medchain.silver.scd2.point_in_time_join`
for why that distinction is the whole point.

The rest are Type 1: a hospital's bed capacity or an insurer's TPA name is a current
fact, and nobody asks what it was in 2023.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medchain.config import Config
from medchain.utils.audit import RunContext
from medchain.utils.keys import surrogate_key
from medchain.utils.logging import get_logger
from medchain.utils.tables import read, register_table, table_exists

log = get_logger("medchain.gold.dims")

HIGH_DATE = "9999-12-31"


def _write(df: DataFrame, spark: SparkSession, cfg: Config, name: str) -> int:
    target = cfg.table_path("gold", name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)
    register_table(spark, cfg, "gold", name)
    n = read(spark, target).count()
    log.info("  %-20s %8d rows", name, n)
    return n


def build_dim_patient(spark: SparkSession, cfg: Config, ctx: RunContext) -> int:
    """SCD Type 2 patient dimension, keyed on the MPI identity.

    The grain is the resolved *person*, not the registration. That is the whole
    value the MPI delivers: one row per human rather than 1.22 rows per human spread
    across hospitals under different identifiers.

    Versioning is on demographic attributes that genuinely change — address, phone,
    city. A patient who moves from Pune to Bangalore gets a new version, and a visit
    from 2023 still resolves to the Pune address it was recorded against.
    """
    master_path = cfg.table_path("silver", "patient_master")
    crosswalk_path = cfg.table_path("silver", "patient_crosswalk")
    if not table_exists(spark, master_path):
        raise FileNotFoundError(f"silver.patient_master missing at {master_path}")

    master = read(spark, master_path)
    crosswalk = read(spark, crosswalk_path)

    # How many source identities each person was assembled from — the evidence that
    # a link happened, exposed to analysts rather than buried in Silver.
    linkage = crosswalk.groupBy("mpi_id").agg(
        F.countDistinct("patient_id").alias("source_patient_id_count"),
        F.countDistinct("hospital_id").alias("registered_hospital_count"),
        F.sort_array(F.collect_set("hospital_id")).alias("registered_hospitals"),
    )

    df = (
        master.join(linkage, on="mpi_id", how="left")
        .withColumn("patient_sk", surrogate_key(F.col("mpi_id")))
        .withColumn("full_name", F.col("full_name_norm"))
        .withColumn("date_of_birth", F.col("dob_parsed"))
        .withColumn(
            "age_years",
            F.floor(F.datediff(F.lit(ctx.logical_date.isoformat()), F.col("dob_parsed")) / 365.25),
        )
        .withColumn(
            "age_band",
            F.when(F.col("age_years") < 1, F.lit("Infant"))
            .when(F.col("age_years") < 13, F.lit("Child"))
            .when(F.col("age_years") < 20, F.lit("Adolescent"))
            .when(F.col("age_years") < 40, F.lit("Adult 20-39"))
            .when(F.col("age_years") < 60, F.lit("Adult 40-59"))
            .when(F.col("age_years") < 75, F.lit("Senior 60-74"))
            .otherwise(F.lit("Elderly 75+")),
        )
        # A person linked across more than one hospital is precisely the cohort that
        # single-hospital reporting cannot see.
        .withColumn("is_multi_hospital_patient", F.col("registered_hospital_count") > 1)
        # This build is a full snapshot, so every row is the current version. The
        # SCD2 columns are present and correct rather than decorative: once a second
        # batch lands, silver.scd2 maintains them.
        .withColumn(
            "effective_from", F.coalesce(F.col("dob_parsed"), F.lit("1900-01-01").cast("date"))
        )
        .withColumn("effective_to", F.to_date(F.lit(HIGH_DATE)))
        .withColumn("is_current", F.lit(True))
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
    )

    out = df.select(
        "patient_sk",
        "mpi_id",
        "full_name",
        F.col("first_name_norm").alias("first_name"),
        F.col("last_name_norm").alias("last_name"),
        F.col("gender_norm").alias("gender"),
        "date_of_birth",
        "age_years",
        "age_band",
        F.col("phone_norm").alias("phone"),
        F.col("city_norm").alias("city"),
        "state",
        "pincode",
        "address_line",
        "blood_group",
        "email",
        "source_patient_id_count",
        "registered_hospital_count",
        "registered_hospitals",
        "is_multi_hospital_patient",
        "effective_from",
        "effective_to",
        "is_current",
        "batch_id",
        "dw_updated_at",
    )
    return _write(out, spark, cfg, "dim_patient")


def build_dim_doctor(spark: SparkSession, cfg: Config, ctx: RunContext) -> int:
    """SCD Type 2 doctor dimension carrying assignment history.

    Passed through from ``silver.dim_doctor_scd2`` with presentation attributes
    added. The effective-dated rows are what let a 2023 consultation be credited to
    the department the doctor worked in during 2023.
    """
    source_path = cfg.table_path("silver", "dim_doctor_scd2")
    if not table_exists(spark, source_path):
        raise FileNotFoundError(f"silver.dim_doctor_scd2 missing at {source_path}")

    df = read(spark, source_path)
    df = (
        df.withColumn(
            "assignment_days",
            F.datediff(
                F.least(F.col("effective_to"), F.lit(ctx.logical_date.isoformat()).cast("date")),
                F.col("effective_from"),
            ),
        )
        .withColumn(
            "is_senior", F.col("designation").isin(["Senior Consultant", "Head of Department"])
        )
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
    )
    out = df.select(
        "doctor_sk",
        "doctor_id",
        "doctor_name",
        "department",
        "hospital_id",
        "specialty",
        "designation",
        "qualification",
        "joining_date",
        "is_senior",
        "version",
        "assignment_days",
        "effective_from",
        "effective_to",
        "is_current",
        "hash_diff",
        "batch_id",
        "dw_updated_at",
    )
    return _write(out, spark, cfg, "dim_doctor")


def build_dim_hospital(spark: SparkSession, cfg: Config, ctx: RunContext) -> int:
    """Type 1 hospital dimension with capacity and ward mix."""
    hospitals = spark.read.parquet(f"{cfg.path('truth')}/hospital_truth.parquet")
    wards = spark.read.parquet(f"{cfg.path('truth')}/ward_truth.parquet")

    ward_summary = wards.groupBy("hospital_id").agg(
        F.count(F.lit(1)).alias("ward_count"),
        F.sum("bed_count").alias("total_beds"),
        F.sum(
            F.when(F.col("ward_type").isin(["ICU", "HDU"]), F.col("bed_count")).otherwise(0)
        ).alias("critical_care_beds"),
    )

    df = (
        hospitals.join(ward_summary, on="hospital_id", how="left")
        .withColumn("hospital_sk", surrogate_key(F.col("hospital_id")))
        .withColumn(
            "critical_care_bed_pct",
            F.round(F.col("critical_care_beds") / F.col("total_beds"), 4),
        )
        .withColumn(
            "size_band",
            F.when(F.col("bed_capacity") >= 450, F.lit("Large"))
            .when(F.col("bed_capacity") >= 280, F.lit("Medium"))
            .otherwise(F.lit("Small")),
        )
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
    )
    out = df.select(
        "hospital_sk",
        "hospital_id",
        "hospital_name",
        "city",
        "state",
        "tier",
        "bed_capacity",
        "total_beds",
        "ward_count",
        "critical_care_beds",
        "critical_care_bed_pct",
        "size_band",
        "opened_year",
        "batch_id",
        "dw_updated_at",
    )
    return _write(out, spark, cfg, "dim_hospital")


def build_dim_insurer(spark: SparkSession, cfg: Config, ctx: RunContext) -> int:
    """Type 1 insurer dimension, enriched with the TPA rule profile in force."""
    insurers = spark.read.parquet(f"{cfg.path('truth')}/insurer_truth.parquet")

    rules_path = cfg.table_path("silver", "tpa_rules")
    if table_exists(spark, rules_path):
        profile = (
            read(spark, rules_path)
            .groupBy("insurer_id")
            .agg(
                F.count(F.lit(1)).alias("rule_count"),
                F.round(F.avg("copay_pct"), 4).alias("avg_copay_pct"),
                F.max("room_rent_cap_per_day").alias("max_room_rent_cap"),
            )
        )
        insurers = insurers.join(profile, on="insurer_id", how="left")

    df = (
        insurers.withColumn("insurer_sk", surrogate_key(F.col("insurer_id")))
        .withColumn("empanelment_date", F.to_date(F.col("empanelment_date")))
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
    )
    return _write(df, spark, cfg, "dim_insurer")


def build_dim_procedure(spark: SparkSession, cfg: Config, ctx: RunContext) -> int:
    """Procedure dimension with the resolved ICD-10 code and its provenance.

    ``icd10_source`` travels all the way into Gold on purpose. An analyst filtering
    on a diagnosis code needs to know whether it was reported by the hospital or
    inferred from a specialty default, and burying that in Silver would hide it from
    exactly the people who need it.
    """
    source_path = cfg.table_path("silver", "procedure_catalog")
    if not table_exists(spark, source_path):
        raise FileNotFoundError(f"silver.procedure_catalog missing at {source_path}")

    df = read(spark, source_path)
    df = (
        df.withColumn("procedure_sk", surrogate_key(F.col("procedure_code")))
        .withColumn("icd10_chapter", F.substring(F.col("icd10_code"), 1, 1))
        .withColumn("is_icd10_inferred", F.col("icd10_source") != F.lit("SOURCE"))
        .withColumn(
            "icd10_reliability",
            F.when(F.col("icd10_confidence") >= 0.95, F.lit("High"))
            .when(F.col("icd10_confidence") >= 0.70, F.lit("Medium"))
            .when(F.col("icd10_confidence") > 0, F.lit("Low"))
            .otherwise(F.lit("None")),
        )
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
    )
    out = df.select(
        "procedure_sk",
        "procedure_code",
        "procedure_name",
        "specialty",
        "procedure_category",
        "base_cost",
        "icd10_code",
        "icd10_chapter",
        "icd10_source",
        "icd10_confidence",
        "is_icd10_inferred",
        "icd10_reliability",
        "batch_id",
        "dw_updated_at",
    )
    return _write(out, spark, cfg, "dim_procedure")


def run(spark: SparkSession, cfg: Config, ctx: RunContext) -> dict[str, int]:
    """Build all five non-date dimensions."""
    return {
        "dim_patient": build_dim_patient(spark, cfg, ctx),
        "dim_doctor": build_dim_doctor(spark, cfg, ctx),
        "dim_hospital": build_dim_hospital(spark, cfg, ctx),
        "dim_insurer": build_dim_insurer(spark, cfg, ctx),
        "dim_procedure": build_dim_procedure(spark, cfg, ctx),
    }
