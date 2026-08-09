"""Doctor dimension — rebuild assignment history from stateless weekly rosters.

The HR system exports a full roster every Sunday showing who is in which department
*this week*. It keeps no history and has no end-date column. When Dr Sharma moves
from Cardiology to Emergency, the next export simply lists her under Emergency, and
every trace that she was ever in Cardiology disappears from the source.

That matters because consultations are attributed to departments. Without the
history, a report on 2023 cardiology throughput would count Dr Sharma's 2023
consultations under Emergency — the department she happens to sit in now. The spec
calls this out directly, and Business Question 4 quantifies how large the error is.

Reconstruction works by treating the sequence of weekly snapshots as a change
stream: collapse consecutive identical rows, and the boundaries that remain are the
transfers. :mod:`medchain.silver.scd2` does the collapsing and the MERGE; this
module supplies the source and decides which attributes are worth versioning.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from medchain.config import Config
from medchain.silver.scd2 import apply_scd2, prepare_versions
from medchain.utils.audit import RunContext
from medchain.utils.logging import get_logger
from medchain.utils.tables import read, register_table, table_exists

log = get_logger("medchain.silver.doctors")

# Attributes that define a new version when they change. Department and hospital are
# the ones that drive attribution. Designation is included because a promotion is a
# genuine historical fact worth keeping.
#
# Deliberately excluded: doctor_name and qualification. A corrected spelling or a
# newly added degree is not a new assignment period, and versioning on them would
# fragment the history with rows that mean nothing for reporting.
TRACKED_COLUMNS = ["department", "hospital_id", "specialty", "designation"]


def prepare_source(spark: SparkSession, cfg: Config) -> DataFrame:
    """Read the weekly HR rosters and derive each doctor's change points.

    ``effective_date`` on the export is the start of the *current* assignment as HR
    understands it. It is preferred over the export date because it is closer to
    when the change actually took effect; the export date is only when we happened
    to observe it. Where it is missing, the export date is the best available proxy.
    """
    bronze_path = cfg.table_path("bronze", "doctor_assignments")
    if not table_exists(spark, bronze_path):
        raise FileNotFoundError(f"Bronze doctor assignments not found at {bronze_path}")

    df = read(spark, bronze_path)
    df = (
        df.withColumn("export_date", F.to_date(F.col("export_date")))
        .withColumn("effective_date_parsed", F.to_date(F.col("effective_date")))
        .withColumn("joining_date", F.to_date(F.col("joining_date")))
        .filter(F.col("doctor_id").isNotNull())
    )
    df = df.withColumn(
        "effective_from_src",
        F.coalesce(F.col("effective_date_parsed"), F.col("export_date")),
    ).filter(F.col("effective_from_src").isNotNull())

    # `_export_order` breaks ties inside prepare_versions when two exports report the
    # same effective date: the later export is the more current statement of fact.
    df = df.withColumn("_export_order", F.col("export_date").cast("long"))

    # The same (doctor, effective_from) appears in every weekly export until the next
    # change. Keeping the latest observation of each is what turns 156 weekly rows
    # per doctor into a handful of genuine assignment periods.
    dedupe = Window.partitionBy("doctor_id", "effective_from_src").orderBy(
        F.col("export_date").desc()
    )
    return df.withColumn("_rn", F.row_number().over(dedupe)).filter(F.col("_rn") == 1).drop("_rn")


def run(spark: SparkSession, cfg: Config, ctx: RunContext) -> dict[str, int]:
    """Build ``silver.dim_doctor_scd2``."""
    source = prepare_source(spark, cfg)
    n_source = source.count()
    n_doctors = source.select("doctor_id").distinct().count()

    versions = prepare_versions(
        source,
        business_key=["doctor_id"],
        tracked_cols=TRACKED_COLUMNS,
        effective_date_col="effective_from_src",
    )

    target = cfg.table_path("silver", "dim_doctor_scd2")
    apply_scd2(
        spark,
        versions,
        target,
        business_key=["doctor_id"],
        tracked_cols=TRACKED_COLUMNS,
        sk_name="doctor_sk",
        extra_cols=["doctor_name", "qualification", "joining_date"],
        batch_id=ctx.batch_id,
    )
    register_table(spark, cfg, "silver", "dim_doctor_scd2")

    stored = read(spark, target)
    n_versions = stored.count()
    n_current = stored.filter(F.col("is_current")).count()
    multi_version = (
        stored.groupBy("doctor_id").agg(F.count(F.lit(1)).alias("n")).filter(F.col("n") > 1).count()
    )

    log.info("  %d roster rows for %d doctors -> %d versions", n_source, n_doctors, n_versions)
    log.info(
        "  doctors with >1 assignment period: %d (%.1f%%)",
        multi_version,
        100 * multi_version / n_doctors if n_doctors else 0,
    )
    log.info("  current (open) versions: %d", n_current)

    # One open version per doctor is an invariant of a correct SCD2 build. More than
    # one means the close-out step failed and every point-in-time join downstream
    # would silently fan out.
    if n_current != n_doctors:
        log.error(
            "  SCD2 invariant violated: %d open versions for %d doctors", n_current, n_doctors
        )

    return {
        "source_rows": n_source,
        "doctors": n_doctors,
        "versions": n_versions,
        "current_versions": n_current,
        "doctors_with_history": multi_version,
    }
