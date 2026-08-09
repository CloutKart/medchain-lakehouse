"""Bed occupancy gap-fill — turn ward movement events into one row per ward per day.

The source system logs *events*: a check-in, sometimes a transfer, usually a
check-out. It never records "ward 3 held 41 patients on Tuesday". Occupancy,
turnover and average length of stay all need that daily state, so it has to be
reconstructed.

Three cases make this harder than pairing check-in with check-out:

* **Mid-stay transfers.** A patient admitted to ICU and moved to a general ward on
  day 3 occupies two different wards during one visit. Pairing only the first and
  last event would credit every one of those days to ICU and overstate critical-care
  occupancy — the exact number a hospital uses to justify capital spend.
* **Unclosed stays.** Around 2% of stays have no check-out event, because the
  patient is still admitted or the event was never logged. Dropping them undercounts
  current occupancy; carrying them forward indefinitely fills the calendar with
  phantom patients. They are capped at the batch date and flagged.
* **Same-day admission and discharge.** Zero nights, but one occupied bed-day. An
  exclusive date range would drop these entirely.

The expansion itself uses ``sequence`` + ``explode`` to generate the date span per
ward segment, which keeps the whole operation a single Spark stage rather than a
per-row loop.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from medchain.config import Config
from medchain.utils.audit import RunContext
from medchain.utils.logging import get_logger
from medchain.utils.tables import read, register_table, table_exists

log = get_logger("medchain.silver.beds")

ENTRY_EVENTS = ["CHECK_IN", "TRANSFER_IN"]
EXIT_EVENTS = ["CHECK_OUT", "TRANSFER_OUT"]

# An open stay is capped at this many days past its start when no exit event exists.
# Without a cap a single unclosed stay from 2022 would occupy a bed on every day
# since, which quietly inflates occupancy for three years.
MAX_OPEN_STAY_DAYS = 45


def build_segments(events: DataFrame, as_of: date) -> DataFrame:
    """Pair entry and exit events into (visit, ward) occupancy segments.

    Events are ordered per visit and each entry is closed by the next event of any
    kind for that visit — a transfer-out closes the segment just as a check-out
    does. This is what produces two segments for a transferred patient instead of
    one long one in the wrong ward.
    """
    ordered = Window.partitionBy("visit_id").orderBy("event_ts", "event_id")

    df = (
        events.withColumn("event_ts", F.to_timestamp(F.col("event_ts")))
        .filter(F.col("event_ts").isNotNull() & F.col("visit_id").isNotNull())
        .withColumn("next_event_ts", F.lead("event_ts").over(ordered))
        .withColumn("next_event_type", F.lead("event_type").over(ordered))
    )

    segments = df.filter(F.col("event_type").isin(ENTRY_EVENTS))
    segments = segments.withColumn("start_date", F.to_date(F.col("event_ts")))
    segments = segments.withColumn(
        "raw_end_date",
        F.when(F.col("next_event_ts").isNotNull(), F.to_date(F.col("next_event_ts"))),
    )

    # An entry with no following event is an unclosed stay.
    segments = segments.withColumn("is_open_stay", F.col("raw_end_date").isNull())
    segments = segments.withColumn(
        "end_date",
        F.when(
            F.col("is_open_stay"),
            F.least(
                F.date_add(F.col("start_date"), MAX_OPEN_STAY_DAYS),
                F.to_date(F.lit(as_of.isoformat())),
            ),
        ).otherwise(F.col("raw_end_date")),
    )

    # A patient transferred out on day 3 occupied the source ward on day 3 as well;
    # the receiving ward's segment starts the same day. Counting distinct patients
    # per ward-day (rather than summing segments) keeps the network total honest
    # while still crediting each ward for the day it provided care.
    segments = segments.withColumn("end_date", F.greatest(F.col("end_date"), F.col("start_date")))

    return segments.select(
        "visit_id",
        "patient_id",
        "hospital_id",
        "ward_id",
        "ward_type",
        "bed_number",
        "start_date",
        "end_date",
        "is_open_stay",
        F.col("event_type").alias("entry_event"),
        F.col("next_event_type").alias("exit_event"),
    )


def expand_to_days(segments: DataFrame) -> DataFrame:
    """Explode each segment into one row per occupied ward-day.

    ``sequence`` is inclusive at both ends, which is exactly right here: a same-day
    admission and discharge yields a single bed-day rather than none.
    """
    return segments.withColumn(
        "occupancy_date",
        F.explode(F.sequence(F.col("start_date"), F.col("end_date"), F.expr("interval 1 day"))),
    ).select(
        "occupancy_date",
        "visit_id",
        "patient_id",
        "hospital_id",
        "ward_id",
        "ward_type",
        "is_open_stay",
    )


def aggregate_ward_days(bed_days: DataFrame, wards: DataFrame, segments: DataFrame) -> DataFrame:
    """Roll occupied bed-days up to the ward-day grain with occupancy and turnover."""
    occupancy = bed_days.groupBy("occupancy_date", "hospital_id", "ward_id", "ward_type").agg(
        # Distinct patients, not row count: a patient who moves out of and back into
        # the same ward on one day must not be counted as two occupied beds.
        F.countDistinct("patient_id").alias("occupied_beds"),
        F.sum(F.col("is_open_stay").cast("int")).alias("open_stay_count"),
    )

    admissions = segments.groupBy(
        F.col("start_date").alias("occupancy_date"), "hospital_id", "ward_id"
    ).agg(F.countDistinct("patient_id").alias("admissions"))

    discharges = (
        segments.filter(~F.col("is_open_stay"))
        .groupBy(F.col("end_date").alias("occupancy_date"), "hospital_id", "ward_id")
        .agg(F.countDistinct("patient_id").alias("discharges"))
    )

    stay_length = segments.withColumn(
        "stay_days", F.datediff(F.col("end_date"), F.col("start_date")) + F.lit(1)
    )
    alos = stay_length.groupBy(
        F.col("end_date").alias("occupancy_date"), "hospital_id", "ward_id"
    ).agg(F.avg("stay_days").alias("avg_length_of_stay"))

    result = (
        occupancy.join(admissions, ["occupancy_date", "hospital_id", "ward_id"], "left")
        .join(discharges, ["occupancy_date", "hospital_id", "ward_id"], "left")
        .join(alos, ["occupancy_date", "hospital_id", "ward_id"], "left")
        .join(wards.select("ward_id", "bed_count"), on="ward_id", how="left")
    )

    result = (
        result.withColumn("admissions", F.coalesce(F.col("admissions"), F.lit(0)))
        .withColumn("discharges", F.coalesce(F.col("discharges"), F.lit(0)))
        .withColumn(
            "occupancy_rate",
            F.when(
                F.col("bed_count") > 0,
                F.round(F.col("occupied_beds") / F.col("bed_count"), 4),
            ),
        )
        .withColumn(
            "turnover_rate",
            F.when(F.col("bed_count") > 0, F.round(F.col("discharges") / F.col("bed_count"), 4)),
        )
        .withColumn("avg_length_of_stay", F.round(F.col("avg_length_of_stay"), 2))
        # Occupancy above 1.0 is real in Indian hospitals (corridor beds during a
        # surge) and is preserved rather than clipped, but flagged so it is visible.
        .withColumn("is_over_capacity", F.col("occupancy_rate") > F.lit(1.0))
    )
    return result


def load_wards(spark: SparkSession, cfg: Config) -> DataFrame:
    """Ward reference with bed counts.

    Derived from the truth file when available (it is the hospital's own capacity
    register); otherwise inferred from the maximum distinct beds ever observed in
    the occupancy log, which is a lower bound but keeps the pipeline runnable.
    """
    truth_path = f"{cfg.path('truth')}/ward_truth.parquet"
    try:
        return spark.read.parquet(truth_path).select(
            "ward_id", "hospital_id", "ward_type", "bed_count"
        )
    except Exception:  # noqa: BLE001 - truth file is optional
        log.warning("Ward capacity file not found; inferring capacity from observed beds")
        events = read(spark, cfg.table_path("bronze", "bed_occupancy_log"))
        return events.groupBy("ward_id", "hospital_id", "ward_type").agg(
            F.countDistinct("bed_number").alias("bed_count")
        )


def run(spark: SparkSession, cfg: Config, ctx: RunContext) -> dict[str, int]:
    """Build ``silver.bed_occupancy_daily`` from ward movement events."""
    bronze_path = cfg.table_path("bronze", "bed_occupancy_log")
    if not table_exists(spark, bronze_path):
        raise FileNotFoundError(f"Bronze bed log not found at {bronze_path}")

    events = read(spark, bronze_path)
    n_events = events.count()

    segments = build_segments(events, ctx.logical_date).cache()
    n_segments = segments.count()
    n_open = segments.filter(F.col("is_open_stay")).count()

    bed_days = expand_to_days(segments)
    wards = load_wards(spark, cfg)
    ward_days = aggregate_ward_days(bed_days, wards, segments)

    ward_days = ward_days.withColumn("batch_id", F.lit(ctx.batch_id)).withColumn(
        "dw_updated_at", F.current_timestamp()
    )

    target = cfg.table_path("silver", "bed_occupancy_daily")
    ward_days.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(
        "occupancy_date"
    ).save(target)
    register_table(spark, cfg, "silver", "bed_occupancy_daily")

    # Segment-level detail is kept too: the ward-day aggregate cannot answer
    # "which patients were in ICU on 12 March", and incident reviews always ask.
    segment_target = cfg.table_path("silver", "bed_stay_segments")
    segments.withColumn("batch_id", F.lit(ctx.batch_id)).write.format("delta").mode(
        "overwrite"
    ).option("overwriteSchema", "true").save(segment_target)
    register_table(spark, cfg, "silver", "bed_stay_segments")

    stored = read(spark, target)
    n_ward_days = stored.count()
    over_capacity = stored.filter(F.col("is_over_capacity")).count()
    total_bed_days = bed_days.count()

    log.info(
        "  %d events -> %d ward segments (%d unclosed, %.1f%%)",
        n_events,
        n_segments,
        n_open,
        100 * n_open / n_segments if n_segments else 0,
    )
    log.info("  %d occupied bed-days -> %d ward-day rows", total_bed_days, n_ward_days)
    log.info(
        "  ward-days over capacity: %d (%.2f%%)",
        over_capacity,
        100 * over_capacity / n_ward_days if n_ward_days else 0,
    )

    segments.unpersist()
    return {
        "events": n_events,
        "segments": n_segments,
        "open_stays": n_open,
        "bed_days": total_bed_days,
        "ward_days": n_ward_days,
        "over_capacity_ward_days": over_capacity,
    }
