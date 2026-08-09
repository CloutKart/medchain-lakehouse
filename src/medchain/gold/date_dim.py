"""Calendar dimension on the Indian financial year.

Two things make this more than a generated date range:

**The financial year runs April to March.** ``FY2024-25`` starts 1 April 2024, and
Q1 is April-June, not January-March. Every financial report the hospital produces is
on this basis, so a calendar-year date dimension would make each of them subtly and
invisibly wrong.

**Holidays are seed data, not computed.** Republic Day is fixed, but Diwali, Holi
and Eid move every year on lunar calendars that no date library derives correctly.
They come from ``conf/seed/india_holidays.csv``, which is maintained. This matters
because admissions genuinely collapse around major festivals — elective surgery is
deferred, discharge is accelerated — so a wrong holiday date puts the seasonality
analysis a day out.
"""

from __future__ import annotations

import csv
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medchain.config import Config
from medchain.utils.logging import get_logger
from medchain.utils.tables import register_table

log = get_logger("medchain.gold.date")

# Indian FY starts in April.
FY_START_MONTH = 4


def load_holidays(spark: SparkSession, cfg: Config) -> DataFrame:
    """Read the maintained holiday list, tolerating its comment header."""
    rows = []
    with (cfg.seed_dir / "india_holidays.csv").open() as fh:
        for row in csv.DictReader(line for line in fh if not line.startswith("#")):
            rows.append(
                {
                    "holiday_date": date.fromisoformat(row["holiday_date"]),
                    "holiday_name": row["holiday_name"],
                    "holiday_type": row["holiday_type"],
                }
            )
    df = spark.createDataFrame(rows)
    # A date can carry two holidays (Ambedkar Jayanti and Mahavir Jayanti both fall
    # on 14 April 2022). Collapse to one row per date so the join cannot fan out the
    # date dimension — a duplicated dim_date row would double every fact it joins.
    return df.groupBy("holiday_date").agg(
        F.concat_ws(" / ", F.sort_array(F.collect_set("holiday_name"))).alias("holiday_name"),
        F.max("holiday_type").alias("holiday_type"),
    )


def build(spark: SparkSession, cfg: Config) -> DataFrame:
    """Generate the calendar dimension across the configured range."""
    start = date.fromisoformat(cfg.get("window", "date_dim_start", default="2022-01-01"))
    end = date.fromisoformat(cfg.get("window", "date_dim_end", default="2027-03-31"))

    df = spark.sql(
        "SELECT explode(sequence("
        f"to_date('{start}'), to_date('{end}'), interval 1 day)) AS date_key"
    )

    df = (
        df.withColumn("year", F.year("date_key"))
        .withColumn("month", F.month("date_key"))
        .withColumn("day", F.dayofmonth("date_key"))
        .withColumn("day_of_week", F.dayofweek("date_key"))
        .withColumn("day_name", F.date_format("date_key", "EEEE"))
        .withColumn("month_name", F.date_format("date_key", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("date_key"))
        .withColumn("quarter", F.quarter("date_key"))
        .withColumn("day_of_year", F.dayofyear("date_key"))
        .withColumn("is_weekend", F.dayofweek("date_key").isin([1, 7]))
        .withColumn("month_start", F.trunc("date_key", "month"))
        .withColumn("month_end", F.last_day("date_key"))
    )

    # Financial year: April-March. A date in Jan-Mar belongs to the FY that began
    # the previous April.
    fy_year = F.when(F.col("month") >= FY_START_MONTH, F.col("year")).otherwise(F.col("year") - 1)
    df = df.withColumn("financial_year_start", fy_year)
    df = df.withColumn(
        "fy_label",
        F.concat(
            F.lit("FY"),
            F.col("financial_year_start").cast("string"),
            F.lit("-"),
            F.lpad(((F.col("financial_year_start") + 1) % 100).cast("string"), 2, "0"),
        ),
    )
    # FY quarter: Q1 = Apr-Jun, Q2 = Jul-Sep, Q3 = Oct-Dec, Q4 = Jan-Mar.
    fy_month_index = (F.col("month") - FY_START_MONTH + 12) % 12
    df = df.withColumn("fy_month_number", fy_month_index + 1)
    df = df.withColumn("fy_quarter", (fy_month_index / 3).cast("int") + 1)
    df = df.withColumn("fy_quarter_label", F.concat(F.lit("Q"), F.col("fy_quarter").cast("string")))

    holidays = load_holidays(spark, cfg)
    df = df.join(holidays, df.date_key == holidays.holiday_date, "left").drop("holiday_date")
    df = df.withColumn("is_public_holiday", F.col("holiday_name").isNotNull())

    # A working day is a weekday that is not a gazetted holiday. Used as the
    # denominator for doctor utilisation, where dividing by calendar days would
    # understate how busy clinics actually are.
    df = df.withColumn("is_working_day", ~F.col("is_weekend") & ~F.col("is_public_holiday"))
    # Monsoon flag: the respiratory and vector-borne admission surge is real and
    # large enough that seasonality analysis needs it as a dimension attribute.
    df = df.withColumn("is_monsoon", F.col("month").isin([6, 7, 8, 9]))

    df = df.withColumn("date_sk", F.date_format("date_key", "yyyyMMdd").cast("int"))
    return df.select(
        "date_sk",
        "date_key",
        "year",
        "quarter",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "week_of_year",
        "day_of_year",
        "month_start",
        "month_end",
        "is_weekend",
        "financial_year_start",
        "fy_label",
        "fy_quarter",
        "fy_quarter_label",
        "fy_month_number",
        "holiday_name",
        "holiday_type",
        "is_public_holiday",
        "is_working_day",
        "is_monsoon",
    )


def run(spark: SparkSession, cfg: Config) -> int:
    target = cfg.table_path("gold", "dim_date")
    df = build(spark, cfg)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)
    register_table(spark, cfg, "gold", "dim_date")
    n = df.count()
    log.info("  dim_date: %d days", n)
    return n
