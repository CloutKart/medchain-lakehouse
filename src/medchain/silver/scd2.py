"""Generic Slowly Changing Dimension Type 2 engine.

One implementation, used by both ``dim_patient`` and ``dim_doctor``. Writing SCD2
twice is how the two dimensions drift apart in subtle ways — a different tie-break
on same-day changes, a different treatment of nulls — and then a report that joins
across both silently disagrees with itself.

The pattern maintained here is the standard one::

    <key>_sk  business_key  ...tracked attrs...  effective_from  effective_to  is_current

with ``effective_to`` set to 9999-12-31 on the open version. Two properties matter
more than anything else:

**Idempotency.** Re-running the same source batch must change nothing. Change
detection is by ``hash_diff`` over the tracked columns, so an unchanged row produces
an identical hash and matches no MERGE condition.

**Point-in-time correctness.** Facts join these dimensions on
``fact_date BETWEEN effective_from AND effective_to``, never on ``is_current``. That
is what attributes a 2023 consultation to the department the doctor was in during
2023 rather than the one they sit in today.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from medchain.utils.keys import hash_diff, surrogate_key
from medchain.utils.logging import get_logger
from medchain.utils.tables import read, table_exists

log = get_logger("medchain.silver.scd2")

# The open-ended high date. A literal beats NULL here: `BETWEEN` and range joins
# both work without special-casing, and a null end date is the single most common
# cause of dropped rows in point-in-time joins.
HIGH_DATE = "9999-12-31"


def prepare_versions(
    source: DataFrame,
    business_key: Sequence[str],
    tracked_cols: Sequence[str],
    effective_date_col: str,
) -> DataFrame:
    """Collapse a source snapshot history into distinct change points.

    Weekly HR exports repeat the same assignment every week. Feeding those straight
    into a MERGE would produce 156 identical versions per doctor. Here consecutive
    duplicates are collapsed by comparing each row's ``hash_diff`` with the previous
    row's for the same business key, keeping only rows where something actually
    changed.
    """
    keys = [F.col(c) for c in business_key]
    tracked = [F.col(c) for c in tracked_cols]

    df = source.withColumn("hash_diff", hash_diff(*tracked))
    df = df.withColumn("effective_from", F.to_date(F.col(effective_date_col)))
    df = df.filter(F.col("effective_from").isNotNull())

    # Several exports can report the same effective_from for one key (the weekly
    # snapshot repeats). Keep one row per (key, effective_from) — the last observed
    # wins, since a later export reflects a correction to an earlier one.
    dedupe = Window.partitionBy(*keys, "effective_from").orderBy(
        F.col("_export_order").desc_nulls_last()
    )
    if "_export_order" not in df.columns:
        df = df.withColumn("_export_order", F.lit(0))
        dedupe = Window.partitionBy(*keys, "effective_from").orderBy(F.lit(0))
    df = df.withColumn("_rn", F.row_number().over(dedupe)).filter(F.col("_rn") == 1).drop("_rn")

    # Drop rows whose tracked attributes match the previous version for the key.
    ordered = Window.partitionBy(*keys).orderBy("effective_from")
    df = df.withColumn("_prev_hash", F.lag("hash_diff").over(ordered))
    df = df.filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("hash_diff")))

    # Close each version at the start of the next one.
    df = df.withColumn(
        "effective_to",
        F.coalesce(
            F.date_sub(F.lead("effective_from").over(ordered), 1),
            F.to_date(F.lit(HIGH_DATE)),
        ),
    )
    df = df.withColumn("is_current", F.col("effective_to") == F.to_date(F.lit(HIGH_DATE)))
    df = df.withColumn("version", F.row_number().over(ordered))
    return df.drop("_prev_hash", "_export_order")


def apply_scd2(
    spark: SparkSession,
    source: DataFrame,
    target_path: str,
    *,
    business_key: Sequence[str],
    tracked_cols: Sequence[str],
    sk_name: str,
    effective_date_col: str = "effective_from",
    extra_cols: Sequence[str] = (),
    batch_id: str | None = None,
) -> dict[str, int]:
    """Merge a prepared version history into an SCD2 dimension.

    ``source`` must already carry ``effective_from`` / ``effective_to`` /
    ``hash_diff`` — call :func:`prepare_versions` first. The merge key is
    (business key, effective_from), which is what makes the operation idempotent:
    replaying a batch matches every existing row and updates them to identical
    values.
    """
    business_key = list(business_key)
    tracked_cols = list(tracked_cols)
    extra_cols = list(extra_cols)

    prepared = source
    if "hash_diff" not in prepared.columns:
        prepared = prepare_versions(source, business_key, tracked_cols, effective_date_col)

    prepared = prepared.withColumn(
        sk_name, surrogate_key(*[F.col(c) for c in business_key], F.col("effective_from"))
    )
    if batch_id:
        prepared = prepared.withColumn("batch_id", F.lit(batch_id))
    prepared = prepared.withColumn("record_source", F.lit("silver.scd2"))
    prepared = prepared.withColumn("dw_updated_at", F.current_timestamp())

    select_cols = [
        sk_name,
        *business_key,
        *tracked_cols,
        *extra_cols,
        "effective_from",
        "effective_to",
        "is_current",
        "hash_diff",
        "version",
        "record_source",
        "dw_updated_at",
    ]
    if batch_id:
        select_cols.append("batch_id")
    # Preserve declaration order while removing duplicates. dict preserves insertion
    # order in Python 3.7+, which makes this both correct and readable.
    select_cols = list(dict.fromkeys(select_cols))
    prepared = prepared.select(*select_cols)

    if not table_exists(spark, target_path):
        prepared.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            target_path
        )
        total = prepared.count()
        log.info("  %-24s initial load: %d versions", target_path.rsplit("/", 1)[-1], total)
        return {"inserted": total, "updated": 0, "closed": 0}

    from delta.tables import DeltaTable

    before = read(spark, target_path).count()
    merge_condition = " AND ".join(
        [f"t.{k} <=> s.{k}" for k in business_key] + ["t.effective_from = s.effective_from"]
    )

    # A single MERGE on (business key, effective_from) handles all three cases:
    # a brand new version inserts, a restated version updates in place, and a
    # version whose successor arrived gets its effective_to/is_current corrected.
    (
        DeltaTable.forPath(spark, target_path)
        .alias("t")
        .merge(prepared.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition=(
                "t.hash_diff <> s.hash_diff OR t.effective_to <> s.effective_to "
                "OR t.is_current <> s.is_current"
            ),
            set={c: f"s.{c}" for c in select_cols},
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    after = read(spark, target_path).count()
    log.info("  %-24s %d versions (%+d)", target_path.rsplit("/", 1)[-1], after, after - before)
    return {"inserted": after - before, "updated": 0, "closed": 0, "total": after}


def point_in_time_join(
    fact: DataFrame,
    dim: DataFrame,
    *,
    business_key: Sequence[str],
    fact_date_col: str,
    sk_name: str,
    dim_cols: Sequence[str] = (),
    how: str = "left",
) -> DataFrame:
    """Join a fact to an SCD2 dimension as the dimension stood on the fact's date.

    This is the join that the whole SCD2 apparatus exists to enable. Joining on
    ``is_current`` instead would attribute every historical consultation to the
    doctor's *present* department — the exact misattribution the spec calls out.
    """
    # Both sides are aliased and every output column is named explicitly. The fact
    # and the dimension always share the business key, and usually share more
    # (hospital_id, specialty), so `select(*fact.columns, ...)` after the join is
    # ambiguous. Spark raises on that here — but where it does not raise, the wrong
    # side's column silently wins, which is the worse outcome.
    fact_columns = list(fact.columns)
    f_alias = fact.alias("f")
    d_alias = dim.alias("d")

    condition = F.col(f"f.{business_key[0]}") == F.col(f"d.{business_key[0]}")
    for key in business_key[1:]:
        condition = condition & (F.col(f"f.{key}") == F.col(f"d.{key}"))
    condition = condition & (F.col(f"f.{fact_date_col}") >= F.col("d.effective_from"))
    condition = condition & (F.col(f"f.{fact_date_col}") <= F.col("d.effective_to"))

    selected = [F.col(f"d.{sk_name}").alias(sk_name)] + [F.col(f"d.{c}").alias(c) for c in dim_cols]
    return f_alias.join(d_alias, condition, how).select(
        *[F.col(f"f.{c}").alias(c) for c in fact_columns], *selected
    )


def close_open_versions_at(df: DataFrame, as_of: date | str) -> DataFrame:
    """Cap open-ended versions at ``as_of``.

    Used when a dimension is snapshotted for reporting and an open-ended
    9999-12-31 would misrepresent a doctor who has since left the network.
    """
    if isinstance(as_of, date):
        as_of = as_of.isoformat()
    return df.withColumn(
        "effective_to",
        F.when(F.col("is_current"), F.to_date(F.lit(as_of))).otherwise(F.col("effective_to")),
    )
