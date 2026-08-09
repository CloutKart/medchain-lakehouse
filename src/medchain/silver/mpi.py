"""Master Patient Index — resolve one human across eight registration systems.

The problem, restated concretely: the same person is registered at H001 as
``H001-P004821`` and at H005 as ``H005-P001190``, with their name spelled slightly
differently, their birth date rendered in a different format, and possibly a
mistyped phone number. Nothing links the two records. Until they *are* linked, a
readmission at a different hospital is invisible and cross-hospital care continuity
cannot be measured at all.

The approach is deterministic-first, probabilistic-second:

1. **Normalise** aggressively — but parse dates using the *source hospital's* format,
   never a global guess. H002 exports ``03/04/1985`` as 3 April and H004 exports the
   same string as 4 March; one global format silently corrupts an eighth of all birth
   dates and then fails to match those people.
2. **Deterministic key** over (name, dob, phone_last4). Exact-key collisions are
   linked immediately and cheaply — this resolves the majority.
3. **Block, then score.** Comparing every record against every other is ~2.4x10^10
   pairs at this volume and will never finish. Candidates are restricted to records
   sharing a blocking key, then scored on name/dob/phone/city similarity.
4. **Three-way outcome.** >=0.90 auto-links, 0.75-0.90 goes to a quarantine table for
   human review, below that stays distinct. Silently auto-linking a weak match
   merges two people's medical histories, which is far worse than leaving a
   duplicate.
5. **Stable ids.** ``mpi_id`` comes from a persisted registry, so a person keeps the
   same identifier across every run. An id derived from row order would renumber the
   entire population on each execution and break every downstream fact table.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from medchain.config import Config
from medchain.utils.audit import RunContext
from medchain.utils.keys import normalize_name, normalize_phone, phone_last4, sha256_key
from medchain.utils.logging import get_logger
from medchain.utils.tables import read, register_table, table_exists, upsert

log = get_logger("medchain.silver.mpi")

# Blocks larger than this are skipped rather than expanded into pairs. A block of
# 5,000 records would generate 12.5M pairs on its own and is almost never a genuine
# cluster — it is a degenerate key (everyone with a null phone, say). The count of
# skipped blocks is reported to the scorecard rather than hidden.
#
# The value trades recall against cost quadratically: a block of n yields n(n-1)/2
# pairs. 120 is the point where the name-and-city block still admits common
# name/city combinations without any single block dominating the join.
DEFAULT_MAX_BLOCK_SIZE = 120

# Python strftime -> Java/Spark date pattern.
_FORMAT_MAP = {
    "%Y-%m-%d": "yyyy-MM-dd",
    "%d/%m/%Y": "dd/MM/yyyy",
    "%m/%d/%Y": "MM/dd/yyyy",
    "%d-%b-%Y": "dd-MMM-yyyy",
    "%Y/%m/%d": "yyyy/MM/dd",
    "%d.%m.%Y": "dd.MM.yyyy",
    "%d-%m-%Y": "dd-MM-yyyy",
}


# --------------------------------------------------------------- normalisation


def load_date_formats(cfg: Config) -> dict[str, str]:
    """Per-hospital Spark date pattern, from the source-format seed."""
    frame = pd.read_csv(cfg.seed_dir / "source_date_formats.csv", comment="#")
    return {
        row.hospital_id: _FORMAT_MAP.get(row.date_format, "yyyy-MM-dd")
        for row in frame.itertuples()
    }


def parse_dob(cfg: Config) -> F.Column:
    """Parse ``dob`` using the exporting hospital's own date convention.

    Built as a chained ``when`` rather than a UDF so it stays a native expression
    and can be pushed down. Falls back to ISO for any hospital not in the seed.
    """
    formats = load_date_formats(cfg)
    iso_fallback = F.to_date(F.col("dob"), "yyyy-MM-dd")
    if not formats:
        return iso_fallback

    expr: F.Column | None = None
    for hospital_id, pattern in formats.items():
        branch = F.to_date(F.col("dob"), pattern)
        expr = (
            F.when(F.col("hospital_id") == hospital_id, branch)
            if expr is None
            else expr.when(F.col("hospital_id") == hospital_id, branch)
        )
    assert expr is not None  # formats is non-empty, checked above
    return F.coalesce(expr, iso_fallback)


def normalize_registrations(spark: SparkSession, cfg: Config, bronze: DataFrame) -> DataFrame:
    """Clean registration records into the canonical form the MPI matches on.

    Only the latest Bronze row per (hospital, patient) is kept: registrations are
    re-exported unchanged in every backfill file, and matching duplicates against
    each other would inflate the cluster sizes with self-matches.
    """
    latest = Window.partitionBy("hospital_id", "patient_id").orderBy(
        F.col("ingestion_ts").desc(), F.col("updated_date").desc_nulls_last()
    )
    df = bronze.withColumn("_rn", F.row_number().over(latest)).filter(F.col("_rn") == 1).drop("_rn")

    df = (
        df.withColumn("first_name_norm", normalize_name(F.col("first_name")))
        .withColumn("last_name_norm", normalize_name(F.col("last_name")))
        .withColumn("dob_parsed", parse_dob(cfg))
        .withColumn("phone_norm", normalize_phone(F.col("phone")))
        .withColumn("phone_last4", phone_last4(F.col("phone")))
        .withColumn("city_norm", F.upper(F.trim(F.col("city"))))
        .withColumn("gender_norm", F.upper(F.substring(F.trim(F.col("gender")), 1, 1)))
    )
    df = df.withColumn(
        "full_name_norm", F.trim(F.concat_ws(" ", "first_name_norm", "last_name_norm"))
    )

    # A stable per-registration identifier. Used as the node id during clustering
    # and as the tie-break when choosing a cluster's surviving attributes.
    df = df.withColumn("record_id", sha256_key(F.col("hospital_id"), F.col("patient_id")))

    # The deterministic match key. Null on records missing any component — those
    # can only be matched probabilistically, which is the correct outcome.
    df = df.withColumn(
        "deterministic_key",
        F.when(
            F.col("full_name_norm").isNotNull()
            & (F.col("full_name_norm") != "")
            & F.col("dob_parsed").isNotNull()
            & F.col("phone_last4").isNotNull(),
            sha256_key(F.col("full_name_norm"), F.col("dob_parsed"), F.col("phone_last4")),
        ),
    )

    # Records that failed date parsing are flagged, not dropped — the original
    # string is still on the row so the failure can be diagnosed.
    df = df.withColumn("dob_parse_failed", F.col("dob").isNotNull() & F.col("dob_parsed").isNull())
    return df


# -------------------------------------------------------------------- blocking


def generate_candidate_pairs(
    df: DataFrame, max_block_size: int = DEFAULT_MAX_BLOCK_SIZE
) -> DataFrame:
    """Produce candidate pairs using several complementary blocking keys.

    No single key is sufficient, because each one depends on a field that some
    defect corrupts: blocking on phone misses everyone whose number was mistyped,
    blocking on name-and-date misses everyone whose birth year was keyed wrong.
    Unioning four complementary keys gives every defect type at least one route
    through, at the cost of duplicate pairs — which are deduplicated before scoring.
    """
    base = df.select(
        "record_id",
        "hospital_id",
        "patient_id",
        "first_name_norm",
        "last_name_norm",
        "full_name_norm",
        "dob_parsed",
        "phone_norm",
        "phone_last4",
        "city_norm",
        "gender_norm",
    )

    blocks = [
        # Exact phone: catches heavy name corruption as long as the number survived.
        ("phone", F.col("phone_norm"), F.col("phone_norm").isNotNull()),
        # Name-sound + exact DOB: catches phone loss and spelling drift together.
        (
            "soundex_dob",
            F.concat_ws("|", F.soundex(F.col("first_name_norm")), F.col("dob_parsed")),
            F.col("first_name_norm").isNotNull() & F.col("dob_parsed").isNotNull(),
        ),
        # Both names by sound + birth year: catches DOB typos within the same year.
        (
            "soundex_year",
            F.concat_ws(
                "|",
                F.soundex(F.col("first_name_norm")),
                F.soundex(F.col("last_name_norm")),
                F.year(F.col("dob_parsed")),
            ),
            F.col("first_name_norm").isNotNull() & F.col("dob_parsed").isNotNull(),
        ),
        # Both names by sound + city, with no date or phone component at all. This
        # is the only route for the worst records — name misspelt *and* phone
        # mistyped *and* birth year wrong — where every other blocking key fails
        # because the field it depends on is the field that got corrupted.
        (
            "soundex_city",
            F.concat_ws(
                "|",
                F.soundex(F.col("first_name_norm")),
                F.soundex(F.col("last_name_norm")),
                F.col("city_norm"),
            ),
            F.col("first_name_norm").isNotNull()
            & F.col("last_name_norm").isNotNull()
            & F.col("city_norm").isNotNull(),
        ),
    ]

    pair_frames = []
    for name, key_expr, condition in blocks:
        keyed = base.withColumn("block_key", key_expr).filter(
            condition & F.col("block_key").isNotNull()
        )

        # Drop oversized blocks before the self-join, not after.
        sizes = keyed.groupBy("block_key").agg(F.count(F.lit(1)).alias("block_size"))
        keyed = keyed.join(
            sizes.filter(F.col("block_size") <= max_block_size).select("block_key"),
            on="block_key",
            how="inner",
        )

        left = keyed.alias("l")
        right = keyed.alias("r")
        pairs = left.join(
            right,
            (F.col("l.block_key") == F.col("r.block_key"))
            # record_id ordering makes each unordered pair appear exactly once and
            # eliminates self-comparison in the same predicate.
            & (F.col("l.record_id") < F.col("r.record_id")),
            "inner",
        ).select(
            F.col("l.record_id").alias("record_id_a"),
            F.col("r.record_id").alias("record_id_b"),
            F.col("l.full_name_norm").alias("name_a"),
            F.col("r.full_name_norm").alias("name_b"),
            F.col("l.dob_parsed").alias("dob_a"),
            F.col("r.dob_parsed").alias("dob_b"),
            F.col("l.phone_norm").alias("phone_a"),
            F.col("r.phone_norm").alias("phone_b"),
            F.col("l.city_norm").alias("city_a"),
            F.col("r.city_norm").alias("city_b"),
            F.col("l.gender_norm").alias("gender_a"),
            F.col("r.gender_norm").alias("gender_b"),
            F.col("l.hospital_id").alias("hospital_a"),
            F.col("r.hospital_id").alias("hospital_b"),
            F.lit(name).alias("block_strategy"),
        )
        pair_frames.append(pairs)

    combined = pair_frames[0]
    for frame in pair_frames[1:]:
        combined = combined.unionByName(frame)

    # The same pair can surface from several blocking strategies; keep one row and
    # record which strategies found it, as evidence for the review queue.
    return combined.dropDuplicates(["record_id_a", "record_id_b"])


def blocking_diagnostics(df: DataFrame, max_block_size: int = DEFAULT_MAX_BLOCK_SIZE) -> DataFrame:
    """Report blocks that were skipped for being oversized.

    Surfaced on the scorecard: a growing number of skipped blocks means recall is
    quietly degrading, and that should be visible rather than inferred.
    """
    keyed = df.filter(F.col("dob_parsed").isNotNull()).withColumn(
        "block_key",
        F.concat_ws("|", F.soundex(F.col("first_name_norm")), F.col("dob_parsed")),
    )
    return (
        keyed.groupBy("block_key")
        .agg(F.count(F.lit(1)).alias("block_size"))
        .filter(F.col("block_size") > max_block_size)
    )


# --------------------------------------------------------------------- scoring


def _jaro_winkler_series(left: pd.Series, right: pd.Series) -> pd.Series:
    """Vectorised Jaro-Winkler over two string columns.

    Implemented here rather than pulled from a library so the cluster needs no extra
    package installed. Jaro-Winkler is the right choice for names because its prefix
    bonus reflects how transcription errors actually behave — people mistype the end
    of a name far more often than the beginning.
    """

    def jaro(s1: str, s2: str) -> float:
        if not s1 or not s2:
            return 0.0
        if s1 == s2:
            return 1.0
        len1, len2 = len(s1), len(s2)
        window = max(len1, len2) // 2 - 1
        if window < 0:
            window = 0
        flags1 = [False] * len1
        flags2 = [False] * len2
        matches = 0
        for i in range(len1):
            start = max(0, i - window)
            end = min(i + window + 1, len2)
            for j in range(start, end):
                if not flags2[j] and s1[i] == s2[j]:
                    flags1[i] = flags2[j] = True
                    matches += 1
                    break
        if matches == 0:
            return 0.0
        transpositions = 0
        k = 0
        for i in range(len1):
            if flags1[i]:
                while not flags2[k]:
                    k += 1
                if s1[i] != s2[k]:
                    transpositions += 1
                k += 1
        transpositions //= 2
        return (matches / len1 + matches / len2 + (matches - transpositions) / matches) / 3.0

    def jaro_winkler(s1: str | None, s2: str | None) -> float:
        if not isinstance(s1, str) or not isinstance(s2, str):
            return 0.0
        score = jaro(s1, s2)
        if score < 0.7:
            return score
        prefix = 0
        for a, b in zip(s1[:4], s2[:4]):
            if a != b:
                break
            prefix += 1
        return score + prefix * 0.1 * (1 - score)

    return pd.Series(
        [jaro_winkler(a, b) for a, b in zip(left, right)], index=left.index, dtype="float64"
    )


def register_similarity_udf(spark: SparkSession):
    """Register the Jaro-Winkler pandas UDF."""
    from pyspark.sql.functions import pandas_udf

    @pandas_udf(DoubleType())  # type: ignore[call-overload]
    def jw_udf(left: pd.Series, right: pd.Series) -> pd.Series:
        return _jaro_winkler_series(left, right)

    return jw_udf


def score_pairs(spark: SparkSession, pairs: DataFrame, weights: dict[str, float]) -> DataFrame:
    """Score candidate pairs on name, DOB, phone and city agreement.

    Weights are **renormalised over the fields actually present**. If a record has no
    phone, the phone component is removed from both numerator and denominator rather
    than scored as zero. Treating "missing" as "disagrees" is the single biggest
    avoidable source of false negatives — it penalises a record for what it does not
    claim.
    """
    jw = register_similarity_udf(spark)

    w_name = float(weights.get("name", 0.40))
    w_dob = float(weights.get("dob", 0.30))
    w_phone = float(weights.get("phone", 0.20))
    w_city = float(weights.get("city", 0.10))

    scored = pairs.withColumn("name_sim", jw(F.col("name_a"), F.col("name_b")))

    # DOB agreement is graded, not binary: an exact match scores 1.0, a
    # day/month transposition or +/-1 day scores 0.75 (a classic keying error), the
    # same birth year scores 0.35, anything else 0.
    day_diff = F.abs(F.datediff(F.col("dob_a"), F.col("dob_b")))
    transposed = (
        (F.dayofmonth(F.col("dob_a")) == F.month(F.col("dob_b")))
        & (F.month(F.col("dob_a")) == F.dayofmonth(F.col("dob_b")))
        & (F.year(F.col("dob_a")) == F.year(F.col("dob_b")))
    )
    scored = scored.withColumn(
        "dob_sim",
        F.when(F.col("dob_a").isNull() | F.col("dob_b").isNull(), F.lit(None).cast("double"))
        .when(F.col("dob_a") == F.col("dob_b"), F.lit(1.0))
        .when(transposed | (day_diff <= 1), F.lit(0.75))
        .when(F.year(F.col("dob_a")) == F.year(F.col("dob_b")), F.lit(0.35))
        .otherwise(F.lit(0.0)),
    )

    # Phone similarity, graded by *how many keystrokes* separate the two numbers.
    #
    # Plain Levenshtein scores a digit transposition as distance 2 (a delete plus an
    # insert), the same as two unrelated wrong digits. That is wrong in this domain:
    # swapping adjacent digits is a single slip and by far the most common phone
    # entry error. Detecting it explicitly — equal length and identical digit
    # multiset — recovers the Damerau-Levenshtein reading without a UDF, and is
    # worth roughly a third of the MPI's recall on phone-corrupted records.
    phone_lev = F.levenshtein(F.col("phone_a"), F.col("phone_b"))
    same_digits = F.sort_array(F.split(F.col("phone_a"), "")) == F.sort_array(
        F.split(F.col("phone_b"), "")
    )
    scored = scored.withColumn(
        "phone_sim",
        F.when(F.col("phone_a").isNull() | F.col("phone_b").isNull(), F.lit(None).cast("double"))
        .when(F.col("phone_a") == F.col("phone_b"), F.lit(1.0))
        .when(phone_lev <= 1, F.lit(0.85))  # one wrong digit
        .when((phone_lev == 2) & same_digits, F.lit(0.85))  # digits transposed
        .when(phone_lev == 2, F.lit(0.45))  # two wrong digits
        .otherwise(F.lit(0.0)),
    )

    scored = scored.withColumn(
        "city_sim",
        F.when(F.col("city_a").isNull() | F.col("city_b").isNull(), F.lit(None).cast("double"))
        .when(F.col("city_a") == F.col("city_b"), F.lit(1.0))
        .otherwise(F.lit(0.0)),
    )

    def contribution(col: str, weight: float) -> F.Column:
        return F.when(F.col(col).isNotNull(), F.col(col) * F.lit(weight)).otherwise(F.lit(0.0))

    def available(col: str, weight: float) -> F.Column:
        return F.when(F.col(col).isNotNull(), F.lit(weight)).otherwise(F.lit(0.0))

    numerator = (
        contribution("name_sim", w_name)
        + contribution("dob_sim", w_dob)
        + contribution("phone_sim", w_phone)
        + contribution("city_sim", w_city)
    )
    denominator = (
        available("name_sim", w_name)
        + available("dob_sim", w_dob)
        + available("phone_sim", w_phone)
        + available("city_sim", w_city)
    )

    scored = scored.withColumn(
        "match_score",
        F.when(denominator > 0, F.round(numerator / denominator, 4)).otherwise(F.lit(0.0)),
    )

    # A hard veto: different recorded sex with a mediocre name score is far more
    # likely to be two relatives sharing a phone and surname than one person.
    scored = scored.withColumn(
        "gender_conflict",
        F.col("gender_a").isNotNull()
        & F.col("gender_b").isNotNull()
        & (F.col("gender_a") != F.col("gender_b")),
    )
    scored = scored.withColumn(
        "match_score",
        F.when(
            F.col("gender_conflict") & (F.col("name_sim") < 0.95), F.col("match_score") * 0.5
        ).otherwise(F.col("match_score")),
    )
    return scored


# ------------------------------------------------------------------ clustering


def connected_components(
    nodes: DataFrame, edges: DataFrame, *, max_iterations: int = 12
) -> DataFrame:
    """Group linked records into clusters by iterative minimum-label propagation.

    Matching is not transitive by construction — A matches B and B matches C without
    A matching C — but identity is. Taking connected components resolves that: every
    record reachable through match edges belongs to one person.

    Convergence is fast here (clusters are 2-4 records) but the iteration cap is a
    genuine safety net: a degenerate blocking key can produce a giant component that
    would otherwise iterate for a long time.
    """
    labels = nodes.select(F.col("record_id"), F.col("record_id").alias("cluster_label"))

    # Symmetric edge list, so a label can travel in both directions.
    symmetric = (
        edges.select(F.col("record_id_a").alias("src"), F.col("record_id_b").alias("dst"))
        .unionByName(
            edges.select(F.col("record_id_b").alias("src"), F.col("record_id_a").alias("dst"))
        )
        .distinct()
    )

    for iteration in range(max_iterations):
        propagated = (
            symmetric.join(labels, symmetric.dst == labels.record_id, "inner")
            .groupBy(F.col("src").alias("record_id"))
            .agg(F.min("cluster_label").alias("neighbour_label"))
        )
        updated = (
            labels.join(propagated, on="record_id", how="left")
            .withColumn(
                "new_label",
                F.least(
                    F.col("cluster_label"),
                    F.coalesce(F.col("neighbour_label"), F.col("cluster_label")),
                ),
            )
            .select("record_id", F.col("new_label").alias("cluster_label"))
        )
        updated = updated.cache()

        changed = (
            updated.join(labels, on="record_id")
            .filter(updated.cluster_label != labels.cluster_label)
            .limit(1)
            .count()
        )

        labels.unpersist()
        labels = updated
        if changed == 0:
            log.info("  connected components converged after %d iteration(s)", iteration + 1)
            break
    else:
        log.warning("  connected components hit the %d-iteration cap", max_iterations)

    return labels


# ----------------------------------------------------------- stable identifiers


def assign_stable_mpi_ids(
    spark: SparkSession, cfg: Config, clusters: DataFrame, ctx: RunContext
) -> DataFrame:
    """Map cluster labels to durable ``mpi_id`` values via a persisted registry.

    The registry is the reason an ``mpi_id`` means the same thing next month as it
    does today. New clusters are appended with ids continuing from the existing high
    water mark; clusters already present keep the id they were given. Nothing here
    depends on row order, partition count or run sequence.
    """
    registry_path = cfg.table_path("silver", "mpi_registry")

    distinct_clusters = clusters.select("cluster_label").distinct()

    if table_exists(spark, registry_path):
        existing = read(spark, registry_path)
        known = existing.select("cluster_label", "mpi_id")
        new_clusters = distinct_clusters.join(known, on="cluster_label", how="left_anti")
        offset = existing.agg(F.coalesce(F.max("mpi_seq"), F.lit(0)).alias("m")).collect()[0]["m"]
    else:
        known = None
        new_clusters = distinct_clusters
        offset = 0

    # row_number over a deterministic ordering of the *new* labels only. Existing
    # ids are never renumbered.
    ordering = Window.orderBy("cluster_label")
    minted = (
        new_clusters.withColumn("mpi_seq", F.row_number().over(ordering) + F.lit(int(offset)))
        .withColumn(
            "mpi_id", F.concat(F.lit("MPI"), F.lpad(F.col("mpi_seq").cast("string"), 9, "0"))
        )
        .withColumn("created_batch_id", F.lit(ctx.batch_id))
        .withColumn("created_at", F.current_timestamp())
    )

    if minted.take(1):
        upsert(spark, minted, registry_path, ["cluster_label"], update=False)

    registry = read(spark, registry_path).select("cluster_label", "mpi_id", "mpi_seq")
    return clusters.join(registry, on="cluster_label", how="left")


# ------------------------------------------------------------------ entrypoint


def run(spark: SparkSession, cfg: Config, ctx: RunContext) -> dict[str, int]:
    """Build the Master Patient Index from Bronze registrations."""
    bronze_path = cfg.table_path("bronze", "patient_registrations")
    if not table_exists(spark, bronze_path):
        raise FileNotFoundError(
            f"Bronze registrations not found at {bronze_path}; run Bronze first"
        )

    normalized = normalize_registrations(spark, cfg, read(spark, bronze_path)).cache()
    total_records = normalized.count()
    log.info("  normalised %d registration records", total_records)

    parse_failures = normalized.filter(F.col("dob_parse_failed")).count()
    if parse_failures:
        log.warning("  %d records failed date parsing", parse_failures)

    # --- deterministic edges -------------------------------------------------
    det = normalized.filter(F.col("deterministic_key").isNotNull())
    det_left = det.select(
        F.col("record_id").alias("record_id_a"), F.col("deterministic_key")
    ).alias("a")
    det_right = det.select(
        F.col("record_id").alias("record_id_b"), F.col("deterministic_key")
    ).alias("b")
    deterministic_edges = (
        det_left.join(det_right, on="deterministic_key")
        .filter(F.col("record_id_a") < F.col("record_id_b"))
        .select(
            "record_id_a",
            "record_id_b",
            F.lit(1.0).alias("match_score"),
            F.lit("DETERMINISTIC").alias("match_method"),
        )
    )
    n_det = deterministic_edges.count()
    log.info("  deterministic edges: %d", n_det)

    # --- probabilistic edges -------------------------------------------------
    weights = cfg.get("mpi", "weights", default={})
    auto_threshold = float(cfg.get("mpi", "auto_link_threshold", default=0.90))
    quarantine_threshold = float(cfg.get("mpi", "quarantine_threshold", default=0.75))

    max_block = int(cfg.get("mpi", "max_block_size", default=DEFAULT_MAX_BLOCK_SIZE))
    pairs = generate_candidate_pairs(normalized, max_block_size=max_block)
    scored = score_pairs(spark, pairs, weights).cache()
    n_pairs = scored.count()
    log.info("  candidate pairs scored: %d", n_pairs)

    probabilistic_edges = scored.filter(F.col("match_score") >= auto_threshold).select(
        "record_id_a",
        "record_id_b",
        "match_score",
        F.lit("PROBABILISTIC").alias("match_method"),
    )
    n_prob = probabilistic_edges.count()
    log.info("  probabilistic edges (>= %.2f): %d", auto_threshold, n_prob)

    # --- quarantine ----------------------------------------------------------
    quarantine = (
        scored.filter(
            (F.col("match_score") >= quarantine_threshold) & (F.col("match_score") < auto_threshold)
        )
        .select(
            "record_id_a",
            "record_id_b",
            "name_a",
            "name_b",
            "dob_a",
            "dob_b",
            "phone_a",
            "phone_b",
            "city_a",
            "city_b",
            "hospital_a",
            "hospital_b",
            "name_sim",
            "dob_sim",
            "phone_sim",
            "city_sim",
            "gender_conflict",
            "match_score",
            "block_strategy",
        )
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("review_status", F.lit("PENDING"))
        .withColumn("created_at", F.current_timestamp())
    )

    quarantine_path = cfg.table_path("quarantine", "mpi_review_queue")
    quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        quarantine_path
    )
    n_quarantine = read(spark, quarantine_path).count()
    log.info(
        "  quarantined for review (%.2f-%.2f): %d",
        quarantine_threshold,
        auto_threshold,
        n_quarantine,
    )

    # --- clustering ----------------------------------------------------------
    edges = deterministic_edges.unionByName(probabilistic_edges).dropDuplicates(
        ["record_id_a", "record_id_b"]
    )
    nodes = normalized.select("record_id")
    labels = connected_components(nodes, edges)
    labelled = assign_stable_mpi_ids(spark, cfg, labels, ctx)

    # --- patient master ------------------------------------------------------
    # Within a cluster, the surviving demographic attributes come from the most
    # recently updated registration — the freshest information the network holds.
    enriched = normalized.join(labelled.select("record_id", "mpi_id"), on="record_id", how="left")
    recency = Window.partitionBy("mpi_id").orderBy(
        F.col("updated_date").desc_nulls_last(), F.col("record_id")
    )
    master = (
        enriched.withColumn("_rn", F.row_number().over(recency))
        .withColumn("source_record_count", F.count(F.lit(1)).over(Window.partitionBy("mpi_id")))
        .withColumn(
            "source_hospital_count",
            F.size(F.collect_set("hospital_id").over(Window.partitionBy("mpi_id"))),
        )
        .filter(F.col("_rn") == 1)
        .select(
            "mpi_id",
            "first_name_norm",
            "last_name_norm",
            "full_name_norm",
            "gender_norm",
            "dob_parsed",
            "phone_norm",
            "city_norm",
            F.col("address_line"),
            F.col("pincode"),
            F.col("state"),
            F.col("blood_group"),
            F.col("email"),
            "source_record_count",
            "source_hospital_count",
        )
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
    )

    master_path = cfg.table_path("silver", "patient_master")
    master.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        master_path
    )
    register_table(spark, cfg, "silver", "patient_master")

    # --- crosswalk: the table every downstream fact joins through -------------
    crosswalk = enriched.select(
        "mpi_id",
        "hospital_id",
        "patient_id",
        "record_id",
        "full_name_norm",
        "dob_parsed",
        "phone_norm",
        F.col("deterministic_key").isNotNull().alias("had_deterministic_key"),
    ).withColumn("batch_id", F.lit(ctx.batch_id))

    crosswalk_path = cfg.table_path("silver", "patient_crosswalk")
    crosswalk.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        crosswalk_path
    )
    register_table(spark, cfg, "silver", "patient_crosswalk")

    n_mpi = read(spark, master_path).count()
    log.info(
        "  MPI: %d registrations -> %d distinct patients (%.1f%% collapse)",
        total_records,
        n_mpi,
        100 * (1 - n_mpi / total_records) if total_records else 0,
    )

    normalized.unpersist()
    scored.unpersist()

    return {
        "registrations": total_records,
        "mpi_ids": n_mpi,
        "deterministic_edges": n_det,
        "probabilistic_edges": n_prob,
        "quarantined": n_quarantine,
        "candidate_pairs": n_pairs,
        "dob_parse_failures": parse_failures,
    }
