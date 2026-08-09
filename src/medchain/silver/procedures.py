"""ICD-10 code inference for the ~8% of procedures exported without one.

An unmapped procedure is invisible to every clinical and regulatory report that
groups by diagnosis. Filling the gap is worthwhile — but *how* a code was arrived at
matters as much as the code itself, so every inferred value carries an
``icd10_source`` column recording which tier produced it and a confidence score.

The tiers, in order of decreasing certainty:

``SOURCE``       the export already had a code. Nothing was inferred.
``EXACT_NAME``   the procedure name matches a catalogue entry exactly once the
                 variant suffix ("- Left", "(Revision)") is stripped. Effectively
                 certain.
``FUZZY_NAME``   the normalised name is within a small edit distance of exactly one
                 catalogue entry. Good, but not certain.
``SPECIALTY``    no name match; the modal code for that specialty is used. This is a
                 placeholder, honest about being one.
``UNMAPPED``     nothing could be inferred.

Reporting a single blended "99% fill rate" would be misleading, because a
specialty-level default is not the same fact as a coded diagnosis. The scorecard
therefore reports the rate per tier.
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

log = get_logger("medchain.silver.procedures")

# Variant suffixes the hospital appends to a catalogue procedure name. Stripping
# them is what turns a "fuzzy" match into an exact one for most rows.
VARIANT_SUFFIXES = [
    r"\s*-\s*LEFT$",
    r"\s*-\s*RIGHT$",
    r"\s*-\s*BILATERAL$",
    r"\s*\(REVISION\)$",
    r"\s*-\s*ELECTIVE$",
    r"\s*-\s*EMERGENCY$",
]

# Maximum edit distance for a fuzzy name match, as a fraction of name length.
FUZZY_MAX_RATIO = 0.15


def normalize_procedure_name(col: F.Column) -> F.Column:
    """Uppercase, strip variant suffixes and collapse punctuation/whitespace."""
    text = F.upper(F.trim(col))
    for pattern in VARIANT_SUFFIXES:
        text = F.regexp_replace(text, pattern, "")
    text = F.regexp_replace(text, r"[^A-Z0-9 ]", " ")
    return F.trim(F.regexp_replace(text, r"\s+", " "))


def load_catalog(spark: SparkSession, cfg: Config) -> DataFrame:
    """The curated ICD-10 reference the hospital's coding team maintains."""
    frame = pd.read_csv(cfg.seed_dir / "icd10_catalog.csv", comment="#")
    df = spark.createDataFrame(frame)
    return df.withColumn("name_norm", normalize_procedure_name(F.col("procedure_name")))


def infer_codes(procedures: DataFrame, catalog: DataFrame) -> DataFrame:
    """Fill missing ICD-10 codes through the tiered strategy."""
    df = procedures.withColumn("name_norm", normalize_procedure_name(F.col("procedure_name")))
    df = df.withColumn(
        "has_source_code", F.col("icd10_code").isNotNull() & (F.trim(F.col("icd10_code")) != "")
    )

    # --- tier 1: exact normalised name --------------------------------------
    exact = catalog.select(
        F.col("name_norm").alias("cat_name_norm"),
        F.col("icd10_code").alias("exact_code"),
        F.col("specialty").alias("cat_specialty"),
    ).dropDuplicates(["cat_name_norm"])
    df = df.join(exact, df.name_norm == exact.cat_name_norm, "left").drop("cat_name_norm")

    # --- tier 2: fuzzy name, only when unambiguous ---------------------------
    # A cross join against the catalogue is affordable because the catalogue is ~120
    # rows and only unmatched procedures participate.
    unmatched = df.filter(~F.col("has_source_code") & F.col("exact_code").isNull()).select(
        "procedure_code", "name_norm", "specialty"
    )
    candidates = unmatched.crossJoin(
        catalog.select(
            F.col("name_norm").alias("cat_name"),
            F.col("icd10_code").alias("fuzzy_code_candidate"),
        )
    )
    candidates = candidates.withColumn(
        "edit_distance", F.levenshtein(F.col("name_norm"), F.col("cat_name"))
    ).withColumn(
        "distance_ratio",
        F.col("edit_distance") / F.greatest(F.length(F.col("name_norm")), F.lit(1)),
    )
    candidates = candidates.filter(F.col("distance_ratio") <= FUZZY_MAX_RATIO)

    # Keep the single best candidate, and only when it is strictly better than the
    # runner-up. An ambiguous fuzzy match is worse than no match: it silently
    # assigns one of two plausible diagnoses.
    ranked = Window.partitionBy("procedure_code").orderBy("edit_distance", "fuzzy_code_candidate")
    best = (
        candidates.withColumn("_rn", F.row_number().over(ranked))
        .withColumn("_next_distance", F.lead("edit_distance").over(ranked))
        .filter(F.col("_rn") == 1)
        .filter(
            F.col("_next_distance").isNull() | (F.col("_next_distance") > F.col("edit_distance"))
        )
        .select(
            "procedure_code",
            F.col("fuzzy_code_candidate").alias("fuzzy_code"),
            F.round(F.lit(1.0) - F.col("distance_ratio"), 3).alias("fuzzy_confidence"),
        )
    )
    df = df.join(best, on="procedure_code", how="left")

    # --- tier 3: modal code for the specialty --------------------------------
    specialty_modal = (
        catalog.groupBy("specialty", "icd10_code")
        .agg(F.count(F.lit(1)).alias("n"))
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("specialty").orderBy(F.col("n").desc(), "icd10_code")
            ),
        )
        .filter(F.col("_rn") == 1)
        .select(F.col("specialty").alias("spec_key"), F.col("icd10_code").alias("specialty_code"))
    )
    df = df.join(specialty_modal, df.specialty == specialty_modal.spec_key, "left").drop("spec_key")

    # --- resolve ------------------------------------------------------------
    df = df.withColumn(
        "icd10_code_final",
        F.coalesce(
            F.when(F.col("has_source_code"), F.trim(F.col("icd10_code"))),
            F.col("exact_code"),
            F.col("fuzzy_code"),
            F.col("specialty_code"),
        ),
    )
    df = df.withColumn(
        "icd10_source",
        F.when(F.col("has_source_code"), F.lit("SOURCE"))
        .when(F.col("exact_code").isNotNull(), F.lit("EXACT_NAME"))
        .when(F.col("fuzzy_code").isNotNull(), F.lit("FUZZY_NAME"))
        .when(F.col("specialty_code").isNotNull(), F.lit("SPECIALTY"))
        .otherwise(F.lit("UNMAPPED")),
    )
    df = df.withColumn(
        "icd10_confidence",
        F.when(F.col("icd10_source") == "SOURCE", F.lit(1.00))
        .when(F.col("icd10_source") == "EXACT_NAME", F.lit(0.95))
        .when(
            F.col("icd10_source") == "FUZZY_NAME",
            F.coalesce(F.col("fuzzy_confidence"), F.lit(0.70)),
        )
        .when(F.col("icd10_source") == "SPECIALTY", F.lit(0.40))
        .otherwise(F.lit(0.0)),
    )
    return df.drop(
        "exact_code", "fuzzy_code", "fuzzy_confidence", "specialty_code", "cat_specialty"
    )


def run(spark: SparkSession, cfg: Config, ctx: RunContext) -> dict[str, int]:
    """Build ``silver.procedure_catalog`` with inferred ICD-10 codes."""
    bronze_path = cfg.table_path("bronze", "procedure_master")
    if not table_exists(spark, bronze_path):
        raise FileNotFoundError(f"Bronze procedure master not found at {bronze_path}")

    # The catalogue is re-exported in full each week; keep the latest version only.
    # The weekly catalogue refresh lands every row with the same ingestion_ts, so
    # source_file breaks what would otherwise be a whole-batch tie.
    latest = Window.partitionBy("procedure_code").orderBy(
        F.col("ingestion_ts").desc(), F.col("source_file").desc()
    )
    procedures = (
        read(spark, bronze_path)
        .withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("base_cost", F.col("base_cost").cast("decimal(18,2)"))
    )
    total = procedures.count()
    missing_before = procedures.filter(
        F.col("icd10_code").isNull() | (F.trim(F.col("icd10_code")) == "")
    ).count()

    catalog = load_catalog(spark, cfg)
    enriched = infer_codes(procedures, catalog)

    output = (
        enriched.select(
            "procedure_code",
            "procedure_name",
            "name_norm",
            "specialty",
            "procedure_category",
            "base_cost",
            F.col("icd10_code").alias("icd10_code_source"),
            F.col("icd10_code_final").alias("icd10_code"),
            "icd10_source",
            "icd10_confidence",
        )
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_updated_at", F.current_timestamp())
    )

    target = cfg.table_path("silver", "procedure_catalog")
    output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)
    register_table(spark, cfg, "silver", "procedure_catalog")

    stored = read(spark, target)
    tiers = {
        row["icd10_source"]: row["n"]
        for row in stored.groupBy("icd10_source").agg(F.count(F.lit(1)).alias("n")).collect()
    }
    filled = total - tiers.get("UNMAPPED", 0)

    log.info(
        "  %d procedures, %d missing a code on arrival (%.1f%%)",
        total,
        missing_before,
        100 * missing_before / total if total else 0,
    )
    for tier in ("SOURCE", "EXACT_NAME", "FUZZY_NAME", "SPECIALTY", "UNMAPPED"):
        n = tiers.get(tier, 0)
        log.info("    %-12s %6d (%5.1f%%)", tier, n, 100 * n / total if total else 0)
    log.info("  overall fill rate: %.1f%%", 100 * filled / total if total else 0)

    return {
        "procedures": total,
        "missing_before": missing_before,
        "filled": filled,
        **{f"tier_{k.lower()}": v for k, v in tiers.items()},
    }
