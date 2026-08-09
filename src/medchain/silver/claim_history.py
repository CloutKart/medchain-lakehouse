"""Reconstruct claim lifecycle history from current-state-only portal exports.

The insurer portal has no history table. Each export lists a claim's status *right
now*. Accumulating those snapshots recovers the transitions:

    export 2024-05-06  CLM123  Under Review        status_date 2024-05-02
    export 2024-05-13  CLM123  Under Review        status_date 2024-05-02   <- same state
    export 2024-05-20  CLM123  Approved            status_date 2024-05-17   <- new state
    export 2024-05-27  CLM123  Settled             status_date 2024-05-24   <- new state

Deduplicating on (claim_id, status_code, status_date) turns four snapshot rows into
three transitions. Because the table is append-only and the MERGE inserts only when
no match exists, replaying any export — or all of them — converges to the same
result. That is the whole reason this is a MERGE rather than an INSERT.

What cannot be recovered is a state that both began and ended between two exports.
The pipeline reports that as a coverage percentage rather than pretending the
history is complete.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from medchain.config import Config
from medchain.utils.audit import RunContext
from medchain.utils.keys import sha256_key
from medchain.utils.logging import get_logger
from medchain.utils.tables import read, register_table, table_exists, upsert

log = get_logger("medchain.silver.claims")


def normalize_claims(spark: SparkSession, cfg: Config, bronze: DataFrame) -> DataFrame:
    """Cast the string-typed Bronze snapshot into typed columns."""
    return (
        bronze.withColumn("status_date", F.to_date(F.col("status_date")))
        .withColumn("submitted_date", F.to_date(F.col("submitted_date")))
        .withColumn("admission_date", F.to_date(F.col("admission_date")))
        .withColumn("discharge_date", F.to_date(F.col("discharge_date")))
        .withColumn("export_date", F.to_date(F.col("export_date")))
        .withColumn("claim_amount", F.col("claim_amount").cast("decimal(18,2)"))
        .withColumn("approved_amount", F.col("approved_amount").cast("decimal(18,2)"))
        .withColumn("claim_status", F.trim(F.col("claim_status")))
        .filter(F.col("claim_id").isNotNull() & F.col("claim_status").isNotNull())
    )


def build_transitions(claims: DataFrame, ctx: RunContext) -> DataFrame:
    """Collapse repeated snapshots into distinct state transitions.

    The transition's identity is (claim, status, status_date) — the export it was
    first observed in is recorded as evidence but deliberately excluded from the
    key, since the same transition appears in many exports.
    """
    ranked = Window.partitionBy("claim_id", "claim_status", "status_date").orderBy(
        F.col("export_date").asc_nulls_last()
    )
    return (
        claims.withColumn("_rn", F.row_number().over(ranked))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            sha256_key(F.col("claim_id"), F.col("claim_status"), F.col("status_date")).alias(
                "transition_key"
            ),
            "claim_id",
            F.col("claim_status").alias("status_code"),
            "status_date",
            F.col("export_date").alias("first_observed_export"),
            "patient_id",
            "hospital_id",
            "insurer_id",
            "policy_number",
            "claim_amount",
            "approved_amount",
            "submitted_date",
            # Stay dates the insurer holds. Carried through because they are the
            # attributes bill-to-claim linkage matches on — the two systems share
            # no key, so dates and amounts are all there is.
            "admission_date",
            "discharge_date",
            "rejection_reason",
            "hospital_ref_no",
        )
        .withColumn("batch_id", F.lit(ctx.batch_id))
        .withColumn("dw_created_at", F.current_timestamp())
    )


def enrich_transitions(transitions: DataFrame, cfg: Config) -> DataFrame:
    """Add sequence, previous state, dwell time and legality to each transition.

    ``days_in_prev_status`` is what makes "where do claims stall?" answerable — it
    is the dwell time of the *previous* state, attributed to that state, not to this
    one.
    """
    # transition_key completes the ordering. (claim, status, date) is already the
    # table's key so a genuine tie is impossible, but relying on that leaves the
    # sequence numbering dependent on an invariant enforced elsewhere.
    ordered = Window.partitionBy("claim_id").orderBy("status_date", "status_code", "transition_key")
    enriched = (
        transitions.withColumn("transition_seq", F.row_number().over(ordered))
        .withColumn("prev_status", F.lag("status_code").over(ordered))
        .withColumn("prev_status_date", F.lag("status_date").over(ordered))
        .withColumn("next_status", F.lead("status_code").over(ordered))
    )
    enriched = enriched.withColumn(
        "days_in_prev_status",
        F.when(
            F.col("prev_status_date").isNotNull(),
            F.datediff(F.col("status_date"), F.col("prev_status_date")),
        ),
    )

    terminal = cfg.get("claims", "terminal_states", default=["Settled", "Rejected"])
    enriched = enriched.withColumn("is_terminal", F.col("status_code").isin(terminal))

    # Classify each transition against the configured state machine.
    #
    # A naive "is it a legal single step?" check is misleading here, because a
    # weekly export legitimately misses states that begin and end between two
    # snapshots — a claim observed going Submitted -> Approved did pass through
    # Under Review, we simply never saw it. Labelling that "illegal" buries the
    # handful of genuine anomalies under thousands of sampling artefacts.
    #
    # So we distinguish three cases using the transitive closure of the state graph:
    #   DIRECT  - a legal single step, fully observed
    #   GAP     - the target is reachable, but only via states we never sampled
    #   ILLEGAL - not reachable at all (e.g. Settled -> Submitted). A real anomaly.
    legal = cfg.get("claims", "legal_transitions", default={})
    reachable = _transitive_closure(legal)

    direct_expr = F.lit(False)
    for from_state, to_states in legal.items():
        if to_states:
            direct_expr = direct_expr | (
                (F.col("prev_status") == F.lit(from_state)) & F.col("status_code").isin(to_states)
            )

    reachable_expr = F.lit(False)
    for from_state, to_states in reachable.items():
        if to_states:
            reachable_expr = reachable_expr | (
                (F.col("prev_status") == F.lit(from_state))
                & F.col("status_code").isin(sorted(to_states))
            )

    enriched = enriched.withColumn(
        "transition_class",
        F.when(F.col("prev_status").isNull(), F.lit("INITIAL"))
        .when(direct_expr, F.lit("DIRECT"))
        .when(reachable_expr, F.lit("GAP"))
        .otherwise(F.lit("ILLEGAL")),
    )
    # Retained for downstream convenience: legal means "not an anomaly", so an
    # unobserved intermediate state still counts as legal.
    enriched = enriched.withColumn(
        "is_legal_transition", F.col("transition_class") != F.lit("ILLEGAL")
    )
    return enriched


def _transitive_closure(legal: dict[str, list[str]]) -> dict[str, set[str]]:
    """All states reachable from each state in one or more steps.

    Plain breadth-first search over a graph of six nodes — no need for anything
    cleverer, and doing it in Python keeps the state machine defined in one place
    (``conf/base.yaml``) rather than duplicated into SQL.
    """
    closure: dict[str, set[str]] = {}
    for start in legal:
        seen: set[str] = set()
        queue = list(legal.get(start, []))
        while queue:
            state = queue.pop()
            if state in seen:
                continue
            seen.add(state)
            queue.extend(legal.get(state, []))
        closure[start] = seen
    return closure


def run(spark: SparkSession, cfg: Config, ctx: RunContext) -> dict[str, int]:
    """Build ``silver.claim_lifecycle`` from Bronze claim snapshots."""
    bronze_path = cfg.table_path("bronze", "insurance_claims")
    if not table_exists(spark, bronze_path):
        raise FileNotFoundError(f"Bronze claims not found at {bronze_path}; run Bronze first")

    snapshots = normalize_claims(spark, cfg, read(spark, bronze_path))
    n_snapshots = snapshots.count()

    transitions = build_transitions(snapshots, ctx)

    # Two tables, deliberately:
    #
    #   claim_transitions  the immutable audit. Append-only, never updated, never
    #                      widened. This is the record of what the insurer told us
    #                      and when, and it is what makes replay safe.
    #   claim_lifecycle    derived. Sequence numbers, dwell times and transition
    #                      classes recomputed in full on every run.
    #
    # Keeping them separate is not tidiness. Deriving in place means the MERGE
    # source (base columns) and the target (base + derived columns) have different
    # schemas, so the second run fails to resolve the INSERT clause — and the
    # derived columns are position-dependent anyway: a late-arriving export inserts
    # a transition in the middle of a claim's history and renumbers everything
    # after it, which an incremental update cannot express.
    audit_target = cfg.table_path("silver", "claim_transitions")
    upsert(spark, transitions, audit_target, ["transition_key"], update=False)
    register_table(spark, cfg, "silver", "claim_transitions")

    target = cfg.table_path("silver", "claim_lifecycle")
    enriched = enrich_transitions(read(spark, audit_target), cfg)
    enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)
    register_table(spark, cfg, "silver", "claim_lifecycle")

    final = read(spark, target)
    n_transitions = final.count()
    n_claims = final.select("claim_id").distinct().count()
    class_counts = {
        row["transition_class"]: row["n"]
        for row in final.groupBy("transition_class").agg(F.count(F.lit(1)).alias("n")).collect()
    }
    n_illegal = class_counts.get("ILLEGAL", 0)
    n_gap = class_counts.get("GAP", 0)
    n_terminal = final.filter(F.col("is_terminal")).select("claim_id").distinct().count()
    multi_state = (
        final.groupBy("claim_id").agg(F.count(F.lit(1)).alias("n")).filter(F.col("n") >= 2).count()
    )

    log.info(
        "  %d snapshots -> %d transitions across %d claims", n_snapshots, n_transitions, n_claims
    )
    log.info(
        "  claims with >=2 observed states: %d (%.1f%%)",
        multi_state,
        100 * multi_state / n_claims if n_claims else 0,
    )
    log.info(
        "  claims reaching a terminal state: %d (%.1f%%)",
        n_terminal,
        100 * n_terminal / n_claims if n_claims else 0,
    )
    log.info(
        "  transitions with an unobserved intermediate state (GAP): %d (%.1f%%)",
        n_gap,
        100 * n_gap / n_transitions if n_transitions else 0,
    )
    if n_illegal:
        log.warning("  genuinely illegal transitions: %d", n_illegal)

    return {
        "snapshots": n_snapshots,
        "transitions": n_transitions,
        "claims": n_claims,
        "multi_state_claims": multi_state,
        "terminal_claims": n_terminal,
        "illegal_transitions": n_illegal,
        "gap_transitions": n_gap,
    }
