"""End-to-end integration: generate → Bronze → Silver → Gold → quality.

Runs the entire platform at ``--scale 0.01`` (~6k visits) in an isolated temp
directory. Slower than the unit and Spark suites, and the only test that proves the
layers actually compose.

The idempotency assertion here is the one that matters most in production: run the
same batch twice and every table must be byte-for-byte equivalent. That property is
what makes a failed nightly load safe to simply re-run.
"""

from __future__ import annotations

import os
from datetime import date

import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.spark]

LOGICAL_DATE = date(2025, 3, 31)


@pytest.fixture(scope="module")
def sandbox(tmp_path_factory):
    """A complete config pointing at a throwaway data directory."""
    root = tmp_path_factory.mktemp("medchain-e2e")
    from medchain.config import CONF_DIR, Config

    base = yaml.safe_load((CONF_DIR / "base.yaml").read_text())
    sources = yaml.safe_load((CONF_DIR / "sources.yaml").read_text())["sources"]

    paths = {
        layer: str(root / layer)
        for layer in ("landing", "bronze", "silver", "gold", "control", "quarantine", "checkpoints")
    }
    paths["truth"] = str(root / "_truth")

    return Config(env="local", raw=base, paths=paths, catalog=None, sources=sources)


@pytest.fixture(scope="module")
def built(spark, sandbox):
    """Generate a small dataset and run every layer once."""
    from medchain.bronze import ingest
    from medchain.generate import cli as gen_cli
    from medchain.gold import pipeline as gold_pipeline
    from medchain.silver import pipeline as silver_pipeline

    os.environ["MEDCHAIN_ENV"] = "local"
    exit_code = gen_cli.main(
        [
            "--scale",
            "0.01",
            "--seed",
            "7",
            "--out",
            sandbox.path("landing"),
            "--truth-out",
            sandbox.path("truth"),
        ]
    )
    assert exit_code == 0, "generator failed"

    bronze = ingest.run(spark, sandbox, LOGICAL_DATE)
    silver = silver_pipeline.run(spark, sandbox, LOGICAL_DATE)
    gold = gold_pipeline.run(spark, sandbox, LOGICAL_DATE, maintenance=False)
    return {"bronze": bronze, "silver": silver, "gold": gold}


class TestPipelineComposition:
    def test_all_sources_land_in_bronze(self, built, sandbox):
        assert set(built["bronze"]) == set(sandbox.source_names)
        assert all(rows > 0 for rows in built["bronze"].values())

    def test_star_schema_shape(self, built):
        """Exactly 6 dimensions and 4 facts, as the spec requires."""
        gold = built["gold"]
        dimensions = {k for k in gold if k.startswith("dim_")}
        facts = {k for k in gold if k.startswith("fact_")}
        assert dimensions == {
            "dim_date",
            "dim_patient",
            "dim_doctor",
            "dim_hospital",
            "dim_insurer",
            "dim_procedure",
        }
        assert facts == {
            "fact_patient_visit",
            "fact_claim_lifecycle",
            "fact_billing_reconciliation",
            "fact_bed_occupancy",
        }
        assert all(gold[t] > 0 for t in dimensions | facts)

    def test_every_silver_step_produced_output(self, built):
        for step, result in built["silver"].items():
            assert result, f"silver.{step} returned nothing"

    def test_foreign_keys_resolve(self, spark, sandbox, built):
        from pyspark.sql import functions as F

        from medchain.utils.tables import read

        visits = read(spark, sandbox.table_path("gold", "fact_patient_visit"))
        for fk, dim_table, dim_key in [
            ("patient_sk", "dim_patient", "patient_sk"),
            ("doctor_sk", "dim_doctor", "doctor_sk"),
            ("hospital_sk", "dim_hospital", "hospital_sk"),
        ]:
            dim = (
                read(spark, sandbox.table_path("gold", dim_table))
                .select(F.col(dim_key).alias("_k"))
                .distinct()
            )
            orphans = (
                visits.filter(F.col(fk).isNotNull())
                .join(dim, visits[fk] == F.col("_k"), "left_anti")
                .count()
            )
            assert orphans == 0, f"{orphans} orphaned {fk} values"

    def test_row_counts_reconcile_across_layers(self, spark, sandbox, built):
        """Bronze claim snapshots must fully account for Silver transitions."""
        from medchain.utils.tables import read

        bronze_claims = read(spark, sandbox.table_path("bronze", "insurance_claims")).count()
        silver_transitions = read(spark, sandbox.table_path("silver", "claim_lifecycle")).count()
        gold_transitions = read(spark, sandbox.table_path("gold", "fact_claim_lifecycle")).count()

        # Deduplication only ever removes rows, and Gold is a pass-through of Silver.
        assert silver_transitions <= bronze_claims
        assert gold_transitions == silver_transitions


class TestIdempotency:
    def test_rerunning_every_layer_changes_nothing(self, spark, sandbox, built):
        """Replay the whole pipeline and assert the warehouse is unchanged.

        This is the property that makes a failed nightly load safe to re-run
        blindly, and the one most easily broken by an innocuous-looking change from
        MERGE to INSERT.
        """
        from medchain.bronze import ingest
        from medchain.gold import pipeline as gold_pipeline
        from medchain.silver import pipeline as silver_pipeline
        from medchain.utils.tables import checksum

        keyed = {
            ("silver", "claim_lifecycle"): ["transition_key"],
            ("silver", "patient_crosswalk"): ["hospital_id", "patient_id", "mpi_id"],
            ("silver", "dim_doctor_scd2"): ["doctor_sk"],
            ("gold", "fact_patient_visit"): ["visit_sk"],
            ("gold", "fact_claim_lifecycle"): ["transition_key"],
            ("gold", "fact_billing_reconciliation"): ["claim_id", "bill_id"],
            ("gold", "fact_bed_occupancy"): ["bed_occupancy_sk"],
            ("gold", "dim_patient"): ["patient_sk"],
            ("gold", "dim_doctor"): ["doctor_sk"],
        }
        before = {
            key: checksum(spark, sandbox.table_path(*key), cols) for key, cols in keyed.items()
        }

        ingest.run(spark, sandbox, LOGICAL_DATE)
        silver_pipeline.run(spark, sandbox, LOGICAL_DATE)
        gold_pipeline.run(spark, sandbox, LOGICAL_DATE, maintenance=False)

        after = {
            key: checksum(spark, sandbox.table_path(*key), cols) for key, cols in keyed.items()
        }

        drifted = {k: (before[k], after[k]) for k in before if before[k] != after[k]}
        assert not drifted, f"tables changed on replay: {drifted}"

    def test_mpi_ids_survive_a_rerun(self, spark, sandbox, built):
        """An mpi_id must mean the same person next run as it does this run."""
        from medchain.silver import mpi
        from medchain.utils.audit import RunContext
        from medchain.utils.tables import read

        path = sandbox.table_path("silver", "patient_crosswalk")
        before = {
            (r["hospital_id"], r["patient_id"]): r["mpi_id"]
            for r in read(spark, path).select("hospital_id", "patient_id", "mpi_id").collect()
        }

        mpi.run(spark, sandbox, RunContext.create(LOGICAL_DATE, layer="silver"))

        after = {
            (r["hospital_id"], r["patient_id"]): r["mpi_id"]
            for r in read(spark, path).select("hospital_id", "patient_id", "mpi_id").collect()
        }
        assert before == after, "mpi_id assignments changed between runs"


class TestQualityGate:
    def test_no_blocking_failures(self, spark, sandbox, built):
        from medchain.quality import scorecard

        result = scorecard.run(spark, sandbox, LOGICAL_DATE, fail_on_blocking=False)
        assert result["blocking_failures"] == 0, "the warehouse failed its own integrity checks"
        assert result["checks"] > 30

    def test_recovery_metrics_meet_targets(self, spark, sandbox):
        """Measured against ground truth, not internal consistency."""
        from medchain.quality import scorecard

        mpi_metrics = {r.check_name: r.actual_value for r in scorecard.measure_mpi(spark, sandbox)}
        assert mpi_metrics["mpi.precision"] >= 0.97
        assert mpi_metrics["mpi.recall"] >= 0.80

        claims = {
            r.check_name: r.actual_value
            for r in scorecard.measure_claim_reconstruction(spark, sandbox)
        }
        assert claims["claims.reconstruction_coverage"] >= 0.85
        # Fidelity must be exactly 1.0: recovering a transition that never happened
        # is invention, not reconstruction.
        assert claims["claims.reconstruction_fidelity"] == 1.0


class TestBusinessQuestions:
    def test_all_seven_return_results(self, spark, sandbox, built):
        from medchain.gold import business_questions as bq

        bq.register_views(spark, sandbox)
        assert len(bq.QUESTIONS) == 7
        for question in bq.QUESTIONS:
            df = spark.sql(question.sql)
            assert df.count() >= 0, f"BQ{question.number} failed to execute"
            df.limit(5).collect()  # force evaluation

    def test_mpi_reveals_additional_readmissions(self, spark, sandbox, built):
        """The headline clinical claim, asserted rather than assumed."""
        from pyspark.sql import functions as F

        from medchain.utils.tables import read

        visits = read(spark, sandbox.table_path("gold", "fact_patient_visit")).filter(
            F.col("is_inpatient")
        )
        totals = visits.agg(
            F.sum(F.col("readmit_30d_network").cast("int")).alias("network"),
            F.sum(F.col("readmit_30d_same_hospital").cast("int")).alias("same"),
        ).collect()[0]
        assert totals["network"] >= totals["same"], (
            "network-wide readmissions must be a superset of single-hospital ones"
        )
