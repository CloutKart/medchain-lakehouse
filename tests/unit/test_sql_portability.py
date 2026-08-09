"""The export SQL has to parse in both engines it runs on.

The export has two backends — DuckDB locally, Spark on the cluster — running the
*same* SQL strings. That is deliberate: one set of queries means the two cannot drift
apart and publish different numbers. It also means a construct valid in only one
dialect is a latent failure that shows up on the cluster, minutes into a run, after
paying for a cluster start.

That happened: `MAX(run_ts)::VARCHAR` is fine in DuckDB and a parse error in Spark,
which has no bare VARCHAR type. Ten minutes of cluster to discover a typo. These
tests are static — no engine required — so the same class of bug fails in under a
second instead.
"""

from __future__ import annotations

import re

import pytest

from medchain.web import export

# Every panel function, so a new one is covered automatically rather than needing to
# be remembered.
PANEL_SOURCES = {name: fn for name, fn in export.PANELS.items()}


def sql_text(name: str) -> str:
    """The SQL literals inside a panel function, as one blob."""
    import inspect

    return inspect.getsource(PANEL_SOURCES[name])


ALL_SQL = "\n".join(sql_text(name) for name in PANEL_SOURCES)


class TestDialectPortability:
    def test_no_postgres_style_casts(self):
        """`expr::TYPE` is DuckDB and Postgres.

        Databricks only accepts `::` from DBR 14 onward and never accepts a bare
        VARCHAR. `CAST(x AS y)` parses identically in both engines and costs nothing.
        """
        offenders = re.findall(r"\w+::[A-Za-z]+", ALL_SQL)
        assert not offenders, (
            f"use CAST(... AS ...) instead of {sorted(set(offenders))} — "
            "`::` is not portable to Spark SQL"
        )

    def test_no_duckdb_only_functions(self):
        """Functions that exist in one engine and not the other."""
        # MEDIAN is deliberately NOT banned: DuckDB has always had it and Spark
        # gained it in 3.4, which DBR 15.4 exceeds. PERCENTILE_APPROX is the trap in
        # the other direction — Spark-only, and swapping to it "for portability"
        # breaks the local path instead. This test caught exactly that mistake.
        banned = {
            "PERCENTILE_APPROX(": "use MEDIAN(x) — percentile_approx is Spark-only",
            "QUANTILE_CONT(": "DuckDB-only; use MEDIAN(x)",
            "LIST(": "use COLLECT_LIST",
            "STRING_AGG(": "use CONCAT_WS with COLLECT_LIST",
            "EPOCH(": "use UNIX_TIMESTAMP",
            "STRFTIME(": "use DATE_FORMAT",
        }
        found = {fn: fix for fn, fix in banned.items() if fn in ALL_SQL.upper()}
        assert not found, f"non-portable functions: {found}"

    def test_no_varchar_type_name(self):
        """Spark's string type is STRING; VARCHAR without a length does not parse."""
        assert "AS VARCHAR" not in ALL_SQL.upper(), "use CAST(... AS STRING)"

    @pytest.mark.parametrize("panel", sorted(PANEL_SOURCES))
    def test_panel_sql_parses_in_duckdb(self, panel):
        """Every query at least parses. Catches typos without touching the warehouse.

        Uses EXPLAIN against empty stand-in tables, so it validates syntax and column
        references without needing a built Gold layer.
        """
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")

        # Minimal stand-ins with the columns the queries reference. If a query starts
        # using a column that Gold does not have, this fails here rather than in a
        # dashboard panel that silently renders empty.
        con.execute("""
            CREATE TABLE fact_patient_visit (visit_sk BIGINT, mpi_id VARCHAR, doctor_id VARCHAR,
              doctor_sk BIGINT, hospital_sk BIGINT, procedure_sk BIGINT, admission_date_sk INT,
              is_inpatient BOOLEAN, length_of_stay INT, readmit_30d_network BOOLEAN,
              readmit_30d_same_hospital BOOLEAN, readmit_cross_hospital_only BOOLEAN,
              department_at_visit VARCHAR, department_current VARCHAR);
            CREATE TABLE dim_hospital (hospital_sk BIGINT, hospital_name VARCHAR, city VARCHAR,
              tier VARCHAR, bed_capacity INT, total_beds INT, size_band VARCHAR);
            CREATE TABLE dim_date (date_sk INT, month_start DATE, fy_label VARCHAR,
              fy_quarter_label VARCHAR);
            CREATE TABLE dim_patient (patient_sk BIGINT, registered_hospital_count INT);
            CREATE TABLE dim_procedure (procedure_sk BIGINT, procedure_name VARCHAR,
              specialty VARCHAR, icd10_source VARCHAR);
            CREATE TABLE dim_insurer (insurer_sk BIGINT, insurer_name VARCHAR, tpa_name VARCHAR,
              scheme_type VARCHAR);
            CREATE TABLE dim_doctor (doctor_sk BIGINT);
            CREATE TABLE fact_bed_occupancy (hospital_sk BIGINT, date_sk INT, ward_id VARCHAR,
              ward_type VARCHAR, occupancy_rate DOUBLE, occupied_beds INT, bed_count INT,
              avg_length_of_stay DOUBLE);
            CREATE TABLE fact_claim_lifecycle (claim_id VARCHAR, hospital_sk BIGINT,
              insurer_sk BIGINT, status_code VARCHAR, prev_status VARCHAR,
              days_in_prev_status INT, claim_amount DOUBLE, rejection_reason VARCHAR);
            CREATE TABLE fact_billing_reconciliation (claim_id VARCHAR, hospital_sk BIGINT,
              insurer_sk BIGINT, billed_amount DOUBLE, net_reimbursement DOUBLE,
              reimbursement_gap DOUBLE, excluded_amount DOUBLE, room_rent_excess DOUBLE,
              copay_amount DOUBLE, other_deduction DOUBLE, is_reconciled BOOLEAN,
              variance_class VARCHAR);
            CREATE TABLE dq_scorecard (run_ts TIMESTAMP, check_name VARCHAR, check_type VARCHAR,
              layer VARCHAR, table_name VARCHAR, severity VARCHAR, passed BOOLEAN,
              actual_value DOUBLE, threshold DOUBLE, comparison VARCHAR, detail VARCHAR);
        """)

        class ExplainOnly:
            def execute(self, sql: str):
                con.execute("EXPLAIN " + sql)
                return con.execute("SELECT 1 WHERE FALSE")

        # `one()` raises on an empty result, which EXPLAIN-only tables always give.
        # The parse is what is being tested, so that exception is the pass signal.
        try:
            PANEL_SOURCES[panel](ExplainOnly())
        except ValueError as exc:
            assert "returned no rows" in str(exc)
        except Exception as exc:  # noqa: BLE001 - anything else is a real SQL fault
            pytest.fail(f"{panel} SQL failed to parse: {str(exc)[:400]}")
