"""Export the Gold layer to pre-aggregated JSON for the web dashboard.

A browser cannot read Delta. Rather than standing up an API, the dashboard is fed
compact JSON computed here: the page then loads instantly, hosts anywhere static,
and needs no server, no cluster and no storage credentials in the browser.

Two properties this module has to preserve:

**Nothing raw leaves the warehouse.** Every query aggregates. The largest output is a
few hundred rows of ward-month occupancy — no patient rows, no claim rows, nothing
that would be a disclosure risk if the site were public. That is checked by a test,
not by intent.

**The numbers are the warehouse's numbers.** The queries here are the same ones in
``medchain.gold.business_questions``; the export runs them rather than restating
them. ``tests/`` asserts the emitted JSON against the Gold tables so the dashboard
cannot quietly drift from the platform it is reporting on.

Reads through ``cfg.table_path("gold", …)``, so ``MEDCHAIN_ENV=azure`` targets ADLS
with no code change.
"""

# ruff: noqa: E501 - the SQL below is aligned for legibility in a SQL editor,
# not to a Python line-length limit. Reflowing it makes the queries harder to read
# and harder to paste out for debugging.

from __future__ import annotations

import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any

from medchain.config import Config, load_config
from medchain.utils.logging import banner, get_logger

log = get_logger("medchain.web.export")

GOLD_TABLES = [
    "dim_date",
    "dim_patient",
    "dim_doctor",
    "dim_hospital",
    "dim_insurer",
    "dim_procedure",
    "fact_patient_visit",
    "fact_claim_lifecycle",
    "fact_billing_reconciliation",
    "fact_bed_occupancy",
    "dq_scorecard",
]

DEFAULT_OUT = Path(__file__).resolve().parents[3] / "dashboards" / "web" / "public" / "data"


def connect(cfg: Config):
    """Register every Gold Delta table as a DuckDB view.

    DuckDB reads Delta through delta-rs, so this needs neither Spark nor a running
    SQL warehouse — it reads exactly the files the pipeline wrote. On Azure the
    credential chain picks up the same ``az login`` used everywhere else.
    """
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("INSTALL delta; LOAD delta;")
    if cfg.is_azure:
        con.execute("INSTALL azure; LOAD azure;")
        con.execute("CREATE SECRET (TYPE azure, PROVIDER credential_chain);")

    registered: list[str] = []
    for table in GOLD_TABLES:
        path = cfg.table_path("gold", table)
        try:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM delta_scan('{path}')")
            registered.append(table)
        except Exception as exc:  # noqa: BLE001 - a missing table is reported, not fatal
            log.warning("  gold.%s unavailable (%s)", table, type(exc).__name__)
    if not registered:
        raise RuntimeError(
            f"No Gold tables found under {cfg.path('gold')}. Build them with `make run-local`."
        )
    log.info("  registered %d/%d Gold tables", len(registered), len(GOLD_TABLES))
    return con


def _clean(value: Any) -> Any:
    """Make a DuckDB value JSON-safe.

    NaN and Infinity are the interesting cases: ``json.dumps`` emits them as bare
    ``NaN``/``Infinity`` tokens, which are valid Python and invalid JSON, so the
    browser's ``JSON.parse`` throws on a file that looked fine when written.
    """
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else round(value, 6)
    if isinstance(value, (int, str, bool)):
        return value
    from decimal import Decimal

    if isinstance(value, Decimal):
        return round(float(value), 6)
    return str(value)


def rows(con, sql: str) -> list[dict[str, Any]]:
    """Run a query and return JSON-safe row dicts."""
    cur = con.execute(sql)
    columns = [d[0] for d in cur.description]
    return [{c: _clean(v) for c, v in zip(columns, record)} for record in cur.fetchall()]


def one(con, sql: str) -> dict[str, Any]:
    """Run a query expected to produce exactly one row."""
    result = rows(con, sql)
    if not result:
        raise ValueError(f"Query returned no rows:\n{sql}")
    return result[0]


# --------------------------------------------------------------------- panels


def headline(con) -> dict[str, Any]:
    """The three findings the dashboard leads with, plus supporting totals.

    These are the numbers a reader should leave with, so they are computed once here
    and reused rather than recomputed per section where they could disagree.
    """
    clinical = one(
        con,
        """
        SELECT
          COUNT(*)                                                    AS visits,
          COUNT(DISTINCT mpi_id)                                      AS patients,
          SUM(CASE WHEN is_inpatient THEN 1 ELSE 0 END)               AS inpatient_visits,
          AVG(CASE WHEN is_inpatient THEN readmit_30d_network::INT END)       AS rate_network,
          AVG(CASE WHEN is_inpatient THEN readmit_30d_same_hospital::INT END) AS rate_hospital,
          SUM(CASE WHEN readmit_cross_hospital_only THEN 1 ELSE 0 END) AS hidden_readmissions
        FROM fact_patient_visit
    """,
    )
    clinical["readmission_gap_pp"] = (
        (clinical["rate_network"] or 0) - (clinical["rate_hospital"] or 0)
    ) * 100

    # Misattribution: consultations that would land in the wrong department without
    # point-in-time SCD2 history. Summed as absolute difference per department.
    attribution = one(
        con,
        """
        WITH pit AS (
          SELECT department_at_visit AS department, COUNT(*) AS correct
          FROM fact_patient_visit WHERE department_at_visit IS NOT NULL GROUP BY 1
        ), naive AS (
          SELECT department_current AS department, COUNT(*) AS naive
          FROM fact_patient_visit WHERE department_current IS NOT NULL GROUP BY 1
        ), joined AS (
          SELECT COALESCE(p.department, n.department) AS department,
                 COALESCE(p.correct, 0) AS correct, COALESCE(n.naive, 0) AS naive
          FROM pit p FULL OUTER JOIN naive n ON p.department = n.department
        )
        SELECT SUM(ABS(naive - correct)) AS misattributed,
               SUM(correct)              AS total_attributed
        FROM joined
    """,
    )

    financial = one(
        con,
        """
        SELECT
          SUM(billed_amount)      AS billed,
          SUM(net_reimbursement)  AS reimbursed,
          SUM(reimbursement_gap)  AS gap,
          SUM(excluded_amount)    AS excluded,
          SUM(room_rent_excess)   AS room_excess,
          SUM(copay_amount)       AS copay,
          SUM(other_deduction)    AS other_deduction,
          AVG(is_reconciled::INT) AS reconciled_rate,
          COUNT(*)                AS claims
        FROM fact_billing_reconciliation
    """,
    )

    return {
        "clinical": clinical,
        "attribution": attribution,
        "financial": financial,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }


def clinical(con) -> dict[str, Any]:
    return {
        # Dumbbell: the gap between the two rates per hospital IS the finding.
        "readmission_by_hospital": rows(
            con,
            """
            SELECT h.hospital_name, h.city,
                   COUNT(*)                                          AS discharges,
                   AVG(v.readmit_30d_same_hospital::INT) * 100        AS rate_hospital,
                   AVG(v.readmit_30d_network::INT) * 100              AS rate_network,
                   SUM(v.readmit_cross_hospital_only::INT)            AS hidden
            FROM fact_patient_visit v
            JOIN dim_hospital h ON v.hospital_sk = h.hospital_sk
            WHERE v.is_inpatient
            GROUP BY 1, 2
            ORDER BY (AVG(v.readmit_30d_network::INT) - AVG(v.readmit_30d_same_hospital::INT)) DESC
        """,
        ),
        # Two series on one time axis, emitted separately so the frontend renders two
        # stacked charts rather than a dual-axis chart.
        "monthly": rows(
            con,
            """
            SELECT d.month_start                                  AS month,
                   COUNT(*)                                       AS visits,
                   SUM(v.is_inpatient::INT)                       AS inpatient,
                   AVG(CASE WHEN v.is_inpatient THEN v.readmit_30d_network::INT END) * 100
                                                                  AS readmission_pct
            FROM fact_patient_visit v
            JOIN dim_date d ON v.admission_date_sk = d.date_sk
            GROUP BY 1 ORDER BY 1
        """,
        ),
        "registration_spread": rows(
            con,
            """
            SELECT registered_hospital_count AS hospitals, COUNT(*) AS patients
            FROM dim_patient GROUP BY 1 ORDER BY 1
        """,
        ),
        "top_procedures": rows(
            con,
            """
            SELECT p.procedure_name, p.specialty,
                   COUNT(*)                       AS episodes,
                   AVG(v.length_of_stay)          AS avg_los,
                   AVG(v.readmit_30d_network::INT) * 100 AS readmission_pct
            FROM fact_patient_visit v
            JOIN dim_procedure p ON v.procedure_sk = p.procedure_sk
            WHERE v.is_inpatient
            GROUP BY 1, 2 HAVING COUNT(*) >= 200
            ORDER BY episodes DESC LIMIT 12
        """,
        ),
    }


def operational(con) -> dict[str, Any]:
    return {
        # Heatmap: grid magnitude, one sequential hue.
        "occupancy_grid": rows(
            con,
            """
            SELECT h.hospital_name, b.ward_type,
                   AVG(b.occupancy_rate) * 100 AS occupancy_pct,
                   SUM(b.occupied_beds)        AS bed_days,
                   MAX(b.bed_count)            AS beds
            FROM fact_bed_occupancy b
            JOIN dim_hospital h ON b.hospital_sk = h.hospital_sk
            GROUP BY 1, 2
        """,
        ),
        "occupancy_monthly": rows(
            con,
            """
            SELECT d.month_start AS month, b.ward_type,
                   AVG(b.occupancy_rate) * 100 AS occupancy_pct
            FROM fact_bed_occupancy b
            JOIN dim_date d ON b.date_sk = d.date_sk
            GROUP BY 1, 2 ORDER BY 1, 2
        """,
        ),
        "pressure_wards": rows(
            con,
            """
            SELECT h.hospital_name, b.ward_id, b.ward_type,
                   MAX(b.bed_count)                                     AS beds,
                   AVG(b.occupancy_rate) * 100                          AS avg_occupancy_pct,
                   SUM(CASE WHEN b.occupancy_rate >= 0.85 THEN 1 ELSE 0 END) AS days_above_85,
                   AVG(b.avg_length_of_stay)                            AS alos
            FROM fact_bed_occupancy b
            JOIN dim_hospital h ON b.hospital_sk = h.hospital_sk
            GROUP BY 1, 2, 3
            HAVING SUM(CASE WHEN b.occupancy_rate >= 0.85 THEN 1 ELSE 0 END) > 20
            ORDER BY days_above_85 DESC LIMIT 20
        """,
        ),
        # Diverging: signed difference around zero.
        "attribution_by_department": rows(
            con,
            """
            WITH pit AS (
              SELECT department_at_visit AS department, COUNT(*) AS correct
              FROM fact_patient_visit WHERE department_at_visit IS NOT NULL GROUP BY 1
            ), naive AS (
              SELECT department_current AS department, COUNT(*) AS naive
              FROM fact_patient_visit WHERE department_current IS NOT NULL GROUP BY 1
            )
            SELECT COALESCE(p.department, n.department) AS department,
                   COALESCE(p.correct, 0)               AS correct,
                   COALESCE(n.naive, 0)                 AS naive,
                   COALESCE(n.naive, 0) - COALESCE(p.correct, 0) AS misattributed
            FROM pit p FULL OUTER JOIN naive n ON p.department = n.department
            ORDER BY misattributed
        """,
        ),
        "doctor_utilisation": rows(
            con,
            """
            SELECT department_at_visit AS department,
                   COUNT(*)                     AS consultations,
                   COUNT(DISTINCT doctor_id)    AS doctors,
                   COUNT(*)::DOUBLE / NULLIF(COUNT(DISTINCT doctor_id), 0) AS per_doctor
            FROM fact_patient_visit
            WHERE department_at_visit IS NOT NULL
            GROUP BY 1 ORDER BY per_doctor DESC
        """,
        ),
    }


def financial(con) -> dict[str, Any]:
    return {
        # Waterfall stages, with each deduction marked recoverable or contractual.
        # That distinction is the actionable part: room-rent excess disappears if
        # admission room category matches policy entitlement; co-pay never does.
        "waterfall": rows(
            con,
            """
            SELECT 'Billed'            AS stage, SUM(billed_amount)     AS amount, 'total'       AS kind, FALSE AS recoverable FROM fact_billing_reconciliation
            UNION ALL SELECT 'Exclusions',       -SUM(excluded_amount),  'deduction', FALSE FROM fact_billing_reconciliation
            UNION ALL SELECT 'Room rent excess', -SUM(room_rent_excess), 'deduction', TRUE  FROM fact_billing_reconciliation
            UNION ALL SELECT 'Co-pay',           -SUM(copay_amount),     'deduction', FALSE FROM fact_billing_reconciliation
            UNION ALL SELECT 'Other deductions', -SUM(other_deduction),  'deduction', FALSE FROM fact_billing_reconciliation
            UNION ALL SELECT 'Net reimbursed',    SUM(net_reimbursement),'total',     FALSE FROM fact_billing_reconciliation
        """,
        ),
        "gap_by_hospital": rows(
            con,
            """
            SELECT h.hospital_name, h.city, i.insurer_name,
                   SUM(r.billed_amount)      AS billed,
                   SUM(r.net_reimbursement)  AS reimbursed,
                   SUM(r.reimbursement_gap)  AS gap,
                   SUM(r.room_rent_excess)   AS recoverable,
                   SUM(r.reimbursement_gap) / NULLIF(SUM(r.billed_amount), 0) * 100 AS gap_pct
            FROM fact_billing_reconciliation r
            JOIN dim_hospital h ON r.hospital_sk = h.hospital_sk
            JOIN dim_insurer  i ON r.insurer_sk = i.insurer_sk
            GROUP BY 1, 2, 3 ORDER BY gap DESC
        """,
        ),
        # Ordered stages rather than a funnel: the question is how many claims
        # reached each state and where they stall, not flow through a pipe.
        "lifecycle_stages": rows(
            con,
            """
            SELECT status_code,
                   COUNT(DISTINCT claim_id) AS claims,
                   AVG(days_in_prev_status) AS avg_days_in_prev
            FROM fact_claim_lifecycle GROUP BY 1
        """,
        ),
        "dwell_by_stage": rows(
            con,
            """
            SELECT i.insurer_name, f.prev_status AS stage,
                   MEDIAN(f.days_in_prev_status) AS median_days,
                   COUNT(*)                      AS transitions
            FROM fact_claim_lifecycle f
            JOIN dim_insurer i ON f.insurer_sk = i.insurer_sk
            WHERE f.prev_status IS NOT NULL AND f.days_in_prev_status IS NOT NULL
            GROUP BY 1, 2 ORDER BY median_days DESC
        """,
        ),
        "denial_reasons": rows(
            con,
            """
            SELECT rejection_reason,
                   COUNT(DISTINCT claim_id)        AS claims,
                   SUM(claim_amount)               AS value,
                   COUNT(DISTINCT hospital_sk)     AS hospitals_affected
            FROM fact_claim_lifecycle
            WHERE status_code = 'Rejected' AND rejection_reason IS NOT NULL
            GROUP BY 1 ORDER BY value DESC
        """,
        ),
        "variance_classes": rows(
            con,
            """
            SELECT variance_class, COUNT(*) AS claims
            FROM fact_billing_reconciliation GROUP BY 1 ORDER BY claims DESC
        """,
        ),
    }


def quality(con) -> dict[str, Any]:
    latest = "(SELECT MAX(run_ts) FROM dq_scorecard)"
    return {
        "checks": rows(
            con,
            f"""
            SELECT check_name, check_type, layer, table_name, severity,
                   passed, actual_value, threshold, comparison, detail
            FROM dq_scorecard WHERE run_ts = {latest}
            ORDER BY check_type, passed, check_name
        """,
        ),
        "summary": one(
            con,
            f"""
            SELECT COUNT(*)                                                   AS total,
                   SUM(passed::INT)                                           AS passed,
                   SUM(CASE WHEN NOT passed AND severity = 'blocking' THEN 1 ELSE 0 END) AS blocking_failures,
                   SUM(CASE WHEN NOT passed AND severity <> 'blocking' THEN 1 ELSE 0 END) AS warnings,
                   -- Cast to text in SQL. Returning a timestamptz makes DuckDB reach
                   -- for pytz to localise it, which is a dependency this export does
                   -- not otherwise need, and the frontend wants a string regardless.
                   MAX(run_ts)::VARCHAR                                       AS run_ts
            FROM dq_scorecard WHERE run_ts = {latest}
        """,
        ),
    }


def reference(con) -> dict[str, Any]:
    """Small dimension summaries the frontend uses for labels and context."""
    return {
        "hospitals": rows(
            con,
            """
            SELECT hospital_name, city, tier, bed_capacity, total_beds, size_band
            FROM dim_hospital ORDER BY city, hospital_name
        """,
        ),
        "insurers": rows(con, "SELECT insurer_name, tpa_name, scheme_type FROM dim_insurer"),
        "icd_provenance": rows(
            con,
            """
            SELECT icd10_source, COUNT(*) AS procedures
            FROM dim_procedure GROUP BY 1 ORDER BY procedures DESC
        """,
        ),
        "counts": one(
            con,
            """
            SELECT (SELECT COUNT(*) FROM fact_patient_visit)            AS visits,
                   (SELECT COUNT(*) FROM dim_patient)                   AS patients,
                   (SELECT COUNT(DISTINCT claim_id) FROM fact_claim_lifecycle) AS claims,
                   (SELECT COUNT(*) FROM fact_claim_lifecycle)          AS claim_transitions,
                   (SELECT COUNT(*) FROM fact_bed_occupancy)            AS ward_days,
                   (SELECT COUNT(*) FROM dim_doctor)                    AS doctor_versions
        """,
        ),
    }


PANELS = {
    "headline": headline,
    "clinical": clinical,
    "operational": operational,
    "financial": financial,
    "quality": quality,
    "reference": reference,
}


def export(cfg: Config, out_dir: Path) -> dict[str, int]:
    """Run every panel and write one JSON file each."""
    out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(cfg)
    sizes: dict[str, int] = {}
    try:
        for name, fn in PANELS.items():
            payload = fn(con)
            target = out_dir / f"{name}.json"
            # separators drops the whitespace; these files are machine-read and the
            # saving is roughly a third of the transfer.
            text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
            target.write_text(text, encoding="utf-8")
            sizes[name] = len(text)
            log.info("  %-12s %8.1f KB", f"{name}.json", len(text) / 1024)
    finally:
        con.close()
    return sizes


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Export Gold to JSON for the web dashboard")
    parser.add_argument("--env", default=None, help="Config environment (default: $MEDCHAIN_ENV)")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output directory")
    args = parser.parse_args(argv)

    cfg = load_config(args.env)
    banner(log, "WEB EXPORT", environment=cfg.env, source=cfg.path("gold"), target=args.out)

    sizes = export(cfg, args.out)
    total = sum(sizes.values())
    log.info("")
    log.info("Wrote %d files, %.1f KB total, to %s", len(sizes), total / 1024, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
