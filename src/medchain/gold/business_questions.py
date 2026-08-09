"""The seven business questions, answered from the Gold star schema.

Each is a real question one of the three stakeholder groups in the spec asks, with
the SQL that answers it. They double as an acceptance test: if the star schema is
built correctly these return sensible numbers, and if a join or a grain is wrong
they return obvious nonsense.

Two of them exist specifically to quantify what the hard engineering bought:

* **BQ1** compares the readmission rate measured network-wide (through the MPI)
  against the same rate measured one hospital at a time. The difference is the
  readmission cohort no individual hospital can see.
* **BQ4** compares doctor utilisation attributed by point-in-time SCD2 department
  against attribution by the doctor's *current* department. The difference is the
  misattribution that tracking history prevents.
"""

# ruff: noqa: E501 - the SQL below is formatted for legibility in a SQL editor,
# not for a Python line-length limit. Reflowing it makes the queries harder to read
# and harder to paste out.

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from medchain.config import Config
from medchain.utils.logging import get_logger

log = get_logger("medchain.gold.bq")


@dataclass
class BusinessQuestion:
    number: int
    audience: str
    question: str
    sql: str
    interpretation: str


QUESTIONS: list[BusinessQuestion] = [
    BusinessQuestion(
        number=1,
        audience="Clinical",
        question=(
            "What is the 30-day readmission rate, and how much higher is it when "
            "measured network-wide through the Master Patient Index than when each "
            "hospital counts only its own returning patients?"
        ),
        sql="""
            SELECT
              h.city,
              h.hospital_name,
              COUNT(*)                                              AS inpatient_discharges,
              SUM(CAST(v.readmit_30d_same_hospital AS INT))         AS readmit_same_hospital,
              SUM(CAST(v.readmit_30d_network AS INT))               AS readmit_network,
              SUM(CAST(v.readmit_cross_hospital_only AS INT))       AS readmit_missed_by_hospital,
              ROUND(100.0 * AVG(CAST(v.readmit_30d_same_hospital AS INT)), 2) AS rate_same_hospital_pct,
              ROUND(100.0 * AVG(CAST(v.readmit_30d_network AS INT)), 2)       AS rate_network_pct,
              ROUND(
                100.0 * AVG(CAST(v.readmit_30d_network AS INT))
                - 100.0 * AVG(CAST(v.readmit_30d_same_hospital AS INT)), 2
              )                                                     AS understatement_pct_points
            FROM fact_patient_visit v
            JOIN dim_hospital h ON v.hospital_sk = h.hospital_sk
            WHERE v.is_inpatient
            GROUP BY h.city, h.hospital_name
            ORDER BY understatement_pct_points DESC
        """,
        interpretation=(
            "Every row where understatement_pct_points > 0 is a readmission the "
            "hospital's own reporting cannot see, because the patient returned "
            "somewhere else in the network under a different patient_id. This is the "
            "direct clinical value of identity resolution."
        ),
    ),
    BusinessQuestion(
        number=2,
        audience="Clinical",
        question=(
            "For the ten most common procedures, how long is the full patient "
            "journey from admission to discharge to claim settlement?"
        ),
        sql="""
            WITH settled AS (
              SELECT claim_id, mpi_id, hospital_sk,
                     MIN(CASE WHEN status_code = 'Submitted' THEN status_date END) AS submitted_on,
                     MIN(CASE WHEN status_code = 'Settled'   THEN status_date END) AS settled_on
              FROM fact_claim_lifecycle
              GROUP BY claim_id, mpi_id, hospital_sk
            ),
            journeys AS (
              SELECT
                p.procedure_name,
                p.specialty,
                v.length_of_stay,
                DATEDIFF(s.settled_on, s.submitted_on) AS days_to_settle,
                DATEDIFF(s.settled_on, v.admission_date) AS total_journey_days
              FROM fact_patient_visit v
              JOIN dim_procedure p ON v.procedure_sk = p.procedure_sk
              JOIN settled s ON s.mpi_id = v.mpi_id AND s.hospital_sk = v.hospital_sk
              WHERE v.is_inpatient AND s.settled_on IS NOT NULL
            )
            SELECT
              procedure_name, specialty,
              COUNT(*)                                              AS episodes,
              ROUND(AVG(length_of_stay), 1)                         AS avg_length_of_stay,
              PERCENTILE_APPROX(days_to_settle, 0.5)                AS median_days_to_settle,
              PERCENTILE_APPROX(total_journey_days, 0.5)            AS median_total_journey_days,
              PERCENTILE_APPROX(total_journey_days, 0.9)            AS p90_total_journey_days
            FROM journeys
            GROUP BY procedure_name, specialty
            HAVING COUNT(*) >= 50
            ORDER BY episodes DESC
            LIMIT 10
        """,
        interpretation=(
            "The gap between median and p90 is where the operational pain sits: the "
            "typical journey is manageable, the tail is what patients complain about "
            "and what ties up working capital."
        ),
    ),
    BusinessQuestion(
        number=3,
        audience="Operational",
        question=(
            "Which hospital-ward combinations run above 85% occupancy for more than "
            "20 days a quarter, and how does their length of stay compare to the "
            "network median?"
        ),
        sql="""
            WITH network_alos AS (
              SELECT PERCENTILE_APPROX(avg_length_of_stay, 0.5) AS median_alos
              FROM fact_bed_occupancy
              WHERE avg_length_of_stay IS NOT NULL
            ),
            quarterly AS (
              SELECT
                h.hospital_name, h.city, b.ward_id, b.ward_type,
                d.fy_label, d.fy_quarter_label,
                COUNT(*)                                             AS days_observed,
                SUM(CASE WHEN b.occupancy_rate >= 0.85 THEN 1 ELSE 0 END) AS days_above_85pct,
                ROUND(AVG(b.occupancy_rate) * 100, 1)                AS avg_occupancy_pct,
                ROUND(AVG(b.avg_length_of_stay), 2)                  AS ward_alos,
                MAX(b.bed_count)                                     AS beds
              FROM fact_bed_occupancy b
              JOIN dim_hospital h ON b.hospital_sk = h.hospital_sk
              JOIN dim_date d     ON b.date_sk = d.date_sk
              GROUP BY h.hospital_name, h.city, b.ward_id, b.ward_type, d.fy_label, d.fy_quarter_label
            )
            SELECT
              q.*, ROUND(n.median_alos, 2) AS network_median_alos,
              ROUND(q.ward_alos - n.median_alos, 2) AS alos_vs_network
            FROM quarterly q CROSS JOIN network_alos n
            WHERE q.days_above_85pct > 20
            ORDER BY q.days_above_85pct DESC, q.avg_occupancy_pct DESC
            LIMIT 30
        """,
        interpretation=(
            "Sustained high occupancy with a longer-than-median stay points at "
            "discharge process, not capacity. Sustained high occupancy with a normal "
            "stay is genuine capacity pressure and the case for more beds."
        ),
    ),
    BusinessQuestion(
        number=4,
        audience="Operational",
        question=(
            "What is consultation volume by department, and how much does it change "
            "when attributed by the doctor's department at the time of the visit "
            "instead of their current department?"
        ),
        sql="""
            WITH pit AS (
              SELECT department_at_visit AS department, COUNT(*) AS visits_point_in_time
              FROM fact_patient_visit
              WHERE department_at_visit IS NOT NULL
              GROUP BY department_at_visit
            ),
            naive AS (
              SELECT department_current AS department, COUNT(*) AS visits_current_dept
              FROM fact_patient_visit
              WHERE department_current IS NOT NULL
              GROUP BY department_current
            )
            SELECT
              COALESCE(p.department, n.department)                  AS department,
              COALESCE(p.visits_point_in_time, 0)                   AS visits_correct,
              COALESCE(n.visits_current_dept, 0)                    AS visits_if_naive,
              COALESCE(n.visits_current_dept, 0) - COALESCE(p.visits_point_in_time, 0)
                                                                    AS misattribution,
              ROUND(
                100.0 * (COALESCE(n.visits_current_dept, 0) - COALESCE(p.visits_point_in_time, 0))
                / NULLIF(COALESCE(p.visits_point_in_time, 0), 0), 1
              )                                                     AS misattribution_pct
            FROM pit p FULL OUTER JOIN naive n ON p.department = n.department
            ORDER BY ABS(COALESCE(n.visits_current_dept, 0) - COALESCE(p.visits_point_in_time, 0)) DESC
        """,
        interpretation=(
            "misattribution is the number of consultations that would be credited to "
            "the wrong department without SCD Type 2 history. Departments with large "
            "positive values absorb work they never did; large negative values lose "
            "credit for work they performed."
        ),
    ),
    BusinessQuestion(
        number=5,
        audience="Financial",
        question=(
            "What is the claim settlement rate by insurer, and at which lifecycle "
            "stage do claims spend the longest?"
        ),
        sql="""
            WITH per_claim AS (
              SELECT
                claim_id, insurer_sk,
                MAX(CASE WHEN status_code = 'Settled'  THEN 1 ELSE 0 END) AS was_settled,
                MAX(CASE WHEN status_code = 'Rejected' THEN 1 ELSE 0 END) AS was_rejected,
                MAX(CASE WHEN is_terminal THEN 1 ELSE 0 END)              AS reached_terminal,
                MAX(days_since_submission)                                AS days_to_resolution
              FROM fact_claim_lifecycle
              GROUP BY claim_id, insurer_sk
            ),
            dwell AS (
              SELECT insurer_sk, prev_status AS stage,
                     PERCENTILE_APPROX(days_in_prev_status, 0.5) AS median_days_in_stage,
                     COUNT(*) AS transitions
              FROM fact_claim_lifecycle
              WHERE prev_status IS NOT NULL AND days_in_prev_status IS NOT NULL
              GROUP BY insurer_sk, prev_status
            )
            SELECT
              i.insurer_name, i.scheme_type, d.stage,
              d.median_days_in_stage, d.transitions,
              c.claims, c.settlement_rate_pct, c.rejection_rate_pct, c.median_days_to_resolution
            FROM dwell d
            JOIN dim_insurer i ON d.insurer_sk = i.insurer_sk
            JOIN (
              SELECT insurer_sk,
                     COUNT(*) AS claims,
                     ROUND(100.0 * AVG(was_settled), 2)  AS settlement_rate_pct,
                     ROUND(100.0 * AVG(was_rejected), 2) AS rejection_rate_pct,
                     PERCENTILE_APPROX(days_to_resolution, 0.5) AS median_days_to_resolution
              FROM per_claim GROUP BY insurer_sk
            ) c ON c.insurer_sk = d.insurer_sk
            ORDER BY i.insurer_name, d.median_days_in_stage DESC
        """,
        interpretation=(
            "The stage with the highest median dwell is where to push the insurer. "
            "This is only answerable because the lifecycle history was reconstructed "
            "— the portal's current-state export cannot support it at all."
        ),
    ),
    BusinessQuestion(
        number=6,
        audience="Financial",
        question=(
            "What are the leading claim rejection reasons by value, and which are "
            "systemic rather than one-off?"
        ),
        sql="""
            SELECT
              f.rejection_reason,
              i.insurer_name,
              COUNT(DISTINCT f.claim_id)                       AS rejected_claims,
              ROUND(SUM(f.claim_amount) / 100000, 2)           AS value_lakh_inr,
              ROUND(AVG(f.claim_amount), 0)                    AS avg_claim_value,
              COUNT(DISTINCT f.hospital_sk)                    AS hospitals_affected,
              CASE
                WHEN COUNT(DISTINCT f.hospital_sk) >= 6 THEN 'Systemic - network wide'
                WHEN COUNT(DISTINCT f.hospital_sk) >= 3 THEN 'Regional'
                ELSE 'Localised'
              END                                              AS pattern
            FROM fact_claim_lifecycle f
            JOIN dim_insurer i ON f.insurer_sk = i.insurer_sk
            WHERE f.status_code = 'Rejected' AND f.rejection_reason IS NOT NULL
            GROUP BY f.rejection_reason, i.insurer_name
            ORDER BY value_lakh_inr DESC
        """,
        interpretation=(
            "A reason appearing across six or more hospitals is a process defect worth "
            "fixing centrally — documentation standards, pre-authorisation discipline. "
            "One confined to a single site is a local training issue."
        ),
    ),
    BusinessQuestion(
        number=7,
        audience="Financial",
        question=(
            "What is the gap between what MedChain bills and what it is reimbursed, "
            "decomposed into co-pay, room-rent excess and exclusions — and which "
            "bucket is the largest recoverable amount?"
        ),
        sql="""
            SELECT
              h.hospital_name, h.city, i.insurer_name,
              COUNT(*)                                            AS claims,
              ROUND(SUM(r.billed_amount)      / 10000000, 2)      AS billed_crore,
              ROUND(SUM(r.net_reimbursement)  / 10000000, 2)      AS reimbursed_crore,
              ROUND(SUM(r.reimbursement_gap)  / 10000000, 2)      AS gap_crore,
              ROUND(100.0 * SUM(r.reimbursement_gap) / NULLIF(SUM(r.billed_amount), 0), 1)
                                                                  AS gap_pct,
              ROUND(SUM(r.excluded_amount)    / 10000000, 2)      AS excluded_crore,
              ROUND(SUM(r.room_rent_excess)   / 10000000, 2)      AS room_excess_crore,
              ROUND(SUM(r.copay_amount)       / 10000000, 2)      AS copay_crore,
              ROUND(SUM(r.other_deduction)    / 10000000, 2)      AS other_deduction_crore,
              -- Room-rent excess is the one the hospital controls: admitting within
              -- the policy's room category eliminates it entirely. Co-pay is the
              -- patient's contractual share and is not recoverable.
              ROUND(SUM(r.room_rent_excess)   / 10000000, 2)      AS recoverable_crore
            FROM fact_billing_reconciliation r
            JOIN dim_hospital h ON r.hospital_sk = h.hospital_sk
            JOIN dim_insurer  i ON r.insurer_sk = i.insurer_sk
            GROUP BY h.hospital_name, h.city, i.insurer_name
            ORDER BY gap_crore DESC
        """,
        interpretation=(
            "recoverable_crore is room rent billed above the policy cap — pure "
            "leakage the hospital can eliminate by matching admission room category "
            "to policy entitlement at the point of admission. Co-pay and contractual "
            "deductions are not recoverable and should not be chased."
        ),
    ),
]


def register_views(spark: SparkSession, cfg: Config) -> None:
    """Expose the Gold tables as temp views so the SQL above reads naturally."""
    from medchain.utils.tables import read, table_exists

    tables = [
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
    ]
    for table in tables:
        path = cfg.table_path("gold", table)
        if table_exists(spark, path):
            read(spark, path).createOrReplaceTempView(table)
        else:
            log.warning("gold.%s not found; queries using it will fail", table)


def answer(spark: SparkSession, cfg: Config, number: int) -> DataFrame:
    """Run one question and return its result."""
    register_views(spark, cfg)
    question = next(q for q in QUESTIONS if q.number == number)
    return spark.sql(question.sql)


def answer_all(spark: SparkSession, cfg: Config) -> dict[int, DataFrame]:
    register_views(spark, cfg)
    return {q.number: spark.sql(q.sql) for q in QUESTIONS}
