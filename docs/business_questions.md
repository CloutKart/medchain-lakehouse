# The Seven Business Questions

Each question below is one a stakeholder group in the problem statement actually asks,
answered from the Gold star schema. They double as an acceptance test: a correctly
built star schema returns sensible numbers, and a wrong join or grain returns obvious
nonsense.

Results are from the full 3-year dataset — 619,601 visits, 208,456 claims, 8 hospitals.
Tables are truncated to the top rows; run them yourself with:

```python
from medchain.gold import business_questions as bq
bq.register_views(spark, cfg)
spark.sql(bq.QUESTIONS[0].sql).show()
```

---

## BQ1 — Clinical

> **What is the 30-day readmission rate, and how much higher is it when measured network-wide through the Master Patient Index than when each hospital counts only its own returning patients?**

### Result

| city | hospital_name | inpatient_discharges | readmit_same_hospital | readmit_network | readmit_missed_by_hospital | rate_same_hospital_pct | rate_network_pct | understatement_pct_points |
|---|---|---|---|---|---|---|---|---|
| Hyderabad | MedChain Gachibowli Hyderabad | 29,330 | 5,152 | 6,145 | 997 | 17.57 | 20.95 | 3.39 |
| Delhi | MedChain Dwarka Delhi | 33,343 | 5,772 | 6,820 | 1,052 | 17.31 | 20.45 | 3.14 |
| Delhi | MedChain Super Speciality Delhi | 33,700 | 5,838 | 6,888 | 1,052 | 17.32 | 20.44 | 3.12 |
| Hyderabad | MedChain Secunderabad | 29,182 | 5,042 | 5,941 | 907 | 17.28 | 20.36 | 3.08 |
| Mumbai | MedChain Navi Mumbai | 34,996 | 5,956 | 7,018 | 1,065 | 17.02 | 20.05 | 3.03 |
| Mumbai | MedChain Andheri Mumbai | 34,899 | 6,007 | 7,041 | 1,041 | 17.21 | 20.18 | 2.96 |

### Interpretation

Every row where understatement_pct_points > 0 is a readmission the hospital's own
reporting cannot see, because the patient returned somewhere else in the network under a
different patient_id. This is the direct clinical value of identity resolution.

<details>
<summary>SQL</summary>

```sql
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
```
</details>

---

## BQ2 — Clinical

> **For the ten most common procedures, how long is the full patient journey from admission to discharge to claim settlement?**

### Result

| procedure_name | specialty | episodes | avg_length_of_stay | median_days_to_settle | median_total_journey_days | p90_total_journey_days |
|---|---|---|---|---|---|---|
| Major Depressive Disorder Management | Psychiatry | 9,819 | 3.2 | 45 | 47 | 516 |
| Lower Segment Caesarean Section | Obstetrics and Gynaecology | 7,479 | 3.2 | 45 | 46 | 450 |
| Alcohol Withdrawal Mgmt | Psychiatry | 5,088 | 3.2 | 45 | 49 | 500 |
| Anxiety Disorder Consultation | Psychiatry | 4,925 | 3.1 | 45 | 47 | 480 |
| Schizophrenia Management | Psychiatry | 4,826 | 3.1 | 44 | 48 | 505 |

### Interpretation

The gap between median and p90 is where the operational pain sits: the typical journey
is manageable, the tail is what patients complain about and what ties up working
capital.

<details>
<summary>SQL</summary>

```sql
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
```
</details>

---

## BQ3 — Operational

> **Which hospital-ward combinations run above 85% occupancy for more than 20 days a quarter, and how does their length of stay compare to the network median?**

### Result

| hospital_name | city | ward_id | ward_type | fy_label | fy_quarter_label | days_observed | days_above_85pct | avg_occupancy_pct | ward_alos | beds | network_median_alos | alos_vs_network |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| MedChain Dwarka Delhi | Delhi | H002-ICU | ICU | FY2024-25 | Q2 | 92 | 89 | 124.7 | 4.23 | 19 | 3.67 | 0.56 |
| MedChain Secunderabad | Hyderabad | H006-ICU | ICU | FY2023-24 | Q2 | 92 | 87 | 128.6 | 4.82 | 17 | 3.67 | 1.15 |
| MedChain Secunderabad | Hyderabad | H006-ICU | ICU | FY2024-25 | Q2 | 92 | 87 | 120.9 | 4.68 | 17 | 3.67 | 1.01 |
| MedChain Navi Mumbai | Mumbai | H004-HDU | HDU | FY2024-25 | Q2 | 92 | 80 | 108.9 | 4.74 | 21 | 3.67 | 1.07 |
| MedChain Secunderabad | Hyderabad | H006-HDU | HDU | FY2023-24 | Q2 | 92 | 79 | 124.3 | 4.6 | 15 | 3.67 | 0.93 |

### Interpretation

Sustained high occupancy with a longer-than-median stay points at discharge process, not
capacity. Sustained high occupancy with a normal stay is genuine capacity pressure and
the case for more beds.

<details>
<summary>SQL</summary>

```sql
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
```
</details>

---

## BQ4 — Operational

> **What is consultation volume by department, and how much does it change when attributed by the doctor's department at the time of the visit instead of their current department?**

### Result

| department | visits_correct | visits_if_naive | misattribution | misattribution_pct |
|---|---|---|---|---|
| Urology | 41,772 | 30,672 | -11,100 | -26.6 |
| Dermatology | 24,184 | 31,258 | 7,074 | 29.3 |
| Oncology | 47,667 | 54,735 | 7,068 | 14.8 |
| General Surgery | 33,816 | 26,937 | -6,879 | -20.3 |
| Obstetrics and Gynaecology | 40,843 | 46,597 | 5,754 | 14.1 |
| Orthopedics | 42,705 | 38,486 | -4,219 | -9.9 |

### Interpretation

misattribution is the number of consultations that would be credited to the wrong
department without SCD Type 2 history. Departments with large positive values absorb
work they never did; large negative values lose credit for work they performed.

<details>
<summary>SQL</summary>

```sql
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
```
</details>

---

## BQ5 — Financial

> **What is the claim settlement rate by insurer, and at which lifecycle stage do claims spend the longest?**

### Result

| insurer_name | scheme_type | stage | median_days_in_stage | transitions | claims | settlement_rate_pct | rejection_rate_pct | median_days_to_resolution |
|---|---|---|---|---|---|---|---|---|
| HealthBridge TPA | Reimbursement-Retail | Approved | 23 | 47,065 | 94,242 | 75.6 | 14.43 | 38 |
| HealthBridge TPA | Reimbursement-Retail | Partially Approved | 23 | 24,180 | 94,242 | 75.6 | 14.43 | 38 |
| HealthBridge TPA | Reimbursement-Retail | Under Review | 15 | 81,095 | 94,242 | 75.6 | 14.43 | 38 |
| HealthBridge TPA | Reimbursement-Retail | Submitted | 8 | 73,127 | 94,242 | 75.6 | 14.43 | 38 |
| NationalCare Insurance | Cashless-Corporate | Approved | 23 | 56,368 | 114,214 | 75.14 | 14.87 | 38 |

### Interpretation

The stage with the highest median dwell is where to push the insurer. This is only
answerable because the lifecycle history was reconstructed — the portal's current-state
export cannot support it at all.

<details>
<summary>SQL</summary>

```sql
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
```
</details>

---

## BQ6 — Financial

> **What are the leading claim rejection reasons by value, and which are systemic rather than one-off?**

### Result

| rejection_reason | insurer_name | rejected_claims | value_lakh_inr | avg_claim_value | hospitals_affected | pattern |
|---|---|---|---|---|---|---|
| Documents incomplete - discharge summary missing | NationalCare Insurance | 2,221 | 3978.39 | 179126 | 8 | Systemic - network wide |
| Treatment excluded under policy terms | NationalCare Insurance | 2,125 | 3845.82 | 180980 | 8 | Systemic - network wide |
| Room rent limit breached - proportionate deduction disputed | NationalCare Insurance | 2,165 | 3777.83 | 174496 | 8 | Systemic - network wide |
| Claim intimation beyond permissible window | NationalCare Insurance | 2,138 | 3690.95 | 172636 | 8 | Systemic - network wide |
| Pre-existing disease within waiting period | NationalCare Insurance | 2,145 | 3666.17 | 170917 | 8 | Systemic - network wide |

### Interpretation

A reason appearing across six or more hospitals is a process defect worth fixing
centrally — documentation standards, pre-authorisation discipline. One confined to a
single site is a local training issue.

<details>
<summary>SQL</summary>

```sql
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
```
</details>

---

## BQ7 — Financial

> **What is the gap between what MedChain bills and what it is reimbursed, decomposed into co-pay, room-rent excess and exclusions — and which bucket is the largest recoverable amount?**

### Result

| hospital_name | city | insurer_name | claims | billed_crore | reimbursed_crore | gap_crore | gap_pct | excluded_crore | room_excess_crore | copay_crore | other_deduction_crore | recoverable_crore |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| MedChain Andheri Mumbai | Mumbai | HealthBridge TPA | 13,181 | 237.32 | 167.09 | 70.23 | 29.6 | 12.38 | 6.3 | 41.07 | 10.48 | 6.3 |
| MedChain Dwarka Delhi | Delhi | HealthBridge TPA | 12,296 | 221.89 | 155.26 | 66.62 | 30 | 11.5 | 6.21 | 39.31 | 9.6 | 6.21 |
| MedChain Navi Mumbai | Mumbai | HealthBridge TPA | 13,037 | 209.84 | 144.32 | 65.52 | 31.2 | 11.76 | 6.67 | 37.55 | 9.55 | 6.67 |
| MedChain Super Speciality Delhi | Delhi | HealthBridge TPA | 12,475 | 198.34 | 137.47 | 60.88 | 30.7 | 11.22 | 5.93 | 34.8 | 8.93 | 5.93 |
| MedChain Secunderabad | Hyderabad | HealthBridge TPA | 10,865 | 193.68 | 135.22 | 58.46 | 30.2 | 10.17 | 5.57 | 34.16 | 8.56 | 5.57 |
| MedChain Whitefield Bangalore | Bangalore | HealthBridge TPA | 11,218 | 186.26 | 130.2 | 56.06 | 30.1 | 10.22 | 5.63 | 32.03 | 8.19 | 5.63 |

### Interpretation

recoverable_crore is room rent billed above the policy cap — pure leakage the hospital
can eliminate by matching admission room category to policy entitlement at the point of
admission. Co-pay and contractual deductions are not recoverable and should not be
chased.

<details>
<summary>SQL</summary>

```sql
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
```
</details>

---
