# Gold Data Dictionary

Every column in the Gold layer. Generated from the built warehouse, so it reflects
what actually exists rather than what was intended.

Row counts are from the full 3-year dataset (`make gen SCALE=1.0`).

## Conventions

- `*_sk` — surrogate key, a stable hash of the business key. Hash-based rather than
  sequential so it needs no coordination, survives re-runs, and lets facts compute
  their dimension keys without reading the dimension.
- `*_date_sk` — foreign key to `dim_date`, stored as `yyyyMMdd`.
- `effective_from` / `effective_to` / `is_current` — SCD Type 2. **Facts join these
  dimensions on the date range, never on `is_current`.**
- `batch_id` / `dw_updated_at` — audit columns on every table.


## `dim_date`

**Grain:** One row per calendar day, 2022-01-01 to 2027-03-31  |  **Rows:** 1,916

| Column | Type | Null | Description |
|---|---|---|---|
| `date_sk` | int | yes | Surrogate key, yyyyMMdd as an integer |
| `date_key` | date | yes | The calendar date |
| `year` | int | yes | Year |
| `quarter` | int | yes | Quarter |
| `month` | int | yes | Month |
| `day` | int | yes | Day |
| `day_of_week` | int | yes | Day of week |
| `day_name` | string | yes | Day name |
| `month_name` | string | yes | Month name |
| `week_of_year` | int | yes | Week of year |
| `day_of_year` | int | yes | Day of year |
| `month_start` | date | yes | Month start |
| `month_end` | date | yes | Month end |
| `is_weekend` | boolean | yes | Is weekend |
| `financial_year_start` | int | yes | Financial year start |
| `fy_label` | string | yes | Indian financial year, e.g. FY2024-25 (April–March) |
| `fy_quarter` | int | yes | Financial-year quarter; Q1 = Apr–Jun |
| `fy_quarter_label` | string | yes | Q1–Q4 label |
| `fy_month_number` | int | yes | Month within the financial year, 1 = April |
| `holiday_name` | string | yes | Holiday name, / separated if two fall on one date |
| `holiday_type` | string | yes | NATIONAL (gazetted) or FESTIVAL (widely observed) |
| `is_public_holiday` | boolean | yes | Gazetted or widely observed Indian holiday |
| `is_working_day` | boolean | yes | Weekday that is not a holiday; the denominator for utilisation |
| `is_monsoon` | boolean | yes | June–September; drives the respiratory admission surge |


## `dim_patient`

**Grain:** One row per resolved person (SCD2)  |  **Rows:** 183,460

| Column | Type | Null | Description |
|---|---|---|---|
| `patient_sk` | bigint | yes | Surrogate key derived from mpi_id |
| `mpi_id` | string | yes | Resolved patient identity across all 8 hospitals |
| `full_name` | string | yes | Full name |
| `first_name` | string | yes | First name |
| `last_name` | string | yes | Last name |
| `gender` | string | yes | Gender |
| `date_of_birth` | date | yes | Date of birth |
| `age_years` | bigint | yes | Age years |
| `age_band` | string | yes | Infant / Child / Adolescent / Adult / Senior / Elderly |
| `phone` | string | yes | Phone |
| `city` | string | yes | City |
| `state` | string | yes | State |
| `pincode` | string | yes | Pincode |
| `address_line` | string | yes | Address line |
| `blood_group` | string | yes | Blood group |
| `email` | string | yes | Email |
| `source_patient_id_count` | bigint | yes | How many source registrations were merged into this person |
| `registered_hospital_count` | bigint | yes | Distinct hospitals this person is registered at |
| `registered_hospitals` | array<string> | yes | Array of hospital ids this person is registered at |
| `is_multi_hospital_patient` | boolean | yes | True when registered at more than one hospital — the cohort single-hospital reporting cannot see |
| `effective_from` | date | yes | Start of this version's validity |
| `effective_to` | date | yes | End of validity; 9999-12-31 when open |
| `is_current` | boolean | yes | True for the open version. Facts must NOT join on this — use the date range |
| `batch_id` | string | yes | Batch that wrote this row; joins to control.batch_registry |
| `dw_updated_at` | timestamp | yes | When the warehouse last wrote this row |


## `dim_doctor`

**Grain:** One row per doctor per assignment period (SCD2)  |  **Rows:** 505

| Column | Type | Null | Description |
|---|---|---|---|
| `doctor_sk` | bigint | yes | Surrogate key; unique per (doctor, assignment period) |
| `doctor_id` | string | yes | Business key (doctor) |
| `doctor_name` | string | yes | Doctor name |
| `department` | string | yes | Department for THIS version's effective period, not today's |
| `hospital_id` | string | yes | Business key (hospital) |
| `specialty` | string | yes | Specialty |
| `designation` | string | yes | Designation |
| `qualification` | string | yes | Qualification |
| `joining_date` | date | yes | Joining date |
| `is_senior` | boolean | yes | Is senior |
| `version` | int | yes | Sequence number of this version for the business key |
| `assignment_days` | int | yes | Days this assignment period has lasted |
| `effective_from` | date | yes | Start of this version's validity |
| `effective_to` | date | yes | End of validity; 9999-12-31 when open |
| `is_current` | boolean | yes | True for the open version. Facts must NOT join on this — use the date range |
| `hash_diff` | string | yes | SHA-256 over tracked attributes; drives SCD2 change detection |
| `batch_id` | string | yes | Batch that wrote this row; joins to control.batch_registry |
| `dw_updated_at` | timestamp | yes | When the warehouse last wrote this row |


## `dim_hospital`

**Grain:** One row per hospital (SCD1)  |  **Rows:** 8

| Column | Type | Null | Description |
|---|---|---|---|
| `hospital_sk` | bigint | yes | Surrogate key from hospital_id |
| `hospital_id` | string | yes | Business key (hospital) |
| `hospital_name` | string | yes | Hospital name |
| `city` | string | yes | City |
| `state` | string | yes | State |
| `tier` | string | yes | Tier1 or Tier2 |
| `bed_capacity` | bigint | yes | Stated capacity |
| `total_beds` | bigint | yes | Sum of ward bed counts; reconciles to bed_capacity |
| `ward_count` | bigint | yes | Ward count |
| `critical_care_beds` | bigint | yes | ICU + HDU beds |
| `critical_care_bed_pct` | double | yes | Critical care share of capacity |
| `size_band` | string | yes | Large (450+) / Medium (280+) / Small |
| `opened_year` | bigint | yes | Opened year |
| `batch_id` | string | yes | Batch that wrote this row; joins to control.batch_registry |
| `dw_updated_at` | timestamp | yes | When the warehouse last wrote this row |


## `dim_insurer`

**Grain:** One row per insurer (SCD1)  |  **Rows:** 2

| Column | Type | Null | Description |
|---|---|---|---|
| `insurer_id` | string | yes | Business key (insurer) |
| `insurer_name` | string | yes | Insurer name |
| `tpa_name` | string | yes | Third-party administrator |
| `scheme_type` | string | yes | Cashless-Corporate or Reimbursement-Retail |
| `claim_id_format` | string | yes | Claim id format |
| `empanelment_date` | date | yes | Empanelment date |
| `rule_count` | bigint | yes | TPA rules configured for this insurer |
| `avg_copay_pct` | double | yes | Mean co-pay across their rules |
| `max_room_rent_cap` | bigint | yes | Highest per-day room cap they allow |
| `insurer_sk` | bigint | yes | Surrogate key from insurer_id |
| `batch_id` | string | yes | Batch that wrote this row; joins to control.batch_registry |
| `dw_updated_at` | timestamp | yes | When the warehouse last wrote this row |


## `dim_procedure`

**Grain:** One row per procedure code (SCD1)  |  **Rows:** 400

| Column | Type | Null | Description |
|---|---|---|---|
| `procedure_sk` | bigint | yes | Surrogate key from procedure_code |
| `procedure_code` | string | yes | Procedure code |
| `procedure_name` | string | yes | Procedure name |
| `specialty` | string | yes | Specialty |
| `procedure_category` | string | yes | Procedure category |
| `base_cost` | decimal(18,2) | yes | Base cost |
| `icd10_code` | string | yes | Resolved ICD-10 code — may be inferred; check icd10_source |
| `icd10_chapter` | string | yes | First character of the ICD-10 code |
| `icd10_source` | string | yes | How the code was obtained: SOURCE / EXACT_NAME / FUZZY_NAME / SPECIALTY / UNMAPPED |
| `icd10_confidence` | double | yes | 1.00 source, 0.95 exact, ~0.85 fuzzy, 0.40 specialty default |
| `is_icd10_inferred` | boolean | yes | True when the code was not supplied by the source |
| `icd10_reliability` | string | yes | High / Medium / Low / None, banded from confidence |
| `batch_id` | string | yes | Batch that wrote this row; joins to control.batch_registry |
| `dw_updated_at` | timestamp | yes | When the warehouse last wrote this row |


## `fact_patient_visit`

**Grain:** One row per visit  |  **Rows:** 619,601

| Column | Type | Null | Description |
|---|---|---|---|
| `visit_sk` | bigint | yes | Surrogate key from visit_id |
| `visit_id` | string | yes | Business key (visit) |
| `patient_sk` | bigint | yes | Surrogate key derived from mpi_id |
| `mpi_id` | string | yes | Resolved patient identity across all 8 hospitals |
| `doctor_sk` | bigint | yes | Surrogate key; unique per (doctor, assignment period) |
| `doctor_id` | string | yes | Business key (doctor) |
| `hospital_sk` | bigint | yes | Surrogate key from hospital_id |
| `hospital_id` | string | yes | Business key (hospital) |
| `procedure_sk` | bigint | yes | Surrogate key from procedure_code |
| `procedure_code` | string | yes | Procedure code |
| `admission_date_sk` | int | yes | FK to dim_date |
| `discharge_date_sk` | int | yes | FK to dim_date |
| `admission_date` | date | yes | Admission date |
| `discharge_date` | date | yes | Discharge date |
| `admission_type` | string | yes | OPD / IPD / EMERGENCY / DAYCARE |
| `is_inpatient` | boolean | yes | IPD or EMERGENCY |
| `length_of_stay` | int | yes | Nights; 0 for OPD and day care |
| `department_at_visit` | string | yes | Department via point-in-time SCD2 join. THE CORRECT ATTRIBUTION |
| `department_current` | string | yes | Department the doctor sits in today. Kept only to quantify misattribution — do not report from this |
| `department_recorded` | string | yes | Department as recorded on the visit record |
| `doctor_specialty` | string | yes | Doctor specialty |
| `doctor_designation` | string | yes | Doctor designation |
| `days_since_prev_discharge` | int | yes | Days since this patient's previous discharge, network-wide |
| `readmit_30d_network` | boolean | yes | Readmitted within 30 days anywhere in the network, via mpi_id |
| `readmit_30d_same_hospital` | boolean | yes | Readmitted within 30 days at the same hospital |
| `readmit_cross_hospital_only` | boolean | yes | Network readmission invisible to the discharging hospital |
| `previous_hospital_id` | string | yes | Hospital of the previous visit |
| `batch_id` | string | yes | Batch that wrote this row; joins to control.batch_registry |
| `dw_updated_at` | timestamp | yes | When the warehouse last wrote this row |


## `fact_claim_lifecycle`

**Grain:** One row per claim state transition  |  **Rows:** 706,422

| Column | Type | Null | Description |
|---|---|---|---|
| `claim_transition_sk` | bigint | yes | Surrogate key per state transition |
| `transition_key` | string | yes | SHA-256 of (claim, status, status_date) |
| `claim_id` | string | yes | Business key (claim) |
| `patient_sk` | bigint | yes | Surrogate key derived from mpi_id |
| `mpi_id` | string | yes | Resolved patient identity across all 8 hospitals |
| `hospital_sk` | bigint | yes | Surrogate key from hospital_id |
| `hospital_id` | string | yes | Business key (hospital) |
| `insurer_sk` | bigint | yes | Surrogate key from insurer_id |
| `insurer_id` | string | yes | Business key (insurer) |
| `status_date_sk` | int | yes | Surrogate key (status_date) |
| `status_date` | date | yes | Status date |
| `transition_seq` | int | yes | Position in this claim's observed history |
| `status_code` | string | yes | One of the six lifecycle states |
| `prev_status` | string | yes | Previously observed state |
| `next_status` | string | yes | Next observed state |
| `days_in_prev_status` | int | yes | Dwell time of the PREVIOUS state, attributed to that state |
| `days_since_submission` | int | yes | Days from submission to this transition |
| `is_terminal` | boolean | yes | Settled or Rejected |
| `transition_class` | string | yes | DIRECT / GAP (intermediate state unobserved) / ILLEGAL (unreachable) |
| `is_legal_transition` | boolean | yes | False only for ILLEGAL — a GAP is a sampling artefact, not an anomaly |
| `claim_amount` | decimal(18,2) | yes | Claim amount |
| `approved_amount` | decimal(18,2) | yes | Approved amount |
| `submitted_date` | date | yes | Submitted date |
| `rejection_reason` | string | yes | Free-text reason, populated on rejection |
| `batch_id` | string | yes | Batch that wrote this row; joins to control.batch_registry |
| `dw_updated_at` | timestamp | yes | When the warehouse last wrote this row |


## `fact_billing_reconciliation`

**Grain:** One row per bill-to-claim linkage  |  **Rows:** 208,456

| Column | Type | Null | Description |
|---|---|---|---|
| `reconciliation_sk` | bigint | yes | Surrogate key from (claim_id, bill_id) |
| `claim_id` | string | yes | Business key (claim) |
| `bill_id` | string | yes | Business key (bill) |
| `patient_sk` | bigint | yes | Surrogate key derived from mpi_id |
| `mpi_id` | string | yes | Resolved patient identity across all 8 hospitals |
| `hospital_sk` | bigint | yes | Surrogate key from hospital_id |
| `hospital_id` | string | yes | Business key (hospital) |
| `insurer_sk` | bigint | yes | Surrogate key from insurer_id |
| `insurer_id` | string | yes | Business key (insurer) |
| `bill_date_sk` | int | yes | Surrogate key (bill_date) |
| `bill_date` | date | yes | Bill date |
| `is_linked` | boolean | yes | Whether a bill was matched to this claim |
| `match_method` | string | yes | REFERENCE (hospital ref on the claim) or ATTRIBUTE (patient + amount + date) |
| `match_confidence` | double | yes | 0.50–0.99 |
| `rule_id` | string | yes | TPA rule applied, from silver.tpa_rules |
| `procedure_category` | string | yes | Procedure category |
| `room_type` | string | yes | Room type |
| `room_days` | int | yes | Room line quantity |
| `billed_amount` | double | yes | Sum of claim line items |
| `bill_gross_amount` | double | yes | Bill gross amount |
| `bill_net_payable` | double | yes | Bill net payable |
| `excluded_amount` | double | yes | Non-payable items removed first |
| `room_rent_excess` | double | yes | Room rent above the policy per-day cap — THE RECOVERABLE BUCKET |
| `eligible_amount` | double | yes | billed − excluded − room excess |
| `copay_pct` | double | yes | Policy co-pay rate |
| `copay_amount` | double | yes | Patient's contractual share; not recoverable |
| `deduction_pct` | double | yes | Residual deduction rate |
| `other_deduction` | double | yes | Residual percentage deduction |
| `net_reimbursement` | double | yes | What the rules say the insurer should pay |
| `reimbursement_gap` | double | yes | billed − net_reimbursement |
| `gap_pct` | double | yes | Gap as a share of billed |
| `insurer_approved_amount` | double | yes | What the insurer actually approved |
| `reconciliation_variance` | double | yes | net_reimbursement − insurer_approved_amount |
| `is_reconciled` | boolean | yes | Variance within ₹1 |
| `variance_class` | string | yes | EXPLAINED / PARTIAL_APPROVAL_DISCRETION / REJECTED_CLAIM / NOT_ADJUDICATED / UNEXPLAINED |
| `latest_status` | string | yes | Most recent observed state |
| `payment_mode` | string | yes | Payment mode |
| `batch_id` | string | yes | Batch that wrote this row; joins to control.batch_registry |
| `dw_updated_at` | timestamp | yes | When the warehouse last wrote this row |


## `fact_bed_occupancy`

**Grain:** One row per ward per day  |  **Rows:** 70,442

| Column | Type | Null | Description |
|---|---|---|---|
| `bed_occupancy_sk` | bigint | yes | Surrogate key from (ward, date) |
| `date_sk` | int | yes | Surrogate key, yyyyMMdd as an integer |
| `occupancy_date` | date | yes | The day |
| `hospital_sk` | bigint | yes | Surrogate key from hospital_id |
| `hospital_id` | string | yes | Business key (hospital) |
| `ward_id` | string | yes | Business key (ward) |
| `ward_type` | string | yes | Ward type |
| `bed_count` | bigint | yes | Ward capacity |
| `occupied_beds` | bigint | yes | Distinct patients in the ward that day |
| `occupancy_rate` | double | yes | occupied_beds / bed_count; can exceed 1.0 during surges |
| `occupancy_band` | string | yes | Critical 95%+ / High 85-95% / Normal / Low |
| `is_high_occupancy` | boolean | yes | At or above 85% |
| `is_over_capacity` | boolean | yes | Above 100% |
| `admissions` | bigint | yes | Ward admissions that day |
| `discharges` | bigint | yes | Ward discharges that day |
| `turnover_rate` | double | yes | discharges / bed_count |
| `avg_length_of_stay` | double | yes | Mean stay of patients discharged that day |
| `open_stay_count` | bigint | yes | Stays with no check-out event, capped and flagged |
| `batch_id` | string | yes | Batch that wrote this row; joins to control.batch_registry |
| `dw_updated_at` | timestamp | yes | When the warehouse last wrote this row |


## `dq_scorecard`

**Grain:** One row per (run, table, check)  |  **Rows:** 49

| Column | Type | Null | Description |
|---|---|---|---|
| `run_id` | string | yes | Pipeline execution id; the ADF run id when triggered by ADF |
| `run_ts` | timestamp | yes | When the check ran |
| `logical_date` | string | yes | Business date of the run |
| `layer` | string | yes | Layer checked |
| `table_name` | string | yes | Table checked |
| `check_name` | string | yes | Check identifier |
| `check_type` | string | yes | Structural type, or 'recovery' for truth-measured metrics |
| `severity` | string | yes | blocking (fails the run) or warn |
| `passed` | boolean | yes | Whether the check passed |
| `actual_value` | double | yes | Measured value |
| `threshold` | double | yes | Required value |
| `comparison` | string | yes | gte / lte / eq / gt / lt |
| `detail` | string | yes | Human-readable explanation |
