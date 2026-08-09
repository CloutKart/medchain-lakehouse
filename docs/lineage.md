# Data Lineage

## Table-level lineage

| Source file | Bronze | Silver | Gold |
|---|---|---|---|
| `patient_registrations.csv` | `bronze.patient_registrations` | `patient_master`, `patient_crosswalk`, `mpi_registry` | `dim_patient` |
| `insurance_claims.csv` | `bronze.insurance_claims` | `claim_transitions` → `claim_lifecycle` | `fact_claim_lifecycle` |
| `claim_line_items.csv` | `bronze.claim_line_items` | `claim_adjudication` | `fact_billing_reconciliation` |
| `billing_transactions.csv` | `bronze.billing_transactions` | `bill_claim_link` | `fact_billing_reconciliation` |
| `doctor_assignments.json` | `bronze.doctor_assignments` | `dim_doctor_scd2` | `dim_doctor` |
| `bed_occupancy_log.csv` | `bronze.bed_occupancy_log` | `bed_stay_segments` → `bed_occupancy_daily` | `fact_bed_occupancy` |
| `procedure_master.csv` | `bronze.procedure_master` | `procedure_catalog` | `dim_procedure` |
| `conf/seed/tpa_rules.csv` | — | `silver.tpa_rules` | `dim_insurer` (profile) |
| `conf/seed/tpa_exclusions.csv` | — | `silver.tpa_exclusions` | — |
| `conf/seed/icd10_catalog.csv` | — | `procedure_catalog` (inference) | `dim_procedure` |
| `conf/seed/india_holidays.csv` | — | — | `dim_date` |
| `conf/seed/source_date_formats.csv` | — | `patient_master` (date parsing) | — |

`fact_patient_visit` is built from the visit spine plus `patient_crosswalk`,
`dim_doctor` (point-in-time) and `dim_procedure`.

## Column-level lineage for the derived fields

Three Gold columns are computed rather than carried through. Anyone reading them needs
to know how they were produced.

### `dim_patient.mpi_id` — resolved patient identity

```
patient_registrations.first_name  ─┐
patient_registrations.last_name   ─┼─► normalize_name()      ─► full_name_norm ─┐
                                   │   (upper, strip titles,                    │
                                   │    punctuation, whitespace)                │
patient_registrations.dob         ─┼─► parse_dob()           ─► dob_parsed     ─┼─► sha256 ─► deterministic_key
   + source_date_formats.csv       │   (per-hospital format)                    │
patient_registrations.phone       ─┼─► normalize_phone()     ─► phone_last4    ─┘
                                   │   (last 10 digits)
                                   │
                                   └─► blocking keys ─► candidate pairs ─► score_pairs()
                                         · exact phone                      name  0.40 (Jaro-Winkler)
                                         · soundex(first)+dob               dob   0.30 (graded)
                                         · soundex(first,last)+year         phone 0.20 (transposition-aware)
                                         · soundex(first,last)+city         city  0.10
                                                                            (renormalised over present fields)
                                                    │
                    deterministic edges ────────────┴──► connected_components()
                    probabilistic edges (≥0.90) ────────►  (min-label propagation)
                                                                │
                                                                ▼
                                                          cluster_label
                                                                │
                                             mpi_registry (persisted) ──► mpi_id
```

Scores in 0.75–0.90 go to `quarantine.mpi_review_queue` instead of linking.
`mpi_id` is minted once per cluster and never renumbered — see
[ADR-001](adr/001-deterministic-first-mpi.md).

### `fact_billing_reconciliation.net_reimbursement` — codified TPA deduction

```
claim_line_items.line_amount ──► SUM by claim ──────────────► billed_amount
                              └► SUM(line × excluded_pct) ──► excluded_amount
                                    ▲
                              tpa_exclusions (insurer, item_category)

claim_line_items where item_category='ROOM'
   .line_amount ──► room_charge
   .quantity    ──► room_days
                       │
tpa_rules (insurer, procedure_category, room_type, most specific by rule_priority)
   .room_rent_cap_per_day ──┴──► allowed_room = cap × room_days
                                 room_rent_excess = max(0, room_charge − allowed_room)

  eligible_amount   = max(0, billed_amount − excluded_amount − room_rent_excess)
  copay_amount      = eligible_amount × copay_pct
  other_deduction   = eligible_amount × deduction_pct
  net_reimbursement = max(0, eligible_amount − copay_amount − other_deduction)

  reconciliation_variance = net_reimbursement − insurance_claims.approved_amount
```

**Order matters.** Exclusions first, then the room cap on the *room line*, then co-pay
on what remains eligible. Applying co-pay to the gross, or the room cap to the bill
total, produces plausible numbers wrong by tens of thousands of rupees.

Verified: all five components match ground truth on 100% of 208,456 claims.

### `fact_patient_visit.readmit_30d_network` — network-wide readmission

```
visit spine ──► join patient_crosswalk on (hospital_id, patient_id) ──► mpi_id
                                                                          │
        Window.partitionBy(mpi_id).orderBy(admission_date)  ◄──────────────┘
                       │
                       ├─► lag(discharge_date) ──► _prev_discharge_net
                       │        └─► days_since_prev_discharge = datediff(admission, prev_discharge)
                       │              └─► readmit_30d_network = is_inpatient
                       │                                        AND 0 ≤ days ≤ 30
                       │
        Window.partitionBy(mpi_id, hospital_id).orderBy(admission_date)
                       └─► readmit_30d_same_hospital  (same logic, hospital-scoped)

        readmit_cross_hospital_only = network AND NOT same_hospital
```

The partition key is the difference. Partitioning by `mpi_id` alone spans all eight
hospitals; adding `hospital_id` reproduces what a single hospital can see. The gap is
2.93 percentage points.

## Provenance columns carried into Gold

Deliberately surfaced rather than left in Silver, because the analyst reading the value
is the person who needs to know how confident to be in it.

| Column | Table | Meaning |
|---|---|---|
| `icd10_source` | `dim_procedure` | `SOURCE` / `EXACT_NAME` / `FUZZY_NAME` / `SPECIALTY` / `UNMAPPED` |
| `icd10_confidence` | `dim_procedure` | 1.00 down to 0.40 by tier |
| `match_method` | `fact_billing_reconciliation` | `REFERENCE` or `ATTRIBUTE` |
| `match_confidence` | `fact_billing_reconciliation` | 0.50–0.99 |
| `methods_agreeing` | `fact_billing_reconciliation` | 2 when both routes independently agree |
| `transition_class` | `fact_claim_lifecycle` | `DIRECT` / `GAP` / `ILLEGAL` |
| `variance_class` | `fact_billing_reconciliation` | Why computed and approved differ |
| `source_patient_id_count` | `dim_patient` | How many registrations were merged |

## Audit trail

Every row in every layer carries `batch_id`. `control.batch_registry` maps
`batch_id → (layer, source, ingest_date, status, row_count, run_id)`, and `run_id` is
the ADF pipeline run, so any row in Gold can be traced back to the orchestrator
execution that produced it.
