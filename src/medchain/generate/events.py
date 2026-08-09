"""Transactional event generation: registrations, visits, bills, claims, bed movements.

Everything here produces the *true* state of the world. The exports that the
pipeline actually reads are degraded copies of this, produced by
:mod:`medchain.generate.corruption`.

Performance note: at scale 1.0 this creates ~600k visits, ~250k bills, ~180k claims
and ~1M claim snapshots. Every step is vectorised with numpy — a per-row Python loop
over 600k visits takes minutes, the array form takes seconds.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

from medchain.generate.entities import ReferenceData

# Daily room tariffs by ward type, in INR. Deliberately above several of the TPA
# room-rent caps in conf/seed/tpa_rules.csv — the excess is non-reimbursable and is
# one of the three buckets the revenue-vs-reimbursement gap decomposes into.
WARD_DAY_RATE = {
    "ICU": 18000,
    "HDU": 12000,
    "DELUXE": 15000,
    "PRIVATE": 9000,
    "SEMI_PRIVATE": 5500,
    "GENERAL": 3200,
}

ADMISSION_TYPES = ["OPD", "IPD", "EMERGENCY", "DAYCARE"]
ADMISSION_WEIGHTS = [0.55, 0.28, 0.11, 0.06]

CITY_CODE = {
    "Delhi": "DEL",
    "Mumbai": "MUM",
    "Hyderabad": "HYD",
    "Pune": "PNQ",
    "Bangalore": "BLR",
}

# Billable line-item categories. The names match conf/seed/tpa_exclusions.csv so the
# rules engine has something real to exclude.
LINE_CATEGORIES = [
    "ROOM",
    "PROCEDURE",
    "PHARMACY",
    "INVESTIGATION",
    "CONSUMABLES",
    "NON_MEDICAL_CONSUMABLES",
    "DIETARY",
    "REGISTRATION",
    "ADMIN_CHARGES",
    "ATTENDANT_CHARGES",
]

REJECTION_REASONS = [
    "Policy lapsed at date of admission",
    "Pre-existing disease within waiting period",
    "Treatment excluded under policy terms",
    "Documents incomplete - discharge summary missing",
    "Non-disclosure of material facts",
    "Claim intimation beyond permissible window",
    "Room rent limit breached - proportionate deduction disputed",
    "Cashless denied - admissible under reimbursement only",
]


@dataclass
class EventData:
    registrations: pd.DataFrame  # patient_id <-> person_id, one row per registration
    visits: pd.DataFrame
    bills: pd.DataFrame
    claims: pd.DataFrame  # one row per claim (final truth)
    claim_transitions: pd.DataFrame  # true lifecycle, one row per state change
    claim_line_items: pd.DataFrame
    bed_events: pd.DataFrame
    tpa_truth: pd.DataFrame  # true deduction breakdown per claim


# --------------------------------------------------------------- registrations


def build_registrations(
    rng: np.random.Generator,
    people: pd.DataFrame,
    hospitals: pd.DataFrame,
    duplicate_rate: float,
    window_start: date,
) -> pd.DataFrame:
    """Register every person at a primary hospital, and some at additional ones.

    Each registration gets a hospital-scoped ``patient_id`` (``H003-P004821``), so
    the same human carries different identifiers at different hospitals with nothing
    linking them. That is the "Patient Identity Fragmentation" failure point from
    the spec, and rebuilding the link is the MPI's job.
    """
    n_people = len(people)
    city_to_hospitals = {
        city: grp["hospital_id"].tolist() for city, grp in hospitals.groupby("city")
    }
    all_hospital_ids = hospitals["hospital_id"].tolist()

    # Primary registration: usually a hospital in the person's own city.
    primary = []
    for city in people["city"].to_numpy():
        local = city_to_hospitals.get(city, all_hospital_ids)
        primary.append(local[int(rng.integers(0, len(local)))])

    rows = pd.DataFrame(
        {
            "person_id": people["person_id"].to_numpy(),
            "hospital_id": primary,
            "is_duplicate": False,
        }
    )

    # Duplicate registrations: a share of people also register elsewhere. 65% pick a
    # different hospital in the same city (referral / second opinion), 35% pick any
    # hospital in the network (relocation, travel) — the latter is what makes
    # network-wide readmission detection impossible without an MPI.
    n_dupes = int(round(n_people * duplicate_rate))
    dupe_person_idx = rng.choice(n_people, size=n_dupes, replace=False)
    dupe_hospitals = []
    for idx in dupe_person_idx:
        person_city = people["city"].iat[int(idx)]
        origin = rows["hospital_id"].iat[int(idx)]
        if rng.random() < 0.65:
            pool = [h for h in city_to_hospitals.get(person_city, all_hospital_ids) if h != origin]
        else:
            pool = [h for h in all_hospital_ids if h != origin]
        if not pool:
            pool = [h for h in all_hospital_ids if h != origin]
        dupe_hospitals.append(pool[int(rng.integers(0, len(pool)))])

    dupes = pd.DataFrame(
        {
            "person_id": people["person_id"].to_numpy()[dupe_person_idx],
            "hospital_id": dupe_hospitals,
            "is_duplicate": True,
        }
    )

    registrations = pd.concat([rows, dupes], ignore_index=True)
    registrations = registrations.sort_values(["hospital_id", "person_id"]).reset_index(drop=True)

    # patient_id is assigned per hospital, mimicking independent registration desks.
    registrations["seq_in_hospital"] = registrations.groupby("hospital_id").cumcount() + 1
    registrations["patient_id"] = (
        registrations["hospital_id"]
        + "-P"
        + registrations["seq_in_hospital"].astype(str).str.zfill(6)
    )

    # Registration dates spread over the years before and during the window.
    offsets = rng.integers(-365 * 6, 365 * 3, size=len(registrations))
    registrations["registered_date"] = [
        (window_start + timedelta(days=int(o))).isoformat() for o in offsets
    ]

    return registrations.drop(columns=["seq_in_hospital"])


# ---------------------------------------------------------------------- visits


def _daily_weights(days: list[date], holidays: set[date], rng: np.random.Generator) -> np.ndarray:
    """Admission-volume weight per day.

    Encodes three real effects that make the time-series analysis non-trivial:
    a monsoon respiratory surge, reduced elective activity at weekends and around
    festivals, and mild year-on-year growth.
    """
    weights = np.ones(len(days), dtype=float)
    for i, day in enumerate(days):
        if day.month in (6, 7, 8, 9):
            weights[i] *= 1.18  # monsoon respiratory + vector-borne surge
        if day.month in (12, 1):
            weights[i] *= 1.06  # winter cardiac / respiratory uptick
        if day.weekday() >= 5:
            weights[i] *= 0.70  # elective OPD largely closed
        if day in holidays:
            weights[i] *= 0.45
        elif (day + timedelta(days=1)) in holidays or (day - timedelta(days=1)) in holidays:
            weights[i] *= 0.78
    # ~4% annual growth across the network.
    year_index = np.array([(d - days[0]).days / 365.0 for d in days])
    weights *= 1.04**year_index
    # Small day-to-day noise so the series is not implausibly smooth.
    weights *= rng.uniform(0.92, 1.08, size=len(days))
    return weights


def _assignment_buckets(
    assignments: pd.DataFrame, days: list[date]
) -> dict[tuple[str, int], np.ndarray]:
    """Index assignment rows by (hospital, month offset) for O(1) doctor lookup.

    A visit needs a doctor who was actually working at that hospital on that date.
    Scanning the assignment table per visit is O(visits x assignments); bucketing by
    month reduces it to a dict lookup plus one random draw.
    """
    start = days[0]
    buckets: dict[tuple[str, int], list[int]] = {}
    for pos, row in enumerate(assignments.itertuples()):
        eff_from = date.fromisoformat(row.effective_from)
        eff_to = date.fromisoformat(row.effective_to)
        first_month = max(0, (eff_from.year - start.year) * 12 + eff_from.month - start.month)
        last_month = (eff_to.year - start.year) * 12 + eff_to.month - start.month
        for month in range(first_month, last_month + 1):
            buckets.setdefault((row.hospital_id, month), []).append(pos)
    return {k: np.array(v, dtype=np.int64) for k, v in buckets.items()}


def build_visits(
    rng: np.random.Generator,
    ref: ReferenceData,
    registrations: pd.DataFrame,
    n_visits: int,
    window_start: date,
    window_end: date,
    holidays: set[date],
) -> pd.DataFrame:
    """Generate patient visits against registrations, doctors and procedures."""
    days = [window_start + timedelta(days=i) for i in range((window_end - window_start).days + 1)]
    weights = _daily_weights(days, holidays, rng)
    day_probs = weights / weights.sum()

    n_reg = len(registrations)
    # Visit frequency is heavy-tailed: most patients come once or twice, a small
    # chronic cohort (dialysis, chemotherapy) accounts for many visits.
    reg_weights = rng.gamma(shape=1.4, scale=1.0, size=n_reg) + 0.05
    reg_probs = reg_weights / reg_weights.sum()

    reg_idx = rng.choice(n_reg, size=n_visits, p=reg_probs)
    day_idx = rng.choice(len(days), size=n_visits, p=day_probs)

    reg_hospital = registrations["hospital_id"].to_numpy()[reg_idx]
    reg_patient = registrations["patient_id"].to_numpy()[reg_idx]
    reg_person = registrations["person_id"].to_numpy()[reg_idx]

    admission_type = rng.choice(ADMISSION_TYPES, size=n_visits, p=ADMISSION_WEIGHTS)

    # --- doctor selection, honouring the assignment history --------------------
    assignments = ref.doctor_assignments
    buckets = _assignment_buckets(assignments, days)
    assign_doctor = assignments["doctor_id"].to_numpy()
    assign_dept = assignments["department"].to_numpy()
    assign_hospital = assignments["hospital_id"].to_numpy()

    month_of_day = np.array(
        [(d.year - window_start.year) * 12 + d.month - window_start.month for d in days]
    )
    visit_month = month_of_day[day_idx]

    # Fallback chain for months where a hospital has no active assignment: any
    # assignment at that hospital, then any assignment at all. The last rung only
    # matters at small --scale, where there can be fewer doctors than hospitals.
    all_assignments = np.arange(len(assignments), dtype=np.int64)
    hospital_fallback = {}
    for h in ref.hospitals["hospital_id"]:
        at_hospital = np.where(assign_hospital == h)[0]
        hospital_fallback[h] = at_hospital if len(at_hospital) else all_assignments

    chosen = np.empty(n_visits, dtype=np.int64)
    randoms = rng.random(n_visits)
    for i in range(n_visits):
        pool = buckets.get((reg_hospital[i], int(visit_month[i])))
        if pool is None or len(pool) == 0:
            pool = hospital_fallback[reg_hospital[i]]
        chosen[i] = pool[int(randoms[i] * len(pool))]

    doctor_id = assign_doctor[chosen]
    department = assign_dept[chosen]

    # --- procedure selection, biased toward the treating department -----------
    procedures = ref.procedures
    proc_by_specialty: dict[str, np.ndarray] = {
        spec: grp.index.to_numpy() for spec, grp in procedures.groupby("specialty")
    }
    all_proc_idx = procedures.index.to_numpy()
    proc_choice = np.empty(n_visits, dtype=np.int64)
    proc_randoms = rng.random(n_visits)
    match_dept = rng.random(n_visits) < 0.88  # 88% treated within the department
    for i in range(n_visits):
        pool = proc_by_specialty.get(department[i], all_proc_idx) if match_dept[i] else all_proc_idx
        if len(pool) == 0:
            pool = all_proc_idx
        proc_choice[i] = pool[int(proc_randoms[i] * len(pool))]

    procedure_code = procedures["procedure_code"].to_numpy()[proc_choice]
    procedure_category = procedures["procedure_category"].to_numpy()[proc_choice]
    base_cost = procedures["base_cost"].to_numpy()[proc_choice]

    # --- length of stay --------------------------------------------------------
    los = np.zeros(n_visits, dtype=int)
    is_ipd = admission_type == "IPD"
    is_emg = admission_type == "EMERGENCY"
    los[is_ipd] = np.clip(rng.lognormal(1.15, 0.62, size=is_ipd.sum()).astype(int), 1, 45)
    los[is_emg] = np.clip(rng.lognormal(0.85, 0.80, size=is_emg.sum()).astype(int), 1, 30)

    admission_dates = np.array([days[i] for i in day_idx], dtype=object)
    discharge_dates = np.array(
        [admission_dates[i] + timedelta(days=int(los[i])) for i in range(n_visits)], dtype=object
    )

    visits = pd.DataFrame(
        {
            "visit_id": [f"VIS{i + 1:08d}" for i in range(n_visits)],
            "patient_id": reg_patient,
            "person_id": reg_person,
            "hospital_id": reg_hospital,
            "doctor_id": doctor_id,
            "department": department,
            "procedure_code": procedure_code,
            "procedure_category": procedure_category,
            "base_cost": base_cost,
            "admission_type": admission_type,
            "admission_date": admission_dates,
            "discharge_date": discharge_dates,
            "length_of_stay": los,
            "is_readmission_source": False,
        }
    )
    return visits


def add_readmissions(
    rng: np.random.Generator,
    visits: pd.DataFrame,
    registrations: pd.DataFrame,
    window_end: date,
    readmit_rate: float = 0.085,
) -> pd.DataFrame:
    """Inject clinically-realistic 30-day readmissions.

    A deliberate share of these land at a *different* hospital in the network, under
    that hospital's own patient_id. Those are invisible to any single-hospital
    report and only become visible once the MPI links the identities — which is what
    Business Question 1 measures.
    """
    inpatient = visits.index[visits["admission_type"].isin(["IPD", "EMERGENCY"])].to_numpy()
    if len(inpatient) == 0:
        return visits

    n_readmit = int(round(len(inpatient) * readmit_rate))
    source_idx = rng.choice(inpatient, size=n_readmit, replace=False)

    # Registrations grouped by person, so a readmission can be routed to another
    # hospital where the same human is registered under a different id.
    reg_by_person: dict[str, list[tuple[str, str]]] = {}
    for row in registrations.itertuples():
        reg_by_person.setdefault(row.person_id, []).append((row.patient_id, row.hospital_id))

    new_rows = []
    max_seq = len(visits)
    for offset, idx in enumerate(source_idx):
        src = visits.loc[idx]
        gap = int(rng.integers(3, 29))  # readmitted within the 30-day window
        admit = src["discharge_date"] + timedelta(days=gap)
        if admit > window_end:
            continue

        options = reg_by_person.get(src["person_id"], [])
        cross_hospital = [o for o in options if o[1] != src["hospital_id"]]
        # 45% of readmissions go to a different hospital when the patient happens to
        # be registered there; the rest return to the original hospital.
        if cross_hospital and rng.random() < 0.45:
            patient_id, hospital_id = cross_hospital[int(rng.integers(0, len(cross_hospital)))]
        else:
            patient_id, hospital_id = src["patient_id"], src["hospital_id"]

        los = int(np.clip(rng.lognormal(1.0, 0.6), 1, 30))
        new_rows.append(
            {
                "visit_id": f"VIS{max_seq + offset + 1:08d}",
                "patient_id": patient_id,
                "person_id": src["person_id"],
                "hospital_id": hospital_id,
                "doctor_id": src["doctor_id"],
                "department": src["department"],
                "procedure_code": src["procedure_code"],
                "procedure_category": src["procedure_category"],
                "base_cost": src["base_cost"],
                "admission_type": "IPD" if rng.random() < 0.7 else "EMERGENCY",
                "admission_date": admit,
                "discharge_date": admit + timedelta(days=los),
                "length_of_stay": los,
                "is_readmission_source": False,
            }
        )

    if not new_rows:
        return visits

    visits.loc[source_idx, "is_readmission_source"] = True
    return pd.concat([visits, pd.DataFrame(new_rows)], ignore_index=True)


# ------------------------------------------------------------------ bed events


def _event_timestamp(day, hour: int, rng: np.random.Generator) -> str:
    """Render a ward event timestamp. Minutes are random within the hour."""
    return f"{day.isoformat()} {hour:02d}:{int(rng.integers(0, 60)):02d}:00"


def _bed_number(rng: np.random.Generator, bed_count) -> str:
    """Pick a bed within the ward's capacity."""
    return f"B{int(rng.integers(1, int(bed_count) + 1)):03d}"


def build_bed_events(
    rng: np.random.Generator,
    visits: pd.DataFrame,
    wards: pd.DataFrame,
    unclosed_rate: float,
) -> pd.DataFrame:
    """Emit ward check-in / transfer / check-out events for inpatient stays.

    The source system records *events*, never daily occupancy state — reconstructing
    one row per ward per day from these events is the gap-fill challenge. Some stays
    include mid-stay ward transfers (producing two ward segments for one visit) and
    a small share have no check-out at all (patient still admitted, or the event was
    simply never logged).
    """
    stays = visits[visits["length_of_stay"] > 0].copy()
    if stays.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "visit_id",
                "patient_id",
                "hospital_id",
                "ward_id",
                "ward_type",
                "bed_number",
                "event_type",
                "event_ts",
            ]
        )

    wards_by_hospital = {h: grp.reset_index(drop=True) for h, grp in wards.groupby("hospital_id")}

    records: list[dict] = []
    n = len(stays)
    # Emergency admissions skew toward ICU/HDU; elective IPD toward general/private.
    transfer_draw = rng.random(n)
    unclosed_draw = rng.random(n)
    hour_in = rng.integers(6, 22, size=n)
    hour_out = rng.integers(8, 20, size=n)
    event_seq = 0

    for i, stay in enumerate(stays.itertuples()):
        hospital_wards = wards_by_hospital.get(stay.hospital_id)
        if hospital_wards is None or hospital_wards.empty:
            continue

        weights = hospital_wards["bed_count"].to_numpy(dtype=float).copy()
        if stay.admission_type == "EMERGENCY":
            weights = weights * np.where(
                hospital_wards["ward_type"].isin(["ICU", "HDU"]).to_numpy(), 4.0, 1.0
            )
        weights = weights / weights.sum()
        first = int(rng.choice(len(hospital_wards), p=weights))

        admit_ts = _event_timestamp(stay.admission_date, int(hour_in[i]), rng)
        discharge_ts = _event_timestamp(stay.discharge_date, int(hour_out[i]), rng)

        event_seq += 1
        records.append(
            {
                "event_id": f"BED{event_seq:09d}",
                "visit_id": stay.visit_id,
                "patient_id": stay.patient_id,
                "hospital_id": stay.hospital_id,
                "ward_id": hospital_wards["ward_id"].iat[first],
                "ward_type": hospital_wards["ward_type"].iat[first],
                "bed_number": _bed_number(rng, hospital_wards["bed_count"].iat[first]),
                "event_type": "CHECK_IN",
                "event_ts": admit_ts,
            }
        )

        current_ward = first
        # Mid-stay transfer, only plausible for stays of 3+ days (e.g. ICU -> general
        # once the patient stabilises). This is the case that breaks naive gap-fill:
        # one visit occupies two different wards on different days.
        if stay.length_of_stay >= 3 and transfer_draw[i] < 0.28:
            transfer_day = int(rng.integers(1, stay.length_of_stay))
            transfer_date = stay.admission_date + timedelta(days=transfer_day)
            transfer_ts = _event_timestamp(transfer_date, int(rng.integers(8, 20)), rng)
            next_ward = int(rng.integers(0, len(hospital_wards)))
            if next_ward != current_ward:
                event_seq += 1
                records.append(
                    {
                        "event_id": f"BED{event_seq:09d}",
                        "visit_id": stay.visit_id,
                        "patient_id": stay.patient_id,
                        "hospital_id": stay.hospital_id,
                        "ward_id": hospital_wards["ward_id"].iat[current_ward],
                        "ward_type": hospital_wards["ward_type"].iat[current_ward],
                        "bed_number": None,
                        "event_type": "TRANSFER_OUT",
                        "event_ts": transfer_ts,
                    }
                )
                event_seq += 1
                records.append(
                    {
                        "event_id": f"BED{event_seq:09d}",
                        "visit_id": stay.visit_id,
                        "patient_id": stay.patient_id,
                        "hospital_id": stay.hospital_id,
                        "ward_id": hospital_wards["ward_id"].iat[next_ward],
                        "ward_type": hospital_wards["ward_type"].iat[next_ward],
                        "bed_number": _bed_number(rng, hospital_wards["bed_count"].iat[next_ward]),
                        "event_type": "TRANSFER_IN",
                        "event_ts": transfer_ts,
                    }
                )
                current_ward = next_ward

        # A small share of stays never get a check-out event logged.
        if unclosed_draw[i] >= unclosed_rate:
            event_seq += 1
            records.append(
                {
                    "event_id": f"BED{event_seq:09d}",
                    "visit_id": stay.visit_id,
                    "patient_id": stay.patient_id,
                    "hospital_id": stay.hospital_id,
                    "ward_id": hospital_wards["ward_id"].iat[current_ward],
                    "ward_type": hospital_wards["ward_type"].iat[current_ward],
                    "bed_number": None,
                    "event_type": "CHECK_OUT",
                    "event_ts": discharge_ts,
                }
            )

    return pd.DataFrame(records)


# ----------------------------------------------------------------------- bills


def build_bills(
    rng: np.random.Generator,
    visits: pd.DataFrame,
    hospitals: pd.DataFrame,
    bed_events: pd.DataFrame,
) -> pd.DataFrame:
    """Raise a hospital bill per billable visit.

    Bill identifiers use the finance application's own format
    (``MC-DEL-2024-000123``), which shares no structure with the insurer's claim
    identifiers. Reconnecting the two is the bill-to-claim linkage challenge.
    """
    billable = visits[visits["admission_type"].isin(["IPD", "EMERGENCY", "DAYCARE"])].copy()
    if billable.empty:
        return pd.DataFrame()

    city_by_hospital = dict(zip(hospitals["hospital_id"], hospitals["city"]))

    # Ward type per visit drives the room tariff; fall back to GENERAL when a stay
    # produced no bed events (day-care admissions).
    ward_by_visit = (
        bed_events[bed_events["event_type"] == "CHECK_IN"]
        .drop_duplicates("visit_id")
        .set_index("visit_id")["ward_type"]
        .to_dict()
    )
    billable["ward_type"] = billable["visit_id"].map(ward_by_visit).fillna("GENERAL")

    n = len(billable)
    los = billable["length_of_stay"].to_numpy()
    day_rate = billable["ward_type"].map(WARD_DAY_RATE).fillna(3200).to_numpy(dtype=float)

    room_charge = day_rate * np.maximum(los, 0)
    procedure_charge = billable["base_cost"].to_numpy(dtype=float) * rng.uniform(0.9, 1.15, n)
    pharmacy = procedure_charge * rng.uniform(0.08, 0.25, n) + np.maximum(los, 1) * rng.uniform(
        400, 2200, n
    )
    investigation = procedure_charge * rng.uniform(0.05, 0.18, n) + rng.uniform(800, 6500, n)
    consumables = procedure_charge * rng.uniform(0.03, 0.10, n)
    non_medical = rng.uniform(300, 2500, n) + np.maximum(los, 0) * rng.uniform(80, 300, n)
    dietary = np.maximum(los, 0) * rng.uniform(350, 900, n)
    attendant = np.where(los > 0, np.maximum(los, 0) * rng.uniform(200, 600, n), 0.0)
    registration_fee = np.full(n, 500.0) + rng.uniform(0, 500, n)
    admin_charges = rng.uniform(250, 1500, n)

    gross = (
        room_charge
        + procedure_charge
        + pharmacy
        + investigation
        + consumables
        + non_medical
        + dietary
        + attendant
        + registration_fee
        + admin_charges
    )
    discount = gross * rng.choice([0.0, 0.02, 0.05, 0.10], size=n, p=[0.72, 0.13, 0.10, 0.05])
    taxable = gross - discount
    tax = taxable * 0.05
    net_payable = taxable + tax

    years = np.array([d.year for d in billable["admission_date"]])
    city_codes = (
        billable["hospital_id"].map(city_by_hospital).map(CITY_CODE).fillna("XXX").to_numpy()
    )
    seq = np.arange(1, n + 1)

    bills = pd.DataFrame(
        {
            "bill_id": [f"MC-{c}-{y}-{s:06d}" for c, y, s in zip(city_codes, years, seq)],
            "visit_id": billable["visit_id"].to_numpy(),
            "patient_id": billable["patient_id"].to_numpy(),
            "person_id": billable["person_id"].to_numpy(),
            "hospital_id": billable["hospital_id"].to_numpy(),
            "admission_date": billable["admission_date"].to_numpy(),
            "discharge_date": billable["discharge_date"].to_numpy(),
            "length_of_stay": los,
            "ward_type": billable["ward_type"].to_numpy(),
            "procedure_code": billable["procedure_code"].to_numpy(),
            "procedure_category": billable["procedure_category"].to_numpy(),
            "room_charge": np.round(room_charge, 2),
            "procedure_charge": np.round(procedure_charge, 2),
            "pharmacy": np.round(pharmacy, 2),
            "investigation": np.round(investigation, 2),
            "consumables": np.round(consumables, 2),
            "non_medical_consumables": np.round(non_medical, 2),
            "dietary": np.round(dietary, 2),
            "attendant_charges": np.round(attendant, 2),
            "registration_fee": np.round(registration_fee, 2),
            "admin_charges": np.round(admin_charges, 2),
            "gross_amount": np.round(gross, 2),
            "discount_amount": np.round(discount, 2),
            "tax_amount": np.round(tax, 2),
            "net_payable": np.round(net_payable, 2),
            "payment_mode": rng.choice(
                ["INSURANCE", "CASH", "CARD", "UPI", "CORPORATE"],
                size=n,
                p=[0.62, 0.10, 0.12, 0.11, 0.05],
            ),
            "bill_date": [d.isoformat() for d in billable["discharge_date"]],
        }
    )
    return bills
