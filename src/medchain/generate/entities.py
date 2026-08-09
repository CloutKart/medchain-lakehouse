"""Reference entities: hospitals, wards, doctors, procedures, insurers, people.

These are the stable nouns of the simulation. Events (visits, claims, bills, bed
movements) are generated against them in :mod:`medchain.generate.events`.

Two design rules hold throughout the generator:

* **Everything is seeded.** A given ``--seed`` reproduces byte-identical output, so
  a test asserting "MPI F1 >= 0.95" means the same thing on every machine.
* **Truth is generated first, defects second.** We build a clean, internally
  consistent world and then damage it (see :mod:`medchain.generate.corruption`).
  The undamaged version is written to ``data/_truth`` and is never read by the
  pipeline — only by the scorecard, to measure how much of the truth the pipeline
  managed to recover.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ----------------------------------------------------------------- hospitals

# 8 hospitals across the 5 cities named in the spec. Bed capacity and tier drive
# occupancy denominators and case-mix, so they are fixed rather than random.
HOSPITALS: list[dict] = [
    {
        "hospital_id": "H001",
        "hospital_name": "MedChain Super Speciality Delhi",
        "city": "Delhi",
        "state": "Delhi",
        "tier": "Tier1",
        "bed_capacity": 520,
        "opened_year": 2004,
    },
    {
        "hospital_id": "H002",
        "hospital_name": "MedChain Dwarka Delhi",
        "city": "Delhi",
        "state": "Delhi",
        "tier": "Tier2",
        "bed_capacity": 240,
        "opened_year": 2013,
    },
    {
        "hospital_id": "H003",
        "hospital_name": "MedChain Andheri Mumbai",
        "city": "Mumbai",
        "state": "Maharashtra",
        "tier": "Tier1",
        "bed_capacity": 480,
        "opened_year": 2007,
    },
    {
        "hospital_id": "H004",
        "hospital_name": "MedChain Navi Mumbai",
        "city": "Mumbai",
        "state": "Maharashtra",
        "tier": "Tier2",
        "bed_capacity": 300,
        "opened_year": 2015,
    },
    {
        "hospital_id": "H005",
        "hospital_name": "MedChain Gachibowli Hyderabad",
        "city": "Hyderabad",
        "state": "Telangana",
        "tier": "Tier1",
        "bed_capacity": 440,
        "opened_year": 2010,
    },
    {
        "hospital_id": "H006",
        "hospital_name": "MedChain Secunderabad",
        "city": "Hyderabad",
        "state": "Telangana",
        "tier": "Tier2",
        "bed_capacity": 210,
        "opened_year": 2017,
    },
    {
        "hospital_id": "H007",
        "hospital_name": "MedChain Kharadi Pune",
        "city": "Pune",
        "state": "Maharashtra",
        "tier": "Tier1",
        "bed_capacity": 360,
        "opened_year": 2012,
    },
    {
        "hospital_id": "H008",
        "hospital_name": "MedChain Whitefield Bangalore",
        "city": "Bangalore",
        "state": "Karnataka",
        "tier": "Tier1",
        "bed_capacity": 400,
        "opened_year": 2009,
    },
]

# Ward mix as a share of each hospital's bed capacity. ICU/HDU carry the highest
# room-rent caps in the TPA rules, so their share drives the reimbursement gap.
WARD_MIX: list[tuple[str, str, float]] = [
    ("ICU", "ICU", 0.08),
    ("HDU", "HDU", 0.07),
    ("GEN", "GENERAL", 0.40),
    ("SEM", "SEMI_PRIVATE", 0.20),
    ("PVT", "PRIVATE", 0.15),
    ("DLX", "DELUXE", 0.05),
    ("MAT", "SEMI_PRIVATE", 0.03),
    ("PED", "GENERAL", 0.02),
]

DEPARTMENTS: list[str] = [
    "Cardiology",
    "Orthopedics",
    "Neurology",
    "Oncology",
    "Pediatrics",
    "Gastroenterology",
    "Pulmonology",
    "Nephrology",
    "Obstetrics and Gynaecology",
    "ENT",
    "Ophthalmology",
    "Dermatology",
    "Urology",
    "Psychiatry",
    "General Medicine",
    "General Surgery",
    "Emergency",
]

DESIGNATIONS = ["Registrar", "Consultant", "Senior Consultant", "Head of Department"]
DESIGNATION_WEIGHTS = [0.28, 0.42, 0.24, 0.06]

QUALIFICATIONS = ["MBBS", "MBBS MD", "MBBS MS", "MBBS MD DM", "MBBS MS MCh", "MBBS DNB"]
QUALIFICATION_WEIGHTS = [0.10, 0.30, 0.22, 0.16, 0.12, 0.10]

INSURERS: list[dict] = [
    {
        "insurer_id": "INS001",
        "insurer_name": "NationalCare Insurance",
        "tpa_name": "NationalCare Health Services",
        "scheme_type": "Cashless-Corporate",
        "claim_id_format": "NCI",
        "empanelment_date": "2019-04-01",
    },
    {
        "insurer_id": "INS002",
        "insurer_name": "HealthBridge TPA",
        "tpa_name": "HealthBridge Third Party Administrators",
        "scheme_type": "Reimbursement-Retail",
        "claim_id_format": "HB",
        "empanelment_date": "2020-07-01",
    },
]

BLOOD_GROUPS = ["O+", "A+", "B+", "AB+", "O-", "A-", "B-", "AB-"]
BLOOD_GROUP_WEIGHTS = [0.36, 0.22, 0.30, 0.06, 0.02, 0.02, 0.015, 0.005]

# Name pools. Faker is used once to build a pool rather than per-row: generating
# 180k names through Faker takes minutes, sampling from a pool takes milliseconds
# and produces the realistic duplicate-name collisions that make MPI matching hard.
FIRST_NAMES_M = [
    "Aarav",
    "Vivaan",
    "Aditya",
    "Vihaan",
    "Arjun",
    "Sai",
    "Reyansh",
    "Ayaan",
    "Krishna",
    "Ishaan",
    "Rohan",
    "Rajesh",
    "Suresh",
    "Ramesh",
    "Mahesh",
    "Anil",
    "Sunil",
    "Vijay",
    "Ajay",
    "Sanjay",
    "Manoj",
    "Deepak",
    "Amit",
    "Rahul",
    "Vikram",
    "Karthik",
    "Naveen",
    "Praveen",
    "Ravi",
    "Kiran",
    "Harish",
    "Girish",
    "Prakash",
    "Satish",
    "Ashok",
    "Vinod",
    "Pramod",
    "Nitin",
    "Sachin",
    "Gaurav",
    "Siddharth",
    "Abhishek",
    "Nikhil",
    "Varun",
    "Akash",
    "Yash",
    "Tarun",
    "Aniket",
]
FIRST_NAMES_F = [
    "Aadhya",
    "Ananya",
    "Diya",
    "Ishita",
    "Kavya",
    "Myra",
    "Pari",
    "Riya",
    "Saanvi",
    "Aarohi",
    "Priya",
    "Pooja",
    "Neha",
    "Sneha",
    "Divya",
    "Shweta",
    "Anjali",
    "Meera",
    "Lakshmi",
    "Sunita",
    "Anita",
    "Kavita",
    "Rekha",
    "Geeta",
    "Sarita",
    "Namita",
    "Shalini",
    "Nandini",
    "Deepika",
    "Swati",
    "Preeti",
    "Jyoti",
    "Bhavana",
    "Madhuri",
    "Sushma",
    "Rashmi",
    "Vandana",
    "Archana",
    "Sangeeta",
    "Rupali",
]
LAST_NAMES = [
    "Sharma",
    "Verma",
    "Gupta",
    "Singh",
    "Kumar",
    "Patel",
    "Shah",
    "Mehta",
    "Reddy",
    "Rao",
    "Naidu",
    "Nair",
    "Menon",
    "Pillai",
    "Iyer",
    "Iyengar",
    "Desai",
    "Joshi",
    "Kulkarni",
    "Deshpande",
    "Patil",
    "Jadhav",
    "Chavan",
    "More",
    "Agarwal",
    "Bansal",
    "Mittal",
    "Goyal",
    "Jain",
    "Malhotra",
    "Kapoor",
    "Chopra",
    "Bhat",
    "Shetty",
    "Hegde",
    "Kamath",
    "Prabhu",
    "Pai",
    "Acharya",
    "Bose",
    "Chatterjee",
    "Banerjee",
    "Mukherjee",
    "Ghosh",
    "Das",
    "Roy",
    "Dutta",
    "Sen",
]

CITY_PINCODES = {
    "Delhi": (110001, 110096),
    "Mumbai": (400001, 400104),
    "Hyderabad": (500001, 500098),
    "Pune": (411001, 411062),
    "Bangalore": (560001, 560103),
}

STREET_TYPES = ["Road", "Marg", "Street", "Lane", "Cross", "Main", "Avenue", "Nagar"]
LOCALITIES = [
    "Gandhi",
    "Nehru",
    "Shivaji",
    "Tilak",
    "Subhash",
    "Ambedkar",
    "Rajaji",
    "Patel",
    "Vivekananda",
    "Tagore",
    "Bose",
    "Azad",
    "Lajpat",
    "Malviya",
    "Sardar",
    "Kasturba",
]


@dataclass
class ReferenceData:
    """Every reference table the event generator needs, plus the truth population."""

    hospitals: pd.DataFrame
    wards: pd.DataFrame
    doctors: pd.DataFrame
    doctor_assignments: pd.DataFrame  # full true assignment history (SCD2 source of truth)
    procedures: pd.DataFrame
    insurers: pd.DataFrame
    people: pd.DataFrame  # distinct real humans, pre-duplication


def build_hospitals() -> pd.DataFrame:
    return pd.DataFrame(HOSPITALS)


def build_wards(hospitals: pd.DataFrame) -> pd.DataFrame:
    """Split each hospital's capacity across wards using :data:`WARD_MIX`.

    Two constraints are enforced, both because violating them produces occupancy
    figures that are arithmetically fine and clinically absurd:

    * **A minimum ward size.** A flat percentage split gives the smallest hospital a
      4-bed paediatric ward, and normal admission variance then puts 10 patients in
      it — a 250% occupancy rate that is a modelling artefact, not a finding. No real
      hospital operates a 4-bed ward as a distinct cost centre.
    * **Totals reconcile exactly.** Rounding drift and the minimum-size top-up are
      both absorbed by the general ward, so ward beds always sum to the hospital's
      stated capacity. Otherwise occupancy exceeds 100% purely from rounding.
    """
    min_ward_beds = 14

    rows = []
    for hospital in hospitals.itertuples():
        ward_rows = []
        for code, ward_type, share in WARD_MIX:
            beds = max(min_ward_beds, int(round(hospital.bed_capacity * share)))
            ward_rows.append(
                {
                    "ward_id": f"{hospital.hospital_id}-{code}",
                    "hospital_id": hospital.hospital_id,
                    "ward_code": code,
                    "ward_type": ward_type,
                    "bed_count": beds,
                }
            )

        # Absorb both the rounding drift and the cost of the minimum-size floor into
        # the general ward, which is large enough to carry it.
        assigned = sum(w["bed_count"] for w in ward_rows)
        drift = hospital.bed_capacity - assigned
        for ward in ward_rows:
            if ward["ward_code"] == "GEN":
                ward["bed_count"] = max(min_ward_beds, ward["bed_count"] + drift)
                break
        rows.extend(ward_rows)
    return pd.DataFrame(rows)


def build_procedures(rng: np.random.Generator, seed_dir: Path, n_procedures: int) -> pd.DataFrame:
    """Draw the procedure catalogue from the curated ICD-10 seed.

    Every procedure traces back to a real catalogue entry, but names are varied
    ("Total Knee Replacement" / "Total Knee Replacement - Left" / "TKR") so that the
    Silver ICD-10 inference has to do actual work: exact match handles some, fuzzy
    match handles others, and the rest fall through to specialty-level defaults.
    """
    catalog = pd.read_csv(seed_dir / "icd10_catalog.csv", comment="#")

    variants = [
        "",
        " - Left",
        " - Right",
        " - Bilateral",
        " (Revision)",
        " - Elective",
        " - Emergency",
    ]
    variant_weights = np.array([0.55, 0.09, 0.09, 0.05, 0.06, 0.09, 0.07])
    variant_weights = variant_weights / variant_weights.sum()

    rows = []
    for i in range(n_procedures):
        base = catalog.iloc[int(rng.integers(0, len(catalog)))]
        canonical = str(base["procedure_name"])
        name = _mangle_procedure_name(rng, canonical) + str(rng.choice(variants, p=variant_weights))
        # Cost varies +/-25% around the catalogue price to create billing spread.
        cost = float(base["base_cost_inr"]) * float(rng.uniform(0.75, 1.25))
        rows.append(
            {
                "procedure_code": f"PRC{i + 1:05d}",
                "procedure_name": name,
                "canonical_name": canonical,
                "icd10_code": base["icd10_code"],
                "specialty": base["specialty"],
                "procedure_category": base["procedure_category"],
                "base_cost": round(cost, 2),
            }
        )
    return pd.DataFrame(rows)


def _mangle_procedure_name(rng: np.random.Generator, name: str) -> str:
    """Distort a catalogue procedure name the way a hospital's own list would.

    Clinical procedure lists are typed by people, over years, across eight sites.
    They contain acronyms, abbreviations, reordered words and plain typos. Emitting
    only tidy suffixed variants would make ICD-10 inference a trivial exact-match
    exercise; these distortions are what force the fuzzy and specialty-fallback
    tiers to do real work, and what make the per-tier fill rate a meaningful metric.
    """
    roll = rng.random()
    words = name.split()

    if roll < 0.62:
        return name  # clean, as catalogued

    if roll < 0.72 and len(words) >= 3:
        # Acronym, e.g. "Total Knee Replacement" -> "TKR". Unrecoverable by name
        # matching; these are what fall through to the specialty default.
        return "".join(w[0].upper() for w in words if len(w) > 2)

    if roll < 0.82 and len(words) >= 2:
        # Common clinical abbreviations of individual words.
        swaps = {
            "Bilateral": "B/L",
            "Left": "Lt",
            "Right": "Rt",
            "Laparoscopic": "Lap",
            "Percutaneous": "Perc",
            "Management": "Mgmt",
            "Evaluation": "Eval",
            "Replacement": "Replmt",
            "Consultation": "Consult",
            "Syndrome": "Synd",
            "Chemotherapy": "Chemo",
            "Investigation": "Invest",
        }
        return " ".join(swaps.get(w, w) for w in words)

    if roll < 0.91 and len(words) >= 3:
        # Reordered wording: "Cataract Surgery - Phacoemulsification" style.
        return f"{' '.join(words[-2:])} {' '.join(words[:-2])}".strip()

    # A single-character typo — the case fuzzy matching is designed to absorb.
    pos = int(rng.integers(1, max(2, len(name) - 1)))
    return name[:pos] + name[pos + 1 :]


def build_doctors(
    rng: np.random.Generator,
    hospitals: pd.DataFrame,
    n_doctors: int,
    window_start: date,
    window_end: date,
    reassign_rate: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create doctors and their true department/hospital assignment history.

    The assignment history is the ground truth for SCD Type 2. Roughly
    ``reassign_rate`` of doctors move each year — the spec's "specialisation
    rotation" — and each move produces a new interval. The weekly HR export only
    ever shows the *current* assignment, so Silver must rebuild these intervals.
    """
    hospital_ids = hospitals["hospital_id"].tolist()
    # Larger hospitals get proportionally more doctors.
    weights = hospitals["bed_capacity"].to_numpy(dtype=float)
    weights = weights / weights.sum()

    doctors = []
    assignments = []
    for i in range(n_doctors):
        doctor_id = f"DOC{i + 1:05d}"
        gender = "M" if rng.random() < 0.62 else "F"
        pool = FIRST_NAMES_M if gender == "M" else FIRST_NAMES_F
        name = f"Dr. {rng.choice(pool)} {rng.choice(LAST_NAMES)}"

        # Doctors joined between 5 and 25 years before the simulation window.
        joining = window_start - timedelta(days=int(rng.integers(365 * 1, 365 * 25)))
        home_hospital = str(rng.choice(hospital_ids, p=weights))
        department = str(rng.choice(DEPARTMENTS))
        specialty = department

        doctors.append(
            {
                "doctor_id": doctor_id,
                "doctor_name": name,
                "gender": gender,
                "joining_date": joining.isoformat(),
                "qualification": str(rng.choice(QUALIFICATIONS, p=QUALIFICATION_WEIGHTS)),
                "designation": str(rng.choice(DESIGNATIONS, p=DESIGNATION_WEIGHTS)),
            }
        )

        # Draw the rotation dates first: each doctor-year independently has a
        # `reassign_rate` chance of one move. Deriving intervals from the moves
        # (rather than emitting an interval per year and changing department each
        # time) is what keeps the observed reassignment rate equal to the
        # configured one — over a 3-year window ~39% of doctors move at least once.
        start = max(joining, window_start)
        move_dates: list[date] = []
        year_start = start
        while year_start < window_end:
            year_end = min(year_start + timedelta(days=365), window_end)
            span = (year_end - year_start).days
            if span > 30 and rng.random() < reassign_rate:
                move_dates.append(year_start + timedelta(days=int(rng.integers(30, span))))
            year_start = year_end
        move_dates.sort()

        boundaries = [start, *move_dates, window_end]
        current_hospital = home_hospital
        current_department = department
        for version, (interval_start, interval_end) in enumerate(
            zip(boundaries[:-1], boundaries[1:]), start=1
        ):
            if interval_end <= interval_start:
                continue
            assignments.append(
                {
                    "doctor_id": doctor_id,
                    "department": current_department,
                    "hospital_id": current_hospital,
                    "specialty": specialty,
                    "effective_from": interval_start.isoformat(),
                    "effective_to": interval_end.isoformat(),
                    "version": version,
                }
            )
            # Apply the rotation that ends this interval. Department always changes;
            # the hospital moves in about a third of rotations. Specialty is
            # deliberately left alone — a cardiologist rotating through Emergency is
            # still a cardiologist, which is exactly why department must be tracked
            # historically while specialty need not be.
            if rng.random() < 0.35:
                current_hospital = str(rng.choice(hospital_ids, p=weights))
            choices = [d for d in DEPARTMENTS if d != current_department]
            current_department = str(rng.choice(choices))

    return pd.DataFrame(doctors), pd.DataFrame(assignments)


def build_people(rng: np.random.Generator, n_people: int, hospitals: pd.DataFrame) -> pd.DataFrame:
    """Generate the population of distinct real humans.

    ``person_id`` is the ground-truth identity. It never appears in any source file
    the pipeline reads — recovering it is exactly what the Master Patient Index is
    being asked to do.
    """
    cities = hospitals["city"].unique().tolist()
    city_weights = (
        hospitals.groupby("city")["bed_capacity"].sum().reindex(cities).to_numpy(dtype=float)
    )
    city_weights = city_weights / city_weights.sum()
    city_to_state = dict(zip(hospitals["city"], hospitals["state"]))

    genders = rng.choice(["M", "F"], size=n_people, p=[0.51, 0.49])
    first = np.where(
        genders == "M",
        rng.choice(FIRST_NAMES_M, size=n_people),
        rng.choice(FIRST_NAMES_F, size=n_people),
    )
    last = rng.choice(LAST_NAMES, size=n_people)
    chosen_cities = rng.choice(cities, size=n_people, p=city_weights)

    # Age distribution skewed toward the 25-60 band that dominates hospital usage,
    # with a paediatric and a geriatric tail.
    age_band = rng.choice([0, 1, 2, 3], size=n_people, p=[0.14, 0.46, 0.28, 0.12])
    age_low = np.array([0, 25, 50, 70])[age_band]
    age_high = np.array([25, 50, 70, 95])[age_band]
    ages = rng.integers(age_low, age_high)
    ref = date(2025, 4, 1)
    birth_offsets = ages * 365 + rng.integers(0, 365, size=n_people)
    dobs = [(ref - timedelta(days=int(o))).isoformat() for o in birth_offsets]

    # Indian mobile numbers start 6-9.
    phones = [f"{rng.integers(6, 10)}{rng.integers(0, 10**9):09d}" for _ in range(n_people)]

    pincodes = []
    addresses = []
    for city in chosen_cities:
        low, high = CITY_PINCODES[city]
        pincodes.append(str(int(rng.integers(low, high + 1))))
        addresses.append(
            f"{rng.integers(1, 400)}, {rng.choice(LOCALITIES)} {rng.choice(STREET_TYPES)}"
        )

    people = pd.DataFrame(
        {
            "person_id": [f"PSN{i + 1:07d}" for i in range(n_people)],
            "first_name": first,
            "last_name": last,
            "gender": genders,
            "dob": dobs,
            "phone": phones,
            "city": chosen_cities,
            "state": [city_to_state[c] for c in chosen_cities],
            "address_line": addresses,
            "pincode": pincodes,
            "blood_group": rng.choice(BLOOD_GROUPS, size=n_people, p=BLOOD_GROUP_WEIGHTS),
        }
    )
    people["email"] = (
        people["first_name"].str.lower()
        + "."
        + people["last_name"].str.lower()
        + people.index.map(lambda i: str(i % 997))
        + "@example.com"
    )
    return people


def build_reference_data(
    rng: np.random.Generator,
    seed_dir: Path,
    *,
    n_doctors: int,
    n_people: int,
    n_procedures: int,
    window_start: date,
    window_end: date,
    reassign_rate: float,
) -> ReferenceData:
    hospitals = build_hospitals()
    wards = build_wards(hospitals)
    procedures = build_procedures(rng, seed_dir, n_procedures)
    doctors, assignments = build_doctors(
        rng, hospitals, n_doctors, window_start, window_end, reassign_rate
    )
    people = build_people(rng, n_people, hospitals)
    insurers = pd.DataFrame(INSURERS)
    return ReferenceData(
        hospitals=hospitals,
        wards=wards,
        doctors=doctors,
        doctor_assignments=assignments,
        procedures=procedures,
        insurers=insurers,
        people=people,
    )


def load_holidays(seed_dir: Path) -> set[date]:
    """Holiday dates, used to dip elective admissions around festivals."""
    holidays: set[date] = set()
    with (seed_dir / "india_holidays.csv").open() as fh:
        for row in csv.DictReader(line for line in fh if not line.startswith("#")):
            holidays.add(date.fromisoformat(row["holiday_date"]))
    return holidays
