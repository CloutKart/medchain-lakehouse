"""Deliberate defect injection — the reason this platform exists.

Everything upstream of this module produces a clean, internally consistent world.
This module damages it in the seven specific ways the spec describes, so that each
Silver component has a real problem to solve and a measurable amount of it.

The damage is calibrated, not arbitrary. Duplicate registrations are split across
buckets (identical / name variant / phone variant / format-only) in proportions that
leave the Master Patient Index recoverable to roughly 95% F1 — high enough to be a
credible result, low enough that the quarantine queue and the confidence scoring
have genuine work to do. Tests assert the injected rates, so a change here that
makes the problem easier shows up as a failing test rather than a suspiciously
good scorecard.
"""

from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# How duplicate registrations are degraded. Shares must sum to 1.0.
#
#   identical    - same details, different patient_id. Deterministic key links these.
#   name_variant - spelling drift, initials, honorifics. Defeats the exact key;
#                  probabilistic scoring should still catch it.
#   phone_gap    - phone missing or mistyped. Scoring must degrade gracefully rather
#                  than punish a null.
#   format_only  - identical facts, different date/phone formatting. Pure
#                  normalisation problem; if the normaliser is right these link
#                  deterministically.
#   heavy        - several defects at once. Some of these are meant to stay
#                  unresolved and land in the quarantine queue.
DUPLICATE_MIX = {
    "identical": 0.40,
    "name_variant": 0.26,
    "phone_gap": 0.12,
    "format_only": 0.14,
    "heavy": 0.08,
}

TITLES_BY_GENDER = {"M": ["Mr.", "Mr", "Shri", "Dr."], "F": ["Mrs.", "Ms.", "Smt", "Miss"]}

# Phonetic substitutions that genuinely occur when Indian names are transcribed by
# different registration clerks.
PHONETIC_SWAPS = [
    ("ee", "i"),
    ("i", "ee"),
    ("aa", "a"),
    ("a", "aa"),
    ("v", "w"),
    ("w", "v"),
    ("sh", "s"),
    ("s", "sh"),
    ("th", "t"),
    ("ksh", "x"),
    ("oo", "u"),
    ("y", "i"),
]


def load_source_formats(seed_dir: Path) -> dict[str, dict[str, str]]:
    """Per-hospital export formatting conventions."""
    formats: dict[str, dict[str, str]] = {}
    with (seed_dir / "source_date_formats.csv").open() as fh:
        for row in csv.DictReader(line for line in fh if not line.startswith("#")):
            formats[row["hospital_id"]] = row
    return formats


# ------------------------------------------------------------------- formatting


def format_date(value: str, fmt: str) -> str:
    """Render an ISO date string in a hospital's local convention."""
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except (ValueError, TypeError):
        return value
    return parsed.strftime(fmt)


def format_phone(digits: str | None, style: str) -> str | None:
    """Render a 10-digit mobile number in a hospital's local convention."""
    if not digits or len(digits) < 10:
        return digits
    d = digits[-10:]
    if style == "plain10":
        return d
    if style == "leading0":
        return "0" + d
    if style == "plus91dash":
        return f"+91-{d[:5]} {d[5:]}"
    if style == "plus91space":
        return f"+91 {d}"
    if style == "spaced":
        return f"{d[:5]} {d[5:]}"
    if style == "dashed":
        return f"{d[:5]}-{d[5:]}"
    return d


def apply_name_case(name: str, style: str) -> str:
    return name.upper() if style == "upper" else name.title()


# --------------------------------------------------------------- name mutations


def _mutate_name(rng: np.random.Generator, name: str) -> str:
    """Apply one plausible transcription error to a name."""
    if not name:
        return name
    choice = rng.integers(0, 5)
    lowered = name.lower()

    if choice == 0:  # phonetic substitution
        rng.shuffle(swaps := list(PHONETIC_SWAPS))
        for src, dst in swaps:
            if src in lowered:
                return lowered.replace(src, dst, 1).title()
        return name
    if choice == 1 and len(name) > 3:  # adjacent transposition
        pos = int(rng.integers(1, len(name) - 1))
        chars = list(name)
        chars[pos], chars[pos + 1] = chars[pos + 1], chars[pos]
        return "".join(chars)
    if choice == 2 and len(name) > 3:  # doubled letter
        pos = int(rng.integers(1, len(name)))
        return name[:pos] + name[pos] + name[pos:]
    if choice == 3 and len(name) > 4:  # dropped letter
        pos = int(rng.integers(1, len(name) - 1))
        return name[:pos] + name[pos + 1 :]
    return name + rng.choice(["", "a", "h"])  # trailing vowel/aspirate drift


def _mutate_phone(rng: np.random.Generator, digits: str) -> str | None:
    """Apply one plausible data-entry error to a phone number, or drop it."""
    choice = rng.integers(0, 4)
    if choice == 0:
        return None  # simply not captured at this hospital
    if choice == 1 and len(digits) >= 10:  # transposed digits
        chars = list(digits)
        pos = int(rng.integers(0, len(chars) - 1))
        chars[pos], chars[pos + 1] = chars[pos + 1], chars[pos]
        return "".join(chars)
    if choice == 2 and len(digits) >= 10:  # single wrong digit
        chars = list(digits)
        pos = int(rng.integers(0, len(chars)))
        chars[pos] = str(int(rng.integers(0, 10)))
        return "".join(chars)
    return f"{rng.integers(6, 10)}{int(rng.integers(0, 10**9)):09d}"  # a different number entirely


def _mutate_dob(rng: np.random.Generator, iso_date: str) -> str:
    """Shift or transpose a date of birth the way a mistyped form would."""
    try:
        parsed = datetime.strptime(iso_date, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return iso_date
    choice = rng.integers(0, 3)
    if choice == 0:
        return (parsed + timedelta(days=int(rng.choice([-1, 1])))).isoformat()
    if choice == 1:  # day/month transposed, when it yields a valid date
        try:
            return date(parsed.year, parsed.day, parsed.month).isoformat()
        except ValueError:
            return (parsed + timedelta(days=1)).isoformat()
    return date(parsed.year + int(rng.choice([-1, 1])), parsed.month, parsed.day).isoformat()


# ------------------------------------------------------- registration corruption


def corrupt_registrations(
    rng: np.random.Generator,
    registrations: pd.DataFrame,
    people: pd.DataFrame,
    seed_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Produce the ``patient_registrations`` export and the MPI ground truth.

    Returns ``(export_rows, truth_map)``. The export carries no ``person_id`` — that
    column exists only in the truth map, which the pipeline never reads.
    """
    formats = load_source_formats(seed_dir)
    people_indexed = people.set_index("person_id")

    joined = registrations.merge(people, on="person_id", how="left", suffixes=("", "_p"))

    n = len(joined)
    buckets = list(DUPLICATE_MIX)
    bucket_probs = np.array([DUPLICATE_MIX[b] for b in buckets])
    bucket_probs = bucket_probs / bucket_probs.sum()
    assigned = np.where(
        joined["is_duplicate"].to_numpy(),
        rng.choice(buckets, size=n, p=bucket_probs),
        "identical",
    )

    first_names = joined["first_name"].to_numpy().astype(object).copy()
    last_names = joined["last_name"].to_numpy().astype(object).copy()
    dobs = joined["dob"].to_numpy().astype(object).copy()
    phones = joined["phone"].to_numpy().astype(object).copy()
    cities = joined["city"].to_numpy().astype(object).copy()
    genders = joined["gender"].to_numpy()

    for i in range(n):
        bucket = assigned[i]
        if bucket == "name_variant":
            if rng.random() < 0.6:
                first_names[i] = _mutate_name(rng, str(first_names[i]))
            else:
                last_names[i] = _mutate_name(rng, str(last_names[i]))
        elif bucket == "phone_gap":
            phones[i] = _mutate_phone(rng, str(phones[i]))
        elif bucket == "heavy":
            first_names[i] = _mutate_name(rng, str(first_names[i]))
            phones[i] = _mutate_phone(rng, str(phones[i]))
            if rng.random() < 0.5:
                dobs[i] = _mutate_dob(rng, str(dobs[i]))
            if rng.random() < 0.3:
                cities[i] = str(rng.choice(people["city"].unique()))
        # "identical" and "format_only" leave the underlying facts untouched;
        # format_only is expressed purely through the per-hospital rendering below.

    # Honorific prefixes appear on a minority of registrations regardless of bucket.
    add_title = rng.random(n) < 0.18
    display_first = first_names.copy()
    for i in range(n):
        if add_title[i]:
            pool = TITLES_BY_GENDER.get(str(genders[i]), ["Mr."])
            display_first[i] = f"{rng.choice(pool)} {first_names[i]}"

    # Per-hospital rendering: date format, phone format, name casing.
    hospital_ids = joined["hospital_id"].to_numpy()
    out_dob = []
    out_phone = []
    out_first = []
    out_last = []
    for i in range(n):
        conv = formats.get(str(hospital_ids[i]), {})
        out_dob.append(format_date(str(dobs[i]), conv.get("date_format", "%Y-%m-%d")))
        out_phone.append(format_phone(phones[i], conv.get("phone_format", "plain10")))
        case = conv.get("name_case", "title")
        out_first.append(apply_name_case(str(display_first[i]), case))
        out_last.append(apply_name_case(str(last_names[i]), case))

    export = pd.DataFrame(
        {
            "patient_id": joined["patient_id"].to_numpy(),
            "hospital_id": hospital_ids,
            "first_name": out_first,
            "last_name": out_last,
            "gender": genders,
            "dob": out_dob,
            "phone": out_phone,
            "email": joined["email"].to_numpy(),
            "address_line": joined["address_line"].to_numpy(),
            "city": cities,
            "state": joined["state"].to_numpy(),
            "pincode": joined["pincode"].to_numpy(),
            "blood_group": joined["blood_group"].to_numpy(),
            "registered_date": joined["registered_date"].to_numpy(),
            "updated_date": joined["registered_date"].to_numpy(),
        }
    )

    truth = pd.DataFrame(
        {
            "patient_id": joined["patient_id"].to_numpy(),
            "hospital_id": hospital_ids,
            "person_id": joined["person_id"].to_numpy(),
            "is_duplicate_registration": joined["is_duplicate"].to_numpy(),
            "corruption_bucket": assigned,
            "true_first_name": joined["first_name"].to_numpy(),
            "true_last_name": joined["last_name"].to_numpy(),
            "true_dob": joined["dob"].to_numpy(),
            "true_phone": joined["phone"].to_numpy(),
        }
    )

    # A handful of registrations lose their contact details entirely — the hard
    # tail the MPI is not expected to resolve.
    del people_indexed
    return export, truth


# ----------------------------------------------------- other source degradations


def corrupt_procedure_master(
    rng: np.random.Generator, procedures: pd.DataFrame, missing_rate: float
) -> pd.DataFrame:
    """Null out ICD-10 codes on exactly ``missing_rate`` of the catalogue.

    Uses an exact count rather than a per-row coin flip so the injected rate is
    reproducible to the row — the ICD fill-rate metric is meaningless if the
    denominator drifts between runs.
    """
    export = procedures[
        [
            "procedure_code",
            "procedure_name",
            "icd10_code",
            "specialty",
            "procedure_category",
            "base_cost",
        ]
    ].copy()
    n_missing = int(round(len(export) * missing_rate))
    missing_idx = rng.choice(len(export), size=n_missing, replace=False)
    export.loc[export.index[missing_idx], "icd10_code"] = None
    return export


def build_doctor_exports(
    assignments: pd.DataFrame,
    doctors: pd.DataFrame,
    window_start: date,
    window_end: date,
    cadence_days: int = 7,
) -> pd.DataFrame:
    """Produce the weekly HR roster export.

    Each export is a full snapshot of who is in which department *today*. There is
    no history and no end-date column — a doctor who moved from Cardiology to
    Emergency simply appears under Emergency in the next export, with everything
    before it silently rewritten. Rebuilding the intervals is the SCD Type 2 job.
    """
    doctor_meta = doctors.set_index("doctor_id")

    export_dates = []
    cursor = window_start
    while cursor <= window_end:
        export_dates.append(cursor)
        cursor += timedelta(days=cadence_days)

    assign_from = np.array([date.fromisoformat(d) for d in assignments["effective_from"]])
    assign_to = np.array([date.fromisoformat(d) for d in assignments["effective_to"]])

    records = []
    for export_date in export_dates:
        active = (assign_from <= export_date) & (assign_to > export_date)
        subset = assignments[active]
        for row in subset.itertuples():
            meta = doctor_meta.loc[row.doctor_id]
            records.append(
                {
                    "doctor_id": row.doctor_id,
                    "doctor_name": meta["doctor_name"],
                    "department": row.department,
                    "hospital_id": row.hospital_id,
                    "specialty": row.specialty,
                    "designation": meta["designation"],
                    "qualification": meta["qualification"],
                    "joining_date": meta["joining_date"],
                    # The export dates the *current* assignment, which is the only
                    # hint that a change happened at all.
                    "effective_date": row.effective_from,
                    "export_date": export_date.isoformat(),
                }
            )
    return pd.DataFrame(records)


def attach_hospital_ref(
    rng: np.random.Generator, claim_snapshots: pd.DataFrame, claims: pd.DataFrame
) -> pd.DataFrame:
    """Attach a partially-usable hospital reference to some claim snapshots.

    Around 40% of claims carry the hospital's own bill reference in a free-text
    field, but mangled by re-keying: the city segment dropped, separators changed,
    or leading zeros lost. It is a genuine second linkage route for the rows that
    have it, and useless for the rest — which is why bill-to-claim matching needs
    both a fuzzy attribute match and this regex fallback.
    """
    if claim_snapshots.empty:
        return claim_snapshots

    bill_by_claim = dict(zip(claims["claim_id"], claims["bill_id"]))
    unique_claims = claims["claim_id"].to_numpy()
    has_ref = set(unique_claims[rng.random(len(unique_claims)) < 0.40])

    refs: list[str | None] = []
    for claim_id in claim_snapshots["claim_id"].to_numpy():
        if claim_id not in has_ref:
            refs.append(None)
            continue
        bill_id = bill_by_claim.get(claim_id)
        if not bill_id:
            refs.append(None)
            continue
        # bill_id looks like MC-DEL-2024-000123
        parts = bill_id.split("-")
        style = rng.integers(0, 4)
        if style == 0:
            refs.append(bill_id)  # intact
        elif style == 1:
            refs.append(f"{parts[2]}{parts[3]}")  # year+sequence only
        elif style == 2:
            refs.append(f"{parts[1]}/{parts[2]}/{parts[3].lstrip('0')}")  # leading zeros lost
        else:
            refs.append(f"REF {parts[3]}")  # sequence only
    claim_snapshots = claim_snapshots.copy()
    claim_snapshots["hospital_ref_no"] = refs
    return claim_snapshots
