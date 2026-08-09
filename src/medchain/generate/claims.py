"""Insurance claims: lifecycle, TPA deduction truth, line items and portal snapshots.

This module produces the spec's two hardest defects at once.

**Lost history.** A claim really does move Submitted -> Under Review -> Approved ->
Settled over several weeks. We record that true transition log to ``_truth``, then
export only what the insurer portal actually exposes: a periodic snapshot of each
claim's *current* status. Transitions that begin and end between two snapshots are
genuinely unrecoverable, which is why reconstruction coverage is a metric rather
than an assertion.

**Uncodified deductions.** The portal reports one ``approved_amount`` and no
breakdown. We compute that number here by applying the TPA rules honestly
(exclusions, room-rent caps, co-pay, percentage deduction) and then throw the
working away. Silver's rules engine has to rediscover it — and
:func:`compute_tpa_truth` is the reference the scorecard measures it against.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from medchain.generate.events import REJECTION_REASONS

# Bill component -> line item category. Categories match conf/seed/tpa_exclusions.csv.
BILL_COMPONENTS: list[tuple[str, str]] = [
    ("room_charge", "ROOM"),
    ("procedure_charge", "PROCEDURE"),
    ("pharmacy", "PHARMACY"),
    ("investigation", "INVESTIGATION"),
    ("consumables", "CONSUMABLES"),
    ("non_medical_consumables", "NON_MEDICAL_CONSUMABLES"),
    ("dietary", "DIETARY"),
    ("attendant_charges", "ATTENDANT_CHARGES"),
    ("registration_fee", "REGISTRATION"),
    ("admin_charges", "ADMIN_CHARGES"),
]

STATUS_CODES = [
    "Submitted",
    "Under Review",
    "Partially Approved",
    "Approved",
    "Rejected",
    "Settled",
]
STATUS_TO_INT = {s: i for i, s in enumerate(STATUS_CODES)}

MAX_TRANSITIONS = 4  # Submitted -> Under Review -> outcome -> Settled


# ------------------------------------------------------------ TPA rules (truth)


def resolve_tpa_rule(
    rules: pd.DataFrame, insurer_id: str, procedure_category: str, room_type: str
) -> pd.Series:
    """Pick the most specific applicable rule.

    Specificity is expressed as ``rule_priority`` in the seed file (1 = exact match
    on insurer + category + room, rising to 4 for the catch-all). Ties are broken by
    ``rule_id`` so resolution is deterministic — an ambiguous rule set must not
    produce different answers on different runs.
    """
    candidates = rules[
        (rules["insurer_id"] == insurer_id)
        & (rules["procedure_category"].isin([procedure_category, "*"]))
        & (rules["room_type"].isin([room_type, "*"]))
    ]
    if candidates.empty:
        raise ValueError(
            f"No TPA rule matches insurer={insurer_id} category={procedure_category} "
            f"room={room_type}. conf/seed/tpa_rules.csv needs a catch-all row."
        )
    return candidates.sort_values(["rule_priority", "rule_id"]).iloc[0]


def compute_tpa_truth(
    bills: pd.DataFrame,
    insurer_ids: np.ndarray,
    rules: pd.DataFrame,
    exclusions: pd.DataFrame,
) -> pd.DataFrame:
    """Apply the TPA deduction rules to every claimed bill.

    The order of operations matters and mirrors how a TPA actually adjudicates:

    1. Remove excluded items (registration, admin, attendant, non-medical) entirely,
       and partially disallow others (pharmacy, dietary).
    2. Remove room rent above the policy's per-day cap.
    3. Apply co-pay to what remains eligible.
    4. Apply the residual percentage deduction.

    Doing co-pay before exclusions, or applying the room cap to the whole bill
    instead of the room line, both produce plausible-looking but wrong numbers —
    which is precisely how the hospital's manual reconciliation drifts.
    """
    n = len(bills)
    # Vectorised exclusion lookup: (insurer, item_category) -> excluded fraction.
    excl_map = {
        (row.insurer_id, row.item_category): float(row.excluded_pct)
        for row in exclusions.itertuples()
    }

    excluded_total = np.zeros(n)
    for column, category in BILL_COMPONENTS:
        amounts = bills[column].to_numpy(dtype=float)
        fractions = np.array(
            [excl_map.get((ins, category), 0.0) for ins in insurer_ids], dtype=float
        )
        excluded_total += amounts * fractions

    # Rule attributes per claim. Distinct (insurer, category, room) combinations
    # number in the dozens, so resolve once per combination rather than per row.
    combos = pd.DataFrame(
        {
            "insurer_id": insurer_ids,
            "procedure_category": bills["procedure_category"].to_numpy(),
            "room_type": bills["ward_type"].to_numpy(),
        }
    )
    unique = combos.drop_duplicates().reset_index(drop=True)
    resolved = {}
    for row in unique.itertuples():
        rule = resolve_tpa_rule(rules, row.insurer_id, row.procedure_category, row.room_type)
        resolved[(row.insurer_id, row.procedure_category, row.room_type)] = (
            float(rule["copay_pct"]),
            float(rule["room_rent_cap_per_day"]),
            float(rule["deduction_pct"]),
            str(rule["rule_id"]),
        )

    keys = list(zip(combos["insurer_id"], combos["procedure_category"], combos["room_type"]))
    copay_pct = np.array([resolved[k][0] for k in keys])
    room_cap = np.array([resolved[k][1] for k in keys])
    deduction_pct = np.array([resolved[k][2] for k in keys])
    rule_id = np.array([resolved[k][3] for k in keys])

    los = bills["length_of_stay"].to_numpy(dtype=float)
    room_charge = bills["room_charge"].to_numpy(dtype=float)
    billed = bills["gross_amount"].to_numpy(dtype=float)

    # A cap of 0 means the rule places no room-rent limit (outpatient categories).
    allowed_room = np.where(room_cap > 0, room_cap * np.maximum(los, 1), room_charge)
    room_rent_excess = np.maximum(0.0, room_charge - allowed_room)

    eligible = np.maximum(0.0, billed - excluded_total - room_rent_excess)
    copay = eligible * copay_pct
    other_deduction = eligible * deduction_pct
    net_reimbursement = np.maximum(0.0, eligible - copay - other_deduction)

    return pd.DataFrame(
        {
            "bill_id": bills["bill_id"].to_numpy(),
            "insurer_id": insurer_ids,
            "rule_id": rule_id,
            "billed_amount": np.round(billed, 2),
            "excluded_amount": np.round(excluded_total, 2),
            "room_rent_excess": np.round(room_rent_excess, 2),
            "eligible_amount": np.round(eligible, 2),
            "copay_pct": copay_pct,
            "copay_amount": np.round(copay, 2),
            "deduction_pct": deduction_pct,
            "other_deduction": np.round(other_deduction, 2),
            "net_reimbursement": np.round(net_reimbursement, 2),
            "reimbursement_gap": np.round(billed - net_reimbursement, 2),
        }
    )


# ----------------------------------------------------------------- claim bodies


def build_claims(
    rng: np.random.Generator,
    bills: pd.DataFrame,
    rules: pd.DataFrame,
    exclusions: pd.DataFrame,
    window_end: date,
    insured_rate: float = 0.72,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create claims, their true state transitions, and the TPA truth breakdown."""
    if bills.empty:
        empty = pd.DataFrame()
        return empty, empty, empty

    n_bills = len(bills)
    insured_mask = rng.random(n_bills) < insured_rate
    claimed = bills[insured_mask].reset_index(drop=True)
    n = len(claimed)
    if n == 0:
        empty = pd.DataFrame()
        return empty, empty, empty

    insurer_ids = rng.choice(["INS001", "INS002"], size=n, p=[0.55, 0.45])
    tpa = compute_tpa_truth(claimed, insurer_ids, rules, exclusions)

    # Claim identifiers follow each insurer's own convention. Neither resembles the
    # hospital's MC-DEL-2024-000123 bill format.
    years = np.array([d.year for d in claimed["discharge_date"]])
    seq = np.arange(1, n + 1)
    claim_ids = np.array(
        [
            f"NCI/{y}/{s:07d}" if ins == "INS001" else f"HB-{y}-{s:07d}"
            for ins, y, s in zip(insurer_ids, years, seq)
        ]
    )

    # --- lifecycle -----------------------------------------------------------
    submitted = np.array(
        [
            d + timedelta(days=int(g))
            for d, g in zip(claimed["discharge_date"], rng.integers(1, 6, n))
        ],
        dtype=object,
    )
    review = np.array(
        [d + timedelta(days=int(g)) for d, g in zip(submitted, rng.integers(2, 12, n))],
        dtype=object,
    )
    outcome_gap = rng.integers(3, 26, n)
    outcome_date = np.array(
        [d + timedelta(days=int(g)) for d, g in zip(review, outcome_gap)], dtype=object
    )
    settle_gap = rng.integers(7, 41, n)
    settle_date = np.array(
        [d + timedelta(days=int(g)) for d, g in zip(outcome_date, settle_gap)], dtype=object
    )

    # Outcome mix. 6% are still mid-flight at the end of the window, which keeps a
    # realistic tail of non-terminal claims in the data.
    outcome = rng.choice(
        ["Approved", "Partially Approved", "Rejected", "IN_PROGRESS"],
        size=n,
        p=[0.52, 0.27, 0.15, 0.06],
    )

    net_truth = tpa["net_reimbursement"].to_numpy()
    approved_amount = np.zeros(n)
    approved_amount[outcome == "Approved"] = net_truth[outcome == "Approved"]

    # Partial approvals carry an extra ad-hoc reduction that no rule can predict —
    # a medical officer's judgement call. The rules engine will legitimately differ
    # on these, and the scorecard reports Approved and Partially Approved separately
    # rather than hiding the difference in one blended accuracy number.
    partial_mask = outcome == "Partially Approved"
    approved_amount[partial_mask] = np.round(
        net_truth[partial_mask] * rng.uniform(0.55, 0.92, partial_mask.sum()), 2
    )

    # A further 4% of clean approvals carry a manual override — the real-world
    # source of the hospital's reconciliation errors.
    override_mask = (outcome == "Approved") & (rng.random(n) < 0.04)
    approved_amount[override_mask] = np.round(
        approved_amount[override_mask] * rng.uniform(0.85, 0.99, override_mask.sum()), 2
    )

    # Build as an object array so the non-rejected entries can hold None; np.where
    # with a None branch would coerce the whole array to a string dtype and turn
    # every null into the literal "None".
    rejection_reason = np.full(n, None, dtype=object)
    rejected = outcome == "Rejected"
    rejection_reason[rejected] = rng.choice(REJECTION_REASONS, size=int(rejected.sum()))

    # Terminal date per claim, used to decide when it drops off the portal export.
    terminal_date = np.array(
        [
            settle_date[i]
            if outcome[i] in ("Approved", "Partially Approved")
            else (outcome_date[i] if outcome[i] == "Rejected" else None)
            for i in range(n)
        ],
        dtype=object,
    )

    claims = pd.DataFrame(
        {
            "claim_id": claim_ids,
            "bill_id": claimed["bill_id"].to_numpy(),
            "visit_id": claimed["visit_id"].to_numpy(),
            "patient_id": claimed["patient_id"].to_numpy(),
            "person_id": claimed["person_id"].to_numpy(),
            "hospital_id": claimed["hospital_id"].to_numpy(),
            "insurer_id": insurer_ids,
            "policy_number": [
                f"{'NC' if i == 'INS001' else 'HB'}P{int(rng.integers(10**8, 10**9))}"
                for i in insurer_ids
            ],
            "claim_amount": claimed["net_payable"].to_numpy(),
            "approved_amount": np.round(approved_amount, 2),
            "final_status": np.where(
                outcome == "IN_PROGRESS",
                "Under Review",
                np.where(outcome == "Rejected", "Rejected", "Settled"),
            ),
            "outcome": outcome,
            "submitted_date": submitted,
            "review_date": review,
            "outcome_date": outcome_date,
            "settle_date": settle_date,
            "terminal_date": terminal_date,
            "rejection_reason": rejection_reason,
            "admission_date": claimed["admission_date"].to_numpy(),
            "discharge_date": claimed["discharge_date"].to_numpy(),
        }
    )

    transitions = _build_transitions(claims, window_end)
    tpa = tpa.assign(claim_id=claim_ids)
    return claims, transitions, tpa


def _build_transitions(claims: pd.DataFrame, window_end: date) -> pd.DataFrame:
    """Expand each claim into its true ordered state transitions."""
    records: list[dict] = []
    for claim in claims.itertuples():
        steps: list[tuple[str, date]] = [("Submitted", claim.submitted_date)]
        if claim.review_date <= window_end:
            steps.append(("Under Review", claim.review_date))
        if claim.outcome != "IN_PROGRESS" and claim.outcome_date <= window_end:
            steps.append((claim.outcome, claim.outcome_date))
            if (
                claim.outcome in ("Approved", "Partially Approved")
                and claim.settle_date <= window_end
            ):
                steps.append(("Settled", claim.settle_date))

        for seq, (status, when) in enumerate(steps, start=1):
            records.append(
                {
                    "claim_id": claim.claim_id,
                    "transition_seq": seq,
                    "status_code": status,
                    "status_date": when,
                }
            )
    return pd.DataFrame(records)


# -------------------------------------------------------------- portal exports


def build_claim_snapshots(
    claims: pd.DataFrame,
    transitions: pd.DataFrame,
    window_start: date,
    window_end: date,
    cadence_days: int = 7,
    grace_days: int = 7,
) -> pd.DataFrame:
    """Turn the true transition log into the snapshots the insurer portal exports.

    The portal has no history table. Each export lists every claim that is currently
    open, plus any that closed since the previous export, showing only its status
    *right now*. Reconstructing the lifecycle means accumulating these snapshots and
    deduplicating on (claim, status, status_date).

    Because the export runs every ``cadence_days`` days, a state that both starts and
    ends inside one interval is never observed. That loss is real and intentional:
    the pipeline reports reconstruction coverage rather than claiming completeness.
    """
    if claims.empty:
        return pd.DataFrame()

    export_dates = []
    cursor = window_start
    horizon = window_end + timedelta(days=90)  # claims settle after the visit window
    while cursor <= horizon:
        export_dates.append(cursor)
        cursor += timedelta(days=cadence_days)
    export_ords = np.array([d.toordinal() for d in export_dates], dtype=np.int64)

    claim_ids = claims["claim_id"].to_numpy()
    claim_index = {cid: i for i, cid in enumerate(claim_ids)}
    n = len(claim_ids)

    # Fixed-width transition matrix so "status as of date" is one vectorised
    # comparison rather than a per-claim search.
    SENTINEL = np.int64(10**9)
    trans_ord = np.full((n, MAX_TRANSITIONS), SENTINEL, dtype=np.int64)
    trans_status = np.zeros((n, MAX_TRANSITIONS), dtype=np.int8)
    for row in transitions.itertuples():
        i = claim_index[row.claim_id]
        slot = row.transition_seq - 1
        if slot < MAX_TRANSITIONS:
            trans_ord[i, slot] = row.status_date.toordinal()
            trans_status[i, slot] = STATUS_TO_INT[row.status_code]

    submitted_ord = np.array([d.toordinal() for d in claims["submitted_date"]], dtype=np.int64)
    terminal_ord = np.array(
        [d.toordinal() if d is not None else SENTINEL for d in claims["terminal_date"]],
        dtype=np.int64,
    )

    # Visibility window per claim, expressed as a slice of the export grid.
    lo = np.searchsorted(export_ords, submitted_ord, side="left")
    hi = np.where(
        terminal_ord == SENTINEL,
        len(export_ords),
        np.searchsorted(export_ords, terminal_ord + grace_days, side="right"),
    )
    hi = np.minimum(hi, len(export_ords))
    counts = np.maximum(0, hi - lo)

    total = int(counts.sum())
    if total == 0:
        return pd.DataFrame()

    row_idx = np.repeat(np.arange(n), counts)
    # Build the per-claim export offsets without a Python loop.
    starts = np.repeat(lo, counts)
    within = np.arange(total) - np.repeat(np.cumsum(counts) - counts, counts)
    export_pos = starts + within
    snap_ord = export_ords[export_pos]

    # Current status = the last transition whose date is on or before the export.
    reached = trans_ord[row_idx] <= snap_ord[:, None]
    n_reached = reached.sum(axis=1)
    valid = n_reached > 0
    slot = np.clip(n_reached - 1, 0, MAX_TRANSITIONS - 1)

    status_int = trans_status[row_idx, slot]
    status_ord = trans_ord[row_idx, slot]

    row_idx = row_idx[valid]
    snap_ord = snap_ord[valid]
    status_int = status_int[valid]
    status_ord = status_ord[valid]

    status_code = np.array(STATUS_CODES, dtype=object)[status_int]

    snapshots = pd.DataFrame(
        {
            "claim_id": claim_ids[row_idx],
            "visit_id": claims["visit_id"].to_numpy()[row_idx],
            "patient_id": claims["patient_id"].to_numpy()[row_idx],
            "hospital_id": claims["hospital_id"].to_numpy()[row_idx],
            "insurer_id": claims["insurer_id"].to_numpy()[row_idx],
            "policy_number": claims["policy_number"].to_numpy()[row_idx],
            "claim_amount": claims["claim_amount"].to_numpy()[row_idx],
            "claim_status": status_code,
            "status_date": [date.fromordinal(int(o)) for o in status_ord],
            "submitted_date": claims["submitted_date"].to_numpy()[row_idx],
            "admission_date": claims["admission_date"].to_numpy()[row_idx],
            "discharge_date": claims["discharge_date"].to_numpy()[row_idx],
            "export_date": [date.fromordinal(int(o)) for o in snap_ord],
        }
    )

    # approved_amount is only populated once the claim has been adjudicated —
    # before that the portal shows a blank, as it would in reality.
    adjudicated = snapshots["claim_status"].isin(
        ["Approved", "Partially Approved", "Settled", "Rejected"]
    )
    snapshots["approved_amount"] = np.where(
        adjudicated, claims["approved_amount"].to_numpy()[row_idx], np.nan
    )
    snapshots["rejection_reason"] = np.where(
        snapshots["claim_status"] == "Rejected",
        claims["rejection_reason"].to_numpy()[row_idx],
        None,
    )
    return snapshots


def build_line_items(
    rng: np.random.Generator, bills: pd.DataFrame, claims: pd.DataFrame
) -> pd.DataFrame:
    """Explode each claimed bill into its itemised charges.

    Line amounts sum exactly to the bill's gross amount, so the rules engine's
    exclusion arithmetic can be checked against the bill total rather than trusted.
    """
    if claims.empty:
        return pd.DataFrame()

    claimed_bills = bills[bills["bill_id"].isin(set(claims["bill_id"]))].reset_index(drop=True)
    bill_to_claim = dict(zip(claims["bill_id"], claims["claim_id"]))

    frames = []
    for column, category in BILL_COMPONENTS:
        amounts = claimed_bills[column].to_numpy(dtype=float)
        keep = amounts > 0
        if not keep.any():
            continue
        subset = claimed_bills[keep]
        frames.append(
            pd.DataFrame(
                {
                    "claim_id": subset["bill_id"].map(bill_to_claim).to_numpy(),
                    "procedure_code": np.where(
                        category == "PROCEDURE", subset["procedure_code"].to_numpy(), None
                    ),
                    "item_category": category,
                    "room_type": subset["ward_type"].to_numpy(),
                    "quantity": np.where(
                        category == "ROOM", np.maximum(subset["length_of_stay"].to_numpy(), 1), 1
                    ),
                    "line_amount": np.round(amounts[keep], 2),
                }
            )
        )

    line_items = pd.concat(frames, ignore_index=True)
    line_items = line_items.sort_values(["claim_id", "item_category"]).reset_index(drop=True)
    line_items["line_item_id"] = [f"LI{i + 1:09d}" for i in range(len(line_items))]
    line_items["unit_price"] = np.round(
        line_items["line_amount"] / line_items["quantity"].clip(lower=1), 2
    )
    # A real itemised claim carries a human-readable description alongside the code;
    # the column order here must match conf/sources.yaml exactly.
    line_items["procedure_name"] = np.where(
        line_items["item_category"] == "PROCEDURE",
        "Procedure " + line_items["procedure_code"].fillna(""),
        line_items["item_category"].str.title().str.replace("_", " ", regex=False) + " charges",
    )
    return line_items[
        [
            "line_item_id",
            "claim_id",
            "procedure_code",
            "procedure_name",
            "item_category",
            "room_type",
            "quantity",
            "unit_price",
            "line_amount",
        ]
    ]
