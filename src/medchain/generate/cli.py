"""``medchain-gen`` — build the MedChain source-system exports.

Usage::

    medchain-gen                      # full 3-year dataset, seed 42
    medchain-gen --scale 0.01         # ~6k visits, the test fixture size
    medchain-gen --scale 1.0 --seed 7 # a different but equally reproducible world

The pipeline is: reference entities -> events -> claims -> corruption -> files.
Truth tables are written alongside so the scorecard can measure what the platform
actually recovered.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from medchain.config import load_config
from medchain.generate import claims as claims_mod
from medchain.generate import corruption, entities, events, writer
from medchain.utils.logging import get_logger, setup_logging

log = get_logger("medchain.generate")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate MedChain source data")
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="Multiplier on the volumes in conf/base.yaml (0.01 = test fixture size)",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="RNG seed; identical seeds give identical output"
    )
    parser.add_argument(
        "--out", type=Path, default=None, help="Landing directory (default: from config)"
    )
    parser.add_argument(
        "--truth-out", type=Path, default=None, help="Truth directory (default: from config)"
    )
    parser.add_argument(
        "--increment-days",
        type=int,
        default=14,
        help="Trailing days emitted as individual dated exports rather than backfill",
    )
    parser.add_argument(
        "--clean", action="store_true", help="Delete existing landing/truth data first"
    )
    parser.add_argument(
        "--env", default=None, help="Config environment (default: $MEDCHAIN_ENV or local)"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    args = build_parser().parse_args(argv)
    cfg = load_config(args.env)

    landing = args.out or Path(cfg.path("landing"))
    truth_root = args.truth_out or Path(cfg.path("truth"))

    if cfg.is_azure and args.out is None:
        log.error(
            "Refusing to generate directly into ADLS. Generate locally, then run "
            "`make upload` to copy the landing directory to the storage account."
        )
        return 2

    window_start = cfg.window_start
    window_end = cfg.window_end
    scale = args.scale

    volume = cfg.get("volume", default={})
    defects = cfg.get("defects", default={})

    n_hospitals = len(entities.HOSPITALS)
    n_people = max(50, int(volume.get("people", 180000) * scale))
    n_visits = max(100, int(volume.get("visits", 600000) * scale))
    # Doctors and procedures scale sub-linearly and have hard floors: every hospital
    # needs a viable roster across all departments even at --scale 0.01, otherwise
    # visits cannot be assigned a doctor who was actually posted there.
    n_doctors = max(
        n_hospitals * len(entities.DEPARTMENTS) // 2,
        int(volume.get("doctors", 350) * min(scale * 4, 1.0)),
    )
    n_procedures = max(40, int(volume.get("procedures", 400) * min(scale * 6, 1.0)))

    if args.clean:
        for path in (landing, truth_root):
            if path.exists():
                shutil.rmtree(path)
                log.info("Removed %s", path)

    rng = np.random.default_rng(args.seed)
    seed_dir = cfg.seed_dir
    started = time.time()

    log.info("=" * 72)
    log.info("MedChain synthetic data generation")
    log.info("  seed=%s scale=%s window=%s..%s", args.seed, scale, window_start, window_end)
    log.info(
        "  people=%s visits=%s doctors=%s procedures=%s",
        n_people,
        n_visits,
        n_doctors,
        n_procedures,
    )
    log.info("=" * 72)

    # ---------------------------------------------------------- reference data
    log.info("[1/7] Reference entities")
    ref = entities.build_reference_data(
        rng,
        seed_dir,
        n_doctors=n_doctors,
        n_people=n_people,
        n_procedures=n_procedures,
        window_start=window_start,
        window_end=window_end,
        reassign_rate=float(defects.get("doctor_reassign_rate", 0.15)),
    )
    holidays = entities.load_holidays(seed_dir)
    log.info(
        "      %d hospitals, %d wards, %d doctors (%d assignment intervals)",
        len(ref.hospitals),
        len(ref.wards),
        len(ref.doctors),
        len(ref.doctor_assignments),
    )

    # ------------------------------------------------------------ registrations
    log.info("[2/7] Registrations")
    registrations = events.build_registrations(
        rng,
        ref.people,
        ref.hospitals,
        duplicate_rate=float(defects.get("duplicate_person_rate", 0.22)),
        window_start=window_start,
    )
    n_dupes = int(registrations["is_duplicate"].sum())
    log.info(
        "      %d registrations for %d people (%d duplicate registrations, %.1f%%)",
        len(registrations),
        len(ref.people),
        n_dupes,
        100 * n_dupes / len(ref.people),
    )

    # -------------------------------------------------------------------- visits
    log.info("[3/7] Visits")
    visits = events.build_visits(
        rng, ref, registrations, n_visits, window_start, window_end, holidays
    )
    visits = events.add_readmissions(rng, visits, registrations, window_end)
    log.info(
        "      %d visits (%d inpatient)",
        len(visits),
        int(visits["admission_type"].isin(["IPD", "EMERGENCY"]).sum()),
    )

    # ---------------------------------------------------------------- bed events
    log.info("[4/7] Bed movements")
    bed_events = events.build_bed_events(
        rng, visits, ref.wards, float(defects.get("unclosed_stay_rate", 0.02))
    )
    log.info("      %d bed events", len(bed_events))

    # --------------------------------------------------------------------- bills
    log.info("[5/7] Bills")
    bills = events.build_bills(rng, visits, ref.hospitals, bed_events)
    log.info(
        "      %d bills, gross INR %.1f crore",
        len(bills),
        bills["gross_amount"].sum() / 1e7 if not bills.empty else 0,
    )

    # -------------------------------------------------------------------- claims
    log.info("[6/7] Claims, lifecycle and TPA adjudication")
    rules = pd.read_csv(seed_dir / "tpa_rules.csv")
    exclusions = pd.read_csv(seed_dir / "tpa_exclusions.csv")
    claim_rows, transitions, tpa_truth = claims_mod.build_claims(
        rng, bills, rules, exclusions, window_end
    )
    snapshots = claims_mod.build_claim_snapshots(
        claim_rows, transitions, window_start, window_end, cadence_days=7
    )
    snapshots = corruption.attach_hospital_ref(rng, snapshots, claim_rows)
    line_items = claims_mod.build_line_items(rng, bills, claim_rows)
    log.info(
        "      %d claims, %d true transitions, %d portal snapshots, %d line items",
        len(claim_rows),
        len(transitions),
        len(snapshots),
        len(line_items),
    )

    # ---------------------------------------------------------------- corruption
    log.info("[7/7] Defect injection and export")
    registration_export, mpi_truth = corruption.corrupt_registrations(
        rng, registrations, ref.people, seed_dir
    )
    procedure_export = corruption.corrupt_procedure_master(
        rng, ref.procedures, float(defects.get("missing_icd10_rate", 0.08))
    )
    doctor_export = corruption.build_doctor_exports(
        ref.doctor_assignments, ref.doctors, window_start, window_end, cadence_days=7
    )

    billing_export = bills[
        [
            "bill_id",
            "hospital_id",
            "patient_id",
            "visit_id",
            "admission_date",
            "discharge_date",
            "gross_amount",
            "discount_amount",
            "tax_amount",
            "net_payable",
            "room_charge",
            "payment_mode",
            "bill_date",
        ]
    ].copy()
    for col in ("admission_date", "discharge_date"):
        billing_export[col] = billing_export[col].map(lambda d: d.isoformat())

    claims_export = snapshots[
        [
            "claim_id",
            "patient_id",
            "hospital_id",
            "insurer_id",
            "policy_number",
            "claim_amount",
            "approved_amount",
            "claim_status",
            "status_date",
            "submitted_date",
            "admission_date",
            "discharge_date",
            "rejection_reason",
            "hospital_ref_no",
            "export_date",
        ]
    ].copy()
    for col in ("status_date", "submitted_date", "admission_date", "discharge_date", "export_date"):
        claims_export[col] = claims_export[col].map(lambda d: d.isoformat() if d else None)

    bed_export = bed_events[
        [
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
    ]

    # Line items inherit their claim's submission date so they land in the same
    # export as the claim they belong to.
    claim_submitted = dict(zip(claim_rows["claim_id"], claim_rows["submitted_date"]))
    line_item_dates = pd.Series(
        [claim_submitted.get(c) for c in line_items["claim_id"]], index=line_items.index
    )

    landing.mkdir(parents=True, exist_ok=True)
    written: dict[str, dict[str, int]] = {}

    written["patient_registrations"] = writer.write_source(
        registration_export,
        landing,
        "patient_registrations",
        date_series=registration_export["registered_date"],
        window_start=window_start,
        window_end=window_end,
        increment_days=args.increment_days,
    )
    written["insurance_claims"] = writer.write_source(
        claims_export,
        landing,
        "insurance_claims",
        date_series=claims_export["export_date"],
        window_start=window_start,
        window_end=window_end,
        increment_days=args.increment_days,
        cadence_days=7,
    )
    written["claim_line_items"] = writer.write_source(
        line_items,
        landing,
        "claim_line_items",
        date_series=line_item_dates,
        window_start=window_start,
        window_end=window_end,
        increment_days=args.increment_days,
    )
    written["billing_transactions"] = writer.write_source(
        billing_export,
        landing,
        "billing_transactions",
        date_series=billing_export["bill_date"],
        window_start=window_start,
        window_end=window_end,
        increment_days=args.increment_days,
    )
    written["doctor_assignments"] = writer.write_source(
        doctor_export,
        landing,
        "doctor_assignments",
        date_series=doctor_export["export_date"],
        window_start=window_start,
        window_end=window_end,
        increment_days=args.increment_days,
        cadence_days=7,
        fmt="json",
    )
    written["bed_occupancy_log"] = writer.write_source(
        bed_export,
        landing,
        "bed_occupancy_log",
        date_series=bed_export["event_ts"],
        window_start=window_start,
        window_end=window_end,
        increment_days=args.increment_days,
    )
    written["procedure_master"] = writer.write_source(
        procedure_export,
        landing,
        "procedure_master",
        date_series=None,
        window_start=window_start,
        window_end=window_end,
        increment_days=args.increment_days,
        cadence_days=7,
    )

    # ------------------------------------------------------------------- truth
    visit_truth = visits[
        [
            "visit_id",
            "patient_id",
            "person_id",
            "hospital_id",
            "doctor_id",
            "department",
            "procedure_code",
            "procedure_category",
            "admission_type",
            "admission_date",
            "discharge_date",
            "length_of_stay",
        ]
    ].copy()
    for col in ("admission_date", "discharge_date"):
        visit_truth[col] = visit_truth[col].map(lambda d: d.isoformat())

    transitions_truth = transitions.copy()
    if not transitions_truth.empty:
        transitions_truth["status_date"] = transitions_truth["status_date"].map(
            lambda d: d.isoformat()
        )

    writer.write_truth(
        {
            "mpi_truth": mpi_truth,
            "visit_truth": visit_truth,
            "claim_transitions_truth": transitions_truth,
            "tpa_truth": tpa_truth,
            "doctor_assignments_truth": ref.doctor_assignments,
            "ward_truth": ref.wards,
            "procedure_truth": ref.procedures,
            "hospital_truth": ref.hospitals,
            "insurer_truth": ref.insurers,
            "doctor_truth": ref.doctors,
        },
        truth_root,
    )

    elapsed = time.time() - started
    log.info("=" * 72)
    log.info("Landing files written to %s", landing)
    log.info("%s", writer.summarise(written))
    log.info("Truth tables written to %s", truth_root)
    log.info("Completed in %.1fs", elapsed)
    log.info("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
