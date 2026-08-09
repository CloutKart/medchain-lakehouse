"""Write generated data out as the source systems would actually deliver it.

Real ingestion does not receive one giant file. It receives a historical
backfill once, then a stream of periodic exports — daily for the HIS and finance
systems, weekly for the insurer portal and HR roster. Reproducing that shape is what
makes the ADF Copy Activity, the ``ingest_date`` partitioning and the incremental
watermark meaningful rather than decorative.

Layout produced under ``landing/``::

    landing/<source>/initial_load/<source>_<window_start>.csv
    landing/<source>/incremental/<source>_<YYYY-MM-DD>.csv
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


def _as_date(value) -> date | None:
    """Coerce the assorted date representations flowing through the generator."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    return None


def write_source(
    df: pd.DataFrame,
    landing_root: Path,
    source_name: str,
    *,
    date_series: pd.Series | None,
    window_start: date,
    window_end: date,
    increment_days: int,
    cadence_days: int = 1,
    fmt: str = "csv",
) -> dict[str, int]:
    """Split one source into an initial load plus dated incremental exports.

    ``date_series`` says which export each row belongs to. When it is ``None`` the
    source is a small reference table with no natural event date (the procedure
    catalogue), and every export is a full snapshot — which is exactly how such
    tables arrive in practice.
    """
    source_dir = landing_root / source_name
    initial_dir = source_dir / "initial_load"
    incr_dir = source_dir / "incremental"
    initial_dir.mkdir(parents=True, exist_ok=True)
    incr_dir.mkdir(parents=True, exist_ok=True)

    cutoff = window_end - timedelta(days=increment_days)
    written: dict[str, int] = {}

    def _emit(frame: pd.DataFrame, path: Path) -> None:
        if fmt == "json":
            # Newline-delimited JSON — what Spark and ADF both expect for a JSON
            # source, and what an HR system's REST export typically produces.
            frame.to_json(path, orient="records", lines=True, date_format="iso")
        else:
            frame.to_csv(path, index=False)
        written[path.name] = len(frame)

    if date_series is None:
        # Reference table: full snapshot at the initial load and at each cadence
        # boundary inside the incremental window.
        _emit(df, initial_dir / f"{source_name}_{window_start.isoformat()}.{fmt}")
        cursor = cutoff + timedelta(days=1)
        while cursor <= window_end:
            _emit(df, incr_dir / f"{source_name}_{cursor.isoformat()}.{fmt}")
            cursor += timedelta(days=cadence_days)
        return written

    dates = date_series.map(_as_date)
    # Rows with no usable date belong to the historical load — that is where an
    # undated legacy record would land in a real migration.
    is_incremental = dates.map(lambda d: d is not None and d > cutoff)

    backfill = df[~is_incremental]
    _emit(backfill, initial_dir / f"{source_name}_{window_start.isoformat()}.{fmt}")

    incremental = df[is_incremental]
    if not incremental.empty:
        for export_date, group in incremental.groupby(dates[is_incremental]):
            _emit(group, incr_dir / f"{source_name}_{export_date.isoformat()}.{fmt}")

    return written


def write_truth(frames: dict[str, pd.DataFrame], truth_root: Path) -> None:
    """Persist the ground-truth tables.

    These are never read by Bronze, Silver or Gold. They exist so the data quality
    scorecard can report *measured* MPI precision and recall, and *measured* claim
    reconstruction coverage, instead of internal consistency checks that would pass
    even if the matching logic were wrong.
    """
    truth_root.mkdir(parents=True, exist_ok=True)
    for name, frame in frames.items():
        if frame is None or frame.empty:
            log.warning("Truth frame %s is empty; skipping", name)
            continue
        frame.to_parquet(truth_root / f"{name}.parquet", index=False)
        log.info("  truth/%-28s %9d rows", f"{name}.parquet", len(frame))


def summarise(written: dict[str, dict[str, int]]) -> str:
    """Human-readable summary of what was written, for the CLI and the runbook."""
    lines = []
    for source, files in sorted(written.items()):
        total = sum(files.values())
        initial = sum(v for k, v in files.items() if "initial" not in k and len(files) == 1)
        del initial
        lines.append(f"  {source:<26} {len(files):>4} files  {total:>10,} rows")
    return "\n".join(lines)
