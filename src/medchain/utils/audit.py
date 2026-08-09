"""Run and batch identity.

Every row written by the platform carries the ``batch_id`` that produced it, and
every pipeline execution carries a ``run_id``. Together they answer the two
questions that come up whenever something looks wrong: *which execution wrote this
row*, and *can I safely delete and replay it*.

``batch_id`` is derived from (logical date, layer, source) rather than from the
wall clock, so replaying 2024-06-01 tomorrow produces the same batch_id it did
originally. That is what makes MERGE-based re-runs converge instead of accumulating
duplicates.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime


def _utcnow() -> datetime:
    return datetime.now(UTC)


def make_batch_id(logical_date: date | str, layer: str, source: str = "all") -> str:
    """Deterministic batch identifier for one (date, layer, source) unit of work."""
    if isinstance(logical_date, date):
        logical_date = logical_date.isoformat()
    digest = hashlib.sha256(f"{logical_date}|{layer}|{source}".encode()).hexdigest()[:12]
    return f"{layer}_{logical_date}_{source}_{digest}"


@dataclass(frozen=True)
class RunContext:
    """Identity and provenance for a single pipeline execution."""

    run_id: str
    logical_date: date
    layer: str
    source: str = "all"
    started_at: datetime = field(default_factory=_utcnow)
    triggered_by: str = field(default_factory=lambda: os.environ.get("MEDCHAIN_TRIGGER", "manual"))

    @classmethod
    def create(
        cls,
        logical_date: date | str,
        layer: str,
        source: str = "all",
        run_id: str | None = None,
    ) -> RunContext:
        if isinstance(logical_date, str):
            logical_date = date.fromisoformat(logical_date)
        # ADF passes its own pipeline run id through so a Databricks failure can be
        # traced straight back to the orchestrator run that caused it.
        resolved = run_id or os.environ.get("MEDCHAIN_RUN_ID") or str(uuid.uuid4())
        return cls(run_id=resolved, logical_date=logical_date, layer=layer, source=source)

    @property
    def batch_id(self) -> str:
        return make_batch_id(self.logical_date, self.layer, self.source)

    def for_source(self, source: str) -> RunContext:
        """Derive a sibling context scoped to a single source."""
        return RunContext(
            run_id=self.run_id,
            logical_date=self.logical_date,
            layer=self.layer,
            source=source,
            started_at=self.started_at,
            triggered_by=self.triggered_by,
        )

    def as_dict(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "logical_date": self.logical_date.isoformat(),
            "layer": self.layer,
            "source": self.source,
            "started_at": self.started_at.isoformat(),
            "triggered_by": self.triggered_by,
        }
