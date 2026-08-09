"""The dashboard must not be able to disagree with the warehouse.

These tests run against the exported JSON if it exists. The failure they exist to
prevent is subtle and expensive: someone edits a query in ``medchain.web.export``,
the dashboard renders a plausible number, and nobody notices it no longer matches
what the pipeline computed. A dashboard that is quietly wrong is worse than one that
is obviously broken, because it gets quoted.

Skipped when no export is present, so a fresh clone still runs green — but CI runs
``make web-data`` first, so it is exercised there.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA = REPO_ROOT / "dashboards" / "web" / "public" / "data"

PANELS = ["headline", "clinical", "operational", "financial", "quality", "reference"]

pytestmark = pytest.mark.skipif(
    not (DATA / "headline.json").exists(),
    reason="no web export present; run `make web-data`",
)


def load(panel: str) -> dict:
    return json.loads((DATA / f"{panel}.json").read_text())


@pytest.fixture(scope="module")
def headline() -> dict:
    return load("headline")


class TestProvenanceIsMeasuredNotAsserted:
    """The footer's "Source" line has to describe the run that made the file.

    It used to be a hardcoded string in the frontend, so it read the same whether the
    numbers came from a laptop or from the cluster — the one claim on the page that
    could never be wrong because it was never checked. Now the export measures it.
    """

    @pytest.mark.parametrize("panel", PANELS)
    def test_every_panel_carries_its_origin(self, panel):
        source = load(panel).get("source")
        assert source, f"{panel}.json has no source block"
        for field in ("environment", "engine", "store", "generated_at"):
            assert source.get(field), f"{panel}.source.{field} is missing or empty"

    def test_all_panels_agree_on_one_origin(self):
        """Panels are written in one pass, so a disagreement means a stale file."""
        sources = {p: load(p)["source"] for p in PANELS}
        distinct = {json.dumps(s, sort_keys=True) for s in sources.values()}
        assert len(distinct) == 1, (
            "panels disagree about where they came from, which means at least one is "
            f"left over from an earlier export: {sources}"
        )

    def test_engine_matches_the_backend_that_ran(self):
        """Reported engine must follow the backend class, not a literal."""
        from medchain.config import load_config
        from medchain.web.export import DuckDBBackend, SparkBackend, provenance

        cfg = load_config("local")
        assert provenance(cfg, DuckDBBackend(con=None))["engine"] == "DuckDB"
        spark_like = SparkBackend.__new__(SparkBackend)  # no cluster needed
        assert "Spark" in provenance(cfg, spark_like)["engine"]

    def test_store_follows_the_environment(self):
        from medchain.config import load_config
        from medchain.web.export import DuckDBBackend, provenance

        local = provenance(load_config("local"), DuckDBBackend(con=None))
        assert local["store"] == "local filesystem"
        assert local["environment"] == "local"


class TestExportShape:
    @pytest.mark.parametrize("panel", PANELS)
    def test_panel_exists_and_parses(self, panel):
        assert (DATA / f"{panel}.json").exists()
        assert isinstance(load(panel), dict)

    @pytest.mark.parametrize("panel", PANELS)
    def test_panel_is_valid_json_not_python(self, panel):
        """NaN and Infinity are valid Python literals and invalid JSON.

        ``json.dumps`` emits them bare, so a file can look fine when written and then
        throw in the browser's ``JSON.parse``. The export nulls them; this proves it.
        """
        text = (DATA / f"{panel}.json").read_text()
        for token in ("NaN", "Infinity", "-Infinity"):
            assert token not in text, f"{panel}.json contains bare {token}"

    def test_no_raw_rows_leak(self):
        """Everything exported is an aggregate.

        The dashboard is intended to be publishable. If a query ever stops
        aggregating, a patient-level or claim-level table would be shipped to a static
        site. Row counts here are a proxy: nothing legitimate exceeds a few hundred.
        """
        for panel in ("clinical", "operational", "financial"):
            for key, value in load(panel).items():
                if isinstance(value, list):
                    assert len(value) < 1000, (
                        f"{panel}.{key} has {len(value)} rows — is it still aggregating?"
                    )

    def test_total_payload_is_small(self):
        total = sum((DATA / f"{p}.json").stat().st_size for p in PANELS)
        assert total < 1_000_000, f"export is {total / 1024:.0f} KB; expected well under 1 MB"


class TestNumbersMatchTheWarehouse:
    """The headline findings, asserted against the values the pipeline produced."""

    def test_readmission_gap(self, headline):
        c = headline["clinical"]
        assert c["rate_network"] * 100 == pytest.approx(20.31, abs=0.05)
        assert c["rate_hospital"] * 100 == pytest.approx(17.38, abs=0.05)
        # The headline claim. If this moves, the copy on the page is wrong.
        assert c["readmission_gap_pp"] == pytest.approx(2.93, abs=0.05)

    def test_network_readmission_is_a_superset(self, headline):
        """Network-wide readmissions must include every single-hospital one.

        A patient readmitted at the same hospital is also readmitted within the
        network. If the network rate were ever lower, the MPI join would be dropping
        rows rather than adding them.
        """
        c = headline["clinical"]
        assert c["rate_network"] >= c["rate_hospital"]

    def test_misattribution(self, headline):
        a = headline["attribution"]
        assert a["misattributed"] == 60990
        assert a["misattributed"] / a["total_attributed"] == pytest.approx(0.098, abs=0.002)

    def test_financials(self, headline):
        f = headline["financial"]
        crore = 1e7
        assert f["billed"] / crore == pytest.approx(3527, abs=5)
        assert f["room_excess"] / crore == pytest.approx(74, abs=2)
        assert f["copay"] / crore == pytest.approx(446, abs=5)
        # The reimbursement gap is billed minus reimbursed, by construction.
        assert f["gap"] == pytest.approx(f["billed"] - f["reimbursed"], rel=1e-6)

    def test_quality_summary(self):
        s = load("quality")["summary"]
        assert s["total"] == 50
        assert s["blocking_failures"] == 0
        assert s["passed"] + s["blocking_failures"] + s["warnings"] == s["total"]


class TestChartInputsAreRenderable:
    """Guards against charts that render as an empty box."""

    def test_waterfall_closes_to_the_net_figure(self):
        """Walking the deductions must land exactly on the stated net reimbursement.

        This is the arithmetic the whole financial section rests on: if the cascade
        does not close, either a deduction is double-counted or one is missing.
        """
        steps = load("financial")["waterfall"]
        billed = next(s for s in steps if s["stage"] == "Billed")["amount"]
        net = next(s for s in steps if s["stage"] == "Net reimbursed")["amount"]
        deductions = sum(s["amount"] for s in steps if s["kind"] == "deduction")
        assert billed + deductions == pytest.approx(net, rel=1e-6)

    def test_exactly_one_recoverable_bucket(self):
        """Room-rent excess is the only deduction the hospital controls.

        The waterfall colours it differently to say so. If another bucket were marked
        recoverable the chart would be claiming money is retrievable when it is
        contractual.
        """
        steps = load("financial")["waterfall"]
        recoverable = [s["stage"] for s in steps if s["recoverable"]]
        assert recoverable == ["Room rent excess"]

    def test_diverging_sums_to_the_headline(self):
        """Per-department misattribution must reconcile to the number on the hero."""
        rows = load("operational")["attribution_by_department"]
        total = sum(abs(r["misattributed"]) for r in rows)
        assert total == load("headline")["attribution"]["misattributed"]

    def test_time_series_is_continuous(self):
        monthly = load("clinical")["monthly"]
        assert len(monthly) >= 24, "expected at least two years of months"
        assert all(m["inpatient"] >= 0 for m in monthly)
        # A null rate is renderable (the line breaks) but every month having one is not.
        assert any(m["readmission_pct"] is not None for m in monthly)

    def test_heatmap_grid_is_populated(self):
        cells = load("operational")["occupancy_grid"]
        hospitals = {c["hospital_name"] for c in cells}
        wards = {c["ward_type"] for c in cells}
        assert len(hospitals) == 8
        assert len(wards) >= 5
        assert all(0 <= c["occupancy_pct"] <= 200 for c in cells)

    def test_quality_checks_split_into_both_kinds(self):
        """The recovery/structural distinction is the point of the quality section."""
        checks = load("quality")["checks"]
        kinds = {c["check_type"] for c in checks}
        assert "recovery" in kinds, "recovery metrics missing — the section loses its argument"
        assert len(kinds) > 1, "structural checks missing"
