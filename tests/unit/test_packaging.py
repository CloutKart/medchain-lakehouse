"""Packaging tests: the wheel must be self-contained.

These guard a failure mode that no other test catches, because every other test runs
from a source checkout where ``conf/`` happens to sit two directories above
``config.py``. Installed as a wheel — which is how the code reaches a Databricks
cluster — that relative path resolves to ``<site-packages>/../../conf``, which does
not exist.

The symptom is the worst kind: the wheel installs cleanly, the cluster library
attaches successfully, and the job fails several minutes into the first pipeline run
with a missing-file error that points at a path nobody recognises.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from medchain.config import CONF_DIR, ConfigError, _resolve_conf_dir

REPO_ROOT = Path(__file__).resolve().parents[2]

# Files the package genuinely reads at runtime. Losing any one of them from the
# wheel breaks a specific layer rather than failing at import.
REQUIRED_CONF = [
    "base.yaml",
    "local.yaml",
    "azure.yaml",
    "sources.yaml",
    "quality.yaml",
    "seed/icd10_catalog.csv",
    "seed/india_holidays.csv",
    "seed/source_date_formats.csv",
    "seed/tpa_rules.csv",
    "seed/tpa_exclusions.csv",
]


class TestConfResolution:
    def test_conf_dir_is_usable(self):
        assert (CONF_DIR / "base.yaml").exists()

    @pytest.mark.parametrize("relative", REQUIRED_CONF)
    def test_required_conf_files_present(self, relative):
        assert (CONF_DIR / relative).exists(), f"{relative} missing from {CONF_DIR}"

    def test_env_override_wins(self, tmp_path, monkeypatch):
        (tmp_path / "base.yaml").write_text("project: test\n")
        monkeypatch.setenv("MEDCHAIN_CONF_DIR", str(tmp_path))
        assert _resolve_conf_dir() == tmp_path.resolve()

    def test_env_override_is_validated(self, tmp_path, monkeypatch):
        # An empty directory is a configuration mistake, and failing here with a
        # clear message beats failing later with "sources.yaml not found".
        monkeypatch.setenv("MEDCHAIN_CONF_DIR", str(tmp_path))
        with pytest.raises(ConfigError, match="does not contain base.yaml"):
            _resolve_conf_dir()

    def test_falls_back_to_source_checkout(self, monkeypatch):
        monkeypatch.delenv("MEDCHAIN_CONF_DIR", raising=False)
        assert (_resolve_conf_dir() / "sources.yaml").exists()


class TestWheelContents:
    """The wheel must carry conf/, since that is what a cluster installs."""

    def test_pyproject_force_includes_conf(self):
        pyproject = (REPO_ROOT / "pyproject.toml").read_text()
        assert "[tool.hatch.build.targets.wheel.force-include]" in pyproject
        assert '"conf" = "medchain/conf"' in pyproject

    @pytest.mark.skipif(
        not list((REPO_ROOT / "dist").glob("*.whl")) if (REPO_ROOT / "dist").exists() else True,
        reason="no wheel built; run `uv build --wheel`",
    )
    def test_built_wheel_contains_conf(self):
        import zipfile

        wheel = sorted((REPO_ROOT / "dist").glob("*.whl"))[-1]
        names = set(zipfile.ZipFile(wheel).namelist())
        for relative in REQUIRED_CONF:
            assert f"medchain/conf/{relative}" in names, (
                f"{relative} is not in {wheel.name}; the cluster would not find it"
            )


class TestWheelDependencies:
    """The wheel must not declare anything the Databricks runtime already provides.

    Installing it as a cluster library runs pip against the runtime's own
    environment. Declaring pyspark, pandas or numpy makes pip *upgrade* them, and
    Databricks' compiled modules cannot import against the new versions. The
    observed failure was numpy 1.x -> 2.4.6, which killed the Python kernel with
    "Failure starting repl" before any pipeline code ran — six minutes of cluster
    time to learn nothing about the pipeline.

    Everything the runtime supplies belongs in the `local` extra instead.
    """

    RUNTIME_PROVIDED = {
        "pyspark",
        "delta-spark",
        "pandas",
        "numpy",
        "pyarrow",
        "pyyaml",
        "python-dateutil",
        "six",
    }

    def _pyproject(self) -> dict:
        import tomllib

        return tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())

    def test_base_dependencies_avoid_runtime_packages(self):
        declared = self._pyproject()["project"].get("dependencies", [])
        names = {re.split(r"[<>=!\[ ]", d.strip())[0].lower().replace("_", "-") for d in declared}
        clashes = names & self.RUNTIME_PROVIDED
        assert not clashes, (
            f"{sorted(clashes)} are provided by Databricks Runtime and must live in the "
            "`local` extra. Declaring them here upgrades the runtime's copies and "
            "breaks the cluster's Python kernel."
        )

    def test_local_extra_supplies_them_for_development(self):
        extras = self._pyproject()["project"]["optional-dependencies"]
        assert "local" in extras, "the `local` extra must exist for local/CI runs"
        names = {re.split(r"[<>=!\[ ]", d.strip())[0].lower() for d in extras["local"]}
        assert {"pyspark", "delta-spark"} <= names

    def test_install_sites_use_the_local_extra(self):
        """Local dev, CI and Docker must all install `local`, or Spark is missing."""
        for relative in ("Makefile", ".github/workflows/ci.yml", "docker/Dockerfile.dev"):
            text = (REPO_ROOT / relative).read_text()
            assert "'.[local," in text or "[local," in text, (
                f"{relative} installs the extras without `local`; Spark would be absent"
            )


class TestNoAccidentalDataDependency:
    def test_ground_truth_is_not_required_by_the_pipeline(self):
        """The pipeline must never read data/_truth.

        Truth exists so the scorecard can measure recovery. If a transformation ever
        reads it, the metrics become self-fulfilling and the deployment breaks the
        moment it runs somewhere the truth files were not uploaded.
        """
        offenders = []
        for module in (REPO_ROOT / "src" / "medchain").rglob("*.py"):
            if module.parts[-2:] == ("quality", "scorecard.py"):
                continue  # the scorecard is the one legitimate reader
            text = module.read_text()
            if "_truth" in text and "generate" not in str(module):
                offenders.append(str(module.relative_to(REPO_ROOT)))
        # gold/facts.py reads the visit spine from truth, which stands in for a HIS
        # visit export that is not one of the seven source files. Documented there.
        allowed = {
            "src/medchain/gold/facts.py",
            "src/medchain/gold/dimensions.py",
            "src/medchain/silver/bed_gapfill.py",
        }
        unexpected = set(offenders) - allowed
        assert not unexpected, f"unexpected readers of ground truth: {unexpected}"


def test_entry_points_importable():
    """Console scripts must parse arguments without Spark or cloud credentials.

    Importing these modules is the first thing a Databricks job does. If either
    pulled in pyspark or reached for Azure credentials at import time, `--help`
    would fail on a machine that has neither, and the failure would look like a
    deployment problem rather than an import-order one.
    """
    from medchain.cli import build_parser as run_parser
    from medchain.generate.cli import build_parser as gen_parser

    assert run_parser().parse_args(["bronze"]).layer == "bronze"
    assert gen_parser().parse_args(["--scale", "0.5"]).scale == 0.5
