"""Pure-Python tests: normalisation, key derivation, config, schemas, state machine.

No Spark. These run in under a second and catch the class of bug that is hardest to
see in a DataFrame — a normaliser that quietly disagrees with itself between the
Python and Spark implementations, or a batch id that is not actually deterministic.
"""

from __future__ import annotations

from datetime import date

import pytest

from medchain.config import ConfigError, load_config
from medchain.silver.claim_history import _transitive_closure
from medchain.utils.audit import RunContext, make_batch_id
from medchain.utils.keys import clean_python_name, clean_python_phone
from medchain.utils.schemas import column_names, parse_type, source_schema


class TestNameNormalisation:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Rajesh Kumar", "RAJESH KUMAR"),
            ("Dr. Rajesh Kumar", "RAJESH KUMAR"),
            ("MR RAJESH KUMAR", "RAJESH KUMAR"),
            ("Shri  Rajesh   Kumar", "RAJESH KUMAR"),
            # Stacked honorifics: two passes are needed, which is why the
            # implementation loops rather than substituting once.
            ("Dr. Mrs. Priya Sharma", "PRIYA SHARMA"),
            ("Priya Sharma-Iyer", "PRIYA SHARMA IYER"),
            ("priya sharma 123", "PRIYA SHARMA"),
            ("", ""),
            (None, ""),
        ],
    )
    def test_normalises(self, raw, expected):
        assert clean_python_name(raw) == expected

    def test_title_only_inside_prefix(self):
        # "DRAVID" starts with "DR" but is not an honorific — a naive prefix strip
        # would mangle it into "AVID".
        assert clean_python_name("Dravid Sharma") == "DRAVID SHARMA"


class TestPhoneNormalisation:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("9876543210", "9876543210"),
            ("09876543210", "9876543210"),
            ("+91-98765 43210", "9876543210"),
            ("+91 9876543210", "9876543210"),
            ("98765-43210", "9876543210"),
            ("  9876 543 210 ", "9876543210"),
        ],
    )
    def test_all_formats_converge(self, raw, expected):
        assert clean_python_phone(raw) == expected

    def test_too_short_is_none(self):
        assert clean_python_phone("12345") is None
        assert clean_python_phone(None) is None


class TestBatchIdentity:
    def test_batch_id_is_deterministic(self):
        # The whole idempotency story rests on this: replaying 2024-06-01 tomorrow
        # must produce the same batch id it did originally.
        a = make_batch_id(date(2024, 6, 1), "bronze", "insurance_claims")
        b = make_batch_id("2024-06-01", "bronze", "insurance_claims")
        assert a == b

    def test_batch_id_varies_by_input(self):
        base = make_batch_id("2024-06-01", "bronze", "claims")
        assert base != make_batch_id("2024-06-02", "bronze", "claims")
        assert base != make_batch_id("2024-06-01", "silver", "claims")
        assert base != make_batch_id("2024-06-01", "bronze", "bills")

    def test_run_context_scoping(self):
        ctx = RunContext.create("2024-06-01", layer="silver")
        scoped = ctx.for_source("mpi")
        assert scoped.run_id == ctx.run_id  # same execution
        assert scoped.batch_id != ctx.batch_id  # different unit of work
        assert scoped.source == "mpi"


class TestSchemas:
    def test_parse_decimal(self):
        from pyspark.sql.types import DecimalType

        parsed = parse_type("decimal(18,2)")
        assert isinstance(parsed, DecimalType)
        assert (parsed.precision, parsed.scale) == (18, 2)

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported type"):
            parse_type("blob")

    def test_all_strings_mode(self):
        cfg = load_config("local")
        schema = source_schema(cfg.source("insurance_claims"), all_strings=True)
        # Bronze reads everything as a string so malformed values survive to Silver
        # with their original text intact.
        assert {f.dataType.typeName() for f in schema.fields} == {"string"}

    def test_declared_columns_match_schema_order(self):
        cfg = load_config("local")
        for name in cfg.source_names:
            source = cfg.source(name)
            schema = source_schema(source, all_strings=True)
            assert [f.name for f in schema.fields] == column_names(source)

    def test_claims_export_has_no_visit_id(self):
        # The insurer portal does not know the hospital's internal visit id. If it
        # ever does, bill-to-claim linkage becomes a trivial equi-join and the
        # matching logic stops being exercised at all.
        cfg = load_config("local")
        assert "visit_id" not in column_names(cfg.source("insurance_claims"))


class TestConfig:
    def test_local_paths_are_absolute(self):
        cfg = load_config("local")
        for layer in ("bronze", "silver", "gold"):
            assert cfg.path(layer).startswith("/")

    def test_table_path_and_fqn(self):
        cfg = load_config("local")
        assert cfg.table_path("silver", "mpi_registry").endswith("/silver/mpi_registry")
        assert cfg.table_fqn("silver", "mpi_registry") is None  # no catalog locally

    def test_unknown_layer_raises(self):
        with pytest.raises(ConfigError):
            load_config("local").path("platinum")

    def test_unknown_source_raises(self):
        with pytest.raises(ConfigError):
            load_config("local").source("nonexistent")


class TestClaimStateMachine:
    def test_transitive_closure(self):
        legal = load_config("local").get("claims", "legal_transitions")
        closure = _transitive_closure(legal)
        # Approved is not reachable from Submitted in one step, but is in two —
        # which is why a weekly export showing Submitted -> Approved is a sampling
        # gap rather than an illegal transition.
        assert "Approved" not in legal["Submitted"]
        assert "Approved" in closure["Submitted"]
        assert "Settled" in closure["Submitted"]

    def test_terminal_states_go_nowhere(self):
        legal = load_config("local").get("claims", "legal_transitions")
        closure = _transitive_closure(legal)
        assert closure["Settled"] == set()
        assert closure["Rejected"] == set()

    def test_submitted_cannot_be_reached_again(self):
        # Nothing returns to Submitted; a transition back into it is a genuine
        # anomaly, not a gap.
        closure = _transitive_closure(load_config("local").get("claims", "legal_transitions"))
        assert all("Submitted" not in reachable for reachable in closure.values())
