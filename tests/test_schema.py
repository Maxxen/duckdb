"""Tests for module.schema.json validation (prefix enforcement, etc.)."""

import json
from pathlib import Path

import jsonschema
import pytest

SCHEMA = json.loads(
    (Path(__file__).parent.parent / "src/capigen/schema/module.schema.json").read_text()
)


def _minimal_module(**overrides):
    mod = {"module": "test"}
    mod.update(overrides)
    return mod


class TestV2PrefixEnforcement:
    """The schema requires all type/function keys to start with duckdb_v2_ or DUCKDB_V2_."""

    @pytest.mark.parametrize(
        "construct,entry",
        [
            ("handles", {"duckdb_conn": {}}),
            ("aliases", {"duckdb_idx": {"underlying": "u32"}}),
            ("structs", {"duckdb_date": {"fields": []}}),
            ("enums", {"DUCKDB_TYPE": {"values": {}}}),
            ("callbacks", {"duckdb_cb": {"return_type": "opaque"}}),
            ("constants", {"DUCKDB_MAX": {"value": 42}}),
            (
                "functions",
                {"duckdb_open": {"summary": "test"}},
            ),
        ],
    )
    def test_missing_v2_prefix_rejected(self, construct, entry):
        mod = _minimal_module(**{construct: entry})
        with pytest.raises(jsonschema.ValidationError, match="does not match"):
            jsonschema.validate(mod, SCHEMA)

    @pytest.mark.parametrize(
        "construct,entry",
        [
            ("handles", {"duckdb_v2_conn": {}}),
            ("aliases", {"duckdb_v2_idx": {"underlying": "u32"}}),
            ("structs", {"duckdb_v2_date": {"fields": []}}),
            ("enums", {"DUCKDB_V2_TYPE": {"values": {}}}),
            ("callbacks", {"duckdb_v2_cb": {"return_type": "opaque"}}),
            ("constants", {"DUCKDB_V2_MAX": {"value": 42}}),
            (
                "functions",
                {"duckdb_v2_open": {"summary": "test"}},
            ),
        ],
    )
    def test_v2_prefix_accepted(self, construct, entry):
        mod = _minimal_module(**{construct: entry})
        jsonschema.validate(mod, SCHEMA)  # should not raise


class TestParameterKind:
    """The `kind` field on Parameter accepts IN/IN_TRANSFER/OUT/OUT_BORROW."""

    def _func_with_kind(self, kind):
        return _minimal_module(
            functions={
                "duckdb_v2_f": {
                    "summary": "x",
                    "parameters": {
                        "p": {
                            "type": "char",
                            "indirection": 1,
                            "kind": kind,
                        }
                    },
                }
            }
        )

    @pytest.mark.parametrize("kind", ["IN", "IN_TRANSFER", "OUT", "OUT_BORROW"])
    def test_valid_values_accepted(self, kind):
        jsonschema.validate(self._func_with_kind(kind), SCHEMA)

    @pytest.mark.parametrize("kind", ["in", "out", "shared", "INOUT"])
    def test_invalid_value_rejected(self, kind):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(self._func_with_kind(kind), SCHEMA)

    def test_default_is_IN(self):
        """When omitted, apply_defaults() fills kind with 'IN'."""
        from capigen.loader import apply_defaults

        mod = _minimal_module(
            functions={
                "duckdb_v2_f": {
                    "summary": "x",
                    "parameters": {"p": {"type": "char", "indirection": 1}},
                }
            }
        )
        apply_defaults(mod, SCHEMA)
        param = mod["functions"]["duckdb_v2_f"]["parameters"]["p"]
        assert param["kind"] == "IN"
