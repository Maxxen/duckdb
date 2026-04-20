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
