"""Tests for module.schema.json validation (identifier rules, etc.)."""

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


class TestPropertyNameValidation:
    """The schema accepts any valid C identifier; rejects names that are not."""

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
    def test_non_v2_prefix_accepted(self, construct, entry):
        mod = _minimal_module(**{construct: entry})
        jsonschema.validate(mod, SCHEMA)  # should not raise

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

    @pytest.mark.parametrize(
        "construct,entry",
        [
            ("handles", {"1invalid": {}}),
            ("aliases", {"bad name": {"underlying": "u32"}}),
            ("structs", {"has-hyphen": {"fields": []}}),
            ("enums", {"": {"values": {}}}),
            ("constants", {"has space": {"value": 42}}),
        ],
    )
    def test_invalid_identifiers_rejected(self, construct, entry):
        mod = _minimal_module(**{construct: entry})
        with pytest.raises(jsonschema.ValidationError, match="does not match"):
            jsonschema.validate(mod, SCHEMA)


class TestQualifiedAliases:
    """aliases with qualified:true are emitted verbatim — no prefix or suffix applied."""

    def test_qualified_alias_accepted(self):
        mod = _minimal_module(
            aliases={"sel_t": {"underlying": "u32", "qualified": True}}
        )
        jsonschema.validate(mod, SCHEMA)  # should not raise

    def test_qualified_alias_invalid_identifier_rejected(self):
        mod = _minimal_module(
            aliases={"1bad": {"underlying": "u32", "qualified": True}}
        )
        with pytest.raises(jsonschema.ValidationError, match="does not match"):
            jsonschema.validate(mod, SCHEMA)

    def test_qualified_alias_missing_underlying_rejected(self):
        mod = _minimal_module(aliases={"sel_t": {"qualified": True}})
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(mod, SCHEMA)


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


class TestStructFieldArraySize:
    """Struct fields may declare array_size for inline fixed-size arrays."""

    def _struct_with_field(self, field):
        return _minimal_module(
            structs={
                "duckdb_v2_s": {
                    "fields": [{"name": "x", "type": "char", **field}],
                }
            }
        )

    def test_array_size_accepted(self):
        mod = self._struct_with_field({"array_size": 256})
        jsonschema.validate(mod, SCHEMA)  # should not raise

    def test_array_size_with_pointer_rejected(self):
        mod = self._struct_with_field({"array_size": 256, "pointer": 1})
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(mod, SCHEMA)

    def test_array_size_zero_rejected(self):
        mod = self._struct_with_field({"array_size": 0})
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(mod, SCHEMA)

    def test_array_size_negative_rejected(self):
        mod = self._struct_with_field({"array_size": -1})
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(mod, SCHEMA)

    def test_array_size_optional(self):
        """Fields without array_size still validate."""
        mod = self._struct_with_field({"pointer": 1})
        jsonschema.validate(mod, SCHEMA)  # should not raise
