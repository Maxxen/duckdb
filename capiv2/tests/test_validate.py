"""Tests for cross-module referential integrity (validate.py)."""

from capigen.validate import validate_semantics


class TestDuplicateDetection:
    def test_duplicate_handle_across_modules(self, metadata, make_module):
        modules = [
            make_module("a", handles={"my_handle": {}}),
            make_module("b", handles={"my_handle": {}}),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("duplicated" in e and "my_handle" in e for e in errors)

    def test_duplicate_function_across_modules(self, metadata, make_module):
        modules = [
            make_module(
                "a",
                functions={
                    "duckdb_v2_do_thing": {
                        "summary": "x",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                    },
                },
            ),
            make_module(
                "b",
                functions={
                    "duckdb_v2_do_thing": {
                        "summary": "y",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                    },
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("duplicated" in e and "duckdb_v2_do_thing" in e for e in errors)

    def test_duplicate_across_construct_types(self, metadata, make_module):
        modules = [
            make_module("a", handles={"clash": {}}),
            make_module("b", aliases={"clash": {"underlying": "u32"}}),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("duplicated" in e and "clash" in e for e in errors)

    def test_no_errors_when_names_are_unique(self, metadata, make_module):
        modules = [
            make_module("a", handles={"handle_a": {}}),
            make_module("b", handles={"handle_b": {}}),
        ]
        errors = validate_semantics(modules, metadata)
        assert errors == []


class TestTypeReferences:
    def test_unknown_alias_underlying(self, metadata, make_module):
        modules = [
            make_module("m", aliases={"t": {"underlying": "nonexistent"}}),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("Unknown underlying type 'nonexistent'" in e for e in errors)

    def test_alias_to_primitive(self, metadata, make_module):
        modules = [
            make_module("m", aliases={"my_int": {"underlying": "u32"}}),
        ]
        errors = validate_semantics(modules, metadata)
        assert errors == []

    def test_alias_to_handle(self, metadata, make_module):
        modules = [
            make_module("common", handles={"ctx": {}}),
            make_module("m", aliases={"my_ctx": {"underlying": "ctx"}}),
        ]
        errors = validate_semantics(modules, metadata)
        assert errors == []

    def test_unknown_struct_field_type(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                structs={
                    "s": {
                        "fields": [
                            {
                                "name": "f",
                                "type": "nonexistent",
                                "pointer": 0,
                                "const": False,
                            }
                        ],
                    }
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("Unknown field type 'nonexistent'" in e for e in errors)

    def test_cross_module_type_reference_is_valid(self, metadata, make_module):
        modules = [
            make_module("common", handles={"handle": {}}),
            make_module(
                "other",
                structs={
                    "s": {
                        "fields": [
                            {
                                "name": "h",
                                "type": "handle",
                                "pointer": 0,
                                "const": False,
                            }
                        ],
                    }
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert errors == []

    def test_unknown_callback_param_type(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                callbacks={
                    "cb": {
                        "return_type": "opaque",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {
                            "p": {
                                "type": "missing",
                                "indirection": 0,
                                "const": False,
                                "direction": "in",
                            }
                        },
                    }
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("Unknown parameter type 'missing'" in e for e in errors)

    def test_unknown_callback_return_type(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                callbacks={
                    "cb": {
                        "return_type": "missing",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                    }
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("Unknown return type 'missing'" in e for e in errors)


class TestFunctionPrefix:
    def test_missing_v2_prefix_rejected(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "duckdb_open": {
                        "summary": "test",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                    },
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("must start with 'duckdb_v2_'" in e for e in errors)

    def test_v2_prefix_accepted(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "duckdb_v2_open": {
                        "summary": "test",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                    },
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert not any("must start with 'duckdb_v2_'" in e for e in errors)


class TestFunctionValidation:
    def test_unknown_param_type(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "duckdb_v2_func": {
                        "summary": "test",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {
                            "p": {
                                "type": "missing",
                                "indirection": 0,
                                "const": False,
                                "direction": "in",
                            }
                        },
                    },
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("Unknown parameter type 'missing'" in e for e in errors)

    def test_unknown_return_type(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "duckdb_v2_func": {
                        "summary": "test",
                        "return_type": "missing",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                    },
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("Unknown return type 'missing'" in e for e in errors)

    def test_unknown_added_version(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "duckdb_v2_func": {
                        "summary": "test",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                        "added": "9.9.9",
                    },
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("Unknown 'added' version '9.9.9'" in e for e in errors)

    def test_unknown_deprecated_version(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "duckdb_v2_func": {
                        "summary": "test",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                        "deprecated": "9.9.9",
                    },
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert any("Unknown 'deprecated' version '9.9.9'" in e for e in errors)

    def test_valid_versions_accepted(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "duckdb_v2_func": {
                        "summary": "test",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                        "added": "1.0.0",
                        "deprecated": "1.1.0",
                    },
                },
            ),
        ]
        errors = validate_semantics(modules, metadata)
        assert errors == []
