"""Shared fixtures for capigen tests."""

import pytest


def _make_module(name, **overrides):
    """Build a minimal module dict with defaults, then apply overrides."""
    mod = {
        "module": name,
        "handles": {},
        "callbacks": {},
        "aliases": {},
        "structs": {},
        "enums": {},
        "constants": {},
        "error_groups": {},
        "functions": {},
    }
    mod.update(overrides)
    return mod


@pytest.fixture()
def metadata():
    """Minimal metadata dict matching metadata.schema.json."""
    return {
        "versions": ["1.0.0", "1.1.0"],
        "suffixes": {
            "handles": "_ptr",
            "callbacks": "_cb",
            "aliases": "_t",
        },
        "primitives": [
            {"name": "opaque", "c_type": "void"},
            {"name": "char", "c_type": "char"},
            {"name": "bool", "c_type": "bool"},
            {"name": "i32", "c_type": "int32_t"},
            {"name": "u32", "c_type": "uint32_t"},
            {"name": "u64", "c_type": "uint64_t"},
            {"name": "idx", "c_type": "idx_t"},
        ],
    }


@pytest.fixture()
def make_module():
    """Factory fixture for building module dicts."""
    return _make_module
