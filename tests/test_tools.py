"""Tests for capigen.tools (handle dependency graph utilities)."""

import pytest

from capigen.tools import handle_dependencies, topo_sort_handles


def test_handle_dependencies_empty(make_module):
    mods = [make_module("empty")]
    assert handle_dependencies(mods) == {}


def test_handle_dependencies_no_deps(make_module):
    """A handle with no cross-handle references has an empty dep set."""
    mod = make_module(
        "m",
        handles={"duckdb_v2_a": {"description": ""}},
        functions={
            "duckdb_v2_a_create": {
                "summary": "",
                "role": "constructor",
                "belongs_to": "duckdb_v2_a",
                "parameters": {
                    "out": {
                        "type": "duckdb_v2_a",
                        "indirection": 1,
                        "kind": "OUT",
                    }
                },
                "return_type": "DUCKDB_V2_API_CALL",
            }
        },
    )
    assert handle_dependencies([mod]) == {"duckdb_v2_a": set()}


def test_handle_dependencies_single_edge(make_module):
    """B's function references A → B depends on A."""
    mod = make_module(
        "m",
        handles={
            "duckdb_v2_a": {"description": ""},
            "duckdb_v2_b": {"description": ""},
        },
        functions={
            "duckdb_v2_b_from_a": {
                "summary": "",
                "role": "constructor",
                "belongs_to": "duckdb_v2_b",
                "parameters": {
                    "a": {"type": "duckdb_v2_a"},
                    "out": {
                        "type": "duckdb_v2_b",
                        "indirection": 1,
                        "kind": "OUT",
                    },
                },
                "return_type": "DUCKDB_V2_API_CALL",
            }
        },
    )
    deps = handle_dependencies([mod])
    assert deps == {"duckdb_v2_a": set(), "duckdb_v2_b": {"duckdb_v2_a"}}


def test_handle_dependencies_ignores_self(make_module):
    """A function that takes its own owning handle does not self-depend."""
    mod = make_module(
        "m",
        handles={"duckdb_v2_a": {"description": ""}},
        functions={
            "duckdb_v2_a_destroy": {
                "summary": "",
                "role": "destructor",
                "belongs_to": "duckdb_v2_a",
                "parameters": {
                    "a": {"type": "duckdb_v2_a", "indirection": 1}
                },
                "return_type": "DUCKDB_V2_API_CALL",
            }
        },
    )
    assert handle_dependencies([mod]) == {"duckdb_v2_a": set()}


def test_topo_sort_preserves_dependency_order(make_module):
    """ctx (no deps) comes before conn (depends on ctx)."""
    mod = make_module(
        "m",
        handles={
            "duckdb_v2_ctx": {"description": ""},
            "duckdb_v2_conn": {"description": ""},
        },
        functions={
            "duckdb_v2_conn_create": {
                "summary": "",
                "role": "constructor",
                "belongs_to": "duckdb_v2_conn",
                "parameters": {
                    "ctx": {"type": "duckdb_v2_ctx"},
                    "out": {
                        "type": "duckdb_v2_conn",
                        "indirection": 1,
                        "kind": "OUT",
                    },
                },
                "return_type": "DUCKDB_V2_API_CALL",
            }
        },
    )
    order = topo_sort_handles([mod])
    assert order.index("duckdb_v2_ctx") < order.index("duckdb_v2_conn")


def test_topo_sort_alphabetical_tie_break(make_module):
    """Independent handles come out in alphabetical order."""
    mod = make_module(
        "m",
        handles={
            "duckdb_v2_c": {"description": ""},
            "duckdb_v2_a": {"description": ""},
            "duckdb_v2_b": {"description": ""},
        },
    )
    assert topo_sort_handles([mod]) == [
        "duckdb_v2_a",
        "duckdb_v2_b",
        "duckdb_v2_c",
    ]


def test_topo_sort_detects_cycle(make_module):
    """Mutual dependency raises ValueError listing the cycle members."""
    mod = make_module(
        "m",
        handles={
            "duckdb_v2_a": {"description": ""},
            "duckdb_v2_b": {"description": ""},
        },
        functions={
            "duckdb_v2_a_needs_b": {
                "summary": "",
                "belongs_to": "duckdb_v2_a",
                "parameters": {"b": {"type": "duckdb_v2_b"}},
                "return_type": "DUCKDB_V2_API_CALL",
            },
            "duckdb_v2_b_needs_a": {
                "summary": "",
                "belongs_to": "duckdb_v2_b",
                "parameters": {"a": {"type": "duckdb_v2_a"}},
                "return_type": "DUCKDB_V2_API_CALL",
            },
        },
    )
    with pytest.raises(ValueError, match="Cycle detected"):
        topo_sort_handles([mod])
