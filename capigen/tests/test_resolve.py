"""Tests for the C adapter's resolve layer (resolve.py + render.py).

These tests use hand-built dicts and do not depend on the real IDL,
so they remain valid after the spec moves to duckdb core.
"""

import pytest

from capigen.adapters.c.resolve import resolve_modules, _format_c_type
from capigen.adapters.c.render import (
    CConstant,
    CEnum,
    CEnumValue,
    CErrorEntry,
    CErrorGroup,
    CField,
    CFuncPtr,
    CFuncPtrParam,
    CFunction,
    CModule,
    CParam,
    CStruct,
    CTypeDef,
)


class TestFormatCType:
    def test_plain(self):
        assert _format_c_type("int32_t") == "int32_t"

    def test_pointer(self):
        assert _format_c_type("void", pointer=1) == "void*"

    def test_double_pointer(self):
        assert _format_c_type("char", pointer=2) == "char**"

    def test_const(self):
        assert _format_c_type("char", is_const=True) == "const char"

    def test_const_pointer(self):
        assert _format_c_type("char", pointer=1, is_const=True) == "const char*"


class TestResolveHandles:
    def test_handle(self, metadata, make_module):
        modules = [make_module("m", handles={"duckdb_conn": {}})]
        result = resolve_modules(modules, metadata)
        t = result[0].types[0]
        assert isinstance(t, CTypeDef)
        assert t.name == "duckdb_conn"
        assert t.canonical_name == "duckdb_conn_ptr"
        assert t.base == "void"
        assert t.is_pointer is True

    def test_handle_with_description(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                handles={
                    "ctx": {"description": "A context handle"},
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        assert result[0].types[0].canonical_name == "ctx_ptr"


class TestResolveAliases:
    def test_alias_to_primitive(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                aliases={
                    "duckdb_idx": {"underlying": "u64"},
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        t = result[0].types[0]
        assert isinstance(t, CTypeDef)
        assert t.canonical_name == "duckdb_idx_t"
        assert t.base == "uint64_t"
        assert t.is_pointer is False

    def test_alias_to_alias(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                aliases={
                    "err_code": {"underlying": "u32"},
                    "api_call": {"underlying": "err_code"},
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        api_call = result[0].types[1]
        assert api_call.base == "err_code_t"

    def test_alias_to_handle(self, metadata, make_module):
        modules = [
            make_module("common", handles={"ctx": {}}),
            make_module("m", aliases={"my_ctx": {"underlying": "ctx"}}),
        ]
        result = resolve_modules(modules, metadata)
        alias = result[1].types[0]
        assert alias.base == "ctx_ptr"


class TestResolveStructs:
    def test_struct_basic(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                structs={
                    "my_struct": {
                        "fields": [
                            {
                                "name": "data",
                                "type": "opaque",
                                "pointer": 1,
                                "const": False,
                            },
                        ],
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        s = result[0].structs[0]
        assert isinstance(s, CStruct)
        assert s.template_alias == "my_struct"
        assert s.pointer_alias is False
        assert isinstance(s.fields[0], CField)
        assert s.fields[0].base == "void"
        assert s.fields[0].pointer == 1

    def test_struct_with_pointer_alias(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                structs={
                    "my_struct": {
                        "pointer_alias": True,
                        "fields": [
                            {
                                "name": "val",
                                "type": "i32",
                                "pointer": 0,
                                "const": False,
                            },
                        ],
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        s = result[0].structs[0]
        assert s.template_alias == "my_struct_t"
        assert s.pointer_alias is True

    def test_struct_with_multiple_fields(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                structs={
                    "pair": {
                        "fields": [
                            {
                                "name": "first",
                                "type": "i32",
                                "pointer": 0,
                                "const": False,
                            },
                            {
                                "name": "second",
                                "type": "u64",
                                "pointer": 0,
                                "const": False,
                            },
                        ],
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        s = result[0].structs[0]
        assert len(s.fields) == 2
        assert s.fields[0].name == "first"
        assert s.fields[1].name == "second"

    def test_struct_const_field(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                structs={
                    "s": {
                        "fields": [
                            {
                                "name": "name",
                                "type": "char",
                                "pointer": 1,
                                "const": True,
                            },
                        ],
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        f = result[0].structs[0].fields[0]
        assert f.const is True
        assert f.pointer == 1

    def test_struct_nested_struct_field(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                structs={
                    "outer": {
                        "fields": [
                            {
                                "name": "inner",
                                "fields": [
                                    {"name": "a", "type": "i32"},
                                    {"name": "b", "type": "u64"},
                                ],
                            },
                        ],
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        f = result[0].structs[0].fields[0]
        assert f.name == "inner"
        assert f.base == ""
        assert f.union_members is None
        assert f.nested_fields is not None
        assert [nf.name for nf in f.nested_fields] == ["a", "b"]
        assert f.nested_fields[0].base == "int32_t"
        assert f.nested_fields[1].base == "uint64_t"

    def test_struct_union_field(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                structs={
                    "string": {
                        "fields": [
                            {
                                "name": "value",
                                "union": [
                                    {
                                        "name": "pointer",
                                        "fields": [
                                            {"name": "length", "type": "u32"},
                                            {
                                                "name": "prefix",
                                                "type": "char",
                                                "array_size": 4,
                                            },
                                            {
                                                "name": "ptr",
                                                "type": "char",
                                                "pointer": 1,
                                            },
                                        ],
                                    },
                                    {
                                        "name": "inlined",
                                        "fields": [
                                            {"name": "length", "type": "u32"},
                                            {
                                                "name": "inlined",
                                                "type": "char",
                                                "array_size": 12,
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        f = result[0].structs[0].fields[0]
        assert f.name == "value"
        assert f.nested_fields is None
        assert f.union_members is not None
        assert [m.name for m in f.union_members] == ["pointer", "inlined"]
        pointer = f.union_members[0]
        assert [mf.name for mf in pointer.fields] == ["length", "prefix", "ptr"]
        assert pointer.fields[1].array_size == 4
        assert pointer.fields[2].pointer == 1
        inlined = f.union_members[1]
        assert inlined.fields[1].base == "char"
        assert inlined.fields[1].array_size == 12

    def test_struct_inline_array_field(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                structs={
                    "my_struct": {
                        "fields": [
                            {
                                "name": "buf",
                                "type": "char",
                                "pointer": 0,
                                "const": False,
                                "array_size": 64,
                            },
                            {
                                "name": "code",
                                "type": "u32",
                                "pointer": 0,
                                "const": False,
                            },
                        ],
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        fields = result[0].structs[0].fields
        assert fields[0].array_size == 64
        assert fields[0].pointer == 0
        assert fields[1].array_size is None


class TestResolveCallbacks:
    def test_callback(self, metadata, make_module):
        modules = [
            make_module("common", handles={"ctx": {}}),
            make_module(
                "m",
                callbacks={
                    "my_callback": {
                        "return_type": "opaque",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {
                            "context": {
                                "type": "ctx",
                                "indirection": 0,
                                "const": False,
                            },
                            "value": {
                                "type": "i32",
                                "indirection": 0,
                                "const": False,
                            },
                        },
                    },
                },
            ),
        ]
        result = resolve_modules(modules, metadata)
        fp = result[1].function_ptrs[0]
        assert isinstance(fp, CFuncPtr)
        assert fp.template_alias == "my_callback_cb"
        assert fp.return_base == "void"
        assert len(fp.params) == 2
        assert isinstance(fp.params[0], CFuncPtrParam)
        assert fp.params[0].base == "ctx_ptr"
        assert fp.params[1].base == "int32_t"

    def test_callback_with_pointer_return(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                callbacks={
                    "getter": {
                        "return_type": "char",
                        "return_pointer": 1,
                        "return_const": True,
                        "parameters": {},
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        fp = result[0].function_ptrs[0]
        assert fp.return_base == "char"
        assert fp.return_pointer == 1
        assert fp.return_const is True

    def test_callback_no_params(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                callbacks={
                    "simple": {
                        "return_type": "opaque",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        assert result[0].function_ptrs[0].params == []


class TestResolveFunctions:
    def test_function_with_all_fields(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "do_thing": {
                        "summary": "Does a thing",
                        "description": "Longer description",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {
                            "name": {
                                "type": "char",
                                "indirection": 1,
                                "const": True,
                            },
                            "count": {
                                "type": "u64",
                                "indirection": 0,
                                "const": False,
                            },
                        },
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        f = result[0].functions["do_thing"]
        assert isinstance(f, CFunction)
        assert f.summary == "Does a thing"
        assert f.description == "Longer description"
        assert f.return_c == "int32_t"
        assert isinstance(f.parameters["name"], CParam)
        assert f.parameters["name"].c_decl == "const char*"
        assert f.parameters["count"].c_decl == "uint64_t"

    def test_function_with_out_parameter(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "get_value": {
                        "summary": "Get a value",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {
                            "out_val": {
                                "type": "u64",
                                "indirection": 1,
                                "const": False,
                                "kind": "OUT",
                            },
                        },
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        p = result[0].functions["get_value"].parameters["out_val"]
        assert p.c_decl == "uint64_t*"

    def test_function_with_pointer_return(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "get_str": {
                        "summary": "Get a string",
                        "return_type": "char",
                        "return_pointer": 1,
                        "return_const": True,
                        "parameters": {},
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        assert result[0].functions["get_str"].return_c == "const char*"

    def test_function_no_params(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "noop": {
                        "summary": "No-op",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        assert result[0].functions["noop"].parameters == {}

    def test_deprecated_function(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "old_func": {
                        "summary": "Old",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                        "deprecated": "1.0.0",
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        assert result[0].functions["old_func"].deprecated == "1.0.0"

    def test_param_description(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "f": {
                        "summary": "x",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {
                            "p": {
                                "type": "i32",
                                "indirection": 0,
                                "const": False,
                                "description": "the param",
                            },
                        },
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        assert result[0].functions["f"].parameters["p"].description == "the param"

    def test_unknown_type_raises(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                functions={
                    "bad": {
                        "summary": "x",
                        "return_type": "nonexistent",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {},
                    },
                },
            )
        ]
        with pytest.raises(ValueError, match="unknown type 'nonexistent'"):
            resolve_modules(modules, metadata)


class TestResolveEnums:
    def test_auto_numbering(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                enums={
                    "MY_ENUM": {
                        "description": "Test enum",
                        "values": {"A": {}, "B": {}, "C": {}},
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        e = result[0].enums[0]
        assert isinstance(e, CEnum)
        assert isinstance(e.values["A"], CEnumValue)
        assert e.values["A"].value == 0
        assert e.values["B"].value == 1
        assert e.values["C"].value == 2

    def test_explicit_value_resets_counter(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                enums={
                    "MY_ENUM": {
                        "description": "",
                        "values": {"A": {}, "B": {"value": 10}, "C": {}},
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        e = result[0].enums[0]
        assert e.values["A"].value == 0
        assert e.values["B"].value == 10
        assert e.values["C"].value == 11

    def test_enum_description(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                enums={
                    "E": {"description": "A test enum", "values": {"X": {}}},
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        assert result[0].enums[0].description == "A test enum"

    def test_enum_value_description(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                enums={
                    "E": {
                        "description": "",
                        "values": {"X": {"description": "the X value"}},
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        assert result[0].enums[0].values["X"].description == "the X value"


class TestResolveConstants:
    def test_integer_constant(self, metadata, make_module):
        modules = [make_module("m", constants={"MAX_SIZE": {"value": 1024}})]
        result = resolve_modules(modules, metadata)
        c = result[0].constants[0]
        assert isinstance(c, CConstant)
        assert c.name == "MAX_SIZE"
        assert c.value == 1024

    def test_string_constant(self, metadata, make_module):
        modules = [make_module("m", constants={"VERSION": {"value": '"1.0"'}})]
        result = resolve_modules(modules, metadata)
        assert result[0].constants[0].value == '"1.0"'

    def test_constant_description(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                constants={
                    "X": {"value": 42, "description": "the answer"},
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        assert result[0].constants[0].description == "the answer"


class TestResolveErrorGroups:
    def test_error_group(self, metadata, make_module):
        modules = [
            make_module(
                "m",
                error_groups={
                    "IO": {
                        "group_id": 1,
                        "description": "I/O errors",
                        "entries": {
                            "FILE_NOT_FOUND": {"code": 1},
                            "PERMISSION_DENIED": {"code": 2},
                        },
                    },
                },
            )
        ]
        result = resolve_modules(modules, metadata)
        g = result[0].error_groups[0]
        assert isinstance(g, CErrorGroup)
        assert g.category == "IO"
        assert g.group_id == 1
        assert g.description == "I/O errors"
        assert len(g.entries) == 2
        assert isinstance(g.entries[0], CErrorEntry)
        assert g.entries[0].name == "FILE_NOT_FOUND"
        assert g.entries[0].code == 1


class TestResolveModule:
    def test_returns_cmodule(self, metadata, make_module):
        modules = [make_module("test_mod")]
        result = resolve_modules(modules, metadata)
        assert len(result) == 1
        assert isinstance(result[0], CModule)
        assert result[0].name == "test_mod"

    def test_cross_module_type_resolution(self, metadata, make_module):
        """Types declared in one module are available in another's functions."""
        modules = [
            make_module("common", handles={"handle": {}}),
            make_module(
                "api",
                functions={
                    "use_handle": {
                        "summary": "x",
                        "return_type": "i32",
                        "return_pointer": 0,
                        "return_const": False,
                        "parameters": {
                            "h": {
                                "type": "handle",
                                "indirection": 0,
                                "const": False,
                            },
                        },
                    },
                },
            ),
        ]
        result = resolve_modules(modules, metadata)
        f = result[1].functions["use_handle"]
        assert f.parameters["h"].c_decl == "handle_ptr"

    def test_empty_module(self, metadata, make_module):
        modules = [make_module("empty")]
        result = resolve_modules(modules, metadata)
        m = result[0]
        assert m.types == []
        assert m.structs == []
        assert m.enums == []
        assert m.constants == []
        assert m.error_groups == []
        assert m.function_ptrs == []
        assert m.functions == {}
