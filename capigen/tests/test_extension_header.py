"""Tests for the extension_header adapter (verify + append + derive)."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from capigen.loader import load_metadata, load_modules
from capigen.validate import validate_semantics
from capigen.adapters.extension_header import (
    _extract_defines,
    _extract_struct,
    _member_types,
    _normalize,
    _param_type,
    generate,
)

EXT_SPEC = Path(__file__).parent / "testspec" / "ext"
TEMPLATE = EXT_SPEC / "template.h.in"
HAS_CC = shutil.which("cc") is not None


def _load():
    return load_modules(EXT_SPEC), load_metadata(EXT_SPEC)


def _run(tmp_path, template_text=None):
    """Write a template, run generate against the ext fixture spec, return (consumer, internal)."""
    modules, metadata = _load()
    tmp_path.mkdir(parents=True, exist_ok=True)
    template = tmp_path / "template.h.in"
    template.write_text(
        template_text if template_text is not None else TEMPLATE.read_text()
    )
    consumer = tmp_path / "out.h"
    internal = tmp_path / "internal.hpp"
    generate(modules, metadata, consumer, template=template, internal_out=internal)
    return consumer.read_text(), internal.read_text()


def _fn(ret="i32", params=None, static_inline=False):
    """Build a minimal spec function dict."""
    return {
        "summary": "x",
        "return_type": ret,
        "return_pointer": 0,
        "return_const": False,
        "static_inline": static_inline,
        "parameters": params or {},
    }


def _run_inline(tmp_path, functions, template_text, exclude=None):
    """Run generate against an inline single-module spec (full control over spec order)."""
    metadata = {
        "schema_version": "0.4",
        "prefix": "t_",
        "versions": ["1.0.0"],
        "suffixes": {"handles": "", "callbacks": "", "aliases": ""},
        "primitives": [
            {"name": "void", "c_type": "void"},
            {"name": "i32", "c_type": "int32_t"},
        ],
        "options": {
            "extension": {
                "unstable_guard": "T_UNSTABLE",
                "create_method": "CreateT",
                "api_version": "v1.0.0",
                "version_macro_prefix": "T_VERSION",
                "internal_include": "t.h",
                "exclude": exclude or [],
            }
        },
    }
    module = {
        "module": "m",
        "handles": {},
        "callbacks": {},
        "aliases": {},
        "structs": {},
        "enums": {},
        "constants": {},
        "error_groups": {},
        "functions": functions,
    }
    tmp_path.mkdir(parents=True, exist_ok=True)
    template = tmp_path / "t.in"
    template.write_text(template_text)
    consumer = tmp_path / "out.h"
    internal = tmp_path / "i.hpp"
    generate([module], metadata, consumer, template=template, internal_out=internal)
    return consumer.read_text(), internal.read_text()


# A minimal inline template: one stable member plus empty append markers.
def _inline_template(members, defines):
    member_block = "\n".join(f"\t{m}" for m in members)
    define_block = "\n".join(f"#define {d} t_api.{d}" for d in defines)
    return f"""#pragma once

typedef struct {{
#if T_VERSION_MINOR > 0 || (T_VERSION_MINOR == 0 && T_VERSION_PATCH >= 0) // v1.0.0
{member_block}
#endif

	// capigen:begin appended
	// capigen:end appended
}} t_api;

#ifndef T_STATIC
{define_block}

// capigen:begin appended
// capigen:end appended
#endif // T_STATIC
"""


STRUCT_SAMPLE = """typedef struct {
#if V > 0 || (V == 0 && P >= 0) // v1.0.0
	int32_t (*a_open)(const char *p);
	void (*a_cb_setter)(void (*cb)(int32_t x, int32_t y), void *data);
	void (*a_noop)(void);
#endif
#if V > 1 || (V == 1 && P >= 0) // v1.1.0
	int32_t (*a_more)(int32_t x);
#endif
// group two
#ifdef GUARD
	// a stray comment inside the region
	int64_t (*a_wrapped)(int32_t first,
	                     int32_t second);
#endif
} sample_api_t;
"""


class TestExtractStruct:
    def test_typename(self):
        typename, _ = _extract_struct(STRUCT_SAMPLE)
        assert typename == "sample_api_t"

    def test_multiple_stable_and_unstable_regions(self):
        _, regions = _extract_struct(STRUCT_SAMPLE)
        kinds = [(r.kind, r.version or r.description) for r in regions]
        assert kinds == [
            ("stable", "1.0.0"),
            ("stable", "1.1.0"),
            ("unstable", "group two"),
        ]

    def test_member_order_and_names(self):
        _, regions = _extract_struct(STRUCT_SAMPLE)
        names = [m.name for r in regions for m in r.members]
        assert names == ["a_open", "a_cb_setter", "a_noop", "a_more", "a_wrapped"]

    def test_function_pointer_parameter_name_is_outer(self):
        # The member name is the outer (*a_cb_setter), not the inner (*cb).
        _, regions = _extract_struct(STRUCT_SAMPLE)
        member = regions[0].members[1]
        assert member.name == "a_cb_setter"
        # The nested function-pointer parameter survives extraction.
        assert "(*cb)(int32_t x, int32_t y)" in member.signature

    def test_function_pointer_parameter_split(self):
        _, regions = _extract_struct(STRUCT_SAMPLE)
        _, params = _member_types(regions[0].members[1].signature)
        assert len(params) == 2
        assert "(*)" in params[0]

    def test_zero_parameter_member(self):
        _, regions = _extract_struct(STRUCT_SAMPLE)
        noop = regions[0].members[2]
        assert noop.name == "a_noop"
        _, params = _member_types(noop.signature)
        assert params == ["void"]

    def test_wrapped_member_joined_to_single_line(self):
        _, regions = _extract_struct(STRUCT_SAMPLE)
        wrapped = regions[2].members[0]
        assert wrapped.name == "a_wrapped"
        assert "\n" not in wrapped.signature
        assert (
            wrapped.signature == "int64_t (*a_wrapped)(int32_t first, int32_t second)"
        )

    def test_stray_comment_not_a_member(self):
        _, regions = _extract_struct(STRUCT_SAMPLE)
        # The unstable region has only the real member, and its description is the
        # group comment, not the stray in-region comment.
        assert [m.name for m in regions[2].members] == ["a_wrapped"]
        assert regions[2].description == "group two"
        assert regions[2].guard == "GUARD"

    def test_missing_struct_errors(self):
        with pytest.raises(ValueError, match="typedef struct"):
            _extract_struct("no struct here")

    def test_unclosed_struct_errors(self):
        with pytest.raises(ValueError, match="not closed"):
            _extract_struct("typedef struct {\n\tint (*a)(void);\n")


class TestExtractDefines:
    def test_api_var_and_order(self):
        api_var, names = _extract_defines(TEMPLATE.read_text())
        assert api_var == "ext_api"
        assert names == [
            "ext_open",
            "ext_close",
            "ext_version",
            "ext_flush",
            "ext_get_kind",
        ]

    def test_inconsistent_api_var_errors(self):
        text = TEMPLATE.read_text().replace(
            "#define ext_close    ext_api.ext_close",
            "#define ext_close    other_api.ext_close",
        )
        with pytest.raises(ValueError, match="inconsistent api variables"):
            _extract_defines(text)

    def test_lhs_rhs_mismatch_errors(self):
        text = TEMPLATE.read_text().replace(
            "#define ext_close    ext_api.ext_close",
            "#define ext_close    ext_api.ext_open",
        )
        with pytest.raises(ValueError, match="ext_close"):
            _extract_defines(text)

    def test_backslash_wrapped_define_is_joined(self):
        text = (
            "#ifndef X\n"
            "#define ext_open ext_api.ext_open\n"
            "#define ext_a_very_long_function_name                                    \\\n"
            "\text_api.ext_a_very_long_function_name\n"
            "#endif\n"
        )
        api_var, names = _extract_defines(text)
        assert api_var == "ext_api"
        assert names == ["ext_open", "ext_a_very_long_function_name"]


class TestNormalize:
    def test_whitespace_and_star_spacing(self):
        ident = lambda t: t  # noqa: E731
        assert _normalize("const char  *", ident) == _normalize("const char*", ident)
        assert _normalize("const  char *", ident) == _normalize("const char *", ident)

    def test_param_type_strips_name(self):
        assert _param_type("const char *path") == "const char *"
        assert _param_type("idx_t col") == "idx_t"
        assert _param_type("void") == "void"


class TestVerify:
    def test_passes_on_frozen_template(self, tmp_path):
        # No exception, and both outputs are produced.
        consumer, internal = _run(tmp_path)
        assert "typedef struct" in consumer
        assert "CreateExtAPI" in internal

    def test_alias_and_enum_spelling_verify_equal(self, tmp_path):
        # The template spells the return as `ext_kind`; the spec renders `EXT_KIND`.
        # The declared alias makes them equivalent, so verification passes.
        consumer, _ = _run(tmp_path)
        assert "ext_get_kind" in consumer

    def test_whitespace_differences_pass(self, tmp_path):
        text = TEMPLATE.read_text().replace(
            "int32_t (*ext_open)(const char *path, ext_db *out_db);",
            "int32_t   (*ext_open)( const char  *path ,  ext_db  *out_db );",
        )
        # Should not raise.
        _run(tmp_path, text)

    def test_changed_param_type_fails(self, tmp_path):
        text = TEMPLATE.read_text().replace(
            "int32_t (*ext_open)(const char *path, ext_db *out_db);",
            "int32_t (*ext_open)(const char *path, ext_db out_db);",
        )
        with pytest.raises(ValueError, match="signature mismatch for 'ext_open'"):
            _run(tmp_path, text)

    def test_return_type_int_width_fails(self, tmp_path):
        text = TEMPLATE.read_text().replace(
            "int32_t (*ext_flush)(ext_db db);",
            "int64_t (*ext_flush)(ext_db db);",
        )
        with pytest.raises(ValueError, match="signature mismatch for 'ext_flush'"):
            _run(tmp_path, text)

    def test_missing_const_fails(self, tmp_path):
        text = TEMPLATE.read_text().replace(
            "int32_t (*ext_open)(const char *path, ext_db *out_db);",
            "int32_t (*ext_open)(char *path, ext_db *out_db);",
        )
        with pytest.raises(ValueError, match="signature mismatch for 'ext_open'"):
            _run(tmp_path, text)

    def test_unknown_template_member_fails(self, tmp_path):
        # Add both the member and its define so the bijection passes and verify runs.
        text = (
            TEMPLATE.read_text()
            .replace(
                "	int32_t (*ext_flush)(ext_db db);",
                "	int32_t (*ext_flush)(ext_db db);\n	void (*ext_bogus)(void);",
            )
            .replace(
                "#define ext_flush    ext_api.ext_flush",
                "#define ext_flush    ext_api.ext_flush\n#define ext_bogus ext_api.ext_bogus",
            )
        )
        with pytest.raises(
            ValueError, match="'ext_bogus' is not a declared spec function"
        ):
            _run(tmp_path, text)

    def test_spec_removed_member_fails(self, tmp_path):
        # A member whose spec function no longer exists reads as an unknown member.
        modules, metadata = _load()
        for mod in modules:
            mod["functions"].pop("flush", None)
        tmp_path.mkdir(parents=True, exist_ok=True)
        template = tmp_path / "t.in"
        template.write_text(TEMPLATE.read_text())
        with pytest.raises(
            ValueError, match="'ext_flush' is not a declared spec function"
        ):
            generate(
                modules,
                metadata,
                tmp_path / "o.h",
                template=template,
                internal_out=tmp_path / "i.hpp",
            )

    def test_static_inline_member_fails(self, tmp_path):
        template = _inline_template(["int32_t (*t_one)(void);"], ["t_one"])
        with pytest.raises(ValueError, match="static_inline"):
            _run_inline(tmp_path, {"one": _fn(static_inline=True)}, template)


class TestAppend:
    def test_renders_in_both_regions(self, tmp_path):
        consumer, _ = _run(tmp_path)
        # Struct region append, wrapped in the unstable guard.
        assert (
            "#ifdef EXT_API_UNSTABLE\n\tvoid (*ext_extra_one)(ext_db db);" in consumer
        )
        # Define region append.
        assert "#define ext_extra_one ext_api.ext_extra_one" in consumer
        assert "#define ext_extra_two ext_api.ext_extra_two" in consumer

    def test_struct_appends_inside_unstable_guard(self, tmp_path):
        consumer, _ = _run(tmp_path)
        region = consumer[consumer.index("// capigen:begin appended") :]
        region = region[: region.index("// capigen:end appended")]
        assert "#ifdef EXT_API_UNSTABLE" in region
        assert "#endif" in region

    def test_zero_param_append_renders_void(self, tmp_path):
        consumer, _ = _run(tmp_path)
        assert "int32_t (*ext_extra_two)(void);" in consumer

    def test_excluded_function_skipped(self, tmp_path):
        consumer, internal = _run(tmp_path)
        assert "ext_skipme" not in consumer
        assert "ext_skipme" not in internal

    def test_append_order_deterministic(self, tmp_path):
        c1, i1 = _run(tmp_path / "a")
        c2, i2 = _run(tmp_path / "b")
        assert c1 == c2
        assert i1 == i2

    def test_appended_members_at_end_of_engine_struct(self, tmp_path):
        _, internal = _run(tmp_path)
        struct = internal[
            internal.index("typedef struct") : internal.index("} ext_api;")
        ]
        names = re.findall(r"\(\*(\w+)\)", struct)
        assert names[-2:] == ["ext_extra_one", "ext_extra_two"]

    def test_appended_members_at_end_of_create_method(self, tmp_path):
        _, internal = _run(tmp_path)
        assigns = re.findall(r"result\.(\w+) =", internal)
        assert assigns[-2:] == ["ext_extra_one", "ext_extra_two"]


class TestEngineSide:
    def test_member_order_equals_template_plus_appends(self, tmp_path):
        _, internal = _run(tmp_path)
        struct = internal[
            internal.index("typedef struct") : internal.index("} ext_api;")
        ]
        names = re.findall(r"\(\*(\w+)\)", struct)
        assert names == [
            "ext_open",
            "ext_close",
            "ext_version",
            "ext_flush",
            "ext_get_kind",
            "ext_extra_one",
            "ext_extra_two",
        ]

    def test_every_member_assigned_in_create_method(self, tmp_path):
        _, internal = _run(tmp_path)
        struct = internal[
            internal.index("typedef struct") : internal.index("} ext_api;")
        ]
        members = re.findall(r"\(\*(\w+)\)", struct)
        assigns = set(re.findall(r"result\.(\w+) =", internal))
        assert set(members) == assigns

    def test_version_defines(self, tmp_path):
        _, internal = _run(tmp_path)
        assert "#define EXT_API_VERSION_MAJOR 1" in internal
        assert "#define EXT_API_VERSION_MINOR 0" in internal
        assert "#define EXT_API_VERSION_PATCH 0" in internal
        assert '#define EXT_API_VERSION_STRING "v1.0.0"' in internal

    def test_region_comments_preserved(self, tmp_path):
        _, internal = _run(tmp_path)
        assert "// v1.0.0" in internal
        assert "// Flush support" in internal
        assert "// Kind support" in internal

    def test_full_member_line_rendered(self, tmp_path):
        # A name-preserving signature mangle must be caught: assert the full line.
        _, internal = _run(tmp_path)
        assert "\tint32_t (*ext_open)(const char *path, ext_db *out_db);" in internal
        assert "\tconst char *(*ext_version)(void);" in internal

    def test_byte_stable_across_two_runs(self, tmp_path):
        _, i1 = _run(tmp_path / "a")
        _, i2 = _run(tmp_path / "b")
        assert i1 == i2

    def test_matches_checked_in_golden(self, tmp_path):
        # Byte-for-byte against committed expected output: a dropped blank line or a
        # mutated include in internal.hpp.j2 fails here, not just a name regex.
        consumer, internal = _run(tmp_path)
        assert consumer == (EXT_SPEC / "expected_consumer.h").read_text()
        assert internal == (EXT_SPEC / "expected_internal.hpp").read_text()


class TestEndToEnd:
    def test_rerun_is_byte_identical(self, tmp_path):
        c1, i1 = _run(tmp_path / "a")
        c2, i2 = _run(tmp_path / "b")
        assert c1 == c2
        assert i1 == i2

    def test_fixture_validates(self):
        modules, metadata = _load()
        assert validate_semantics(modules, metadata) == []


class TestMissingArguments:
    def test_template_required(self, tmp_path):
        modules, metadata = _load()
        with pytest.raises(ValueError, match="--template"):
            generate(
                modules, metadata, tmp_path / "o.h", internal_out=tmp_path / "i.hpp"
            )

    def test_internal_out_required(self, tmp_path):
        modules, metadata = _load()
        with pytest.raises(ValueError, match="--internal-out"):
            generate(modules, metadata, tmp_path / "o.h", template=TEMPLATE)


class TestEmptyAppend:
    """The real V1 case: nothing to append, both marker regions stay empty."""

    def test_consumer_equals_template_with_markers_emptied(self, tmp_path):
        modules, metadata = _load()
        template = EXT_SPEC / "template_full.h.in"
        consumer = tmp_path / "out.h"
        internal = tmp_path / "i.hpp"
        generate(modules, metadata, consumer, template=template, internal_out=internal)
        # No appends: output is the template verbatim (markers present, regions empty).
        assert consumer.read_text() == template.read_text()
        assert (
            "// capigen:begin appended\n\t// capigen:end appended"
            in consumer.read_text()
        )

    def test_engine_struct_has_exactly_template_members(self, tmp_path):
        modules, metadata = _load()
        template = EXT_SPEC / "template_full.h.in"
        internal = tmp_path / "i.hpp"
        generate(
            modules,
            metadata,
            tmp_path / "o.h",
            template=template,
            internal_out=internal,
        )
        text = internal.read_text()
        struct = text[text.index("typedef struct") : text.index("} ext_api;")]
        assert re.findall(r"\(\*(\w+)\)", struct) == [
            "ext_open",
            "ext_close",
            "ext_version",
            "ext_flush",
            "ext_get_kind",
            "ext_extra_one",
            "ext_extra_two",
        ]


class TestAppendOrderIsSpecOrder:
    """Appends follow loader order, not alphabetical order."""

    def test_non_alphabetical_loader_order_preserved(self, tmp_path):
        # Loader/spec order is [t_base, t_zeta, t_alpha]; alphabetical would swap the appends.
        functions = {"base": _fn(), "zeta": _fn(), "alpha": _fn()}
        template = _inline_template(["int32_t (*t_base)(void);"], ["t_base"])
        consumer, internal = _run_inline(tmp_path, functions, template)
        struct = internal[internal.index("typedef struct") : internal.index("} t_api;")]
        assert re.findall(r"\(\*(\w+)\)", struct) == ["t_base", "t_zeta", "t_alpha"]
        # And the define region append preserves that order too.
        assert consumer.index("t_zeta") < consumer.index("t_alpha")


class TestStructuralChecks:
    """The restored strict cross-check and malformed-member guards."""

    def test_wrapped_define_counts_toward_bijection(self, tmp_path):
        # A backslash-wrapped define is seen, so the member<->define bijection holds.
        functions = {"base": _fn(), "wrapped": _fn()}
        template = (
            "#pragma once\n\n"
            "typedef struct {\n"
            "#if T_VERSION_MINOR > 0 || (T_VERSION_MINOR == 0 && T_VERSION_PATCH >= 0) // v1.0.0\n"
            "\tint32_t (*t_base)(void);\n"
            "\tint32_t (*t_wrapped)(void);\n"
            "#endif\n\n"
            "\t// capigen:begin appended\n"
            "\t// capigen:end appended\n"
            "} t_api;\n\n"
            "#ifndef T_STATIC\n"
            "#define t_base t_api.t_base\n"
            "#define t_wrapped                                                          \\\n"
            "\tt_api.t_wrapped\n\n"
            "// capigen:begin appended\n"
            "// capigen:end appended\n"
            "#endif // T_STATIC\n"
        )
        # Must not raise: the wrapped define satisfies the strict check.
        _run_inline(tmp_path, functions, template)

    def test_member_without_define_errors(self, tmp_path):
        text = TEMPLATE.read_text().replace(
            "#define ext_flush    ext_api.ext_flush\n", ""
        )
        with pytest.raises(
            ValueError, match=r"struct members without a define mapping.*ext_flush"
        ):
            _run(tmp_path, text)

    def test_define_without_member_errors(self, tmp_path):
        text = TEMPLATE.read_text().replace(
            "#define ext_get_kind ext_api.ext_get_kind",
            "#define ext_get_kind ext_api.ext_get_kind\n#define ext_ghost ext_api.ext_ghost",
        )
        with pytest.raises(
            ValueError, match=r"define mappings without a struct member.*ext_ghost"
        ):
            _run(tmp_path, text)

    def test_duplicate_member_errors(self, tmp_path):
        text = TEMPLATE.read_text().replace(
            "	int32_t (*ext_flush)(ext_db db);",
            "	int32_t (*ext_flush)(ext_db db);\n	int32_t (*ext_flush)(ext_db db);",
        )
        with pytest.raises(ValueError, match=r"declared more than once.*ext_flush"):
            _run(tmp_path, text)

    def test_trailing_content_after_semicolon_errors(self):
        struct = (
            "typedef struct {\n"
            "#if V > 0 // v1.0.0\n"
            "\tint (*a)(void); int (*b)(void);\n"
            "#endif\n"
            "} api_t;\n"
        )
        with pytest.raises(ValueError, match="content after ';'"):
            _extract_struct(struct)

    def test_missing_semicolon_errors(self):
        struct = (
            "typedef struct {\n"
            "#if V > 0 // v1.0.0\n"
            "\tint (*a)(void)\n"
            "#endif\n"
            "} api_t;\n"
        )
        with pytest.raises(ValueError, match="missing terminating ';'"):
            _extract_struct(struct)

    def test_swallowed_member_errors(self):
        # A missing ';' that swallows the next member is caught as a malformed member.
        struct = (
            "typedef struct {\n"
            "#if V > 0 // v1.0.0\n"
            "\tint (*a)(void) int (*b)(void);\n"
            "#endif\n"
            "} api_t;\n"
        )
        with pytest.raises(ValueError, match="missing ';'"):
            _extract_struct(struct)


@pytest.mark.skipif(not HAS_CC, reason="no C compiler available")
class TestCompile:
    """Both generated fixture headers are syntactically valid."""

    PRELUDE = (
        "#include <stdint.h>\n"
        "typedef void *ext_db;\n"
        "typedef enum { EXT_KIND_A = 0, EXT_KIND_B = 1 } EXT_KIND;\n"
        "typedef EXT_KIND ext_kind;\n"
        "int32_t ext_open(const char *path, ext_db *out_db);\n"
        "void ext_close(ext_db db);\n"
        "const char *ext_version(void);\n"
        "int32_t ext_flush(ext_db db);\n"
        "ext_kind ext_get_kind(ext_db db);\n"
        "void ext_extra_one(ext_db db);\n"
        "int32_t ext_extra_two(void);\n"
    )

    def _compile(self, tmp_path, header, lang):
        (tmp_path / "ext.h").write_text(self.PRELUDE)
        src = tmp_path / f"probe.{'c' if lang == 'c' else 'cpp'}"
        src.write_text(f'#include "{header}"\n')
        result = subprocess.run(
            [
                "cc",
                "-fsyntax-only",
                f"-x{'c' if lang == 'c' else 'c++'}",
                "-I",
                str(tmp_path),
                str(src),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr

    def test_consumer_compiles_as_c(self, tmp_path):
        _run(tmp_path)  # writes out.h + internal.hpp into tmp_path
        self._compile(tmp_path, "out.h", "c")

    def test_internal_compiles_as_cpp(self, tmp_path):
        _run(tmp_path)
        self._compile(tmp_path, "internal.hpp", "cpp")
