"""Integration tests: round-trip generation and C compilation."""

import subprocess
import shutil
from pathlib import Path

import jsonschema
import pytest

from capigen.loader import load_metadata, load_modules
from capigen.validate import validate_semantics
from capigen.adapters.c import generate


REPO_ROOT = Path(__file__).parent.parent
TESTSPEC_DIR = Path(__file__).parent / "testspec" / "v2"


class TestRoundTrip:
    """Generate from the bundled test spec and verify the output is valid."""

    def test_generates_valid_header(self, tmp_path):
        metadata = load_metadata(TESTSPEC_DIR)
        modules = load_modules(TESTSPEC_DIR)

        errors = validate_semantics(modules, metadata)
        assert errors == [], f"Semantic validation errors: {errors}"

        output = tmp_path / "duckdb_v2.h"
        generate(modules, metadata, output)

        content = output.read_text()
        assert "duckdb_v2_open" in content
        assert "duckdb_v2_close" in content
        assert "duckdb_v2_ctx_ptr" in content
        assert "duckdb_v2_database_ptr" in content
        assert "DUCKDB_V2_TYPE" in content
        assert "DUCKDB_V2_API_ERROR" in content

    def test_output_is_deterministic(self, tmp_path):
        """Running the generator twice produces identical output."""
        metadata = load_metadata(TESTSPEC_DIR)
        modules = load_modules(TESTSPEC_DIR)

        out1 = tmp_path / "first.h"
        out2 = tmp_path / "second.h"
        generate(modules, metadata, out1)
        generate(modules, metadata, out2)

        assert out1.read_text() == out2.read_text()


class TestInlineArrayStructRendering:
    """Struct fields with array_size render as C fixed-size arrays."""

    def _metadata(self):
        return {
            "schema_version": "0.2.0",
            "versions": ["1.0.0"],
            "suffixes": {"handles": "_ptr", "callbacks": "_cb", "aliases": "_t"},
            "primitives": [
                {"name": "opaque", "c_type": "void"},
                {"name": "char", "c_type": "char"},
                {"name": "u32", "c_type": "uint32_t"},
            ],
        }

    def _module(self):
        return {
            "module": "m",
            "handles": {},
            "callbacks": {},
            "aliases": {},
            "structs": {
                "duckdb_v2_err": {
                    "pointer_alias": False,
                    "fields": [
                        {
                            "name": "code",
                            "type": "u32",
                            "pointer": 0,
                            "const": False,
                        },
                        {
                            "name": "message",
                            "type": "char",
                            "pointer": 0,
                            "const": False,
                            "array_size": 64,
                        },
                    ],
                },
            },
            "enums": {},
            "constants": {},
            "error_groups": {},
            "functions": {},
        }

    def test_array_size_renders_bracket(self, tmp_path):
        output = tmp_path / "out.h"
        generate([self._module()], self._metadata(), output)
        content = output.read_text()
        # The array field should render with a bracketed size.
        assert "char message[64];" in content
        # The non-array field should not.
        assert "uint32_t code;" in content


class TestSchemaVersion:
    def test_missing_schema_version(self, tmp_path):
        """metadata.yaml without schema_version is rejected by JSON Schema validation."""
        spec = tmp_path / "spec"
        spec.mkdir()
        (spec / "metadata.yaml").write_text(
            "versions: ['1.0.0']\nprimitives: [opaque]\n"
        )
        with pytest.raises(jsonschema.ValidationError, match="schema_version"):
            load_metadata(spec)

    def test_schema_version_present(self):
        """The bundled test spec has a schema_version field."""
        metadata = load_metadata(TESTSPEC_DIR)
        assert "schema_version" in metadata


HAS_CC = shutil.which("cc") is not None


@pytest.mark.skipif(not HAS_CC, reason="no C compiler available")
class TestCompile:
    """Verify the generated header is syntactically valid C."""

    @pytest.mark.xfail(
        reason="IDL has undefined types (idx_t, duckdb_v2_ctx_ptr used before declaration)"
    )
    def test_header_compiles_as_c(self, tmp_path):
        metadata = load_metadata(TESTSPEC_DIR)
        modules = load_modules(TESTSPEC_DIR)
        output = tmp_path / "duckdb_v2.h"
        generate(modules, metadata, output)

        test_c = tmp_path / "test.c"
        test_c.write_text('#include "duckdb_v2.h"\nint main(void) { return 0; }\n')

        result = subprocess.run(
            ["cc", "-fsyntax-only", "-xc", "-I", str(tmp_path), str(test_c)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Header failed to compile:\n{result.stderr}"

    @pytest.mark.xfail(
        reason="IDL has undefined types (idx_t, duckdb_v2_ctx_ptr used before declaration)"
    )
    def test_header_compiles_as_cpp(self, tmp_path):
        metadata = load_metadata(TESTSPEC_DIR)
        modules = load_modules(TESTSPEC_DIR)
        output = tmp_path / "duckdb_v2.h"
        generate(modules, metadata, output)

        test_cpp = tmp_path / "test.cpp"
        test_cpp.write_text('#include "duckdb_v2.h"\nint main() { return 0; }\n')

        result = subprocess.run(
            ["cc", "-fsyntax-only", "-xc++", "-I", str(tmp_path), str(test_cpp)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"Header failed to compile as C++:\n{result.stderr}"
        )
