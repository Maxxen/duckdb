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
SPEC_DIR = REPO_ROOT / "api_spec"
CHECKED_IN_HEADER = REPO_ROOT.parent / "src" / "include" / "duckdb_v2.h"


class TestRoundTrip:
    """Regenerate the header from the spec and compare to the checked-in version."""

    def test_output_matches_checked_in_header(self, tmp_path):
        metadata = load_metadata(SPEC_DIR)
        modules = load_modules(SPEC_DIR)

        errors = validate_semantics(modules, metadata)
        assert errors == [], f"Semantic validation errors: {errors}"

        output = tmp_path / "duckdb_v2.h"
        generate(modules, metadata, output)

        expected = CHECKED_IN_HEADER.read_text()
        actual = output.read_text()
        assert actual == expected, (
            "Generated output differs from checked-in duckdb_v2.h. "
            "Run 'just run' to regenerate."
        )


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
        """The bundled spec has a schema_version field."""
        metadata = load_metadata(SPEC_DIR)
        assert "schema_version" in metadata


HAS_CC = shutil.which("cc") is not None


@pytest.mark.skipif(not HAS_CC, reason="no C compiler available")
class TestCompile:
    """Verify the generated header is syntactically valid C."""

    @pytest.mark.xfail(
        reason="IDL has undefined types (idx_t, duckdb_ctx_ptr used before declaration)"
    )
    def test_header_compiles_as_c(self, tmp_path):
        metadata = load_metadata(SPEC_DIR)
        modules = load_modules(SPEC_DIR)
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
        reason="IDL has undefined types (idx_t, duckdb_ctx_ptr used before declaration)"
    )
    def test_header_compiles_as_cpp(self, tmp_path):
        metadata = load_metadata(SPEC_DIR)
        modules = load_modules(SPEC_DIR)
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
