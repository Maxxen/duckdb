"""Tests for the schema-version compatibility contract and CLI version flags."""

import subprocess
import sys
from importlib.metadata import version

import pytest

import capigen
from capigen.loader import SchemaVersionError, check_schema_version


class TestSchemaVersionConstant:
    def test_agrees_with_package_metadata(self):
        expected = ".".join(version("capigen").split(".")[:2])
        assert capigen.SCHEMA_VERSION == expected

    def test_is_major_minor(self):
        assert capigen.SCHEMA_VERSION.count(".") == 1


class TestCompatCheck:
    def test_equal_accepted(self):
        check_schema_version("0.4", "0.4.0")

    def test_older_minor_accepted(self):
        check_schema_version("0.3", "0.4.0")

    def test_legacy_three_part_accepted(self):
        check_schema_version("0.4.7", "0.4.0")

    def test_shipped_spec_pins_older_minor(self):
        # The shipped specs pin "0.3.0" and must load under a 0.4 tool.
        check_schema_version("0.3.0", "0.4.0")

    def test_newer_minor_rejected(self):
        with pytest.raises(SchemaVersionError, match=r"install 'capigen~=0.5.0'"):
            check_schema_version("0.5", "0.4.0")

    def test_other_major_rejected(self):
        with pytest.raises(SchemaVersionError):
            check_schema_version("1.0", "0.4.0")

    def test_malformed_rejected(self):
        with pytest.raises(SchemaVersionError, match="invalid schema_version"):
            check_schema_version("nope", "0.4.0")

    def test_message_names_both_versions(self):
        with pytest.raises(SchemaVersionError) as exc:
            check_schema_version("0.5", "0.4.2")
        msg = str(exc.value)
        assert "0.5" in msg
        assert "0.4.2" in msg
        assert "supports 0.4" in msg


class TestCliVersionFlags:
    def _run(self, flag):
        return subprocess.run(
            [sys.executable, "-m", "capigen", flag],
            capture_output=True,
            text=True,
        )

    def test_version_flag(self):
        result = self._run("--version")
        assert result.returncode == 0
        assert result.stdout.strip() == version("capigen")

    def test_schema_version_flag(self):
        result = self._run("--schema-version")
        assert result.returncode == 0
        assert result.stdout.strip() == capigen.SCHEMA_VERSION
