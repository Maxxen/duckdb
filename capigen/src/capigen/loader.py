"""Load and validate YAML IDL files against JSON Schema, applying defaults."""

import json
from pathlib import Path

import yaml
import jsonschema

from . import __version__
from .tools import sort_modules_by_deps

# Schema files ship with capigen — they define what the tool understands.
_SCHEMA_DIR = Path(__file__).parent / "schema"


class SchemaVersionError(Exception):
    """Raised when a spec's schema_version is not supported by this capigen."""


def _load_schema(name: str) -> dict:
    return json.loads((_SCHEMA_DIR / name).read_text())


def _parse_major_minor(value: str, label: str) -> tuple[int, int]:
    """Parse 'X.Y' or 'X.Y.Z' into (major, minor); reject anything else."""
    parts = value.split(".")
    if len(parts) not in (2, 3) or not all(p.isdigit() for p in parts):
        raise SchemaVersionError(
            f"invalid {label} {value!r}: expected MAJOR.MINOR or MAJOR.MINOR.PATCH"
        )
    return int(parts[0]), int(parts[1])


def check_schema_version(spec_version: str, tool_version: str) -> None:
    """Accept when majors match and the spec minor is at most the tool minor."""
    spec_major, spec_minor = _parse_major_minor(spec_version, "schema_version")
    tool_major, tool_minor = _parse_major_minor(tool_version, "capigen version")
    if spec_major != tool_major or spec_minor > tool_minor:
        raise SchemaVersionError(
            f"spec requires schema {spec_major}.{spec_minor}; "
            f"capigen {tool_version} supports {tool_major}.{tool_minor}; "
            f"install 'capigen~={spec_major}.{spec_minor}.0'"
        )


def apply_defaults(data: dict, schema: dict, defs: dict | None = None) -> dict:
    """Walk a JSON Schema and fill missing keys with declared default values."""
    if defs is None:
        defs = schema.get("$defs", {})

    props = schema.get("properties", {})
    for key, prop_schema in props.items():
        prop_schema = _resolve_ref(prop_schema, defs)

        if key not in data:
            if "default" in prop_schema:
                data[key] = _copy_default(prop_schema["default"])
        elif isinstance(data[key], dict) and _is_object_schema(prop_schema):
            apply_defaults(data[key], prop_schema, defs)
        elif isinstance(data[key], list) and prop_schema.get("type") == "array":
            _apply_defaults_to_array(data[key], prop_schema, defs)

    # Handle additionalProperties (for maps like functions, parameters)
    additional = schema.get("additionalProperties")
    if isinstance(additional, dict):
        additional = _resolve_ref(additional, defs)
        if _is_object_schema(additional):
            for value in data.values():
                if isinstance(value, dict) and not any(value is data[k] for k in props):
                    apply_defaults(value, additional, defs)

    return data


def _apply_defaults_to_array(items: list, schema: dict, defs: dict) -> None:
    item_schema = _resolve_ref(schema.get("items", {}), defs)
    if _is_object_schema(item_schema):
        for item in items:
            if isinstance(item, dict):
                apply_defaults(item, item_schema, defs)


def _resolve_ref(schema: dict, defs: dict) -> dict:
    if "$ref" in schema:
        ref_name = schema["$ref"].split("/")[-1]
        return defs.get(ref_name, schema)
    return schema


def _is_object_schema(schema: dict) -> bool:
    return schema.get("type") == "object" or "properties" in schema


def _copy_default(value):
    if isinstance(value, list):
        return list(value)
    if isinstance(value, dict):
        return dict(value)
    return value


def load_metadata(spec_dir: Path) -> dict:
    """Load and validate metadata.yaml from spec_dir."""
    schema = _load_schema("metadata.schema.json")
    data = yaml.safe_load((spec_dir / "metadata.yaml").read_text())
    jsonschema.validate(data, schema)
    check_schema_version(data["schema_version"], __version__)
    apply_defaults(data, schema)
    return data


def load_modules(spec_dir: Path) -> list[dict]:
    """Load and validate all module YAML files from spec_dir."""
    schema = _load_schema("module.schema.json")
    modules = []
    for path in sorted(spec_dir.rglob("*.yaml")):
        if path.name == "metadata.yaml":
            continue
        data = yaml.safe_load(path.read_text())
        try:
            jsonschema.validate(data, schema)
        except jsonschema.ValidationError as e:
            raise jsonschema.ValidationError(
                f"{path.relative_to(spec_dir)}: {e.message} in {e.json_path}"
            ) from e
        apply_defaults(data, schema)
        modules.append(data)
    return sort_modules_by_deps(modules)
