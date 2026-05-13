"""Load and validate YAML IDL files against JSON Schema, applying defaults."""

import json
from pathlib import Path

import yaml
import jsonschema

# Schema files ship with capigen — they define what the tool understands.
_SCHEMA_DIR = Path(__file__).parent / "schema"


def _load_schema(name: str) -> dict:
    return json.loads((_SCHEMA_DIR / name).read_text())


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
    schema = _load_schema("metadata.schema.json")
    data = yaml.safe_load((spec_dir / "metadata.yaml").read_text())
    jsonschema.validate(data, schema)
    apply_defaults(data, schema)
    return data


def load_modules(spec_dir: Path) -> list[dict]:
    schema = _load_schema("module.schema.json")
    modules = []
    for path in sorted((spec_dir / "v2").rglob("*.yaml")):
        data = yaml.safe_load(path.read_text())
        try:
            jsonschema.validate(data, schema)
        except jsonschema.ValidationError as e:
            raise jsonschema.ValidationError(
                f"{path.relative_to(spec_dir)}: {e.message}"
            ) from e
        apply_defaults(data, schema)
        modules.append(data)
    return modules
