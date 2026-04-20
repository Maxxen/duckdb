"""Language-agnostic cross-module referential integrity checks."""


def validate_semantics(modules: list[dict], metadata: dict) -> list[str]:
    """Validate cross-module constraints. Returns list of error strings."""
    errors: list[str] = []

    primitives = {p["name"] for p in metadata["primitives"]}
    versions = set(metadata["versions"])

    # Pass 1: collect all declared symbols, detect duplicates
    all_types: dict[str, str] = {}  # name → module
    all_functions: dict[str, str] = {}

    for mod in modules:
        module_name = mod["module"]

        for construct in ("handles", "callbacks", "aliases", "structs", "enums"):
            for name in mod.get(construct, {}):
                if name in all_types:
                    errors.append(
                        f"{module_name}::{name}: Type name '{name}' is duplicated "
                        f"(first in '{all_types[name]}')"
                    )
                all_types[name] = module_name

        for func_name in mod.get("functions", {}):
            if func_name in all_functions:
                errors.append(
                    f"{module_name}::{func_name}: Function name '{func_name}' is duplicated "
                    f"(first in '{all_functions[func_name]}')"
                )
            all_functions[func_name] = module_name

    def is_valid_type(name: str) -> bool:
        return name in primitives or name in all_types

    # Pass 2: validate type references
    for mod in modules:
        module_name = mod["module"]

        for name, a in mod.get("aliases", {}).items():
            if not is_valid_type(a["underlying"]):
                errors.append(
                    f"{module_name}::{name}: Unknown underlying type '{a['underlying']}'"
                )

        for name, s in mod.get("structs", {}).items():
            for f in s.get("fields", []):
                if not is_valid_type(f["type"]):
                    errors.append(
                        f"{module_name}::{name}.{f['name']}: "
                        f"Unknown field type '{f['type']}'"
                    )

        for name, cb in mod.get("callbacks", {}).items():
            if not is_valid_type(cb["return_type"]):
                errors.append(
                    f"{module_name}::{name}: Unknown return type '{cb['return_type']}'"
                )
            for pname, p in cb.get("parameters", {}).items():
                if not is_valid_type(p["type"]):
                    errors.append(
                        f"{module_name}::{name}.{pname}: "
                        f"Unknown parameter type '{p['type']}'"
                    )

    # Pass 3: validate functions
    for mod in modules:
        module_name = mod["module"]

        for func_name, func in mod.get("functions", {}).items():
            if func.get("added") and func["added"] not in versions:
                errors.append(
                    f"{module_name}::{func_name}: "
                    f"Unknown 'added' version '{func['added']}'"
                )
            if func.get("deprecated") and func["deprecated"] not in versions:
                errors.append(
                    f"{module_name}::{func_name}: "
                    f"Unknown 'deprecated' version '{func['deprecated']}'"
                )
            if not is_valid_type(func["return_type"]):
                errors.append(
                    f"{module_name}::{func_name}: "
                    f"Unknown return type '{func['return_type']}'"
                )
            for pname, p in func.get("parameters", {}).items():
                if not is_valid_type(p["type"]):
                    errors.append(
                        f"{module_name}::{func_name}.{pname}: "
                        f"Unknown parameter type '{p['type']}'"
                    )

    return errors
