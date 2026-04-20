"""Resolve API spec dicts into C-specific render objects for Jinja2 templates."""

from .render import (
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


def resolve_modules(modules: list[dict], metadata: dict) -> list[CModule]:
    """Transform validated API spec dicts into typed C render objects."""
    primitives = {p["name"]: p["c_type"] for p in metadata["primitives"]}
    suffixes = metadata["suffixes"]
    registry = _build_registry(modules, suffixes)
    return [_resolve_module(mod, registry, primitives, suffixes) for mod in modules]


# ---------------------------------------------------------------------------
# Registry: maps every known type name (raw + aliased) to its C name
# ---------------------------------------------------------------------------


def _build_registry(modules: list[dict], suffixes: dict[str, str]) -> dict[str, str]:
    """Build a name -> C-name registry from all declared types."""
    registry: dict[str, str] = {}

    for mod in modules:
        for name in mod.get("handles", {}):
            canonical = f"{name}{suffixes['handles']}"
            registry[name] = canonical
            registry[canonical] = canonical

        for name in mod.get("callbacks", {}):
            canonical = f"{name}{suffixes['callbacks']}"
            registry[name] = canonical
            registry[canonical] = canonical

        for name in mod.get("aliases", {}):
            canonical = f"{name}{suffixes['aliases']}"
            registry[name] = canonical
            registry[canonical] = canonical

        for name, s in mod.get("structs", {}).items():
            if s.get("pointer_alias"):
                alias = f"{name}{suffixes['aliases']}"
            else:
                alias = name
            registry[name] = alias
            if alias != name:
                registry[alias] = alias

        for name in mod.get("enums", {}):
            registry[name] = name

    return registry


# ---------------------------------------------------------------------------
# C name resolution
# ---------------------------------------------------------------------------


def _resolve_c_name(
    symbol: str,
    registry: dict[str, str],
    primitives: dict[str, str],
    context: str,
) -> str:
    """Resolve a spec symbol name to its C type string."""
    if symbol in primitives:
        return primitives[symbol]
    if symbol in registry:
        return registry[symbol]
    raise ValueError(f"{context}: unknown type '{symbol}'")


def _format_c_type(base: str, pointer: int = 0, is_const: bool = False) -> str:
    prefix = "const " if is_const else ""
    return f"{prefix}{base}{'*' * pointer}"


# ---------------------------------------------------------------------------
# Per-module resolution
# ---------------------------------------------------------------------------


def _resolve_module(
    mod: dict,
    registry: dict[str, str],
    primitives: dict[str, str],
    suffixes: dict[str, str],
) -> CModule:
    return CModule(
        name=mod["module"],
        types=(
            [_resolve_handle(name, suffixes) for name in mod.get("handles", {})]
            + [
                _resolve_alias(name, a, registry, primitives, suffixes)
                for name, a in mod.get("aliases", {}).items()
            ]
        ),
        structs=[
            _resolve_struct(name, s, registry, primitives, suffixes)
            for name, s in mod.get("structs", {}).items()
        ],
        enums=[_resolve_enum(name, e) for name, e in mod.get("enums", {}).items()],
        constants=[
            CConstant(name=name, **c) for name, c in mod.get("constants", {}).items()
        ],
        error_groups=[
            _resolve_error_group(category, g)
            for category, g in mod.get("error_groups", {}).items()
        ],
        function_ptrs=[
            _resolve_callback(name, cb, registry, primitives, suffixes)
            for name, cb in mod.get("callbacks", {}).items()
        ],
        functions={
            fname: _resolve_function(fname, func, registry, primitives)
            for fname, func in mod.get("functions", {}).items()
        },
    )


def _resolve_handle(name: str, suffixes: dict[str, str]) -> CTypeDef:
    return CTypeDef(
        name=name,
        canonical_name=f"{name}{suffixes['handles']}",
        base="void",
        is_pointer=True,
    )


def _resolve_alias(
    name: str,
    a: dict,
    registry: dict[str, str],
    primitives: dict[str, str],
    suffixes: dict[str, str],
) -> CTypeDef:
    base = _resolve_c_name(a["underlying"], registry, primitives, f"Alias '{name}'")
    return CTypeDef(
        name=name,
        canonical_name=f"{name}{suffixes['aliases']}",
        base=base,
        is_pointer=False,
    )


def _resolve_struct(
    name: str,
    s: dict,
    registry: dict[str, str],
    primitives: dict[str, str],
    suffixes: dict[str, str],
) -> CStruct:
    if s.get("pointer_alias"):
        alias = f"{name}{suffixes['aliases']}"
    else:
        alias = name

    fields = []
    for f in s.get("fields", []):
        base = _resolve_c_name(
            f["type"], registry, primitives, f"Struct '{name}' field '{f['name']}'"
        )
        fields.append(
            CField(
                name=f["name"],
                base=base,
                pointer=f["pointer"],
                const=f["const"],
            )
        )

    return CStruct(
        name=name,
        template_alias=alias,
        pointer_alias=s.get("pointer_alias", False),
        fields=fields,
    )


def _resolve_callback(
    name: str,
    cb: dict,
    registry: dict[str, str],
    primitives: dict[str, str],
    suffixes: dict[str, str],
) -> CFuncPtr:
    alias = f"{name}{suffixes['callbacks']}"

    params = []
    for pname, p in cb.get("parameters", {}).items():
        base = _resolve_c_name(
            p["type"], registry, primitives, f"Callback '{name}' param '{pname}'"
        )
        params.append(
            CFuncPtrParam(
                name=pname,
                base=base,
                pointer=p["indirection"],
                const=p["const"],
            )
        )

    return CFuncPtr(
        name=name,
        template_alias=alias,
        return_base=_resolve_c_name(
            cb["return_type"], registry, primitives, f"Callback '{name}' return"
        ),
        return_pointer=cb["return_pointer"],
        return_const=cb["return_const"],
        params=params,
    )


def _resolve_function(
    fname: str, func: dict, registry: dict[str, str], primitives: dict[str, str]
) -> CFunction:
    params: dict[str, CParam] = {}
    for pname, p in func["parameters"].items():
        base = _resolve_c_name(
            p["type"], registry, primitives, f"Function '{fname}' param '{pname}'"
        )
        c_decl = _format_c_type(base, p["indirection"], p["const"])
        params[pname] = CParam(
            name=pname,
            c_decl=c_decl,
            description=p.get("description") or None,
        )

    return_base = _resolve_c_name(
        func["return_type"], registry, primitives, f"Function '{fname}' return"
    )
    return_c = _format_c_type(return_base, func["return_pointer"], func["return_const"])

    return CFunction(
        name=fname,
        summary=func.get("summary") or None,
        description=func.get("description") or None,
        deprecated=func.get("deprecated") or None,
        return_c=return_c,
        parameters=params,
    )


def _resolve_enum(name: str, enum: dict) -> CEnum:
    """Auto-number enum values (sequential from 0, reset on explicit value)."""
    resolved_values: dict[str, CEnumValue] = {}
    current = 0
    for vname, entry in enum["values"].items():
        if entry.get("value") is not None:
            current = entry["value"]
        resolved_values[vname] = CEnumValue(
            value=current,
            description=entry.get("description", ""),
        )
        current += 1

    return CEnum(
        name=name,
        description=enum.get("description", ""),
        values=resolved_values,
    )


def _resolve_error_group(category: str, group: dict) -> CErrorGroup:
    return CErrorGroup(
        category=category,
        group_id=group["group_id"],
        description=group.get("description", ""),
        entries=[
            CErrorEntry(
                name=ename, code=e["code"], description=e.get("description", "")
            )
            for ename, e in group["entries"].items()
        ],
    )
