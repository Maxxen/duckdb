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
    prefix = metadata.get("prefix", "")
    c_options = metadata.get("options", {}).get("c", {})
    handle_opts = c_options.get("handles", {})
    handle_style = handle_opts.get("default_style", "void_ptr")
    void_ptr_handles = frozenset(
        name
        for name, style in handle_opts.get("override_style", {}).items()
        if style == "void_ptr"
    )
    registry = _build_registry(modules, suffixes, prefix)
    return [
        _resolve_module(
            mod, registry, primitives, suffixes, prefix, handle_style, void_ptr_handles
        )
        for mod in modules
    ]


def _is_tagged_struct(
    name: str, handle_style: str, void_ptr_handles: frozenset[str]
) -> bool:
    return handle_style == "tagged_struct" and name not in void_ptr_handles


def _apply_prefix(prefix: str, name: str) -> str:
    """Prepend prefix, uppercasing it when name starts with an uppercase letter."""
    if name and name[0].isupper():
        return f"{prefix.upper()}{name}"
    return f"{prefix}{name}"


# ---------------------------------------------------------------------------
# Registry: maps every known spec name (unprefixed) to its C name (prefixed)
# ---------------------------------------------------------------------------


def _build_registry(
    modules: list[dict], suffixes: dict[str, str], prefix: str = ""
) -> dict[str, str]:
    """Build a name -> C-name registry from all declared types."""
    registry: dict[str, str] = {}

    for mod in modules:
        for name in mod.get("handles", {}):
            canonical = f"{_apply_prefix(prefix, name)}{suffixes['handles']}"
            registry[name] = canonical
            registry[canonical] = canonical

        for name in mod.get("callbacks", {}):
            canonical = f"{_apply_prefix(prefix, name)}{suffixes['callbacks']}"
            registry[name] = canonical
            registry[canonical] = canonical

        for name, a in mod.get("aliases", {}).items():
            if a.get("qualified"):
                registry[name] = name
            else:
                canonical = f"{_apply_prefix(prefix, name)}{suffixes['aliases']}"
                registry[name] = canonical
                registry[canonical] = canonical

        for name, s in mod.get("structs", {}).items():
            prefixed = _apply_prefix(prefix, name)
            if s.get("pointer_alias"):
                alias = f"{prefixed}{suffixes['aliases']}"
            else:
                alias = prefixed
            registry[name] = alias
            registry[prefixed] = alias
            if alias != prefixed:
                registry[alias] = alias

        for name in mod.get("enums", {}):
            prefixed = _apply_prefix(prefix, name)
            registry[name] = prefixed
            registry[prefixed] = prefixed

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
    prefix: str = "",
    handle_style: str = "void_ptr",
    void_ptr_handles: frozenset[str] = frozenset(),
) -> CModule:
    uprefix = prefix.upper()
    return CModule(
        name=mod["module"],
        types=(
            [
                _resolve_handle(
                    name,
                    h,
                    suffixes,
                    prefix,
                    _is_tagged_struct(name, handle_style, void_ptr_handles),
                )
                for name, h in mod.get("handles", {}).items()
            ]
            + [
                _resolve_alias(name, a, registry, primitives, suffixes, prefix)
                for name, a in mod.get("aliases", {}).items()
            ]
        ),
        structs=[
            _resolve_struct(name, s, registry, primitives, suffixes, prefix)
            for name, s in mod.get("structs", {}).items()
        ],
        enums=[
            _resolve_enum(name, e, prefix) for name, e in mod.get("enums", {}).items()
        ],
        constants=[
            CConstant(
                name=f"{uprefix}{name}",
                value=c["value"],
                description=c.get("description", ""),
            )
            for name, c in mod.get("constants", {}).items()
        ],
        error_groups=[
            _resolve_error_group(category, g, prefix)
            for category, g in mod.get("error_groups", {}).items()
        ],
        function_ptrs=[
            _resolve_callback(name, cb, registry, primitives, suffixes, prefix)
            for name, cb in mod.get("callbacks", {}).items()
        ],
        functions={
            f"{prefix}{fname}": _resolve_function(
                f"{prefix}{fname}", func, registry, primitives
            )
            for fname, func in mod.get("functions", {}).items()
        },
    )


def _resolve_handle(
    name: str,
    h: dict,
    suffixes: dict[str, str],
    prefix: str = "",
    tagged_struct: bool = False,
) -> CTypeDef:
    prefixed = _apply_prefix(prefix, name)
    return CTypeDef(
        name=prefixed,
        canonical_name=f"{prefixed}{suffixes['handles']}",
        base="void",
        is_pointer=True,
        tagged_struct=tagged_struct,
        description=h.get("description", ""),
    )


def _resolve_alias(
    name: str,
    a: dict,
    registry: dict[str, str],
    primitives: dict[str, str],
    suffixes: dict[str, str],
    prefix: str = "",
) -> CTypeDef:
    base = _resolve_c_name(a["underlying"], registry, primitives, f"Alias '{name}'")
    if a.get("qualified"):
        return CTypeDef(
            name=name,
            canonical_name=name,
            base=base,
            is_pointer=False,
            is_qualified=True,
            description=a.get("description", ""),
        )
    prefixed = _apply_prefix(prefix, name)
    return CTypeDef(
        name=prefixed,
        canonical_name=f"{prefixed}{suffixes['aliases']}",
        base=base,
        is_pointer=False,
        description=a.get("description", ""),
    )


def _resolve_struct(
    name: str,
    s: dict,
    registry: dict[str, str],
    primitives: dict[str, str],
    suffixes: dict[str, str],
    prefix: str = "",
) -> CStruct:
    prefixed = _apply_prefix(prefix, name)
    if s.get("pointer_alias"):
        alias = f"{prefixed}{suffixes['aliases']}"
    else:
        alias = prefixed

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
                array_size=f.get("array_size"),
            )
        )

    return CStruct(
        name=prefixed,
        template_alias=alias,
        pointer_alias=s.get("pointer_alias", False),
        fields=fields,
        description=s.get("description", ""),
    )


def _resolve_callback(
    name: str,
    cb: dict,
    registry: dict[str, str],
    primitives: dict[str, str],
    suffixes: dict[str, str],
    prefix: str = "",
) -> CFuncPtr:
    prefixed = _apply_prefix(prefix, name)
    alias = f"{prefixed}{suffixes['callbacks']}"

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
        name=prefixed,
        template_alias=alias,
        return_base=_resolve_c_name(
            cb["return_type"], registry, primitives, f"Callback '{name}' return"
        ),
        return_pointer=cb["return_pointer"],
        return_const=cb["return_const"],
        params=params,
        description=cb.get("description", ""),
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

    status = func.get("status", [])
    current_state = status[0][0] if status else None
    if current_state == "deprecated":
        deprecated = status[0][1]
    else:
        deprecated = func.get("deprecated") or None

    return CFunction(
        name=fname,
        summary=func.get("summary") or None,
        description=func.get("description") or None,
        deprecated=deprecated,
        return_c=return_c,
        static_inline=bool(func.get("static_inline", False)),
        parameters=params,
    )


def _resolve_enum(name: str, enum: dict, prefix: str = "") -> CEnum:
    """Auto-number enum values (sequential from 0, reset on explicit value)."""
    uprefix = prefix.upper()
    resolved_values: dict[str, CEnumValue] = {}
    current = 0
    for vname, entry in enum["values"].items():
        if entry.get("value") is not None:
            current = entry["value"]
        resolved_values[f"{uprefix}{vname}"] = CEnumValue(
            value=current,
            description=entry.get("description", ""),
        )
        current += 1

    return CEnum(
        name=_apply_prefix(prefix, name),
        description=enum.get("description", ""),
        values=resolved_values,
    )


def _resolve_error_group(category: str, group: dict, prefix: str = "") -> CErrorGroup:
    uprefix = prefix.upper()
    return CErrorGroup(
        category=category,
        group_id=group["group_id"],
        description=group.get("description", ""),
        entries=[
            CErrorEntry(
                name=f"{uprefix}{ename}",
                code=e["code"],
                description=e.get("description", ""),
            )
            for ename, e in group["entries"].items()
        ],
    )
