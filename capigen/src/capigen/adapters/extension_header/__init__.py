"""Extension header adapter: verify a frozen template struct against the spec and append.

One invocation produces two lockstep outputs: the consumer header (the frozen
template with the append markers filled) and the derived engine-side header (the
ungated struct plus a create method assigning every member). The two must be
generated together; a divergence between them is silent memory corruption.

The template is the source of ABI order. Each function-pointer member is verified
against the spec: the name must resolve to a declared function and the parameter
and return types must match. Spec functions absent from the template are appended
into both marker regions in deterministic loader order.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from ..c.render import CFunction
from ..c.resolve import _build_registry, resolve_modules

_TEMPLATES_DIR = Path(__file__).parent / "templates"

_BEGIN = "// capigen:begin appended"
_END = "// capigen:end appended"


@dataclass
class _Member:
    name: str
    signature: str  # full member text without the trailing semicolon


@dataclass
class _Region:
    kind: str  # "stable" or "unstable"
    version: str = ""  # stable regions: e.g. "1.2.0"
    description: str = ""  # unstable regions: the group comment
    guard: str = ""  # unstable regions: the #ifdef guard token
    members: list[_Member] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Template extraction
# ---------------------------------------------------------------------------


def _member_name(sig: str) -> str:
    """Return the member name, validating the text is exactly one function-pointer decl.

    Assumes members return by value or by pointer, never a function pointer, which
    holds for this API surface. A trailing token past the parameter list means a
    swallowed member (a missing ';'), not a valid declaration.
    """
    name = re.search(r"\(\s*\*\s*([A-Za-z_]\w*)\s*\)", sig)
    if not name:
        raise ValueError(f"cannot parse a member name from: {sig!r}")
    rest = sig[name.end() :].lstrip()
    if not rest.startswith("("):
        raise ValueError(f"malformed struct member (no parameter list): {sig!r}")
    depth = 0
    end = None
    for idx, ch in enumerate(rest):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                end = idx
                break
    if end is None or rest[end + 1 :].strip():
        raise ValueError(f"malformed struct member (missing ';'?): {sig!r}")
    return name.group(1)


def _extract_struct(text: str) -> tuple[str, list[_Region]]:
    """Extract the struct typename and its ordered, gated members from the template."""
    marker = "typedef struct {"
    if marker not in text:
        raise ValueError("template has no 'typedef struct {' function-pointer struct")
    after = text.index(marker) + len(marker)
    close = re.search(r"\n\}\s*([A-Za-z_]\w*)\s*;", text[after:])
    if not close:
        raise ValueError("template struct is not closed by '} <name>;'")
    typename = close.group(1)
    lines = text[after : after + close.start()].split("\n")

    regions: list[_Region] = []
    current: _Region | None = None
    pending_comment: str | None = None
    i = 0
    while i < len(lines):
        s = lines[i].strip()
        if not s:
            i += 1
            continue
        if s.startswith("#if") and not s.startswith(("#ifdef", "#ifndef")):
            block = s
            while block.rstrip().endswith("\\"):
                i += 1
                block += "\n" + lines[i].strip()
            match = re.search(r"//\s*v(\d+\.\d+\.\d+)", block)
            if not match:
                raise ValueError(
                    f"stable gate without a '// vX.Y.Z' comment: {block!r}"
                )
            current = _Region(kind="stable", version=match.group(1))
            regions.append(current)
            pending_comment = None
            i += 1
            continue
        if s.startswith("#ifdef"):
            current = _Region(
                kind="unstable",
                description=pending_comment or "",
                guard=s[len("#ifdef") :].strip(),
            )
            regions.append(current)
            pending_comment = None
            i += 1
            continue
        if s.startswith("#endif"):
            current = None
            i += 1
            continue
        if s.startswith("//"):
            pending_comment = s[2:].strip()
            i += 1
            continue
        if s.startswith("#"):
            i += 1
            continue
        acc = s
        while ";" not in acc:
            i += 1
            nxt = lines[i].strip() if i < len(lines) else ""
            if not nxt or nxt.startswith(("#", "//")):
                raise ValueError(f"struct member missing terminating ';': {acc!r}")
            acc += " " + nxt
        semi = acc.index(";")
        trailing = acc[semi + 1 :].strip()
        if trailing:
            raise ValueError(
                f"unexpected content after ';' in struct member: {trailing!r}"
            )
        if current is None:
            raise ValueError(f"struct member outside a gated region: {acc[:semi]!r}")
        sig = acc[:semi].strip()
        current.members.append(_Member(name=_member_name(sig), signature=sig))
        pending_comment = None
        i += 1

    return typename, regions


def _extract_defines(text: str) -> tuple[str, list[str]]:
    """Extract the api variable name and the mapping names, joining wrapped defines."""
    api_vars: set[str] = set()
    names: list[str] = []
    lines = text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        while line.rstrip().endswith("\\"):
            i += 1
            line = line.rstrip()[:-1] + " " + lines[i].strip()
        match = re.match(r"[ \t]*#define\s+(\w+)\s+(\w+)\.(\w+)\s*$", line)
        if match:
            lhs, api_var, rhs = match.group(1), match.group(2), match.group(3)
            if lhs != rhs:
                raise ValueError(
                    f"define '{lhs}' does not map to '{lhs}' (found '{rhs}')"
                )
            api_vars.add(api_var)
            names.append(lhs)
        i += 1
    if not names:
        raise ValueError("template has no '#define <fn> <api>.<fn>' mapping entries")
    if len(api_vars) != 1:
        raise ValueError(
            f"define mappings use inconsistent api variables: {sorted(api_vars)}"
        )
    return api_vars.pop(), names


# ---------------------------------------------------------------------------
# Signature comparison
# ---------------------------------------------------------------------------


def _build_canonicalizer(modules: list[dict], metadata: dict):
    """Return a function mapping a C type token to its ultimate underlying spelling.

    Only spellings the spec itself declares equivalent (an alias and its
    underlying) collapse together, so genuinely distinct types never match.
    """
    prefix = metadata.get("prefix", "")
    suffixes = metadata["suffixes"]
    primitives = {p["name"]: p["c_type"] for p in metadata["primitives"]}
    registry = _build_registry(modules, suffixes, prefix)

    alias_underlying: dict[str, str] = {}
    for mod in modules:
        for name, alias in mod.get("aliases", {}).items():
            c_name = name if alias.get("qualified") else registry.get(name, name)
            underlying = alias["underlying"]
            alias_underlying[c_name] = registry.get(underlying) or primitives.get(
                underlying, underlying
            )

    def canonicalize(token: str) -> str:
        seen: set[str] = set()
        while token in alias_underlying and token not in seen:
            seen.add(token)
            token = alias_underlying[token]
        return token

    return canonicalize


def _normalize(text: str, canonicalize) -> str:
    """Collapse whitespace, tighten spacing around punctuation, canonicalize type tokens."""
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s*([*(),])\s*", r"\1", text)
    return re.sub(r"[A-Za-z_]\w*", lambda m: canonicalize(m.group(0)), text)


def _split_params(param_list: str) -> list[str]:
    """Split a parameter list on top-level commas, respecting nested parentheses."""
    parts: list[str] = []
    depth = 0
    current = ""
    for ch in param_list:
        if ch == "(":
            depth += 1
            current += ch
        elif ch == ")":
            depth -= 1
            current += ch
        elif ch == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += ch
    if current.strip():
        parts.append(current)
    return [p.strip() for p in parts]


def _param_type(param: str) -> str:
    """Strip the parameter name, leaving its type (handles function-pointer params).

    Assumes every parameter is named, which holds for this API surface; an unnamed
    multi-token by-value type would lose its last token. The spec side never runs
    this (it renders types directly), so any mismatch here is a false positive only.
    """
    param = param.strip()
    ptr = re.search(r"\(\s*\*\s*([A-Za-z_]\w*)\s*\)", param)
    if ptr:
        return param[: ptr.start()] + "(*)" + param[ptr.end() :]
    named = re.match(r"(.*?)([A-Za-z_]\w*)\s*$", param)
    if named and named.group(1).strip():
        return named.group(1).strip()
    return param


def _member_types(signature: str) -> tuple[str, list[str]]:
    """Parse a member signature into (return type, ordered parameter types)."""
    name = re.search(r"\(\s*\*\s*[A-Za-z_]\w*\s*\)", signature)
    if not name:
        raise ValueError(f"cannot parse a member name from: {signature!r}")
    return_type = signature[: name.start()].strip()
    rest = signature[name.end() :].lstrip()
    depth = 0
    param_list = ""
    for idx, ch in enumerate(rest):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                param_list = rest[1:idx]
                break
    params = _split_params(param_list)
    if params == ["void"] or not params:
        return return_type, ["void"] if params == ["void"] else []
    return return_type, [_param_type(p) for p in params]


def _spec_types(func: CFunction) -> tuple[str, list[str]]:
    """The return type and ordered parameter types the spec declares for a function."""
    if not func.parameters:
        return func.return_c, ["void"]
    return func.return_c, [p.c_decl for p in func.parameters.values()]


def _render_decl(name: str, func: CFunction) -> str:
    """Render a function-pointer struct member declaration for a spec function."""
    if func.parameters:
        params = ", ".join(f"{p.c_decl} {p.name}" for p in func.parameters.values())
    else:
        params = "void"
    return f"{func.return_c} (*{name})({params})"


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------


def _fill_markers(text: str, struct_block: str, define_block: str) -> str:
    """Replace the content between the two append marker pairs (struct first, defines second)."""
    pattern = re.compile(
        rf"([ \t]*{re.escape(_BEGIN)}\n)(.*?)([ \t]*{re.escape(_END)})", re.DOTALL
    )
    blocks = iter([struct_block, define_block])

    def replace(match: re.Match) -> str:
        return match.group(1) + next(blocks) + match.group(3)

    filled, count = pattern.subn(replace, text)
    if count != 2:
        raise ValueError(f"expected exactly 2 append marker pairs, found {count}")
    return filled


def generate(
    modules: list[dict],
    metadata: dict,
    output_path: Path,
    template: Path | None = None,
    internal_out: Path | None = None,
    invocation: str | None = None,
) -> None:
    """Verify the template struct against the spec, then write both lockstep headers."""
    if template is None:
        raise ValueError("extension_header adapter requires --template")
    if internal_out is None:
        raise ValueError("extension_header adapter requires --internal-out")

    opts = metadata.get("options", {}).get("extension", {})
    unstable_guard = opts["unstable_guard"]
    create_method = opts["create_method"]
    api_version = opts["api_version"]
    version_macro_prefix = opts["version_macro_prefix"]
    internal_include = opts["internal_include"]
    exclude = set(opts.get("exclude", []))
    prefix = metadata.get("prefix", "")

    template_text = Path(template).read_text()
    typename, regions = _extract_struct(template_text)
    api_var, define_names = _extract_defines(template_text)

    member_list = [m.name for r in regions for m in r.members]
    duplicates = sorted({n for n in member_list if member_list.count(n) > 1})
    if duplicates:
        raise ValueError(f"struct member(s) declared more than once: {duplicates}")

    # Struct members and define mappings must be a name-for-name bijection.
    struct_names = set(member_list)
    define_set = set(define_names)
    members_without_define = sorted(struct_names - define_set)
    defines_without_member = sorted(define_set - struct_names)
    if members_without_define:
        raise ValueError(
            f"struct members without a define mapping: {members_without_define}"
        )
    if defines_without_member:
        raise ValueError(
            f"define mappings without a struct member: {defines_without_member}"
        )

    render_modules = resolve_modules(modules, metadata)
    func_by_name: dict[str, CFunction] = {}
    for mod in render_modules:
        func_by_name.update(mod.functions)

    # Verify each template member against the spec.
    canonicalize = _build_canonicalizer(modules, metadata)
    for region in regions:
        for member in region.members:
            func = func_by_name.get(member.name)
            if func is None:
                raise ValueError(
                    f"template member '{member.name}' is not a declared spec function"
                )
            if func.static_inline:
                raise ValueError(
                    f"template member '{member.name}' resolves to a static_inline "
                    "function, which has no vtable symbol"
                )
            t_ret, t_params = _member_types(member.signature)
            s_ret, s_params = _spec_types(func)
            t_norm = (
                _normalize(t_ret, canonicalize),
                [_normalize(p, canonicalize) for p in t_params],
            )
            s_norm = (
                _normalize(s_ret, canonicalize),
                [_normalize(p, canonicalize) for p in s_params],
            )
            if t_norm != s_norm:
                spec_sig = _render_decl(member.name, func)
                raise ValueError(
                    f"signature mismatch for '{member.name}':\n"
                    f"  template: {member.signature}\n"
                    f"  spec:     {spec_sig}"
                )

    # Append spec functions absent from the template, in deterministic loader order.
    appended: list[tuple[str, CFunction]] = []
    for mod in render_modules:
        for name, func in mod.functions.items():
            if name in struct_names or func.static_inline:
                continue
            bare = name[len(prefix) :] if name.startswith(prefix) else name
            if bare in exclude or name in exclude:
                continue
            appended.append((name, func))

    # Consumer header: fill the append markers (struct region is guarded).
    if appended:
        struct_lines = [f"#ifdef {unstable_guard}"]
        struct_lines += [f"\t{_render_decl(n, f)};" for n, f in appended]
        struct_lines.append("#endif")
        struct_block = "\n".join(struct_lines) + "\n"
        define_block = (
            "\n".join(f"#define {n} {api_var}.{n}" for n, _ in appended) + "\n"
        )
    else:
        struct_block = ""
        define_block = ""
    consumer = _fill_markers(template_text, struct_block, define_block)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(consumer)

    # Engine-side header: derived from the extracted order plus appends.
    major, minor, patch = api_version.lstrip("v").split(".")
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATES_DIR)),
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
        undefined=StrictUndefined,
    )
    internal_regions = [
        {
            "comment": f"// v{r.version}"
            if r.kind == "stable"
            else f"// {r.description}",
            "gap": r.kind != "stable",
            "members": [m.signature for m in r.members],
        }
        for r in regions
    ]
    all_names = [m.name for r in regions for m in r.members] + [n for n, _ in appended]
    internal = env.get_template("internal.hpp.j2").render(
        include=internal_include,
        typename=typename,
        create_method=create_method,
        version_macro_prefix=version_macro_prefix,
        major=major,
        minor=minor,
        patch=patch,
        regions=internal_regions,
        appended=[_render_decl(n, f) for n, f in appended],
        all_names=all_names,
    )
    internal_out = Path(internal_out)
    internal_out.parent.mkdir(parents=True, exist_ok=True)
    internal_out.write_text(internal)

    print(
        f"Generated {output_path} and {internal_out} "
        f"({len(member_list) + len(appended)} members, {len(appended)} appended)"
    )
