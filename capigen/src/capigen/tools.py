"""Reusable utilities for adapters.

capigen itself does not consume these; they are provided for adapters that
need to reason about the spec beyond what `resolve.py`-style translation
gives them (e.g. for emission ordering, dependency analysis).
"""


def handle_dependencies(modules: list[dict]) -> dict[str, set[str]]:
    """Return the handle dependency graph.

    A handle X is said to depend on handle Y if any function with
    ``belongs_to: X`` references Y via a parameter type or return type
    (excluding self-references to X).

    Every handle appears as a key, even if it has no dependencies. The
    returned sets contain only handle names (not primitives or aliases).
    """
    handles: set[str] = set()
    for mod in modules:
        handles.update(mod.get("handles", {}).keys())

    deps: dict[str, set[str]] = {h: set() for h in handles}

    for mod in modules:
        for func in mod.get("functions", {}).values():
            owner = func.get("belongs_to")
            if owner not in handles:
                continue

            for param in func.get("parameters", {}).values():
                t = param.get("type")
                if t in handles and t != owner:
                    deps[owner].add(t)

            rt = func.get("return_type")
            if rt in handles and rt != owner:
                deps[owner].add(rt)

    return deps


def topo_sort_handles(modules: list[dict]) -> list[str]:
    """Topologically sort handles so dependencies precede dependents.

    Uses Kahn's algorithm with alphabetical tie-breaking for deterministic
    output. Raises ``ValueError`` on cycles, listing the handles involved.
    """
    deps = handle_dependencies(modules)
    remaining: dict[str, set[str]] = {h: set(d) for h, d in deps.items()}
    result: list[str] = []

    while remaining:
        ready = sorted(h for h, d in remaining.items() if not d)
        if not ready:
            raise ValueError(f"Cycle detected among handles: {sorted(remaining)}")
        for h in ready:
            result.append(h)
            del remaining[h]
        for d in remaining.values():
            d.difference_update(ready)

    return result


def sort_modules_by_deps(modules: list[dict]) -> list[dict]:
    """Sort modules so that type declarations precede cross-module references.

    Builds a module-level dependency graph: module M depends on module N if M
    references a type (handle, alias, struct, enum, or callback) declared in N.
    Uses Kahn's algorithm with alphabetical tie-breaking for determinism.
    Raises ``ValueError`` on cycles.
    """
    decl: dict[str, str] = {}
    for mod in modules:
        mname = mod["module"]
        for section in (
            "handles",
            "aliases",
            "qualified_aliases",
            "structs",
            "enums",
            "callbacks",
        ):
            for name in mod.get(section, {}):
                decl[name] = mname

    def _refs(mod: dict) -> set[str]:
        out: set[str] = set()
        for func in mod.get("functions", {}).values():
            out.add(func.get("return_type", ""))
            for p in func.get("parameters", {}).values():
                out.add(p.get("type", ""))
        for s in mod.get("structs", {}).values():
            for f in s.get("fields", []):
                out.add(f.get("type", ""))
        for cb in mod.get("callbacks", {}).values():
            out.add(cb.get("return_type", ""))
            for p in cb.get("parameters", {}).values():
                out.add(p.get("type", ""))
        for a in mod.get("aliases", {}).values():
            out.add(a.get("underlying", ""))
        return out

    mod_by_name = {mod["module"]: mod for mod in modules}
    deps: dict[str, set[str]] = {mod["module"]: set() for mod in modules}
    for mod in modules:
        mname = mod["module"]
        for tname in _refs(mod):
            declaring = decl.get(tname)
            if declaring and declaring != mname:
                deps[mname].add(declaring)

    remaining = {m: set(d) for m, d in deps.items()}
    result: list[str] = []
    while remaining:
        ready = sorted(m for m, d in remaining.items() if not d)
        if not ready:
            raise ValueError(f"Cycle detected among modules: {sorted(remaining)}")
        for m in ready:
            result.append(m)
            del remaining[m]
        for d in remaining.values():
            d.difference_update(ready)

    return [mod_by_name[m] for m in result]


__all__ = ["handle_dependencies", "topo_sort_handles", "sort_modules_by_deps"]
