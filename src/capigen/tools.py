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
            raise ValueError(
                f"Cycle detected among handles: {sorted(remaining)}"
            )
        for h in ready:
            result.append(h)
            del remaining[h]
        for d in remaining.values():
            d.difference_update(ready)

    return result


__all__ = ["handle_dependencies", "topo_sort_handles"]
