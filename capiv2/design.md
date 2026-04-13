# capigen Design

## Concepts

There are four things:

| Concept | What it is | Where it lives | Versioned with |
|---|---|---|---|
| **IDL schema** | JSON Schema files defining the structure of the API spec — what constructs exist, their fields, allowed values | `src/capigen/schema/` | capigen |
| **API spec** | YAML files describing the actual DuckDB C API — types, functions, enums, etc. | `api_spec/` (will move to duckdb core) | DuckDB releases |
| **capigen** | Tool that validates the spec against the schema and dispatches to an adapter | `src/capigen/` | capigen |
| **Adapters** | Pluggable code generators, one per target language | `src/capigen/adapters/` | capigen |

The schema, capigen, and adapters are tightly coupled. A schema change requires updating capigen and all adapters. The spec is independent — adding a function to the API only changes YAML files.

The spec declares which schema version it was written against via `schema_version` in `metadata.yaml`. This lets CI verify compatibility when the spec and the tool live in different repos.

## IDL schema

### What it can express today

The schema defines seven constructs that can appear in a module YAML file:

| Construct | Semantics | C output |
|---|---|---|
| **handles** | Opaque handle type. Consumers don't know the internal structure. | `typedef void* duckdb_connection_ptr;` |
| **callbacks** | Callback function pointer: return type + typed parameter list. | `typedef void (*duckdb_delete_callback_cb)(void *data);` |
| **aliases** | Named type alias for a primitive or declared type. | `typedef uint32_t duckdb_error_code_t;` |
| **structs** | Composite type with consumer-visible, ordered fields. | `typedef struct { int32_t days; } duckdb_date;` |
| **enums** | Enumeration with named, auto-numbered integer values. | `typedef enum { DUCKDB_TYPE_INVALID = 0, ... } DUCKDB_TYPE;` |
| **constants** | Named compile-time value (integer or string). | `#define DUCKDB_API_ERROR 0xFFFFFFFF` |
| **error_groups** | Hierarchical error codes: `(group_id << 16) \| code`. | `#define DUCKDB_ERROR_IO_FILE_NOT_FOUND ((1 << 16) \| 1)` |
| **functions** | API function: return type, typed parameters with direction, version tracking, semantic role. | `DUCKDB_C_API duckdb_error_code_t duckdb_open(...);` |

Five of these (handles, callbacks, aliases, structs, enums) produce `typedef` in C but are separate constructs in the schema because they're semantically different. An adapter for another language would emit entirely different constructs for each. The naming suffix is implicit: handles are always `_ptr`, callbacks always `_cb`, aliases always `_t`. There is no `kind` field — the construct *is* the kind.

Functions have roles (`constructor`, `destructor`, `getter`, `setter`, `method`), version tracking (`added`, `deprecated`), and typed parameters with direction (`in`/`out`), pointer indirection, and const qualification.

The metadata system (`metadata.yaml`) defines:
- **Suffixes** — ABI naming conventions per construct type (`handles` → `_ptr`, `callbacks` → `_cb`, `aliases` → `_t`). Every adapter reads these to compute canonical names.
- **Primitives** — abstract type names with their C ABI type names (`u32` → `uint32_t`). Adapters use the `c_type` field for type resolution.
- **Versions** — known API versions for validating `added`/`deprecated` fields

### Gaps relative to duckdb.h v1

The current v1 header (`duckdb.h`) contains ~545 functions across ~6200 lines. Most constructs map cleanly to the schema. The following do not:

| v1 construct | Example | Schema support | Impact |
|---|---|---|---|
| **Unions** | `duckdb_string_t` has a union with nested structs and fixed-size arrays | Not supported | Needed for string representation; could be added as a schema construct or handled as a special-case struct |
| **Fixed-size arrays in structs** | `char prefix[4]`, `char inlined[12]` | Not supported (struct fields only have `pointer` indirection) | Needed inside the string union; rare elsewhere |
| **Forward declarations** | `struct ArrowArray`, `struct ArrowSchema` | Not supported | Arrow C Data Interface types; could be handled as opaque types or a dedicated construct |
| **Function pointers as struct fields** | `duckdb_extension_access` struct contains callback members | Partially supported (struct fields have a type, but no way to inline a callback signature) | Can reference a declared callback type; the struct field just uses that type |
| **Conditional compilation** | `#ifndef DUCKDB_API_NO_DEPRECATED` blocks | Not supported | v2 handles deprecation differently (version-tracked functions, no deprecated functions in new API surface) |
| **Parameterized macros** | None in v1 (only simple `#define` constants) | N/A | Not needed |

**Assessment:** The schema covers the vast majority of the v1 API surface. The main gap is **unions** (one occurrence in v1, but it's a core type). Fixed-size arrays and forward declarations are minor and could be added incrementally. The deprecation model is intentionally different in v2 — deprecated v1 functions are not ported; only the active API surface is expressed.

### v2 API conventions (changes from v1)

The v2 API enforces conventions that simplify the spec:

- Every function returns an error code (`DUCKDB_API_CALL`). v1 mixed return types (`duckdb_state`, direct values, pointers).
- First parameter is always a context handle (`duckdb_context_t`). v1 had no consistent pattern.
- Output values use `out` pointer parameters. v1 returned values directly from some functions.
- Error context lives on the context handle. v1 had separate error handle types (`duckdb_error_data`).
- Deprecated functions are not ported. Only the active API surface exists in v2.

These conventions mean the schema doesn't need to express every pattern present in v1 — only the patterns used in v2.

## Generator design

### Pipeline

```
                    ┌─────────────────────────────────────┐
                    │           capigen (core)             │
                    │                                     │
 YAML spec ──────> │  1. Load + validate against schema   │
                    │  2. Apply schema defaults            │
                    │  3. Cross-module integrity checks    │
                    │                                     │
                    │         validated spec dicts         │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │        adapter (per language)        │
                    │                                     │
                    │  4. Build type registry              │
                    │  5. Resolve spec → render objects    │
                    │  6. Render templates → output file   │
                    │                                     │
                    └─────────────────────────────────────┘
```

Steps 1-3 are language-agnostic and live in capigen's core. Steps 4-6 are adapter-specific.

### Why JSON Schema as source of truth

The IDL schema is expressed as JSON Schema files rather than Python code (e.g. Pydantic models). Rationale:

- **Language-agnostic.** The schema can be consumed by any toolchain — YAML editor validation, CI linters, non-Python generators. Pydantic models are Python-specific.
- **Single source of truth.** The schema defines structure, defaults, and constraints in one place. There's no second model that must stay in sync.
- **IDE support.** YAML files reference the schema via `# yaml-language-server: $schema=...`, giving autocompletion and inline validation while editing the spec.

### Why cross-module validation exists in capigen

JSON Schema validates one file at a time. It cannot check:
- A type referenced in module B is declared in module A
- A `kind` value matches one defined in `metadata.yaml`
- An `added` version string appears in the known versions list
- No two modules declare the same type name

These are cross-module referential integrity checks. They live in `validate.py` (~120 lines) and run after schema validation but before the adapter. Every adapter would need these checks, so they belong in the core.

### Adapters

An adapter is a Python module with one function:

```python
def generate(modules: list[dict], metadata: dict, output_path: Path) -> None
```

It receives validated spec dicts (with defaults applied) and produces output. It never touches the JSON Schema — the schema shaped the dicts, but the adapter only sees the result.

Adapters are pluggable. capigen resolves the adapter name as a built-in (`capigen.adapters.<name>`) first, then as an external Python module path:

```bash
capigen c -o duckdb_v2.h              # built-in C adapter
capigen my_package.go_adapter -o out   # external adapter
```

### C adapter architecture

The built-in C adapter enforces a strict boundary between spec concepts and C output:

```
spec dicts ──> resolve.py ──> render objects ──> templates ──> C code
                  │                │
         reads spec dicts    typed dataclasses
         + type registry     (CFunction, CTypeDef, ...)
```

- **`resolve.py`** is the only file that reads spec dicts. It builds a type registry (mapping abstract type names to C names), resolves all references, and emits typed render objects.
- **`render.py`** defines dataclasses (`CModule`, `CFunction`, `CTypeDef`, `CStruct`, etc.) that represent C-language concepts only. No `kind`, `direction`, `role`, or `underlying` — just base types, pointer levels, const qualifiers, and formatted declarations.
- **`templates/`** are Jinja2 files that consume render objects and produce C source text. They never see spec concepts.

This separation means:
- Template authors don't need to understand the spec format
- Render objects are the documented contract between resolution and rendering
- Adding a new spec construct requires updating `resolve.py` and `render.py`, but templates only change if the C output changes

### Why adapters belong in the capigen repo (for now)

Adapters are coupled to the schema — when the schema adds a construct, every adapter must be updated to handle it. Keeping adapters alongside the schema and capigen means a schema change is one PR that updates everything.

When the adapter set grows, it may make sense to split them out. But as long as there are only 1-3 adapters, co-location keeps things simple. External adapters (installed as separate Python packages) are supported for cases where this doesn't apply.

## Repo layout (target state)

```
duckdb/duckdb (core)
├── capi/
│   ├── metadata.yaml        ← the API spec (source of truth)
│   ├── v2/**/*.yaml          ← module definitions
│   └── duckdb_v2.h           ← generated, checked in
└── CI: capigen c -o /tmp/h --spec-dir capi/ && diff

duckdb/duckdb-capi (this repo)
├── src/capigen/
│   ├── schema/              ← IDL schema (JSON Schema)
│   ├── loader.py, validate.py
│   └── adapters/c/          ← C adapter
├── api_spec/                ← dev copy of the spec (mirrors core)
├── tests/
└── tagged releases
```

The spec lives in core because it's versioned with the API. capigen is a standalone tool that operates on any spec directory via `--spec-dir`. Consumer repos (duckdb-go, duckdb-python) run capigen against core's spec to generate their bindings.
