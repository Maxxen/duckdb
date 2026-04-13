# capigen

IDL schema and code generator for the DuckDB C API v2.

There are four distinct things in play:

- **The IDL schema** (`src/capigen/schema/`) — JSON Schema files defining what a valid API spec looks like: what 
  constructs exist (types, functions, enums, ...), what fields they have, and what values are allowed. This is the 
  grammar. See [schema_reference.md](schema_reference.md) for a complete reference.
- **The API spec** (`api_spec/`) — YAML files describing the actual DuckDB C API surface. This is the content. It will live in duckdb core, versioned with DuckDB releases.
- **capigen** (`src/capigen/`) — a tool that validates the spec against the schema and dispatches to a pluggable adapter for code generation.
- **Adapters** (`src/capigen/adapters/`) — pluggable code generators, one per target language. Each adapter reads validated spec dicts and produces output. capigen ships with a C adapter (`adapters/c/`) that produces `duckdb_v2.h`; additional adapters (Go, CPython, etc.) can be built-in or external Python modules.

The schema, capigen, and adapters are tightly coupled and versioned together — a schema change (new construct, new field) requires updating capigen and all adapters. The spec is independent — adding a function to the API only changes YAML files.

## Usage

```bash
uv run capigen c -o duckdb_v2.h
```

By default, capigen looks for the API spec in `./api_spec/`. Use `--spec-dir` to point at a different location:

```bash
uv run capigen c -o duckdb_v2.h --spec-dir /path/to/duckdb/capi/api_spec
```

To use an external adapter:

```bash
uv run capigen my_package.my_adapter -o output.rs
```

The adapter name is resolved as a built-in (`capigen.adapters.<name>`) first, then as a full module path.

## Project layout

```
api_spec/                       # API spec (YAML) — will move to duckdb core
  metadata.yaml                 # primitive type vocabulary, API versions, schema_version
  v2/                           # module definitions
    common/common.yaml
    database/database.yaml
    ...
src/capigen/                    # the generator tool
  schema/                       # IDL schema (JSON Schema) — versioned with capigen
    metadata.schema.json
    module.schema.json
  loader.py                     # YAML loading + JSON Schema validation
  validate.py                   # cross-module referential integrity checks
  adapters/c/                   # C adapter (resolve, render, templates)
tests/                          # pytest suite
duckdb_v2.h                     # generated reference output
```

## How it works

1. **Load** — read the API spec (YAML), validate each file against the IDL schema (JSON Schema), fill omitted fields from schema-declared defaults
2. **Cross-module validation** — check referential integrity across modules (type references resolve, versions exist in metadata). This is the one thing JSON Schema can't express, since it validates one file at a time.
3. **Adapter** — resolve spec dicts into language-specific render objects, then render templates
4. **Write** — write the generated file

Steps 1-2 are language-agnostic. Steps 3-4 are handled entirely by the adapter.

## Versioning

The API spec declares which schema version it was written against:

```yaml
# metadata.yaml
schema_version: "0.1.0"
```

This connects two independent version streams:

| What changed                     | Schema version bump? | capigen release? | Spec change? |
|----------------------------------|---|---|---|
| New API module, function or type | No | No | Yes |
| New field on existing construct  | Yes (minor) | Yes | Yes (uses new field) |
| New construct (e.g. `unions`)    | Yes (major) | Yes | Yes (uses new construct) |
| Bug fix in capigen rendering     | No | Yes | No |

The spec lives in duckdb core and is versioned with DuckDB releases. The schema and capigen are versioned together in this repo. The `schema_version` field is the bridge — it lets CI verify that the spec and the tool are compatible.

## Writing an adapter

An adapter is a Python module that exposes:

```python
def generate(modules: list[dict], metadata: dict, output_path: Path) -> None
```

`modules` is a list of validated spec dicts (with defaults applied). `metadata` contains `primitives` (with C ABI type names), `suffixes` (naming conventions per construct type), `versions`, and `schema_version`. The adapter uses this to resolve types and compute canonical names.

Built-in adapters live under `capigen.adapters`. External adapters are any importable Python module with a `generate` function.

## C adapter

The built-in C adapter lives in `src/capigen/adapters/c/` and has three layers:

```
adapters/c/
  __init__.py    # entry point: wires resolve -> Jinja2 -> file
  resolve.py     # bridges spec dicts to C render objects
  render.py      # dataclass definitions for the template layer
  templates/     # Jinja2 templates that produce C code
```

### Architecture

The adapter enforces a strict boundary between spec concepts and C output:

- **`render.py`** defines typed dataclasses (`CModule`, `CFunction`, `CTypeDef`, etc.) that represent C-language concepts only. There is no `direction`, `role`, or `underlying` here — only things like `base` type strings, pointer indirection levels, `const` qualifiers, and pre-formatted C declarations.

- **`resolve.py`** is the bridge. It reads spec dicts, resolves abstract type names to concrete C type strings using a registry, and emits render objects. This is the only file that understands both the spec structure and C semantics.

- **`templates/`** consume render objects and produce C source text. Templates never see spec concepts — they only access attributes defined in `render.py`.

### Render objects

The render layer (`render.py`) defines these types:

| Class | Purpose |
|-------|---------|
| `CModule` | Top-level container: one per spec module |
| `CTypeDef` | `typedef` alias (`typedef void* duckdb_connection_ptr;`) |
| `CStruct` | `typedef struct { ... }` with optional pointer alias |
| `CField` | A struct field (base type + pointer + const) |
| `CEnum` | `typedef enum { ... }` with auto-numbered values |
| `CEnumValue` | Single enum entry (value + description) |
| `CConstant` | `#define NAME value` |
| `CErrorGroup` | Error category (group_id bits + entries) |
| `CErrorEntry` | Single error code entry |
| `CFuncPtr` | `typedef ret (*name)(params);` |
| `CFuncPtrParam` | Function pointer parameter (base + pointer + const) |
| `CFunction` | API function declaration |
| `CParam` | Function parameter with pre-formatted `c_decl` string |

### Type resolution

`resolve.py` builds a registry that maps every declared type name to its C name. Suffixes come from `metadata.suffixes` (e.g. handles → `_ptr`). The resolution order is:

1. Check primitives from `metadata.primitives` (e.g. `u64` → `uint64_t` via the `c_type` field)
2. Check the registry (handles, callbacks, aliases, structs, enums registered with their canonical C names)
3. Raise `ValueError` if unknown

The registry is built once from all modules before any per-module resolution happens. This means types declared in one module (e.g. `common.yaml`) are available to all other modules.

### Changing the C adapter when the schema changes

When the JSON Schema gains a new construct or an existing construct changes shape, the C adapter needs to be updated:

1. **Update the schema** — add or modify definitions in `module.schema.json` (and `metadata.schema.json` if relevant).
2. **Update `render.py`** — add or modify dataclasses for the new C output. Ask: "what does this look like in a C header?"
3. **Update `resolve.py`** — add a `_resolve_*` function that reads spec dicts and produces render objects. If the new construct introduces referenceable types, register them in `_build_registry`.
4. **Update templates** — add or modify Jinja2 templates to render the new dataclass.
5. **Bump `schema_version`** in `api_spec/metadata.yaml`.
6. **Verify** — `just run` and `just test`.

#### Example: adding a new construct

Suppose the schema adds `unions`. The steps: add `UnionDefinition` to the schema, add `CUnion` to `render.py`, add `_resolve_union()` to `resolve.py`, add a `_union.j2` template.

#### Example: adding a field to an existing construct

Suppose functions gain an optional `since` field. The steps: add `since` with a default to the Function definition in the schema, add `since: str | None` to `CFunction`, pass it through in `_resolve_function()`, use it in the template.

## Development

This project uses [just](https://github.com/casey/just) as a command runner. Install it with:

```bash
uv tool install rust-just
```

Then:

```bash
just run        # generate duckdb_v2.h
just watch      # re-run on any .yaml/.py/.j2 change
just test       # run the test suite
just check      # lint (ruff) + type check (ty)
just format     # auto-format with ruff
```

### Pre-commit hooks (optional)

To run formatting, linting, and type checking automatically on each commit:

```bash
uv sync --group dev
uv run pre-commit install
```

### CI

GitHub Actions runs `just check` and `just test` on every push to `main` and on pull requests.
