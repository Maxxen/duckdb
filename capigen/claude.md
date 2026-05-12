# API Conventions

This document is the authoritative guide for DuckDB C API v2 spec definitions
(`api_spec/v2/*/*.yaml`).

The goal is:
- deterministic, scriptable transformation
- maintain API compatibility semantics
- enforce new versioned conventions and types
- make it straightforward for another LLM to perform the migration automatically

## Project structure

```
api_spec/                      # API spec (YAML) — will move to duckdb core
  metadata.yaml           # suffixes, primitives (with c_type), versions, schema_version
  v2/                     # module YAML files
src/capigen/
  schema/                 # IDL schema (JSON Schema) — versioned with capigen
  __main__.py             # CLI: uv run capigen c -o duckdb_v2.h
  loader.py               # YAML loading + JSON Schema validation
  validate.py             # cross-module referential integrity checks
  adapters/c/
    resolve.py            # spec → C type resolution (includes primitive mapping)
    render.py             # C render dataclasses (template contract)
    templates/            # Jinja2 templates
tests/                    # pytest suite
```

The spec (`api_spec/`) and capigen (`src/capigen/`) are versioned independently.
`metadata.yaml` declares a `schema_version` that bridges the two.

## Module constructs

Each module YAML file can contain these top-level sections:

| Construct | Purpose |
|---|---|
| `handles` | Opaque handle types (always `_ptr` in C) |
| `callbacks` | Callback function pointer typedefs (always `_cb` in C) |
| `aliases` | Named type aliases (always `_t` in C) |
| `structs` | Structs with consumer-visible fields |
| `enums` | Enumerations with auto-numbered values |
| `constants` | Named compile-time values |
| `error_groups` | Hierarchical error codes: `(group_id << 16) | code` |
| `functions` | API function declarations |

## Core rules

1. Every function must return an error code as the first-level return type.
   - legacy `duckdb_state`, `duckdb_error` map to `DUCKDB_V2_API_CALL` in v2.

2. **Every fallible function takes a `duckdb_v2_error_info *err` as its LAST parameter** (with `kind: OUT`, `indirection: 1`, i.e. a pointer-to-handle out-parameter).
   - `duckdb_v2_error_info` is an opaque handle declared in `api_spec/v2/common/common.yaml`. On failure the library allocates an info and writes its pointer into `*err`; the caller owns it.
   - Contract: the return value always carries the error code and is authoritative. `err` is optional — callers may pass `nullptr` to opt out of detail. On success the library leaves `*err == nullptr`. On failure, if `err != nullptr`, the library allocates an info and stores it in `*err`; the caller destroys it with `duckdb_v2_error_info_destroy`. If `*err` is already non-null on entry, the library destroys the previous info before writing a new one — callers who want to preserve info across calls must detach first.
   - Canonical `err` parameter description in YAML: `"Optional. On failure, receives an opaque info handle the caller must destroy via duckdb_v2_error_info_destroy."`
   - Implementations must tolerate `err == nullptr` on every path — use the `SetErrorInfo` / `ClearErrorInfo` helpers in `capi_v2_internal.hpp`, which already guard on null and manage replace-and-free semantics.
   - There is no longer a context handle. The first parameter is the primary subject of the call (the object being operated on) if any; otherwise skip straight to the arguments.

3. For functions that need to return data, use an `out` pointer parameter before the trailing `err` parameter.
   - Example: `duckdb_logical_type *out_type` becomes `out_type` typed pointer with `kind: OUT`.
   - Canonical order: primary subject → inputs → `out_*` → `err`.

4. Type naming conventions (determined by construct type, not a `kind` field):
   - `handles` → suffix `_ptr`
   - `callbacks` → suffix `_cb`
   - `aliases` → suffix `_t`
   - keep old function names exactly (backward compatibility) unless renamed by policy.

5. Parameter conventions:
   - pointer argument: `type: char`, `indirection: 1`, for input strings.
   - `kind` declares both direction and ownership. One of: `IN` (default; input, caller keeps ownership), `IN_TRANSFER` (input, callee takes ownership), `OUT` (output, caller must free/destroy), `OUT_BORROW` (output, callee retains; caller views). `OUT` / `OUT_BORROW` require `indirection >= 1`. See `schema_reference.md` for the full matrix.

## Module conversion mapping

### Typical module name handling
- `appender` group → `api_spec/v2/appender/appender.yaml`
- `scalar` group → `api_spec/v2/scalar/scalar.yaml`
- `common`/shared types → `api_spec/v2/common/common.yaml`

## New v2 form details

### handles
- declare shared opaque handles in `common/common.yaml` once.
- avoid duplicates: if a handle exists in common, do not redeclare in other modules.
- module-specific handles go in the module file.

### aliases
- use for named type aliases: `duckdb_error_code: {underlying: u32}`
- can alias primitives or other declared types.

### functions
- Each legacy function becomes a `functions` entry.
- Maintain `role` from behavior:
  - `constructor` for `create` family
  - `destructor` for `destroy`
  - `getter` for `column_count`, `column_type`, `error_data`, etc.
  - `setter/method` for mutating operations
- Do not port deprecated APIs; create only active API surface in v2.

### Return value cross-assignments
- Prefer `DUCKDB_API_CALL` return with `out` parameters for values:
  - `duckdb_appender_column_count`: provide `out_column_count` as `idx` with `kind: OUT`
  - `duckdb_appender_column_type`: provide `out_column_type` as `duckdb_logical_type` with `kind: OUT`

### error_groups
- Error codes are 32-bit integers: `(group_id << 16) | code`
- `group_id` is the upper 16 bits (error category)
- `code` is the lower 16 bits (error within category)

### primitives in metadata.yaml
Primitives define the type vocabulary with their C ABI names (`c_type`).
Ensure there are entries for: opaque (void), bool, char, i8-i64, u8-u64, f32/f64, idx (idx_t)

### common handles in common/common.yaml
- duckdb_connection, duckdb_data_chunk, duckdb_vector, duckdb_logical_type, duckdb_value

> Errors are reported via the `duckdb_v2_error` struct passed as the trailing `err` parameter to every fallible function. Do not create `duckdb_v2_error` as a handle or define getter/setter functions for it — it is a plain struct that the caller stack-allocates.

## Validation/sanity pipeline

1. add/ensure primitives in `api_spec/metadata.yaml`.
2. add shared handles in `api_spec/v2/common/common.yaml`.
3. add module YAML under `api_spec/v2/<group>/<module>.yaml`.
4. run generator:
   - `just run` (or `uv run capigen c -o duckdb_v2.h`)
5. handle errors:
   - JSON Schema validation errors → invalid field or value in YAML
   - `Type name 'X' is duplicated` → remove duplicate type declarations
   - `unknown type 'X'` → add to common or correct existing declarations.
6. run tests:
   - `just test`

## Helpful naming rubric

- appender handle → `duckdb_appender` (in `handles`, renders as `duckdb_appender_ptr`)
- scalar handles → `duckdb_ctx`, `duckdb_data_chunk`, etc.
- all string data: `type: char`, `indirection: 1`.

## Extra LLM guidance

- avoid baseless renaming unless requested by status or policy.
- preserve previous semantics and extant API surface.
- YAML modules are validated against `src/capigen/schema/module.schema.json`.
- only include fields supported by schema: `module`, `handles`, `callbacks`, `aliases`, `structs`, `enums`, `constants`, `error_groups`, `functions`.

## API lexical style guide

- `Connection` → `conn`
- `Callback` → `cb`
- `Statement` → `stmt`
- `Execution` → `exec`
- `Error` → `error`, `error_data`...
- `Destroy` → `destroy`
- `Begin/End` → `begin_...`, `end_...` as in `appender_begin_row`, `appender_end_row`
