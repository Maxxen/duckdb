# Editing the API spec

This directory holds the canonical API definitions in YAML. `v2/` is the focus of the work; `v1/` is a declarative reconstruction of the existing V1 surface. The public C headers and the bridge stubs are **generated** from these files by capigen, so a spec edit is not complete until the generated output is regenerated and committed.

Full design rationale for everything below: `api_spec/C_API_V2.md`. Build, regeneration, and CI mechanics: the "C API V2 development" section of `CONTRIBUTING.md`.

## The regeneration contract

- The header (`src/include/duckdb_v2.h`) and stub skeleton (`src/main/capi_v2/capi_v2_stubs.cpp`) are committed. After editing any `v2/**/*.yaml`, run `./scripts/capi_v2_regen.sh` (V1 spec edits use `./scripts/capi_v1_regen.sh`).
- The `capi-v2-regen` / `capi-v1-regen` pre-commit hooks do this automatically when you stage a spec file, so committing without a manual run also works: the hook regenerates, the format hook reformats, pre-commit asks you to re-stage.
- Forgetting to regenerate shows up as drift in `git status` and fails the CI `git diff --exit-code` check.

## A function declaration

Each module file defines handles, types, enums, and functions. Example:

```yaml
functions:
  duckdb_v2_option_create:
    summary: "Creates an option handle carrying a name and a setting."
    role: constructor
    belongs_to: duckdb_v2_option
    parameters:
      name:
        type: char
        indirection: 1
        const: true
        description: "Null-terminated option name."
      out_option:
        type: duckdb_v2_option
        indirection: 1
        kind: OUT
        description: "Receives the new option handle."
      err:
        type: duckdb_v2_error_info
        indirection: 1
        kind: OUT
        description: "Optional. On failure, receives an opaque info handle the caller must destroy via duckdb_v2_error_info_destroy."
    return_type: status
```

## Authoring conventions

- **Names.** Everything is `duckdb_v2_` / `DUCKDB_V2_` prefixed, but the IDL is prefix-free: `prefix:` in `metadata.yaml` applies it at generation time. Do not bake the prefix into type or function names. The generator refuses to generate if the convention is violated.
- **Function roles.** Set `role` from behavior: `constructor`, `destructor`, `getter`, `setter`, or `method`. Destructors are infallible and take no `err`.
- **Parameter order.** Primary subject, then inputs, then `out_*`, then the trailing `err`. Every fallible function returns the `DUCKDB_V2_ERROR` enum and takes `err` last (`kind: OUT`, `indirection: 1`). See the error-slot contract in the reference doc.
- **Shared handles** are declared once in `common/common.yaml`, never redeclared per module. Use `qualified: true` for names owned elsewhere (`idx_t`, `sel_t`).
- **Lexical style.** `Connection` -> `conn`, `Callback` -> `cb`, `Statement` -> `stmt`, `Execution` -> `exec`, `Destroy` -> `destroy`, `Begin` / `End` -> `begin_...` / `end_...`. String data is `type: char, indirection: 1`.
- **Descriptions.** One description per function, leading with the contract. Annotate enum values only where the name alone is insufficient. No forward references to in-flight work (it ages badly). No first-person editorialising ("at this moment", "for now"): state the contract, and put deferral rationale in the file's top-of-file commentary. For string types write "null-terminated byte string", not "UTF-8" (DuckDB does not enforce encoding).
- **Borrowed vs owned out-params** use those exact words. Borrowed string out-params return `NULL` for "no value" (not `""`); they are null-terminated and carry a length.
- **Numeric enum-id round-trip.** V2 enum values stay numerically identical to their internal counterparts (`duckdb::LogicalTypeId`, `PhysicalType`, ...). If a new internal variant is added, add a matching id in the same change, or the bridge cast silently produces an undefined enum value.

## Use the schema, don't hand-roll

- `description:` on `handles` / `aliases` / `structs` renders as `//!` comments. Put per-handle docs there, not in the function descriptions that mention them.
- `tagged_struct` handle style is the default (`options.c.handles.default_style`); `override_style` opts an individual handle back to `void *`. Handle typedefs carry the `_handle` suffix.
- `qualified` on an alias references an external type name unchanged (no prefix, no `_t` suffix).
- The IDL field reference and spec-language features live in the capigen repository (`schema_reference.md` and its `CLAUDE.md`).
