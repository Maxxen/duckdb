# C API specification

This directory holds the declarative specification of DuckDB's C APIs. The
committed sources are generated from it:

- `src/include/duckdb_v1.h`, the V1 C API header, re-exported by the
  `src/include/duckdb.h` shim
- `src/include/duckdb_extension.h`, the header for C extensions
- `src/include/duckdb/main/capi/extension_api.hpp`, the engine-side function
  pointer struct
- `src/include/duckdb_v2.h`, the V2 C API header
- `src/main/capi_v2/capi_v2_stubs.cpp`, generated stubs for V2 bridge
  functions not yet implemented

Do not edit those files by hand. Edit the YAML here and regenerate.

## Layout

- `v1/` and `v2/`: one spec tree per API version. One directory per module
  (query, value, appender, ...), plus `metadata.yaml` (prefix, primitives,
  lifecycle states, known DuckDB versions) and `common/` (shared enums,
  structs, callbacks).
- `v1/extension/duckdb_extension.h.in`: the template for the extension
  header. Its member order is the extension ABI. The generator verifies every
  member against the spec and appends new functions at the marker comments.
  `extension_api.hpp` is derived from it, so both stay in lockstep.
- `pyproject.toml`: the generation environment. It pins capigen (the
  generator, https://github.com/duckdb/capigen); the `dev` group adds the
  formatter and pre-commit tooling. `uv.lock` records the exact resolution.

Design reference and authoring conventions: `C_API_V2.md`.

## Regenerating

    ./scripts/capi_v1_regen.sh
    ./scripts/capi_v2_regen.sh

The scripts provision themselves through uv. The pre-commit hooks run them
automatically when spec files change, and CI fails when the committed
sources do not match the spec.

## Adding a function

Declare it in the module YAML with a `lifecycle` entry naming the DuckDB
release it ships in. Add that release to `versions` in `metadata.yaml` if it
is not there yet. Regenerate. The diff in the generated files is part of the
review.

## Versioning

`schema_version` in `metadata.yaml` must be compatible with the installed
capigen (same major, spec minor at most the tool minor). To bump capigen:
adjust the constraint in `pyproject.toml` if the minor moved, run
`uv lock -P capigen`, regenerate, and commit the lockfile together with any
generated diffs.

## Editor schema validation

The YAML here is validated against JSON Schemas that ship inside the pinned
capigen package. Point your editor at them for inline errors and completion
while editing. Because the schemas come from the pinned install, your editor
always checks the exact schema version the generator uses. Nothing is
vendored or committed, and the schemas move with the pin.

Resolve the schema directory (machine-specific, do not commit it; re-run
after changing the pin):

    uv run --project api_spec python -c \
        "import importlib.resources as r; print(r.files('capigen') / 'schema')"

Map two schemas in any editor backed by yaml-language-server (VS Code with
the Red Hat YAML extension, Neovim yamlls, Emacs, Sublime, Helix):

- `<schema-dir>/metadata.schema.json` validates `api_spec/*/metadata.yaml`
- `<schema-dir>/module.schema.json` validates the module files one level
  deeper, `api_spec/*/*/*.yaml`

The globs are workspace-relative and never overlap. In VS Code the mapping
is the `yaml.schemas` setting; keep it in your User settings, or in a
`.vscode/settings.json` listed in `.git/info/exclude`, since the path is
machine-specific. In Neovim the same map goes in the yamlls
`settings.yaml.schemas` table.

Editor validation is a convenience, not the gate. capigen validates every
file against these same schemas during regeneration, and CI fails on the
resulting drift, so an invalid spec cannot land regardless of editor setup.
