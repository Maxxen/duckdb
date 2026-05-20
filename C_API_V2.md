# DuckDB C API V2

This repository is a DuckDB fork where we are **prototyping a new C API (V2)** alongside the upstream codebase. The existing V1 C API (`duckdb.h`) remains fully functional and untouched; V2 work lives in parallel directories (`api_spec/v2/`, `src/main/capi_v2/`, `src/include/duckdb_v2.h`, `test/api/capi_v2/`). The upstream DuckDB `README.md` and `CLAUDE.md` describe DuckDB the database; this file is the V2-specific context.

V2 is:

- **Prefixed** — all identifiers use `duckdb_v2_` / `DUCKDB_V2_`, so there are no symbol collisions with V1.
- **Declarative** — the API surface is defined in YAML under `api_spec/v2/`. The public C header (`src/include/duckdb_v2.h`) and the per-function stub skeleton (`src/main/capi_v2/capi_v2_stubs.cpp`) are generated from those specs. Real implementations are hand-written next to the stubs; the generator drops a stub once it finds a matching hand-written definition.
- **Validated** through (a) generator unit tests under `capigen/tests/` and (b) Catch2 bridge tests under `test/api/capi_v2/`. A Python client (`python_client/`, scaffolded) is planned as an end-to-end validation surface.

The V2 design is still being iterated — see "Companion docs" at the bottom of this file for current design discussions and parked questions.

## Design philosophy: C ABI as the canonical product

All language bindings — Rust, Python, Node.js, Julia — FFI into the C ABI; they don't
get an alternative one. The IDL avoids raw C syntax (unions, non-typedef structs, function
pointer fields) so specs stay readable and consumable by tooling that doesn't speak C. But
the canonical output is always the C header. Constructs like `emit: adapter` are honest
acknowledgments of this: "this type must exist in the C ABI; its layout is C-specific and
inexpressible in the IDL." A Rust binding would still FFI into the same type — it just uses
`#[repr(C)] union` on its side.

The practical rule: when something can't be expressed in the IDL, it belongs in the C
adapter/template — not because C is an afterthought, but because C is the product and the
IDL is an abstraction layer over it.

## Repository layout

```
api_spec/                        API spec (YAML) -- the V2 API definition
  metadata.yaml                  Primitives, suffixes, versions
  v2/
    common/common.yaml           Shared handles and aliases
    common/error_codes.yaml      DUCKDB_V2_ERROR_* codes
    error/error.yaml             Error info accessors
    configuration/configuration.yaml  Option handle API

capigen/                         Code generator (vendored via git subtree from duckdblabs/capiv2)
  pyproject.toml                 capigen's own project metadata; installed editably into the root venv
  src/capigen/                   Generator: c adapter (header), bridge adapter (stubs)
  tests/                         Generator pytest suite

pyproject.toml                   Root dev-environment shell; pulls in capigen as a path source and pins the formatter toolchain
scripts/capi_v2_regen.sh         Regenerates header + stubs and formats the output
src/include/duckdb_v2.h          Generated V2 C header (committed)
src/main/capi_v2/                V2 bridge implementations (C++ -> C)
  capi_v2_internal.hpp           Internal header with wrapper structs
  capi_v2_stubs.cpp              Auto-generated stubs for unimplemented functions
test/api/capi_v2/                V2 Catch2 tests

python_client/                   Python client (scaffolded)
```

## Getting started

```bash
git clone git@github.com:duckdb/duckdb-capi-v2.git
cd duckdb-capi-v2
```

## Prerequisites

Install [Astral uv](https://docs.astral.sh/uv/getting-started/installation/) (the Python package manager used by the generator and the formatter):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then provision the root virtual environment, which installs `capigen` (editable) and the formatter's runtime (clang-format, black, …) pinned to the versions CI uses. `cmake-format` is deliberately not in the root venv — it runs only inside its pre-commit hook's isolated environment (see below):

```bash
uv sync --group dev
```

You also need the standard DuckDB build dependencies: a C++17 compiler, CMake, and Ninja (optional but recommended).

## Pre-commit hook

`.pre-commit-config.yaml` configures four hooks that own different parts of the formatting pipeline:

- **`capi-v2-regen`** — fires when any `api_spec/**/*.yaml` is staged. Calls `scripts/capi_v2_regen.sh` to regenerate the header and stubs.
- **`duckdb-format`** — runs `scripts/format.py` on staged C/C++/Python/test changes (and on the files the regen hook just produced).
- **`cmake-format`** — from `cheshirekow/cmake-format-precommit`. Formats `CMakeLists.txt` and `*.cmake` files. pre-commit installs it into its own isolated venv (typically Python 3.11/3.12), so it works even when your terminal runs Python 3.14 where the unmaintained `cmakelang` would otherwise crash.
- **`ty`** — type-checks Python.

One-time setup per clone (alongside `uv sync --group dev`):

```bash
uv run pre-commit install
```

When a hook modifies a staged file, pre-commit aborts the commit and prints the list of changed files — re-`git add` them and commit again. To bypass the hook for a single commit (not recommended), use `git commit --no-verify`.

## Making changes to the API spec

The API is defined in YAML files under `api_spec/v2/`. Each file defines a module with handles, types, enums, and function declarations.

Edit the relevant YAML file. Example from `configuration/configuration.yaml`:

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
      setting:
        type: char
        indirection: 1
        const: true
        description: "Null-terminated setting (string-encoded value)."
      out_option:
        type: duckdb_v2_option
        indirection: 1
        kind: OUT
        description: "Receives the new option handle."
      err:
        type: duckdb_v2_error_info
        indirection: 1
        kind: OUT
        description: "Optional. On failure, receives an opaque info handle the caller must destroy via duckdb_v2_destroy_error_info."
    return_type: DUCKDB_V2_API_CALL
```

All function names must start with `duckdb_v2_` and all type names must start with `duckdb_v2_` or `DUCKDB_V2_`. The generator validates this and will refuse to generate if the convention is violated.

See `capigen/claude.md` for the full spec conventions.

## Error handling

Every fallible V2 function returns a `DUCKDB_V2_API_CALL_t` error code. On success the returned value is `DUCKDB_V2_ERROR_NONE`; on failure it is a non-zero code from `api_spec/v2/common/error_codes.yaml` (or the sentinel `DUCKDB_V2_API_ERROR` for an unspecified internal failure).

Functions also take a trailing `duckdb_v2_error_info_ptr *err` out-parameter that, on failure, receives an opaque handle carrying richer detail (currently the message, with room to grow).

- **The return value is authoritative.** It always carries the error code, regardless of whether `err` was provided.
- **`err` is optional — callers may pass `NULL`** on any call to opt out of detail.
- **On success**, the library leaves `*err` as `NULL`.
- **On failure**, if `err != NULL` the library allocates a `duckdb_v2_error_info_ptr` and stores it in `*err`. The caller owns it and must destroy it with `duckdb_v2_error_info_destroy` (null-safe).
- **Reusing `err` across calls.** If `*err` is already non-null on entry, the library destroys the previous info before writing a new one. To preserve info across calls, detach first: `saved = *err; *err = NULL;`.

The message is borrowed and valid until the info is destroyed:

```c
duckdb_v2_option_ptr opt = NULL;
duckdb_v2_error_info_ptr err = NULL;

if (duckdb_v2_option_create("memory_limit", "1GB", &opt, &err) != DUCKDB_V2_ERROR_NONE) {
    const char *msg = NULL;
    duckdb_v2_error_info_get_message(err, &msg, NULL);
    fprintf(stderr, "option_create failed: %s\n", msg ? msg : "(no detail)");
    duckdb_v2_error_info_destroy(&err, NULL);
}

// Opt-out form — only the return code is inspected:
if (duckdb_v2_option_create("memory_limit", "1GB", &opt, NULL) != DUCKDB_V2_ERROR_NONE) {
    fprintf(stderr, "option_create failed\n");
}
```

Implementations in `src/main/capi_v2/` report errors through the `SetErrorInfo` / `ClearErrorInfo` helpers in `capi_v2_internal.hpp`. Both are safe to call with `err == NULL`; `SetErrorInfo` transparently destroys any previous info in the slot before allocating a new one.

## V2 conventions

These rules apply when writing V2 spec YAML, bridge implementations, and tests. Most have been hard-won from PR1 review; the canonical reference is `design_pr1_to_pr4.md` → "V2 conventions to carry forward". A short reminder list:

- **Handle layout is load-bearing.** A V2 handle is a raw pointer to the underlying C++ object — *not* a wrapper struct — unless the wrapper is documented as load-bearing (`EnvironmentWrapperV2`, `OptionWrapperV2`, etc.). `duckdb_v2_logical_type_ptr` specifically is a `duckdb::LogicalType *`; V1 and V2 share the same `new LogicalType(...)` allocation, so V2 destroy can free a V1-built handle. Direction matters: V1 → V2 destroy is OK and exploited in PR1 tests; V2 → V1 destroy is not asserted and must not be relied on. **Do not wrap `duckdb_v2_logical_type` in a struct** — the PR1 test suite relies on the identity for V1-built composite fixtures.

- **Cast helpers** (`ToEnv`, `ToDb`, `ToLogicalType`, …) live in `capi_v2_internal.hpp` next to the matching wrapper struct — not in per-module `.cpp` files.

- **File-private helpers** in `src/main/capi_v2/*.cpp` go in an anonymous namespace (`namespace { … }`) inside `namespace duckdb { … }`. It is the modern C++ idiom for TU-local symbols (block-scoped, applies to types as well as functions, preferred by Core Guidelines and clang-tidy). Note: under a unity build it does *not* isolate names between concatenated files — all `namespace { … }` blocks in one TU share the same unnamed namespace, so two helpers with the same name in two `*-v2.cpp` files still collide. Name uniqueness is the actual defense.

- **No exceptions across the C ABI.** Allocating sites (`new ...`, builders) wrap in `try { ... } catch (std::exception &e) { ... } catch (...) { ... }`. Both catch arms are required — a non-`std::exception` throw would otherwise abort. Non-allocating accessors are unwrapped only when the file's exception-policy comment explains why (typically: id-checks above the call make the internal `Cast<T>` unreachable). One exception to the policy: accessors that go through DuckDB internals which throw `InternalException` on shape violations (e.g. `FlatVector::GetData<T>` on a non-FLAT vector) must be wrapped, even if today's call paths feed only well-shaped inputs.

- **Borrowed vs owned out-params** — use these exact words in spec descriptions. Borrowed string out-params return `NULL` for "no value" (not `""`); they are null-terminated *and* carry a length. Pin at least one `strlen(out) == len` check per module to prove both forms agree.

- **Numeric enum-id round-trip.** V2 enum values are kept numerically identical to their internal counterparts (`duckdb::LogicalTypeId`, `PhysicalType`, …). If a new internal variant is added, the V2 spec must add a matching id *in the same PR* — otherwise the bridge cast silently produces an undefined enum value.

- **Vocabulary.** "Vector" at the chunk/vector level; "column" only at the result-schema level — never mix. "Logical type" vs "physical type" — never swap. `LogicalTypeId` (semantic) and `PhysicalType` (storage) map many-to-many.

- **Test fixture-builder ordering.** Helpers that allocate intermediate fixtures (struct/union member types, list child types) must destroy the intermediates *before* any `REQUIRE`. Catch2 throws on failure; destroys after a `REQUIRE` would otherwise be skipped, leaking. Pattern:

  ```cpp
  auto v1 = duckdb_create_struct_type(members, names, n);
  duckdb_destroy_logical_type(&members[0]);
  duckdb_destroy_logical_type(&members[1]);
  REQUIRE(v1 != nullptr);
  return V1ToV2(v1);
  ```

- **V1/V2 hybrid prototyping.** Public V2 headers stay V1-free (`duckdb_v2.h` does not include `duckdb.h`). V2 `.cpp` implementations are free to wrap V1 or internal C++ machinery — the V2 contract is what we ship, not the implementation. **Tests are the only place V1 and V2 C headers may co-exist**; tests may build composite fixtures via V1 and cast to V2 when the handle invariant above holds.

- **Spec / YAML style.** One description per function, lead with the contract. Annotate enum values only where the name alone is insufficient. No forward references to in-flight PRs ("(PR4)" ages badly). No first-person editorialising ("at this moment", "for now") — state the contract; deferral rationale lives in the spec's top-of-file commentary. For VARCHAR / string types use "null-terminated byte string", not "UTF-8" — DuckDB doesn't enforce encoding.

## Generating the header and stubs

After changing the YAML specs, regenerate the header (`src/include/duckdb_v2.h`) and the bridge stubs (`src/main/capi_v2/capi_v2_stubs.cpp`):

```bash
./scripts/capi_v2_regen.sh
```

This runs both `capigen` adapters (`c` for the header, `bridge` for the stubs) and then formats the output via `scripts/format.py`. The same script is invoked automatically by the `capi-v2-regen` pre-commit hook whenever you stage an `api_spec/**/*.yaml` change, so committing without a manual run is also fine — the hook regenerates, the format hook re-formats, and pre-commit asks you to re-stage.

To run the capigen generator's own tests:

```bash
uv run --group dev pytest capigen/tests
```

If you add new bridge implementation files to `src/main/capi_v2/`, add them to `src/main/capi_v2/CMakeLists.txt`.

## Building

The V2 capi is compiled into the standard DuckDB build:

```bash
make debug    # or: make release
```

## Implementing stubs and running C API V2 tests

The generated `capi_v2_stubs.cpp` contains stub implementations for any declared function not yet implemented elsewhere. Each stub returns `DUCKDB_V2_API_ERROR`. To implement a function:

1. Create a new `.cpp` file in `src/main/capi_v2/` (e.g., `option-v2.cpp`).
2. Include `capi_v2_internal.hpp` and write the implementation.
3. Add the file to `src/main/capi_v2/CMakeLists.txt`.
4. Re-run `./scripts/capi_v2_regen.sh` — the bridge generator will drop the stub for any function it finds implemented in your new file.
5. Rebuild and test.

Example implementation (excerpt from `src/main/capi_v2/option-v2.cpp`):

```cpp
#include "capi_v2_internal.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_option_create(const char *name, const char *setting,
                                             duckdb_v2_option_ptr *out_option,
                                             duckdb_v2_error_info_ptr *err) {
    if (!name || !setting || !out_option) {
        return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
                                    "null argument to duckdb_v2_option_create");
    }
    auto *wrapper = new duckdb::OptionWrapperV2();
    wrapper->name = name;
    wrapper->setting = setting;
    *out_option = static_cast<duckdb_v2_option_ptr>(wrapper);
    return duckdb::ClearErrorInfo(err);
}
```

Run the V2 C API tests:

```bash
./build/debug/test/unittest "[capi_v2]"
```

The test file is at `test/api/capi_v2/test_capi_v2.cpp`. Add new test cases there as you implement functions; for a new module, add `test/api/capi_v2/test_capi_v2_<module>.cpp` and wire it into `test/api/capi_v2/CMakeLists.txt`.

## Gotchas

A handful of things that recur:

- **V1 must remain functional.** Run the `[capi]` tests as a regression check after any non-trivial change to shared DuckDB code: `./build/debug/test/unittest "[capi]"`.
- **YAML edits require regeneration.** The header and stubs are committed; forgetting to run `./scripts/capi_v2_regen.sh` (or letting the pre-commit hook do it) shows up as drift in `git status`.
- **Hand-written bridges are not overwritten by stub regeneration.** The bridge adapter scans `src/main/capi_v2/` for existing implementations and skips the matching stub. If you delete or rename a function in the spec while an implementation still exists, the orphan lingers in the `.cpp` until you remove it manually.
- **Bridge stub detection is regex-based — write each implementation out as a literal function definition.** The adapter matches `\bduckdb_v2_\w+\s*\(` in the `.cpp` files. Macro-generated function bodies (`SOME_MACRO(duckdb_v2_value_get_int8, …)`) are *not* detected, and the stub for that name will silently remain in `capi_v2_stubs.cpp` — producing a link-time multiple-definition error or, worse, a stubbed-out function shadowing the real one. Unroll macros into explicit `DUCKDB_V2_API_CALL_t duckdb_v2_value_get_int8(...) { ... }` definitions.
- **Out-param zeroing on failure is partial by design.** Pointer-typed out-params (`out_value`, `out_type`, `out_data`, `out_string`) are set to `nullptr` on every `INVALID_INPUT` path so caller code can't accidentally dereference a dangling pointer. Scalar out-params (`out_micros`, `out_lower`, `out_width`, …) are left *unspecified* on failure. Callers must always check the return code before reading any out-param; defensive callers should not assume scalars were touched.
- **Error codes are 32-bit: `(group_id << 16) | code`.** Don't hard-code the numeric value — use the generated macro name (`DUCKDB_V2_ERROR_*`).
- **Every declared function must return an error code** (`DUCKDB_V2_API_CALL_t` or an alias). Don't use `void` or pointer-returning signatures; results come back through out-params.
- **Primitives are declared in `api_spec/metadata.yaml`.** If you need a new one, add it there first with its C ABI type under `c_type`.

## Running everything

```bash
# Full build
make debug

# V2 bridge tests
./build/debug/test/unittest "[capi_v2]"

# Verify V1 is unaffected
./build/debug/test/unittest "[capi]"
```

## CI

The `.github/workflows/v2-capi.yml` workflow runs on every push and PR. It runs two jobs:
- `format-check` — provisions the root venv with `uv sync --group dev`, then runs `pre-commit run --all-files` (default stages: regen, ty, ruff, check-yaml, yamlfmt) followed by `pre-commit run --all-files --hook-stage manual` (full-tree `scripts/format.py --all --check`). Finally `git diff --exit-code` fails the job if the committed header or stubs are out of sync with `api_spec/`.
- `build-and-test` — `make release`, then `./build/release/test/unittest "[capi_v2]"`. Uses ninja + ccache (via the `./.github/actions/ccache-action` composite action) for build speed.

## Companion docs

- **`design_pr1_to_pr4.md`** (untracked) — design decisions and cross-PR conventions for the logical-types / values / query-results / data-chunks-and-vectors PRs currently in flight. Contains the full "V2 conventions to carry forward" reference of which the section above is a summary.
- **`ctx_centric.md`** (untracked) — parked design doc exploring a context-centric API shape. Captures the extended discussion: ctx hierarchy, caching architecture, config handling, thread safety, cross-language callback contexts, classes of objects that don't naturally belong to a single ctx. Read before making major API-shape decisions.
- **`config_design.md`** (untracked) — design notes for the configuration / options surface that landed in PR #9.
- **`capigen/README.md`** — generator usage from the generator's perspective (if you're hacking on capigen itself). capigen is vendored in-tree; it is not a git subtree or submodule.
- **`capigen/claude.md`** — authoritative conventions for the YAML spec (function naming, handle conventions, role semantics).
- **`schema_reference.md`** — top-level reference for the module-level JSON Schema (`capigen/src/capigen/schema/module.schema.json`).
