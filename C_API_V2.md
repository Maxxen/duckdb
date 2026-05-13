# DuckDB C API V2

This repository is a DuckDB fork for developing and testing the V2 C API. The V1 C API remains fully functional and untouched alongside V2.

The V2 API is defined declaratively in YAML specs, from which we generate both the public C header (`duckdb_v2.h`) and stub implementations. Real implementations are written manually and exercised through Catch2 tests; a Python client is scaffolded as an upcoming end-to-end validation surface.

All V2 identifiers use a `duckdb_v2_` / `DUCKDB_V2_` prefix, so there are no symbol collisions with V1.

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

The test file is at `test/api/capi_v2/test_capi_v2.cpp`. Add new test cases there as you implement functions.

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
