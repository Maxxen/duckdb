# DuckDB C API V2

This repository is a DuckDB fork for developing and testing the V2 C API. The V1 C API remains fully functional and untouched alongside V2.

The V2 API is defined declaratively in YAML specs, from which we generate both the public C header (`duckdb_v2.h`) and stub implementations. Real implementations are written manually and tested against two extension case studies (JSON, SQLite) and a Python client.

All V2 identifiers use a `duckdb_v2_` / `DUCKDB_V2_` prefix, so there are no symbol collisions with V1.

## Repository layout

```
api_spec/                        API spec (YAML) -- the V2 API definition
  metadata.yaml                  Primitives, suffixes, versions
  v2/                            Module YAML files (one per API area)

capiv2/                          Code generator (git submodule -> duckdblabs/capiv2)
  src/capigen/                   Generator: c adapter (header), bridge adapter (stubs)
  tests/                         Generator pytest suite

src/include/duckdb_v2.h          Generated V2 C header (committed)
src/main/capi_v2/                V2 bridge implementations (C++ -> C)
  capi_v2_internal.hpp           Internal header with wrapper structs
  capi_v2_stubs.cpp              Auto-generated stubs for unimplemented functions
test/api/capi_v2/                V2 Catch2 tests

extension_v2/json/               JSON extension (copy of extension/json, registered as json_v2)
extension_v2/sqlite/             SQLite extension (from duckdb-sqlite, registered as sqlite_v2)
test/sql/json_v2/                JSON extension SQL tests
test/sql/sqlite_v2/              SQLite extension SQL tests

python_client/                   Python client (to be set up)
```

## Getting started

```bash
git clone --recurse-submodules git@github.com:duckdb/duckdb-capi-v2.git
cd duckdb-capi-v2
```

If you already cloned without `--recurse-submodules`:

```bash
git submodule update --init
```

## Prerequisites

Install [Astral uv](https://docs.astral.sh/uv/getting-started/installation/) (the Python package manager used by the generator):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

You also need the standard DuckDB build dependencies: a C++17 compiler, CMake, and Ninja (optional but recommended).

## Pre-commit hook

A pre-commit hook is configured at `.pre-commit-config.yaml` to run `scripts/format.py` on staged changes before every commit. This is the same formatter CI runs, so if the hook is installed you won't see format-check failures in CI after regenerating the header or stubs.

One-time setup per clone:

```bash
pip install pre-commit
pre-commit install
```

The hook shells out to `black`, `clang-format` (exact version `11.0.1`), and `cmake-format`, which must be on your PATH.

- Linux: `make format_tools` installs all three.
- macOS: `brew install clang-format@11` and `pip install 'black==24.*' cmake-format 'clang_format==11.0.1'`.

When the hook modifies a staged file, pre-commit aborts the commit and prints the list of changed files — re-`git add` them and commit again. To bypass the hook for a single commit (not recommended), use `git commit --no-verify`.

## Making changes to the API spec

The API is defined in YAML files under `api_spec/v2/`. Each file defines a module (e.g., `database/database.yaml`, `query_result/query_result.yaml`) with handles, types, enums, and function declarations.

To add or modify a function, edit the relevant YAML file. Example from `database/database.yaml`:

```yaml
functions:
  duckdb_v2_open:
    summary: "Creates a new database or opens an existing database file."
    role: constructor
    belongs_to: duckdb_v2_database
    parameters:
      context:
        type: duckdb_v2_ctx
        direction: in
      path:
        type: char
        indirection: 1
        const: true
        direction: in
      out_database:
        type: duckdb_v2_database
        indirection: 1
        direction: out
    return_type: DUCKDB_V2_API_CALL
```

All function names must start with `duckdb_v2_` and all type names must start with `duckdb_v2_` or `DUCKDB_V2_`. The generator validates this and will refuse to generate if the convention is violated.

See `capiv2/claude.md` for the full spec conventions.

## Generating the header and stubs

After changing the YAML specs, regenerate the header and stubs:

```bash
cd capiv2

# Install dependencies (first time only)
uv sync

# Generate the C header
just run

# Generate/update stub implementations (skips already-implemented functions)
just stubs

# Run the generator tests
uv run --group dev pytest
```

For continuous development, you can watch for changes and regenerate automatically:

```bash
cd capiv2
just watch   # requires watchexec: cargo install watchexec-cli
```

If you add new stub source files to `src/main/capi_v2/`, add them to `src/main/capi_v2/CMakeLists.txt`.

## Building

The V2 capi and both V2 extensions are compiled into the standard DuckDB build:

```bash
make debug    # or: make release
```

The V2 extensions are registered in `extension/extension_config_local.cmake`, which is checked in (unlike upstream DuckDB where it is gitignored).

## Implementing stubs and running C API V2 tests

The generated `capi_v2_stubs.cpp` contains stub implementations for all declared functions. Each stub returns `DUCKDB_V2_API_ERROR`. To implement a function:

1. Create a new `.cpp` file in `src/main/capi_v2/` (e.g., `database-v2.cpp`)
2. Include `capi_v2_internal.hpp` and write the implementation
3. Add the file to `src/main/capi_v2/CMakeLists.txt`
4. Re-run the bridge generator -- it will drop the stub for any function it finds implemented in your new file:
   ```bash
   cd capiv2 && just stubs
   ```
5. Rebuild and test

Example implementation (`src/main/capi_v2/database-v2.cpp`):

```cpp
#include "capi_v2_internal.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_open(duckdb_v2_ctx_ptr context, const char *path,
                                     duckdb_v2_database_ptr *out_database) {
    auto wrapper = new duckdb::DatabaseWrapperV2();
    try {
        wrapper->database = duckdb::make_shared_ptr<duckdb::DuckDB>(path);
    } catch (...) {
        delete wrapper;
        return DUCKDB_V2_API_ERROR;
    }
    *out_database = reinterpret_cast<duckdb_v2_database_ptr>(wrapper);
    return DUCKDB_V2_ERROR_NONE;
}
```

Run the V2 C API tests:

```bash
./build/debug/test/unittest "[capi_v2]"
```

The test file is at `test/api/capi_v2/test_capi_v2.cpp`. Add new test cases there as you implement functions.

## Making changes to extensions and running extension tests

The `json_v2` and `sqlite_v2` extensions are initially identical to their upstream counterparts (using C++ internals). They will be incrementally ported to use the V2 C API.

Extension sources are in `extension_v2/json/` and `extension_v2/sqlite/`. After making changes, rebuild and run tests:

```bash
# Rebuild
make debug

# Run JSON V2 extension tests
./build/debug/test/unittest "test/sql/json_v2/*"

# Run SQLite V2 extension tests
./build/debug/test/unittest "test/sql/sqlite_v2/*"

# Run a specific test
./build/debug/test/unittest "test/sql/json_v2/scalar/test_json_extract*"
```

## Running everything

```bash
# Full build
make debug

# All V2 tests
./build/debug/test/unittest "[capi_v2]"
./build/debug/test/unittest "test/sql/json_v2/*"
./build/debug/test/unittest "test/sql/sqlite_v2/*"

# Verify V1 is unaffected
./build/debug/test/unittest "[capi]"
```

## CI

The `.github/workflows/v2-capi.yml` workflow runs on every push and PR. It checks:
- Code formatting (`scripts/format.py`)
- Generator tests and header freshness
- Full build with V2 capi and both extensions
- V2 capi tests, JSON V2 tests, SQLite V2 tests
