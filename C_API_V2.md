# DuckDB C API V2

This repository is a DuckDB fork where we are **prototyping a new C API (V2)** alongside the upstream codebase. The existing V1 C API (`duckdb.h`) remains fully functional and untouched; V2 work lives in parallel directories (`api_spec/v2/`, `src/main/capi_v2/`, `src/include/duckdb_v2.h`, `test/api/capi_v2/`). The upstream DuckDB `README.md` and `CLAUDE.md` describe DuckDB the database; this file is the V2-specific context.

V2 is:

- **Prefixed** — all identifiers use `duckdb_v2_` / `DUCKDB_V2_`, so there are no symbol collisions with V1.
- **Declarative** — the API surface is defined in YAML under `api_spec/v2/`. The public C header (`src/include/duckdb_v2.h`) and the per-function stub skeleton (`src/main/capi_v2/capi_v2_stubs.cpp`) are generated from those specs by capigen, a pinned PyPI dependency (source at github.com/duckdb/capigen). Real implementations are hand-written next to the stubs; the generator drops a stub once it finds a matching hand-written definition.
- **Validated** through Catch2 bridge tests under `test/api/capi_v2/` (the generator's own unit tests live in the capigen repository).

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
api_spec/                        API specs (YAML) -- the canonical API definitions
  v2/                            The V2 API (the focus of this work)
    metadata.yaml                Primitives, suffixes, versions, prefix (duckdb_v2_)
    common/common.yaml           Shared handles and aliases
    common/error_codes.yaml      DUCKDB_V2_ERROR_* codes
    common/logger.yaml           Logger handle + log-storage API
    error/error.yaml             Error info accessors
    configuration/configuration.yaml  Option handle API
    connection/, database/, environment/          Lifecycle handles
    query_result/, sql_statement/, prepared_statement/  Streaming results, parsed + prepared statements
    value/, logical_type/, data_chunk/, vector/   Data + type surface
    expression/, replacement_scan/, filesystem/   Bound expressions, replacement scans, VFS
    function/                    scalar / aggregate / table / cast / copy builders
  v1/                            Declarative reconstruction of the V1 surface. Regenerates
                                 src/include/duckdb_v1.h, and from extension/duckdb_extension.h.in
                                 the extension header (src/include/duckdb_extension.h) and its
                                 engine-side struct (src/include/duckdb/main/capi/extension_api.hpp)

pyproject.toml                   Root dev-environment shell; depends on capigen (PyPI, pinned ~=0.4.0) and pins the formatter toolchain
uv.lock                          Locks the exact capigen version so regenerated output is reproducible
scripts/capi_v2_regen.sh         Regenerates the V2 header + stubs and formats the output
scripts/capi_v1_regen.sh         Regenerates the V1 header and the extension headers, and formats them
src/include/duckdb_v2.h          Generated V2 C header (committed)
src/include/duckdb_v1.h          Generated V1 C header reconstruction (committed)
src/include/duckdb_extension.h   Generated V1 extension header (committed)
src/include/duckdb_cpp.hpp       Stable C++ API (experimental) public header (see below)
src/main/capi_v2/                V2 bridge implementations (C++ -> C)
  capi_v2_internal.hpp           Internal header with wrapper structs
  capi_v2_stubs.cpp              Auto-generated stubs for unimplemented functions
  duckdb_cpp.cpp                 Stable C++ API (experimental) implementation
test/api/capi_v2/                V2 Catch2 tests
  test_capi_v2_*.cpp             C API bridge tests, tag [capi_v2]
  test_cpp_api_*.cpp             Stable C++ API test suite, tag [cpp_api]
```

## Stable C++ API (experimental)

Alongside the C API, V2 carries a new C++ API: namespace `duckdb_api`, with its public
header at `src/include/duckdb_cpp.hpp` and its implementation at
`src/main/capi_v2/duckdb_cpp.cpp`; the Catch2 suite lives in
`test/api/capi_v2/test_cpp_api_*.cpp` (tag `[cpp_api]`). When V2 documents or discussions say
"the stable C++ API", this is what they mean, NOT DuckDB's existing internal C++ API
(`duckdb.hpp`), which makes no stability promises.

Two properties define it:

- **Built exclusively on the V2 C API.** `duckdb_cpp.cpp` talks only to `duckdb_v2.h`, never to
  DuckDB internal headers. The public header goes one step further and includes no DuckDB
  header at all: the `detail::HandleTraits` indirection keeps C handle types out of it, and
  each wrapper stores its handle as `void *impl` (specializations live in the `.cpp`).
- **Intended to become ABI stable.** Design decisions are made with a future stable binary
  interface in mind. This is why callback registration favors raw function pointers plus an
  explicit user-data channel (`SetUserData<T>`, backed by `detail::TypedDelete<T>`) over
  `std::function`. New surface should follow that style, and must use only C++17 features:
  the build floor is C++17, and the public header in particular must not require anything
  newer from consumers. The experimental surface still
  carries pre-stability open items to settle before any stability promise is made: std
  library types in public signatures (`std::string`, `std::optional`, `std::string_view`),
  the `std::function` parameter of `Connection::WithTransaction` (an outlier, not
  precedent), and the virtual destructor on the `detail::Handle` base.

Internal patterns new cpp_api surface should reuse rather than reinvent:

- `CheckedAPICall(fn, args...)` wraps a V2 C call: it appends the err slot, and on failure
  throws `duckdb_api::Exception` carrying the V2 error code and message.
- `WithExceptionGuard(err, fn)` is the callback-boundary guard: trampolines that the engine
  invokes (transactions, log storage, function callbacks) wrap the user's C++ code in it, so
  thrown exceptions become code + text on the borrowed err slot instead of crossing the C
  ABI. Idiom: throw to fail, with a typed facade exception when the error class matters.
  Full contract (tiers, typed subclasses, round trip, growth rules): "The stable C++ API's
  exception model" under Error handling.
- Wrapper objects follow the `detail::Handle<T>` / `detail::Factory` pattern; constructors
  taking raw handles stay private.
- Nullable borrowed strings (the C API returns `{NULL, 0}` for "no value") map to an empty
  `std::string_view` in the wrapper — e.g. `Database::ReplacementScanInput::GetCatalogName`,
  `DatabaseOption::GetDefaultValue`. The C API keeps NULL (the right C idiom); per-language
  idiom mapping is what the wrapper is for.
- Callback registration uses raw function pointers plus the C API's `opaque` user-data
  channel. When a surface has a single registration call rather than a builder (e.g.
  `Database::AddReplacementScan<T>`), the wrapper bundles its own C++ callback together with
  the caller's user data into that one opaque slot, destroying both via `detail::TypedDelete`
  at engine teardown.
- Data crosses as `Value` (one owned cell) or `Vector` / `VectorView` (columnar batch); both
  expose the committed layout through shared structs. Full contract: "The stable C++ API's
  value and vector data model" below.

### The stable C++ API's value and vector data model

Two representations of data, opposite grains. Pick by whether you hold one cell or a batch.

| | `Value` | `Vector` (via `VectorView`) |
| --- | --- | --- |
| shape | one owned cell | columnar batch |
| ownership | owns type + payload; movable, standalone | borrowed; valid only while the chunk lives |
| self-describing | yes (carries its own type) | no (dispatch on the column type once) |
| use for | bind parameters, out-of-band single-cell reads, any type incl. VARIANT / GEOMETRY (no flat layout) | bulk read/write on the hot path |

**Naming: four families, kept distinct.**

| family | role | convention | examples |
| --- | --- | --- | --- |
| type | logical / SQL type | `TypeId::NAME`, `LogicalType` | `TypeId::HUGEINT` |
| value | build / read one cell | `Value::Name(...)` / `v.AsName()`, named by **logical type**, no `From` | `Value::Hugeint(...)`, `v.AsBigint()` |
| layout | committed byte mirror (cast target) | `NameLayout` | `HugeintLayout`, `IntervalLayout`, `StringLayout` |
| convenience | decoded form of an encoded kind | `DecodedName` (pairs with `Decode*`) | `DecodedBit`, `DecodedTimeTz`, `DecodedUuid`, `DecodedBignum` |

Value factories/getters are named by the value's **logical type**, never its physical width
(`Value::Bigint`, not `FromI64`): a value's type is what it *is*, and many logical types share a
width (int64 backs BIGINT, TIMESTAMP, TIME). The generic raw trio is the deliberate exception:
`GetData` / `GetDataAs<T>` / `FromData` name the *operation* (the committed payload), so
`FromData` reads "from raw data", not "from a logical type".

**Layout is committed and shared.** The physical layout (`api_spec value.yaml`) is public API
on both paths. Fixed multi-field kinds cast to one shared set of top-level `*Layout` structs:
`IntervalLayout`, `HugeintLayout`, `UhugeintLayout`, `StringLayout`, used by both
`VectorView::Data<T>()` and `Value::GetDataAs<T>()`. A fixed-layout leaf with more than one
field earns a `*Layout` struct mirroring its committed bytes. Kinds whose storage is an
*encoded* form rather than the value get a `convenience` struct, produced by `Decode*`: BIT,
TIME_TZ, and UUID decode inline in C++ (committed bit-layouts, allocation-free, so they inline
into a per-row loop); BIGNUM keeps a C codec (`bignum_decode`) because un-inverting a negative
magnitude must allocate an owned buffer.

**Raw payload access is symmetric.**

| | read (borrowed) | read (copy) | build / write |
| --- | --- | --- | --- |
| `Value` | `GetData() -> {ptr, len}` | `GetDataAs<T>()` | `FromData(type, ptr, len)` |
| `Vector` | `VectorView::data` / `Data<T>()` | (stride the pointer) | `GetDataMutable<T>()` |

`GetData` borrows for the lifetime of the value (a view off a temporary dangles); `GetDataAs`
copies. Idiom on both paths: **dispatch on the logical type, then cast to the layout struct.**
This is the complete path. It covers every type with no per-type code, including the
metadata-resolved kinds (DECIMAL: backing integer + `GetDecimalScale`; ENUM: index +
`GetEnumValue`) and unknown extension types. Var-length differs by design: a `Value` payload is
the decoded wire bytes (VARCHAR content), a vector slot is the `StringLayout` string_t.

**Typed `As*` / `From*` are gated sugar** over `GetData` / `FromData` for the everyday kinds
(bool, ints, floats, temporal, interval, the 128-bit widths): a type-id gate plus a name. They
are not the completeness mechanism; the long tail is not hand-wrapped, and reading an unknown
type never requires a VARCHAR cast.

**Value-only semantic operations** (no raw-layout equivalent): `UnwrapVariant` (dynamic inner
type), `Cast` (conversion), `ToString` (human display).

**Raw layout lives in C++, not C-only.** The committed layout is already public on the vector
path, and language bindings (which reconstruct native types from bytes) are C++ consumers of
this API, so byte access belongs here. `Value::GetData` / `FromData` wrap the C
`value_get_data` / `value_create_from_data`; they add no ABI surface beyond what the vector
path already commits.

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

Then provision the root virtual environment, which installs `capigen` (from PyPI, pinned) and the formatter's runtime (clang-format, black, …) pinned to the versions CI uses. `cmake-format` is deliberately not in the root venv — it runs only inside its pre-commit hook's isolated environment (see below):

```bash
uv sync --group dev
```

You also need the standard DuckDB build dependencies: a C++17 compiler, CMake, and Ninja (optional but recommended).

## Pre-commit hook

`.pre-commit-config.yaml` configures the hooks that own the regeneration and formatting pipeline:

- **`capi-v2-regen`** — fires when any `api_spec/v2/**/*.yaml` is staged. Calls `scripts/capi_v2_regen.sh` to regenerate the V2 header and stubs.
- **`capi-v1-regen`** — fires when any `api_spec/v1/**/*.yaml` or the extension seed (`api_spec/v1/extension/duckdb_extension.h.in`) is staged. Calls `scripts/capi_v1_regen.sh` to regenerate the V1 header (`src/include/duckdb_v1.h`) and the extension header (`src/include/duckdb_extension.h` and `extension_api.hpp`).
- **`duckdb-format`** — runs `scripts/format.py` on staged C/C++/Python/test changes (and on the files the regen hooks just produced). A manual-stage variant, **`duckdb-format-check`**, runs the full-tree `--all --check` pass in CI.
- **`cmake-format`** — from `cheshirekow/cmake-format-precommit`. Formats `CMakeLists.txt` and `*.cmake` files. pre-commit installs it into its own isolated venv pinned to Python 3.12, so it works even when your terminal runs Python 3.14 where the unmaintained `cmakelang` would otherwise crash.
- **`check-yaml` / `yamlfmt`** — validate and format the `api_spec/` YAML.

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
        description: "Optional. On failure, receives an opaque info handle the caller must destroy via duckdb_v2_error_info_destroy."
    return_type: DUCKDB_V2_API_CALL
```

All function names must start with `duckdb_v2_` and all type names must start with `duckdb_v2_` or `DUCKDB_V2_`. The generator validates this and will refuse to generate if the convention is violated.

### Spec authoring conventions

The IDL field reference and the spec-language features (prefix application, handle styles,
qualified aliases, descriptions) are documented in the capigen repository
(`schema_reference.md` and `CLAUDE.md`). The conventions specific to this fork's spec:

- **Function roles.** Set `role` from behavior: `constructor` (create family), `destructor`
  (destroy), `getter`, `setter`, or `method`. Destructors are infallible and take no `err`.
- **Parameter order.** Primary subject, then inputs, then `out_*`, then the trailing `err`.
  Every fallible function returns `DUCKDB_V2_API_CALL` and takes `err` last (`kind: OUT`,
  `indirection: 1`). See Error handling below for the slot contract.
- **Shared handles** are declared once in `common/common.yaml`, never redeclared per
  module. Use `qualified: true` for names owned elsewhere, such as `idx_t` and `sel_t`.
- **Lexical style.** `Connection` becomes `conn`, `Callback` becomes `cb`, `Statement`
  becomes `stmt`, `Execution` becomes `exec`, `Destroy` becomes `destroy`, and `Begin` /
  `End` become `begin_...` / `end_...`. String data is `type: char, indirection: 1`.

## Error handling

Every fallible V2 function returns a `DUCKDB_V2_API_CALL_t` error code. On success the returned value is `DUCKDB_V2_ERROR_NONE`; on failure it is a non-zero code from `api_spec/v2/common/error_codes.yaml` (or the sentinel `DUCKDB_V2_API_ERROR` for an unspecified internal failure).

Fallible functions also take a trailing `duckdb_v2_error_info_handle *err` out-parameter that, on failure, receives an opaque handle carrying richer detail (currently the message, with room to grow). Destructors are the exception — see below.

- **The return value is authoritative.** It always carries the error code, regardless of whether `err` was provided. Always check the return code — never infer success or failure from the state of `*err`.
- **`err` is optional — callers may pass `NULL`** on any call to opt out of detail.
- **The library writes the slot only on failure; success leaves it untouched.** On a failing call with `err != NULL` the info is lazy-allocated on the slot's first use and overwritten in place thereafter. On a successful call the library does *nothing* to the slot — it neither allocates, clears, nor nulls it. (Writing on every successful call would force an allocation per call for no benefit, since the return code already says "success".)
- **On failure**, if `err != NULL` the slot holds an info carrying the code and message. The caller owns the slot and must destroy it once with `duckdb_v2_error_info_destroy` (null-safe).
- **Read `*err` only after a failing return.** Because success never touches the slot, a stale info from an earlier failure can survive a later successful call. This is harmless as long as you key off the return code (which you must) and read `*err` only when the immediately preceding call returned a non-`NONE` code. There is no "clear" step and no clear function — the slot is not state you reset between calls.
- **The only lifecycle duty is to destroy once.** Reuse the same `err` variable across as many calls as you like; when you are done (typically after consuming a failure's detail), call `duckdb_v2_error_info_destroy(&err)`, which frees the info and re-nulls the slot. To keep an info alive past a destroy of the slot, detach it first: `saved = *err; *err = NULL;`.

### Destructors are infallible and take no `err`

Destructors — `duckdb_v2_close`, `duckdb_v2_disconnect`, `duckdb_v2_destroy_environment`, `duckdb_v2_option_destroy`, `duckdb_v2_value_destroy`, `duckdb_v2_logical_type_destroy`, `duckdb_v2_data_chunk_destroy`, `duckdb_v2_result_destroy`, `duckdb_v2_error_info_destroy`, `duckdb_v2_file_handle_destroy`, `duckdb_v2_sql_statement_destroy`, `duckdb_v2_statement_iterator_destroy`, `duckdb_v2_prepared_statement_destroy`, the function-builder destructors (`duckdb_v2_scalar_function_builder_destroy`, `_aggregate_function_builder_destroy`, `_table_function_builder_destroy`, `_cast_function_builder_destroy`, `_copy_function_builder_destroy`, `_custom_type_builder_destroy`, `_log_storage_builder_destroy`), and the column-data-collection / scan-state destructors — take only their handle slot and **no `err` out-parameter**. `duckdb_v2_scalar_function_builder_destroy(duckdb_v2_scalar_function_builder_handle *func)` is the canonical shape.

- They are **null-safe**: a null pointer-to-handle, or a slot already set to `NULL`, is a no-op.
- On return the handle slot is set to `NULL` to prevent double-free.
- They still return `DUCKDB_V2_API_CALL_t`, so the return value remains the channel for the rare refusal that is genuinely actionable — e.g. `duckdb_v2_destroy_environment` returns `DUCKDB_V2_ERROR_RESOURCE_IN_USE` (and leaves the handle intact) while databases opened through it are still alive. Without an `err` slot there is no message detail for these cases; the code alone is the contract.

The message is borrowed and valid until the info is destroyed:

Strings cross the boundary as `duckdb_v2_str` — a borrowed `{const char *ptr; idx_t len;}`
view that is NOT null-terminated (see "Length-delimited strings" below). A small
helper turns a C literal into one:

```c
static duckdb_v2_str v2str(const char *s) { return (duckdb_v2_str){s, s ? strlen(s) : 0}; }

duckdb_v2_option_handle opt = NULL;
duckdb_v2_error_info_handle err = NULL;

if (duckdb_v2_option_create(v2str("memory_limit"), v2str("1GB"), &opt, &err) != DUCKDB_V2_ERROR_NONE) {
    duckdb_v2_str msg = {NULL, 0};
    duckdb_v2_error_info_get_text(err, &msg);
    fprintf(stderr, "option_create failed: %.*s\n", (int)msg.len, msg.ptr ? msg.ptr : "");
    duckdb_v2_error_info_destroy(&err);
}

// Opt-out form — only the return code is inspected:
if (duckdb_v2_option_create(v2str("memory_limit"), v2str("1GB"), &opt, NULL) != DUCKDB_V2_ERROR_NONE) {
    fprintf(stderr, "option_create failed\n");
}
```

Implementations in `src/main/capi_v2/` report failures through the `SetErrorInfo` helper in `capi_v2_internal.hpp`: it lazy-allocates an info in the slot if empty and otherwise overwrites the existing one in place, never destroys the slot (end-of-life is the caller's job via `error_info_destroy`), and is safe to call with `err == NULL`. There is no success-path helper — successful returns simply leave the slot alone and `return DUCKDB_V2_ERROR_NONE`. Most entry points don't call `SetErrorInfo` directly: they wrap their body in the `WithErrorHandler` template (below), which catches and writes the slot only on failure.

### One uniform model: every `err` is a slot

The err semantics are the same wherever they appear — at external entry points, inside callback parameters, anywhere. `err` is always `error_info_handle *err`: a pointer-to-handle out-parameter (a "slot"). The library writes an `error_info` into the slot only on failure when the slot is non-null; on success it leaves the slot untouched, and it never destroys the slot. The backing `error_info` may live on the heap (external entry points lazy-allocate one) or on the library's stack (callback trampolines hand the callback a slot pointing at a stack-allocated info). There is no second pattern, no "info handle vs slot" distinction, no translation layer between callbacks and the rest of the API.

Two consequences of the uniform model:

- **Inside a callback, propagating an error from a downstream V2 call is trivial.** Pass the same `err` slot through to the nested call; the nested call populates it on failure exactly the way it populates any other slot. The callback inspects whether it now holds an error code and either returns (the trampoline will see the populated slot and act on it) or unwinds further.

- **Inside a callback, initiating a custom error** is done through the public setters on the handle DuckDB provides: `duckdb_v2_error_info_set_code(*err, code)` and `duckdb_v2_error_info_set_text(*err, message)`. In a callback `*err` is always a valid handle owned by DuckDB; the callee sets its code and message and returns. **The callee must never destroy the handle** — its storage may be on the library's stack. After the callback returns, the trampoline reads the code and, if it is not `DUCKDB_V2_ERROR_NONE`, raises a DuckDB exception that the outer `WithErrorHandler` translates back into the V2 error code.

The `WithErrorHandler` template in `capi_v2_internal.hpp` is the bridge-level helper for external entry points: it wraps a lambda with try/catch, translates thrown DuckDB exceptions to V2 error codes via `GetErrorCodeFromExceptionType`, and writes the resulting code/message into the slot **only on failure** (lazy-allocating on first use, never destroying); on success it returns `DUCKDB_V2_ERROR_NONE` and leaves the slot untouched. Callback trampolines (e.g. inside `connection_execute_with_context` or the scalar-function bind/init/exec bridges) allocate a stack `ErrorInfoV2`, hand the callback a slot pointing at it, inspect its code after the callback returns, and translate a set code into a thrown DuckDB exception that the outer `WithErrorHandler` catches.

### The stable C++ API's exception model

Exceptions are the failure channel in both directions. One type family. The C
error-code vocabulary never appears in the public header; it lives only in
`duckdb_cpp.cpp`.

**Incoming** (C call fails → C++ exception). Every wrapper calls its C function
through `CheckedAPICall(fn, args...)`, which on failure throws
`duckdb_api::Exception` carrying:

- `GetCode()`: raw `uint32_t`, opaque, preserved so it can cross back into C. Do not interpret numerically.
- `what()`: rendered message. `GetRawMessage()`: body without the "<Type> Error: " prefix.

New wrapper surface MUST use `CheckedAPICall`; hand-rolled code checks are a review smell.

**Outgoing** (callback throws → C error). Engine-invoked trampolines
(transactions, functions, replacement scans, log storage) wrap the implementor's
code in `WithExceptionGuard(err, fn)`. Three tiers:

| thrown | code on slot | class survives? |
| --- | --- | --- |
| `duckdb_api::Exception` / subclass | its `GetCode()` | yes, round-trips end to end |
| other `std::exception` | `DUCKDB_V2_API_ERROR` | no; message kept |
| anything else | `DUCKDB_V2_API_ERROR` | no; "An unknown error occurred." |

Implementor rule: **throw to fail; throw a typed exception when the class
matters** (it usually does: it decides what a Python/JS/... consumer catches).

**Typed exceptions** name an error class without exposing a code. Subclasses of
`Exception`, constructors DEFINED IN THE `.cpp` (class→code mapping in one place,
no code in the header):

```cpp
throw duckdb_api::InvalidInputException("scalar function returned the wrong arity");
throw duckdb_api::InterruptException("query cancelled");
```

`WithExceptionGuard` catches the base by reference, so subclasses need zero guard
changes. Rules:

- **Grow on demand.** Add a subclass when a real thrower or catcher needs it. Never a bulk mirror of the code table (hand-maintained mirrors drift; the C macros already serve C consumers).
- **Constructor in the `.cpp`, never the header.**
- **Base `Exception` stays public and catchable** (the "any facade error" handler); `GetCode()` stays the raw passthrough for bindings that translate codes wholesale (a binding's job, not a C++ consumer's).
- **Not built yet: typed catching on incoming.** `CheckedAPICall` throws the base for every C failure. To let a C++ consumer `catch (const CatalogException &)`, map incoming codes to the subclass family at that one throw site, not in consumer code. Until then, discriminate via `GetCode()` vs `duckdb_v2.h` macros (accepting the include).

**Destructors and moves never throw.** They call the infallible C destructors (no
err slot). A wrapper destructor that can fail is a spec-level design error, not
something to catch around.

**Round trip** (Python UDF, every layer): implementor throws
`InvalidInputException` → `WithExceptionGuard` writes code+message to the
trampoline's stack slot → bridge rethrows engine-side as the mapped DuckDB
exception (`ErrorInfoV2::ThrowAsException` / `InvokeWithErrorSlot`, both via
`TryGetExceptionTypeFromErrorCode`) → engine unwinds → outer `WithErrorHandler`
writes the same code to the caller's slot → caller's `CheckedAPICall` throws
`duckdb_api::Exception` → binding maps the code to its native class. One class,
chosen once, visible at every layer in its native vocabulary.

## Streaming result model

Query results are streaming-only; there is no materialized result surface and no random access into a result. The mechanics below are contracts, and the motivations are part of them — they explain which alternatives were considered and rejected, so they don't get re-litigated.

- **Statements are parsed lazily, one at a time.** `parse_sql(conn, sql, out_iterator, err)` sets up an iterator over the connection's parser options and extensions but parses nothing itself; `statement_iterator_next` parses the next statement on demand and yields an owned `sql_statement` handle (NULL at exhaustion). Statements are *raw* parser output, with no binding, no catalog access, and no transaction; statement-level rewrites (pragma reparse, expansion unpacking, transaction wrapping) happen inside `statement_execute`, so a statement group is never split across the API boundary. Because statement N is parsed only after statement N-1 has executed, a statement that registers grammar (e.g. `LOAD` an extension) is parsed and executed before a following statement in the same string uses it. A parse error surfaces from the `next()` that reaches the failing statement, after the prior statements have been yielded, not up front from `parse_sql`; it is rendered through the engine's public `ProcessError` (errors_as_json JSON, else LINE/caret), the same shape as an eagerly parsed query. A parse error terminates iteration the same way clean exhaustion does: the erroring `next()` reports it, and every `next()` after it reports clean exhaustion (a NULL handle, no error). *Motivation:* a string-taking, single-statement execute verb forces callers to split SQL themselves, which is effectively to parse; the iterator makes multi-statement input explicit and per-statement, raw output guarantees one user statement is always one handle, and lazy parsing lets a statement's side effects (grammar registration, catalog changes) reach the parse of the next.

- **`statement_execute` is lazy, borrows one parsed statement (non-consuming), binds optional parameters, and owns statement expansion.** It preprocesses the statement (pragma reparsing, expansion unpacking, transaction wrapping), prepares it, and executes nothing until the result is stepped; prepare-time errors (binder/catalog/preprocessing) return from `statement_execute` itself, execution errors from the steps. The statement is borrowed, not consumed: a copy is executed, so the same statement re-runs (for example with a different value set) and the caller destroys it with `sql_statement_destroy`. Positional `parameter_values` fold in as constants (the i-th value binds `$(i+1)`, the engine's positional convention); pass `(NULL, 0)` for an unparameterized statement. The busy and null-arg refusals never reach the engine. Preprocessing can expand one user statement into a group of engine statements (auto-PIVOT; `ALTER ... ADD COLUMN` with a non-constant DEFAULT, which the engine wraps in an auto-rollback transaction): the group executes as one result through the same steps, and result selection mirrors `ClientContext::Query`'s chain-append rule — the caller sees the first row-producing fragment, or the last fragment when none produces rows; other fragments' output is discarded exactly as the engine discards it. When a group cannot complete (error, cancellation, destroy), the bridge rolls back the injected auto-rollback transaction, mirroring the eager loop's error handling. For expanding statements the metadata getters fail with `INVALID_INPUT` until stepping has prepared the result-producing fragment; for every non-expanding statement metadata stays available from `statement_execute` on. An expansion with more than one row-producing fragment reports `NOT_IMPLEMENTED` (none exists today; the eager engine would chain them as separate results).

- **`statement_bind` inspects a parsed statement without executing it.** It binds the statement and returns its signature (the output schema and the parameter schema, both `duckdb_v2_schema` handles), then discards the bound plan. Read-only and non-consuming: it builds no result and does not disturb an in-flight query, so a live stream survives the bind. Bind errors surface from the call. Use it to learn a statement's shape before deciding how to execute it.

- **Prepared statements are the opt-in cached-execution path, separate from stateless `statement_execute`.** `statement_prepare(conn, stmt, require_cacheable, out_prepared)` turns a parsed statement into a `prepared_statement` handle via the engine's `Prepare`, borrowing the statement (a copy is prepared). `prepared_execute(prepared, values, count, out_result)` runs it with positional parameter values and returns the same streaming `result` as `statement_execute`, reusing the cached plan when cacheable. Reuse is catalog-version guarded, so a concurrent DDL on a dependency forces a re-bind. `prepared_reuses_plan` reports honestly whether reuse happens: true only for what the engine caches (an unparameterized statement, or a parameterized one with no table scan whose parameters are fully type-anchored), false when every execution re-binds anyway (an unanchored parameter, or a table scan). When false, `prepared_execute` is no faster than `statement_execute`; the introspection is what keeps "prepared" from implying a speedup the engine will not deliver. `require_cacheable` fails the prepare with `INVALID_INPUT` when the plan would not be reused. Prepared execution is behaviorally identical to `statement_execute` (same streaming, drain, schema, rows-changed, statement type), differing only in plan retention; both reach the result state machine through one shared seam, and a prepared statement is a single engine statement, so the fragment-expansion machinery does not apply. `statement_prepare` participates in the one-live-result rule below (its `Prepare` runs `InitialCleanup`, which would cancel a live stream); destroy the handle with `prepared_statement_destroy`.

- **The primitive is `result_step`; everything else is a convenience over it.** `result_step` is non-blocking (never waits indefinitely; it runs a bounded amount of execution work) and reports a `RESULT_STEP_STATUS`. `result_wait` ("block until stepping is worth it") and `result_fetch_chunk` ("block until the next chunk or end-of-stream") are layered on top and gain no capability the primitive lacks. *Motivation:* async runtimes (e.g. Python asyncio) need "can I make progress?" inversion of control. A per-chunk **callback** consumption model was considered and rejected: it inverts control the wrong way for event loops (so step would have to ship anyway), forces a trampoline on every FFI binding, and reopens context-lock reentrancy questions inside the callback. Step stays the only primitive; scoped conveniences may be layered over it later, never under it.

- **`RESULT_STEP_STATUS` conventions.** `WAITING = 0`, so a zero-initialized status out-param reads as "no work product yet", never as CHUNK (same convention as `VECTOR_TYPE_OTHER` being 0). The enum deliberately does **not** numerically mirror an internal enum — it collapses `PendingExecutionResult` and `StreamExecutionResult` into the four states a consumer can act on. It is the documented exception to the numeric round-trip rule; do not "fix" it.

- **Cancellation has two channels, chosen by the entry point.** In `result_step` an interrupt surfaces as the sticky status `CANCELLED` (not an error); in `result_fetch_chunk`, which has no status out-param, the same event surfaces as `ERROR_RUNTIME_INTERRUPT`. Errors proper are always return-code + `err` slot, never a status, and are sticky.

- **A result is a cursor on the connection's single execution slot, not a box of data.** Creating the pending query opens a transaction (under autocommit); the engine commits/rolls back only when the stream is drained to completion — or, if an undrained result is destroyed, lazily when the connection next begins a query. Two consequences callers must understand:
  - An undrained result **holds a transaction open** (version cleanup and checkpointing are deferred behind it). Drain or destroy promptly.
  - Side-effecting statements (`INSERT`, `CREATE`, `SET`, `CALL`, …) **take effect only when their result is drained**. Destroying an undrained result abandons execution *including the side effects* — silently. `result_drain(result, out_rows_changed, err)` is the sanctioned convenience for fire-and-forget statements: it runs the result to completion, discards rows, and reports the changed-row count. The result type (`result_get_result_type`) is prepare-time metadata, so callers decide between consuming rows and draining without inspecting the SQL. (A separate eager `connection_execute` entry point was considered and rejected in review: the caller cannot know up front whether e.g. `INSERT ... RETURNING` produces rows, so the result handle must carry that information.)

- **One live result per connection.** While a connection has a live, non-terminal result, starting a new query (`statement_execute` or `prepared_execute`) fails with `RESOURCE_IN_USE` (the same refuse-on-busy convention as `duckdb_v2_destroy_environment`); `statement_prepare` refuses the same way, because its `Prepare` runs the engine's `InitialCleanup`, which would cancel the live stream. *Motivation:* the engine's native behavior is to silently cancel the previous stream when a new query starts — action at a distance that every major C database API (libpq, MySQL's `mysql_use_result`, ODBC without MARS) rejects loudly instead. The busy check is **bridge-level bookkeeping on handle liveness** (a shared slot set at query time, released on terminal state or destroy), *not* an engine-state inspection: `ClientContext::active_query` is private and cleared lazily at the next query's `InitialCleanup` (so checking it would deadlock the query → destroy → query pattern), and "open transaction" is the wrong predicate under explicit `BEGIN`. With the handle-liveness rule enforced, the engine's lazy cleanup becomes unobservable again — exactly the implementation detail it was meant to be. Callers who want concurrency open a second connection (cheap; same `DatabaseInstance`); `connection_interrupt` is the escape hatch for abandoning a stream from another thread.

- **Single consumer, cross-thread control through the connection.** Step/fetch/wait from one thread at a time. `connection_interrupt` (atomic store) and `connection_query_progress` (atomic reads) are the sanctioned cross-thread entry points. Note that `connection_query_progress` reports real values only when `enable_progress_bar` is set — the engine creates the `ProgressBar` (and updates the progress cache) only then; `-1`/`0`/`0` means "no information available". This mirrors V1's `duckdb_query_progress`; the bridge does not auto-enable tracking (a getter must not change execution behavior).

- **Do not consume a result inside `connection_execute_with_context`.** The callback runs holding the context lock that step/fetch/wait need; draining a stream there deadlocks. Drain before entering the callback. (Appending chunks to a `ColumnDataCollection` is not transactional — only CDC *creation* needs the context — so this costs no expressiveness.)

- **`duckdb_v2_result_handle` is a documented load-bearing wrapper** (`ResultWrapperV2` in `capi_v2_internal.hpp`): a state machine `PENDING → STREAMING → FINISHED | CANCELLED | ERRORED` over the engine's own two-phase objects (`PendingQueryResult`, then the `QueryResult` from `Execute()`), driven exclusively through their public surface. Prepare-time metadata (types, names, statement type, properties) is copied into the wrapper at construction so the metadata getters stay valid before the first step, during consumption, and after close. The result handle is *not* on the no-wrapper list (that list is logical_type / data_chunk / vector).

## Replacement scans

`duckdb_v2_replacement_scan_register(db, callback, user_data, err)` registers a callback the binder consults when a table name cannot be resolved (what makes `SELECT * FROM 'file.csv'` work). The callback claims the name by naming a target table function (`set_function_name` plus positional / named parameters), declines by returning without claiming, or fails by setting code/text on the err slot. It mirrors the table-function callback family (`(info, context, err)`) and uses the same `opaque` user-data channel (`{ptr, destroy, equals}`) as the function builders, not a bespoke destroy parameter. Three points to know:

- **Registration is not synchronized with running queries — register before issuing queries.** Registration appends to `DBConfig::replacement_scans`, which the binder iterates without a lock. V2 inherits this and documents the restriction rather than solving it. The underlying engine race is broader than the C API (extensions append at LOAD / autoload time while other connections bind, and `ReadCSVReplacement` can autoload parquet *mid-iteration* — `src/function/table/read_csv.cpp` → `extension/parquet/parquet_extension.cpp` — invalidating the very vector being walked). Engine-level synchronization (or a bridge-level dispatcher) is something to look into at some point.

- **Unqualified name parts read as the empty view, not `""`.** The engine reports an absent catalog / schema as an empty string; the `get_catalog_name` / `get_schema_name` getters translate that to the canonical `duckdb_v2_str` empty view `{NULL, 0}` per the borrowed-string convention. The table name is always a non-empty view.

- **Only the table-function target form is exposed.** The engine accepts any `TableRef` from a replacement callback (wrapping non-table-function, non-subquery refs in a `SubqueryRef` itself); the V2 surface deliberately exposes only the table-function form for now, leaving room for a view-query-style target later.

## V2 conventions

These rules apply when writing V2 spec YAML, bridge implementations, and tests. Most have been hard-won from PR1 review. A short reminder list:

- **The C API carries primitives only.** A function earns a place in the C API only when its capability is not composable client-side without engine knowledge; conveniences layer on top in the stable C++ API. Exceptions need a stated reason, with `vector_get_value` / `vector_set_value` as the template: they stay for totality (the only cell path over every vector representation and over kinds without a committed view layout).

- **Handle layout is load-bearing.** A V2 handle is a raw pointer to the underlying C++ object — *not* a wrapper struct — unless the wrapper is documented as load-bearing (`EnvironmentWrapperV2`, `OptionWrapperV2`, etc.). Handles are emitted as `tagged_struct` typedefs (`struct _duckdb_v2_x { void *internal_ptr; } * duckdb_v2_x_handle`), but that struct is a compile-time type tag only: the bridge `reinterpret_cast`s the C++ object pointer straight to it, so the runtime identity is still the bare object pointer. `duckdb_v2_logical_type_handle` specifically is a `duckdb::LogicalType *`; V1 and V2 share the same `new LogicalType(...)` allocation, so V2 destroy can free a V1-built handle. Direction matters: V1 → V2 destroy is OK; V2 → V1 destroy is not asserted and must not be relied on. **Do not wrap `duckdb_v2_logical_type` in a struct** — the identity is relied on by the deliberate V1-interop pins: the V1-oracle and the two cross-version round-trip tests in `test_capi_v2_logical_type.cpp`, the V1 `string_t` cross-validation in `test_capi_v2_string_layout.cpp`, the `[v1_v2_bridge]` value test, and the V1-only zero-entry-enum pins (SQL forbids the type; V2 inspection degrades gracefully). Same rule for `duckdb_v2_data_chunk_handle` (a `duckdb::DataChunk *`) and `duckdb_v2_vector_handle` (a `duckdb::Vector *`): no wrappers. PR4 verified neither needs to carry per-handle state — cardinality flows through the API explicitly, and the single untyped view-getter is thin enough to extract `(data, validity, sel)` directly from the core helpers without caching a `UnifiedVectorFormat`.

- **Cast helpers** (`ToEnv`, `ToDb`, `ToLogicalType`, …) live in `capi_v2_internal.hpp` next to the matching wrapper struct — not in per-module `.cpp` files.

- **Opaque user data crosses as one struct — `duckdb_v2_opaque` (#95).** Any caller-owned pointer the engine must hold (user data, bind data, init / global / local state) crosses the ABI as the single by-value `duckdb_v2_opaque {void *ptr; …destroy; …equals;}`, never a bespoke `(void *user_data, user_data_destroy)` pair — that pattern and the `user_data_copy` callback were removed in #95. In the bridge, hold it in the `OpaqueDataHandle` RAII wrapper (`capi_v2_internal.hpp`): `shared_ptr<OpaqueDataHandle>` when the data is threaded / copied across phases (bind data, builder user data), a plain `OpaqueDataHandle` by value for single-owner state. The stable C++ API builds one via `detail::MakeUserData<T>` (raw fn pointer + this channel). New user-data-taking surface must use this, not reinvent it.

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

- **V1/V2 hybrid prototyping.** Public V2 headers stay V1-free (`duckdb_v2.h` does not include `duckdb.h`). V2 `.cpp` implementations are free to wrap V1 or internal C++ machinery — the V2 contract is what we ship, not the implementation. **Tests are the only place V1 and V2 C headers may co-exist**, and only as deliberate interop validation pins. Fixture convenience is not a sanctioned reason: test fixtures use the V2 construction paths.

- **Spec / YAML style.** One description per function, lead with the contract. Annotate enum values only where the name alone is insufficient. No forward references to in-flight PRs ("(PR4)" ages badly). No first-person editorialising ("at this moment", "for now") — state the contract; deferral rationale lives in the spec's top-of-file commentary. For VARCHAR / string types use "null-terminated byte string", not "UTF-8" — DuckDB doesn't enforce encoding.

- **Vector reads are a single untyped view.** PR4 ships one `duckdb_v2_vector_view` struct (`{const void *data; const uint64_t *validity; const duckdb_v2_sel_t *sel, idx_t count; }`) and one `duckdb_v2_vector_get_view(vec, &view, err)` getter for every kind. Callers dispatch on `LogicalTypeId` and cast `view.data` to the matching typed pointer or layout typedef (`duckdb_v2_hugeint_t`, `duckdb_v2_interval_t`, `duckdb_v2_list_entry`, `duckdb_v2_varchar_t`/`blob_t`/`bit_t`/`bignum_t`, all aliases for `duckdb_v2_string`).

- **Structural descent uses two generic accessors.** `duckdb_v2_vector_get_child_count(vec, &n, err)` returns the number of children for a nested vector; `duckdb_v2_vector_get_child(vec, idx, &child, err)` borrows one. Per-kind index convention: LIST → `[0]` = elements; MAP → `[0]` = keys, `[1]` = values (V2 hides MAP's internal `LIST<STRUCT(K,V)>`); ARRAY → `[0]` = elements; STRUCT → `[i]` = field `i`; UNION → `[0]` = tag, `[1..N]` = members. "How many fields" / "how many members" are answered by `logical_type_get_param_count`, not by separate vector-side accessors. Every vector carries its own logical size, read with `duckdb_v2_vector_get_size(vec, &n, err)` and set with `duckdb_v2_vector_set_size(vec, n, err)` (which auto-reserves when growing past capacity). There is no list-specific size accessor and no chunk-level size setter: a LIST/MAP's element count is the size of its borrowed child handle (`vector_get_size(child)`, equivalent to `ListVector::GetListSize`), and `data_chunk_get_size` survives only for the read path, where the engine sets the chunk's cardinality.

- **`DUCKDB_V2_VECTOR_TYPE_OTHER` is the 0-value.** Zero-init of a `DUCKDB_V2_VECTOR_TYPE` out-param reads as "unspecified / needs flatten" rather than silently looking like "FLAT". The V2 enum doesn't round-trip with `duckdb::VectorType`; the bridge maps via an explicit switch.

- **Selection vectors mirror `UnifiedVectorFormat`.** `view.sel == NULL` for FLAT vectors (means identity); non-null for CONSTANT (zero singleton) and DICTIONARY (the dictionary's own sel). Row resolution is the inline expression `sel ? sel[i] : i`; the `sel == NULL` identity convention is part of the V2 contract, and there is no bridge helper (it would fail the primitives test). **Validity follows sel, not the loop counter:** for DICTIONARY the validity index is `sel[i]`, not `i`. Reading `validity[i]` directly produces wrong answers; always resolve through `sel` first.

- **Length-delimited strings (`duckdb_v2_str`).** Every borrowed string crossing the boundary, both inputs (option/SQL/function names, file paths, log messages) and outputs (option getters, `schema_get_field` (the field name), `logical_type_get_name`, `logical_type_get_param` (the name), `expression_get_function_name`, `error_info_get_text`), is a `duckdb_v2_str { const char *ptr; idx_t len; }`. It is a *borrowed view*: NOT null-terminated, may contain interior NULs, and `{NULL, 0}` is the canonical empty view (`ptr` must not be dereferenced when `len == 0`). Lifetime is documented per producing function (typically "valid until the owning handle is destroyed"). Validation contract for inputs: `{NULL, 0}` is a valid empty string; only a null pointer with a *nonzero* length is malformed and rejected with `ERROR_INVALID_INPUT`. Do not confuse `duckdb_v2_str` (the decoded view) with `duckdb_v2_string` (the 16-byte VARCHAR/BLOB/BIT/BIGNUM *storage*, a transparent union mirroring `duckdb::string_t`). Owned string returns that the caller must `free()` (e.g. `value_to_string`, `library_version`) keep their `char **` shape and are not `duckdb_v2_str`.

- **String-backed kinds: transparent storage, decoders only for real wire codecs.** The storage type `duckdb_v2_string` is a **transparent** union mirroring `duckdb::string_t` (`value.pointer.{length,prefix,ptr}` / `value.inlined.{length,inlined}`, inline cutoff `DUCKDB_V2_STRING_INLINE_LENGTH`), spec-declared and capigen-emitted: callers read its fields directly and stride `arr[row]`; `static_assert`s in `capi_v2_internal.hpp` guard the layout match. VARCHAR / BLOB carry no wire encoding, so reading them is that direct field read and there is no varchar/blob decoder (one would be a redundant field read, failing the primitives test). BIT and BIGNUM do carry one and keep their bridge decoders: `duckdb_v2_bit_decode` peels the padding byte, `duckdb_v2_bignum_decode` produces an owned magnitude buffer from the bit-inverted negative storage (sharing `DecodeBignumStringT` with `value_get_bignum`). Per-kind read conveniences live in the stable C++ API.

- **Writing string-backed values is decomposed: get-heap, allocate, assemble, place.** There is no per-value `assign_string` and no copy-in bridge. Borrow the vector's string heap once with `duckdb_v2_vector_get_string_heap` (the single string-ness check; valid for the physical-VARCHAR kinds VARCHAR / BLOB / BIT / BIGNUM), reserve vector-lifetime bytes with `duckdb_v2_string_heap_allocate` (raw arena allocation: any `byte_len` incl. 0, no string semantics, no gating), write them, assemble a `duckdb_v2_string` over the transparent layout, and place it through `duckdb_v2_vector_get_data_mutable`. Values fitting `DUCKDB_V2_STRING_INLINE_LENGTH` need no allocation. The raw C layer is deliberately *only* arena allocation: now that `duckdb_v2_string` is transparent, assembling the value is plain layout work the caller (or the C++ layer) does without crossing the boundary, so a copy-in bridge would be redundant. Lifetime: allocated bytes, and any non-inlined `duckdb_v2_string` referencing them, are valid only in a slot of the owning vector; inlined values are self-contained. Ergonomics live in the C++ API: `Vector::AssignString` / `AssignStrings` for in-order fills, and `Vector::GetStringHeap` -> `StringHeap::Allocate` / `Add` / `AddMany` -> `Vector::SetString` for dedup / scatter / write-in-place, with `StringLayout` mirroring `duckdb_v2_string`.

- **Expressions are one generic handle, scoped to *bound* expressions.** `duckdb_v2_expression` is a single borrowed handle over `duckdb::Expression` — the post-binding tree. Unbound/parser-level `duckdb::ParsedExpression` is a separate internal hierarchy and out of scope, so `get_class` only ever yields `BOUND_*` values; the parsed-class values exist in `EXPRESSION_CLASS` purely for numeric fidelity with `duckdb::ExpressionClass` (switch on `BOUND_COLUMN_REF`, not the parsed `COLUMN_REF`, etc.). The generic core never errors on class grounds: `get_class` / `get_type` / `get_return_type`; `get_child_count` / `get_child`, which traverse via the engine's own `ExpressionIterator` so they're **total over every bound class** (a node this API doesn't model specially, e.g. `BOUND_CASE`, still exposes its children rather than looking like a leaf — child order follows the iterator). The class-specific accessors return a class-mismatch error off their class: `get_function_name` (`BOUND_FUNCTION`; the registered name — an internal symbol like `__comparison` for comparisons, so dispatch on `get_type` for the operator), `get_constant_value` (`BOUND_CONSTANT`), `get_column_binding` (`BOUND_COLUMN_REF` — the logical `{table_index, column_index}` seen during binding/optimization incl. filter pushdown), and `get_reference_index` (`BOUND_REF` — the execution-stage chunk slot assigned after physical planning, *not* what pushdown sees). We deliberately did *not* introduce a base handle plus narrowed sub-handles (`expression_as_function` / `_as_constant` / …): with so few class-specific accessors, a narrowing gate fails in exactly the same way and at the same call site as the direct accessor. Revisit when the class-specific surface grows (cast `try_cast` / target type, aggregate `distinct` / `filter`, function bind-info) — at that point a successful `expression_as_X` becomes a single checkpoint validating a whole cluster of X-only accessors. `get_name`/`get_function_name` naming and `get_column_binding` vs `get_reference_index` mirror the bound class names exactly.

- **The spec schema has grown beyond bare declarations.** Use these capigen features rather than hand-rolling equivalents:
  - `description:` on `handles` / `aliases` / `structs` (rendered as `//!` comments via `_c_line_comment`) — don't put per-handle docs in the function descriptions that mention them.
  - `prefix:` in `metadata.yaml` — the IDL is prefix-free; `duckdb_v2_` is applied at generation time. New module YAML must not bake the prefix into type/function names.
  - `tagged_struct` handle style is the **default** (`options.c.handles.default_style` in `metadata.yaml`), so handles are typed `struct _duckdb_v2_x *` rather than a bare `void *`; the per-handle `override_style` map opts an individual handle back to `void *` when needed. Handle typedefs carry the `_handle` suffix (the old `_ptr` suffix was renamed).
  - `qualified` flag on aliases — lets an alias reference an external type name unchanged (no prefix, no `_t` suffix).
  - See the capigen repository's `schema_reference.md` and `CLAUDE.md` (spec-language features) for YAML syntax, generated-C output, and caveats per feature.

## Generating the header and stubs

After changing the YAML specs, regenerate the header (`src/include/duckdb_v2.h`) and the bridge stubs (`src/main/capi_v2/capi_v2_stubs.cpp`):

```bash
./scripts/capi_v2_regen.sh
```

This runs both `capigen` adapters (`c` for the header, `bridge` for the stubs) and then formats the output via `scripts/format.py`. The same script is invoked automatically by the `capi-v2-regen` pre-commit hook whenever you stage an `api_spec/v2/**/*.yaml` change, so committing without a manual run is also fine — the hook regenerates, the format hook re-formats, and pre-commit asks you to re-stage.

The generator's own tests live in the capigen repository.

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

DUCKDB_V2_API_CALL_t duckdb_v2_option_create(duckdb_v2_str name, duckdb_v2_str setting,
                                             duckdb_v2_option_handle *out_option,
                                             duckdb_v2_error_info_handle *err) {
    return duckdb::WithErrorHandler(err, [&]() {
        // A {NULL, 0} view is a valid empty string; only a null pointer with a
        // nonzero length is malformed.
        if ((!name.ptr && name.len > 0) || (!setting.ptr && setting.len > 0) || !out_option) {
            throw duckdb::InvalidInputException("null argument to duckdb_v2_option_create");
        }
        *out_option = nullptr;
        auto wrapper = duckdb::make_uniq<duckdb::OptionWrapperV2>();
        wrapper->name = duckdb::ToString(name);
        wrapper->setting = duckdb::ToString(setting);
        *out_option = reinterpret_cast<_duckdb_v2_option *>(wrapper.release());
    });
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
- **Out-param zeroing on failure is partial and not guaranteed.** Pointer-typed out-params (`out_value`, `out_type`, `out_data`, `out_string`) are *usually* set to `nullptr` on `INVALID_INPUT` paths, but not on every path; scalar out-params (`out_micros`, `out_lower`, `out_width`, …) are left *unspecified* on failure. The return code is authoritative: never read any out-param after a non-`NONE` return.
- **Error codes are 32-bit: `(group_id << 16) | code`.** Don't hard-code the numeric value — use the generated macro name (`DUCKDB_V2_ERROR_*`).
- **Every declared function must return an error code** (`DUCKDB_V2_API_CALL_t` or an alias). Don't use `void` or pointer-returning signatures; results come back through out-params.
- **Primitives are declared in `api_spec/v2/metadata.yaml`.** If you need a new one, add it there first with its C ABI type under `c_type`.

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

The `.github/workflows/v2-capi.yml` workflow runs on every push to `main` and on PRs, as two jobs:
- `format` — provisions the root venv with `uv sync --group dev`, then runs `pre-commit run --all-files` (default stages: regen, check-yaml, yamlfmt) followed by `pre-commit run --all-files --hook-stage manual` (full-tree `scripts/format.py --all --check`). Finally `git diff --exit-code` fails the job if the committed headers or stubs are out of sync with `api_spec/`.
- `build` — builds with `make relassert` (`FORCE_DEBUG=1 FORCE_ASSERT=1`, i.e. RelWithDebInfo + ASan/UBSan/LSan plus the `-DDEBUG` slow verifiers; clang-20, ninja + ccache via the `./.github/actions/ccache-action` composite action), then runs `make unittest_relassert T="[capi_v2],[capi]"` — both the V2 bridge tests and the V1 `[capi]` regression. Two further steps run the SQL `SET` regression suites (`duckdb_settings*`, `[settings]`, `[reset]`), which exercise the same `PhysicalSet::ApplyVariable` path the V2 `*_option_set` bridges delegate to.

A second workflow, `.github/workflows/sqllogic-cpp-api.yml`, runs nightly (and on demand). It runs the full sqllogic suite through the stable C++ API executor (`CppApiSQLLogicExecutor`) and diffs it against the internal `ClientContext::Query` path across the upstream configuration matrix and platforms (Linux configs, Linux/macOS/Windows default, sanitizer configs), failing only on tests that regress under the C-API runner but pass under the internal one.

## Companion docs

- **capigen** (github.com/duckdb/capigen): the code generator, published to PyPI and pinned in the root `pyproject.toml`. Its `README.md` covers generator usage, `schema_reference.md` is the IDL field reference, and `CLAUDE.md` documents the spec-language features. The spec conventions specific to this fork are under "Making changes to the API spec" above.
