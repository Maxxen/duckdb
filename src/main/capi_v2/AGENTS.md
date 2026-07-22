# Writing V2 bridge implementations

This directory holds the C++ code that implements the V2 C ABI (the bridge from C down to DuckDB internals). `capi_v2_internal.hpp` carries the wrapper structs and shared helpers; `capi_v2_stubs.cpp` is generated and holds a stub for every declared function not yet implemented here.

Full design rationale for everything below: `api_spec/C_API_V2.md` (error model, handle layout, the C++ API). Build and test mechanics: the "C API V2 development" section of `CONTRIBUTING.md`.

## Implementing a stub

1. Create a `.cpp` in this directory (e.g. `option-v2.cpp`), include `capi_v2_internal.hpp`, write the implementation.
2. Add the file to `CMakeLists.txt`.
3. Re-run `./scripts/capi_v2_regen.sh`: the bridge generator drops the stub for any function it finds implemented here.
4. Rebuild and test.

```cpp
#include "capi_v2_internal.hpp"

DUCKDB_V2_ERROR duckdb_v2_option_create(duckdb_v2_str name, duckdb_v2_str setting,
                                             duckdb_v2_option_handle *out_option,
                                             duckdb_v2_error_info_handle *err) {
    return duckdb::WithErrorHandler(err, [&]() {
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

## Error reporting

- Wrap entry-point bodies in `WithErrorHandler(err, [&]{ ... })`: it try/catches, translates thrown DuckDB exceptions to V2 error codes, and writes the slot **only on failure** (lazy-allocate on first use, never destroy). On success it returns `DUCKDB_V2_ERROR_NONE` and leaves the slot untouched.
- Callback trampolines allocate a stack `ErrorInfoV2`, hand the callback a slot pointing at it, inspect its code after the callback returns, and rethrow a set code as a DuckDB exception the outer `WithErrorHandler` catches.
- `SetErrorInfo` is the low-level slot writer (lazy-allocate, overwrite in place, never destroy, `NULL`-safe). There is no success-path helper.
- The return code is authoritative. Out-param zeroing on failure is partial and not guaranteed: never rely on reading an out-param after a non-`NONE` return.

## Load-bearing conventions

- **Handle layout.** A V2 handle is a raw pointer to the underlying C++ object, not a wrapper struct, unless the wrapper is documented as load-bearing (`EnvironmentWrapperV2`, `OptionWrapperV2`, `ResultWrapperV2`, ...). `logical_type` / `data_chunk` / `vector` handles are bare `LogicalType *` / `DataChunk *` / `Vector *`: **do not wrap them**, the identity is relied on by V1-interop test pins.
- **No exceptions across the C ABI.** Allocating sites (`new`, builders) wrap in `try { ... } catch (std::exception &e) { ... } catch (...) { ... }`. Both arms are required. Non-allocating accessors are left unwrapped only when a file-level exception-policy comment explains why; accessors that reach internals which throw `InternalException` on shape violations (e.g. `FlatVector::GetData<T>` on a non-FLAT vector) must still be wrapped.
- **Cast helpers** (`ToEnv`, `ToDb`, `ToLogicalType`, ...) live in `capi_v2_internal.hpp` next to their wrapper struct, not in per-module `.cpp` files.
- **Opaque user data** crosses as the single by-value `duckdb_v2_opaque {void *ptr; ...destroy; ...equals;}`. Hold it in the `OpaqueDataHandle` RAII wrapper: `shared_ptr<OpaqueDataHandle>` when threaded across phases, by value for single-owner state. Never reinvent a bespoke `(void *user_data, destroy)` pair.
- **File-private helpers** go in an anonymous namespace inside `namespace duckdb { ... }`. Under a unity build this does not isolate names between concatenated files: name uniqueness is the real defense against collisions.

## Stub-detection gotcha

The bridge adapter finds implementations by matching `\bduckdb_v2_\w+\s*\(` in the `.cpp` files. **Write each implementation as a literal function definition.** A macro-generated body (`SOME_MACRO(duckdb_v2_value_get_int8, ...)`) is not detected, so its stub silently survives in `capi_v2_stubs.cpp`, producing a multiple-definition link error or a stub shadowing the real function. Unroll macros into explicit `DUCKDB_V2_ERROR duckdb_v2_...(...) { ... }` definitions. Hand-written bridges are never overwritten by regeneration; a renamed/removed spec function leaves its orphaned implementation until you delete it manually.
