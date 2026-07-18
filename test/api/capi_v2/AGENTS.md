# Writing V2 tests

Catch2 bridge tests for the V2 C API and the stable C++ API. Two suites, two tags:

- `test_capi_v2_*.cpp`, tag `[capi_v2]`: the C API bridge tests.
- `test_cpp_api_*.cpp`, tag `[cpp_api]`: the stable C++ API (`duckdb_api`) tests.

Add cases to the matching module file; for a new module add `test_capi_v2_<module>.cpp` (or `test_cpp_api_<module>.cpp`) and wire it into `CMakeLists.txt`.

Full design rationale: `api_spec/C_API_V2.md`. Build and run mechanics: the "C API V2 development" section of `CONTRIBUTING.md`.

## Conventions

- **Dogfood V2.** Test fixtures use the V2 construction paths, not V1 for convenience. Tests must exercise the surface the way a real consumer would.
- **V1 and V2 headers co-exist only here.** Tests are the one place the V1 and V2 C headers may both be included, and only as deliberate interop validation pins (the cross-version round-trip and `string_t` cross-validation tests). Fixture convenience is not a sanctioned reason.
- **Fixture-builder ordering.** A helper that allocates intermediate fixtures (struct/union member types, list child types) must destroy the intermediates **before** any `REQUIRE`. Catch2 throws on failure, so a destroy placed after a `REQUIRE` is skipped and leaks:

  ```cpp
  auto v1 = duckdb_create_struct_type(members, names, n);
  duckdb_destroy_logical_type(&members[0]);
  duckdb_destroy_logical_type(&members[1]);
  REQUIRE(v1 != nullptr);
  return V1ToV2(v1);
  ```

- **Borrowed-string agreement.** Pin at least one `strlen(out) == len` check per module to prove the null-terminated form and the length agree.

## Regression guard

V1 must stay functional. After any non-trivial change to shared DuckDB code, run the V1 C API tests as a regression check:

```bash
./build/debug/test/unittest "[capi]"
```
