# DuckDB V2 Python Client

Python client for DuckDB using the V2 C API.

## Goals

- Reduce pain of the integration with DuckDB internals by further decoupling: move to C API v2.
- Support for parallel Python UDFs.
- Reduce the build matrix by using Python's stable ABI / Limited API.
- Features:
   - DB-API v2
   - Replacement scans of dataframes (Polars, Pandas) and Arrow (tables, record batches, Arrow C streams)
   - Pushdown of projections and filters into scanned objects, if libraries are available
   - Parallel UDFs: table functions, scalar functions
 
This translates to:
- Compatibility with free-threading (FT) Python (3.13t, 3.14t and beyond). This cannot use the stable ABI, so we will need to isolate and gate what we need for this and build 3.13t and 3.14t wheels for all platforms and duckdb versions.
- Compatibility with `concurrent.interpreters` to achieve parallelism (Python >= 3.12).
- Limited API / Stable ABI only for non FT Python builds. This means: one wheel per platform per duckdb version for all python >= 3.12. Python < 3.12 does not have support for multiple interpreters.
- Compatibility with C++ 17.
- Usable on OSX, Linux and Windows, and on each of these for X86_64 and ARM64.

If this proofs doable and useful then we need to answer the following:
- What should we do with the Relational API? There is no C API support for that right now.
- What should we do with the PySpark compatibility API? This depends heavily on the Relational API.
- Should we expose the appender API?

## Design Decisions

- Since Neo, nanobind landed Stable ABI (Python >= 3.12) and free-threading support. We'll not use our own `capiext` framework anymore.
- Scikit-build-core instead of Meson, because we are all using cmake.
- UDFs:
   - Zero-copy, both for duckdb native.

Open questions:
- Allocation: we use buffer-protocol/memoryview wrappers where we can. Maybe we should also have thread-local allocation?
