# PhysicalType::GEOMETRY — notes for review

Working notes for the `geometry-physical-type` branch. Plan lives at
`~/.claude/plans/i-want-to-implement-serene-lake.md`. Delete this file before the PR.

## Build environment

- After your clean rebuild, `ninja -C build/reldebug unittest` works with no env-var workaround.
  (Earlier in the session the vendored spatial FetchContent checkout was blocking CMake
  regeneration and needed `DUCKDB_SKIP_APPLYING_PATCHES=1`; that is no longer necessary.)
- **`build/reldebug` does not have assertions on**, despite the name: `CMAKE_BUILD_TYPE=RelWithDebInfo`,
  `FORCE_ASSERT:BOOL=OFF`, and the compile line carries `-DNDEBUG`. Passing
  `FORCE_DEBUG=1 FORCE_ASSERT=1` to `make reldebug` does *not* change the cached values — CMake keeps
  the existing cache. Use `make relassert` (separate `build/relassert` dir, passes `-DFORCE_ASSERT=1`)
  when assertions matter.

- **The formatter has not been run.** Neither `clang-format` nor `black` is installed in the
  environment I'm working in, so `make format-fix` / `scripts/format.py` cannot run. Code was written
  to match the surrounding style (tabs, <=120 cols) by hand, but **please run `make format-fix`
  before the PR** — expect churn, especially in `test/common/test_geometry_type.cpp` where the WKT
  corpus array is space-aligned.

## Open / needs your call

### 1. Verify assertion coverage on every future change  (Part 1)

Because `-DNDEBUG` is set in `build/reldebug`, `D_ASSERT` is a no-op there and
**`geometry_t::Verify()` compiles to nothing**. Everything this design leans on for safety —
recursive `Verify()`, the `PointerIsAligned` checks in `SetVertices` / `SetParts` / `CopyToHeap`,
the `flags == 0` check — is dead code in that build.

So a green `[geometry_type]` run in `build/reldebug` proves the codecs round-trip and nothing about
the alignment invariant, which is the entire reason the type exists. Always re-run
`build/relassert/test/unittest "[geometry_type]"` before believing an alignment-related change.

### 2. Behaviour changes that will show up in tests

- **Deeply nested WKB is now rejected.** `AnalyzeWKB` (geometry.cpp:900) walks the blob with a flat
  `while (!reader.IsAtEnd())` loop, so it has never seen nesting depth at all — `st_geomfromwkb`
  will happily store a 10k-deep `GEOMETRYCOLLECTION` today. Every `geometry_t` operation recurses,
  so that becomes a stack overflow. The limit is enforced in the recursive readers
  (`geometry_t::DeserializeWKB` / `Deserialize`) against `MAX_RECURSION_DEPTH = 16`.
  This is a user-visible change for pathological WKB input.
- `"POLYGON (EMPTY)"` is **not** accepted by the WKT parser (a ring cannot be `EMPTY`), so it is not
  in the test corpus. `MULTIPOINT (EMPTY)` and `MULTILINESTRING (EMPTY)` are.

### 3. Deferred within Part 1

Vector-arena overloads (`Deserialize(string_t, Vector&)`, and all vectorized
`Serialize`/`Deserialize`/`SerializeWKB`/`DeserializeWKB` over `Vector`) are **not declared yet** —
they need `GeometryVector::GetArena`, which arrives in Part 3. The header only declares what is
implemented.

## Decisions worth re-checking later

- `geometry_t::operator<` is **structural** (type, vertex type, count, then coordinate bytes), not
  WKB byte order. Fine per the "sort order is not a contract" decision, but it means `ORDER BY geom`
  results change. Grep `test/sql/types/geo/` for orderings that need re-baselining before Part 6.
- **ART keys stay on WKB** deliberately. `art.cpp`'s conversion gate is per-database, so it cannot
  re-encode an index that v1.5 already persisted; reusing the native sort-key encoding there would
  silently invalidate them. Guards: `test/sql/storage/rowid_gaps/indexed_geometry*`.
- The reserved `uint16_t flags` in `geometry_t` is checked for zero on the native decode path, so it
  is a real forward-compatibility gate, not just padding.

## Landmines found in existing code (fix during Part 6)

- `src/function/scalar/geometry/geometry_functions.cpp:103` —
  `StringVector::AddHeapReference(input.data[0], result)` has its **arguments backwards**: it adds
  *result*'s heap to *input* rather than the other way round.
- `CMHelper::GeometryIsAllPointXY` (`src/optimizer/compressed_materialization.cpp:760-773`) relies on
  "an empty POINT is a 21-byte NaN blob". With `geometry_t` a `POINT EMPTY` has **no vertex array**,
  so compressed materialization would read through a null pointer. Needs
  `&& !GeometryStats::GetFlags(stats).HasEmptyGeometry()` plus a `POINT EMPTY` test case.
- `GeometryStats::Verify` (`src/storage/statistics/geometry_stats.cpp:196`) is an empty TODO.
- `duck_table_entry.cpp:118` — CRS on pre-1.5 storage logs a warning with an explicit
  `// TODO: Turn this into a hard error`.
