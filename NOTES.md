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

## How to verify this work

`build/reldebug` compiles with `-DNDEBUG`, so `D_ASSERT` is a no-op there and
**`geometry_t::Verify()` compiles to nothing**. A green `[geometry_type]` run in `build/reldebug`
proves the codecs round-trip and says *nothing* about the alignment invariant, which is the whole
reason the type exists.

Use `build/relassert` for anything alignment-related — `make relassert` gives you
`-DDUCKDB_FORCE_ASSERT` **plus ASan and UBSan**:

```
ninja -C build/relassert unittest && build/relassert/test/unittest "[geometry_type]"
```

(Watch out: `make ... | tail` reports *tail's* exit code, so a failed build can look like success.
Grep the output for `error:` instead. Also redirect stdin — `unittest` backgrounded without
`</dev/null` blocks on the terminal and sits at 0% CPU forever, which looks like a hang.)

### Full-suite baseline in this checkout

`build/reldebug/test/unittest` at Part 3: **5245 passed, 18 failed, 418 skipped**.

All 18 failures are `test/extension/*` and `test/sql/extensions/*` trying to `LOAD` the
loadable-demo extensions, which are not built in this configuration
(`build/reldebug/test/extension/*.duckdb_extension` does not exist). They are environmental and
unrelated to this branch — but they mean "18 failed" is the **green** baseline here, so don't read
it as a regression. All 28 `test/sql/types/geo/` tests pass.

## Where this got to

Parts 1-4 of the plan are done and committed, one commit each (plus a small fixup). Part 5 is
**partially** done - see below.

| Part | State |
| --- | --- |
| 1. `geometry_t` + byte codecs | done |
| 2. `PhysicalType::GEOMETRY`, dormant | done |
| 3. `VectorGeometryBuffer` | done |
| 4. Uncompressed GEOMETRY storage, dormant | done |
| 5. Row layout / containers / sort+ART keys | **tuple-data only**, see below |
| 6. The flip | not started |

### Part 5: what is done and what is left

Done (tuple data):
- `tuple_data_layout.cpp` - GEOMETRY gets a fixed 16-byte row slot plus heap
- `tuple_data_scatter_gather.cpp` - `TupleDataValueStore<geometry_t>`, `TupleDataValueVerify<geometry_t>`,
  `ComputeGeometryHeapSizesInternal` and its dispatch, scatter/gather function cases
- `tuple_data_allocator.cpp` - `RecomputeHeapPointers` and `FindHeapPointers` cases
- **A generic change worth reviewing carefully:** `TupleDataTemplatedGatherInternal` now does its
  `Load<T>` *inside* the valid branch instead of unconditionally. An invalid row's payload is
  uninitialized, and for a pointer-carrying type like `geometry_t` we would then `Verify()` a garbage
  pointer. This affects **every** type, not just geometry, which is why the full suite matters here.

Still to do in Part 5:
- `ColumnDataCollection` - `GeometryValueCopy` for the in-memory allocators, the buffer-manager
  swizzle path (needs `+7` slack and an `AlignPointer<8>` on the heap base - the old prototype
  ignored alignment here entirely), and the `ReadVector` case in `column_data_collection_segment.cpp`
- `vector_hash.cpp`, `vector_copy.cpp`, `comparison_operators.cpp`,
  `value_operations/comparison_operations.cpp`, `row_matcher.cpp`
- `list_segment.cpp`
- `create_sort_key.cpp` - `SortKeyGeometryOperator` wrapping the native codec
- `art.cpp` / `art_key.cpp` - keep ART keys on WKB

### Not started: Part 6 (the flip)

`src/common/types.cpp:169` still returns `PhysicalType::VARCHAR` for GEOMETRY. Everything above is
additive and unreachable until that line changes. The plan's Part 6 checklist has the full ordering;
the landmines section below lists the specific bugs to fix while doing it.

## Open / needs your call

### 1. Behaviour changes that will show up in tests

- **Deeply nested WKB is now rejected.** `AnalyzeWKB` (geometry.cpp:900) walks the blob with a flat
  `while (!reader.IsAtEnd())` loop, so it has never seen nesting depth at all — `st_geomfromwkb`
  will happily store a 10k-deep `GEOMETRYCOLLECTION` today. Every `geometry_t` operation recurses,
  so that becomes a stack overflow. The limit is enforced in the recursive readers
  (`geometry_t::DeserializeWKB` / `Deserialize`) against `MAX_RECURSION_DEPTH = 16`.
  This is a user-visible change for pathological WKB input.
- `"POLYGON (EMPTY)"` is **not** accepted by the WKT parser (a ring cannot be `EMPTY`), so it is not
  in the test corpus. `MULTIPOINT (EMPTY)` and `MULTILINESTRING (EMPTY)` are.

### 2. Deferred within Part 1

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
