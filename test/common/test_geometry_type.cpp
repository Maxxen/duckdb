#include "catch.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/geometry_type.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

//! The WKT corpus: every geometry type crossed with every vertex type, plus the empty cases
const char *const GEOMETRY_CORPUS[] = {
    "POINT (1 2)",
    "POINT Z (1 2 3)",
    "POINT M (1 2 4)",
    "POINT ZM (1 2 3 4)",
    "POINT EMPTY",
    "POINT Z EMPTY",
    "POINT M EMPTY",
    "POINT ZM EMPTY",
    "LINESTRING (1 2, 3 4)",
    "LINESTRING Z (1 2 3, 4 5 6)",
    "LINESTRING M (1 2 4, 4 5 7)",
    "LINESTRING ZM (1 2 3 4, 4 5 6 7)",
    "LINESTRING EMPTY",
    "POLYGON ((0 0, 1 0, 1 1, 0 0))",
    "POLYGON ((0 0, 10 0, 10 10, 0 0), (1 1, 2 1, 2 2, 1 1))",
    "POLYGON Z ((0 0 1, 1 0 1, 1 1 1, 0 0 1))",
    "POLYGON M ((0 0 1, 1 0 1, 1 1 1, 0 0 1))",
    "POLYGON ZM ((0 0 1 2, 1 0 1 2, 1 1 1 2, 0 0 1 2))",
    "POLYGON EMPTY",
    "MULTIPOINT (1 2, 3 4)",
    "MULTIPOINT Z (1 2 3, 4 5 6)",
    "MULTIPOINT EMPTY",
    "MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))",
    "MULTILINESTRING ZM ((1 2 3 4, 5 6 7 8))",
    "MULTILINESTRING EMPTY",
    "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))",
    "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))",
    "MULTIPOLYGON M (((0 0 1, 1 0 1, 1 1 1, 0 0 1)))",
    "MULTIPOLYGON EMPTY",
    "GEOMETRYCOLLECTION (POINT (1 2))",
    "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))",
    "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 2)))",
    "GEOMETRYCOLLECTION Z (POINT Z (1 2 3))",
    "GEOMETRYCOLLECTION EMPTY",
    // Degenerate shapes that still have to round-trip
    "MULTIPOINT (EMPTY)",
    "MULTILINESTRING (EMPTY)",
    "GEOMETRYCOLLECTION (POINT EMPTY)",
};

//! WKT -> the normalized WKB the rest of the system stores
string_t ToWKB(const char *wkt, StringHeap &heap) {
	string_t result;
	REQUIRE(Geometry::FromString(string_t(wkt), result, heap, true, optional_idx::Invalid()));
	return result;
}

//! Hex so a mismatch prints something readable
string ToHex(const string_t &blob) {
	static const char *HEX = "0123456789ABCDEF";
	string result;
	const auto data = const_data_ptr_cast(blob.GetData());
	for (idx_t i = 0; i < blob.GetSize(); i++) {
		result.push_back(HEX[data[i] >> 4]);
		result.push_back(HEX[data[i] & 0x0F]);
	}
	return result;
}

} // namespace

TEST_CASE("geometry_t WKB codec round-trips", "[geometry_type]") {
	StringHeap heap;
	ArenaAllocator arena(Allocator::DefaultAllocator());

	for (const auto wkt : GEOMETRY_CORPUS) {
		INFO(wkt);
		const auto wkb = ToWKB(wkt, heap);

		// WKB -> geometry_t -> WKB must be byte-identical
		const auto geom = geometry_t::DeserializeWKB(wkb, arena);
		REQUIRE(geometry_t::GetWKBSize(geom) == wkb.GetSize());

		string round_tripped;
		geometry_t::SerializeWKB(geom, round_tripped);
		REQUIRE(ToHex(string_t(round_tripped)) == ToHex(wkb));

		// ...and the WKT we get back out has to match what we put in
		REQUIRE(Geometry::ToString(heap, string_t(round_tripped)).GetString() == string(wkt));
	}
}

TEST_CASE("geometry_t native codec round-trips", "[geometry_type]") {
	StringHeap heap;
	ArenaAllocator arena(Allocator::DefaultAllocator());

	for (const auto wkt : GEOMETRY_CORPUS) {
		INFO(wkt);
		const auto geom = geometry_t::DeserializeWKB(ToWKB(wkt, heap), arena);

		string native;
		native.resize(geometry_t::GetSerializedSize(geom));
		const auto written = geometry_t::Serialize(geom, data_ptr_cast(&native[0]));
		REQUIRE(written == native.size());

		const auto decoded = geometry_t::Deserialize(string_t(native), arena);
		REQUIRE(decoded == geom);
		REQUIRE(!(decoded < geom));
		REQUIRE(!(geom < decoded));
	}
}

TEST_CASE("geometry_t heap codec round-trips", "[geometry_type]") {
	StringHeap heap;
	ArenaAllocator arena(Allocator::DefaultAllocator());

	for (const auto wkt : GEOMETRY_CORPUS) {
		INFO(wkt);
		const auto geom = geometry_t::DeserializeWKB(ToWKB(wkt, heap), arena);

		const auto heap_size = geometry_t::GetHeapSize(geom);
		// Every allocation is a whole number of 8-byte units, which is what keeps the arrays aligned
		REQUIRE(heap_size % 8 == 0);

		auto block = make_unsafe_uniq_array<data_t>(heap_size + 8);
		auto block_ptr = AlignPointer<8>(block.get());

		const auto copied = geometry_t::CopyToHeap(geom, block_ptr, heap_size);
		copied.Verify();
		REQUIRE(copied == geom);

		// Move the block and fix the pointers up, exactly as the row layout does
		auto moved = make_unsafe_uniq_array<data_t>(heap_size + 8);
		auto moved_ptr = AlignPointer<8>(moved.get());
		memcpy(moved_ptr, block_ptr, heap_size);

		auto relocated = copied;
		geometry_t::RecomputeHeapPointers(relocated, block_ptr, moved_ptr);
		relocated.Verify();
		REQUIRE(relocated == geom);
	}
}

TEST_CASE("geometry_t NULL geometry is inert", "[geometry_type]") {
	ArenaAllocator arena(Allocator::DefaultAllocator());

	auto null_geom = geometry_t::CreateNull();
	null_geom.Verify();

	REQUIRE(null_geom.IsNull());
	REQUIRE(null_geom.IsEmpty());
	REQUIRE(null_geom.GetDataPointer() == nullptr);

	// The row layout stores NullValue<geometry_t>() for invalid rows and then scatters it, so
	// none of these may allocate or throw
	REQUIRE(geometry_t::GetHeapSize(null_geom) == 0);
	const auto copied = geometry_t::CopyToHeap(null_geom, nullptr, 0);
	REQUIRE(copied.IsNull());

	auto relocated = null_geom;
	geometry_t::RecomputeHeapPointers(relocated, nullptr, nullptr);
	REQUIRE(relocated.IsNull());

	REQUIRE(geometry_t::Copy(null_geom, arena).IsNull());
	REQUIRE(null_geom == geometry_t::CreateNull());
}

TEST_CASE("geometry_t deep copy is independent", "[geometry_type]") {
	StringHeap heap;
	ArenaAllocator source_arena(Allocator::DefaultAllocator());
	ArenaAllocator target_arena(Allocator::DefaultAllocator());

	for (const auto wkt : GEOMETRY_CORPUS) {
		INFO(wkt);
		const auto geom = geometry_t::DeserializeWKB(ToWKB(wkt, heap), source_arena);
		const auto copy = geometry_t::Copy(geom, target_arena);
		copy.Verify();
		REQUIRE(copy == geom);
		if (!geom.IsEmpty()) {
			// A real copy, not an alias
			REQUIRE(copy.GetDataPointer() != geom.GetDataPointer());
		}
	}
}

TEST_CASE("geometry_t rejects WKB nested past the recursion limit", "[geometry_type]") {
	ArenaAllocator arena(Allocator::DefaultAllocator());

	// AnalyzeWKB walks the blob with a flat loop, so it happily accepts arbitrarily deep nesting.
	// The recursive geometry_t reader is where the limit has to be enforced.
	const auto build_nested = [](idx_t depth) {
		string wkb;
		for (idx_t i = 0; i < depth; i++) {
			wkb.push_back(1); // little-endian
			const uint32_t type = 7;
			const uint32_t one = 1;
			wkb.append(const_char_ptr_cast(&type), sizeof(uint32_t));
			wkb.append(const_char_ptr_cast(&one), sizeof(uint32_t));
		}
		wkb.push_back(1);
		const uint32_t point = 1;
		const double zero = 0;
		wkb.append(const_char_ptr_cast(&point), sizeof(uint32_t));
		wkb.append(const_char_ptr_cast(&zero), sizeof(double));
		wkb.append(const_char_ptr_cast(&zero), sizeof(double));
		return wkb;
	};

	const auto shallow = build_nested(4);
	REQUIRE_NOTHROW(geometry_t::DeserializeWKB(string_t(shallow), arena));

	const auto deep = build_nested(geometry_t::MAX_RECURSION_DEPTH + 4);
	REQUIRE_THROWS(geometry_t::DeserializeWKB(string_t(deep), arena));
}
