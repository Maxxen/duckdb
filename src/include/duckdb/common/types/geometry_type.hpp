//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/geometry_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

class ArenaAllocator;
class StringHeap;
class Vector;

enum class GeometryType : uint8_t {
	INVALID = 0,
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7,
};

enum class VertexType : uint8_t { XY = 0, XYZ = 1, XYM = 2, XYZM = 3 };

//! A geometry: a node in a tree of parts, bottoming out in arrays of interleaved coordinates.
//! Leaves (POINT, LINESTRING) point to a `double` array of `count * GetDims()` coordinates.
//! Containers (POLYGON, MULTI*, GEOMETRYCOLLECTION) point to a `geometry_t` array of `count` parts.
//! Both arrays are always 8-byte aligned - that guarantee is the reason this type exists.
//!
//! The memory is not owned: it belongs to a vector's arena, a row-layout heap block, or a
//! storage block. The exact field layout is private on purpose - go through the accessors so
//! it can be changed later.
class geometry_t {
public:
	//! Maximum nesting depth, enforced when parsing untrusted bytes
	static constexpr idx_t MAX_RECURSION_DEPTH = 16;

public:
	geometry_t() = default;

	//! The NULL geometry. Also the value stored for invalid rows in row layouts.
	static geometry_t CreateNull() {
		return geometry_t();
	}
	//! A leaf (POINT / LINESTRING) over `vert_count` vertices of `GetDims()` doubles each
	static geometry_t CreateLeaf(GeometryType type, VertexType vert_type, double *verts, uint32_t vert_count) {
		geometry_t result(type, vert_type);
		result.SetVertices(verts, vert_count);
		return result;
	}
	//! A container (POLYGON / MULTI* / GEOMETRYCOLLECTION) over `part_count` parts
	static geometry_t CreateContainer(GeometryType type, VertexType vert_type, geometry_t *parts,
	                                  uint32_t part_count) {
		geometry_t result(type, vert_type);
		result.SetParts(parts, part_count);
		return result;
	}
	//! An empty geometry of the given type
	static geometry_t CreateEmpty(GeometryType type, VertexType vert_type) {
		return geometry_t(type, vert_type);
	}

public:
	GeometryType GetGeometryType() const {
		return part_type;
	}
	VertexType GetVertexType() const {
		return vert_type;
	}
	bool IsNull() const {
		return part_type == GeometryType::INVALID;
	}
	bool IsEmpty() const {
		return count == 0;
	}
	//! Is this a POINT or LINESTRING, i.e. does it point directly at coordinates?
	bool IsLeaf() const {
		return part_type == GeometryType::POINT || part_type == GeometryType::LINESTRING;
	}
	bool HasZ() const {
		return vert_type == VertexType::XYZ || vert_type == VertexType::XYZM;
	}
	bool HasM() const {
		return vert_type == VertexType::XYM || vert_type == VertexType::XYZM;
	}
	//! Coordinates per vertex: 2, 3 or 4
	idx_t GetDims() const {
		switch (vert_type) {
		case VertexType::XY:
			return 2;
		case VertexType::XYZ:
		case VertexType::XYM:
			return 3;
		default:
			D_ASSERT(vert_type == VertexType::XYZM);
			return 4;
		}
	}
	//! Bytes per vertex
	idx_t GetVertexWidth() const {
		return GetDims() * sizeof(double);
	}

	uint32_t GetVertexCount() const {
		D_ASSERT(IsLeaf());
		return count;
	}
	uint32_t GetPartCount() const {
		D_ASSERT(!IsLeaf() && !IsNull());
		return count;
	}

	const double *GetVertexData() const {
		D_ASSERT(IsLeaf());
		return static_cast<const double *>(array);
	}
	double *GetVertexData() {
		D_ASSERT(IsLeaf());
		return static_cast<double *>(array);
	}
	const geometry_t *GetPartData() const {
		D_ASSERT(!IsLeaf() && !IsNull());
		return static_cast<const geometry_t *>(array);
	}
	geometry_t *GetPartData() {
		D_ASSERT(!IsLeaf() && !IsNull());
		return static_cast<geometry_t *>(array);
	}
	const geometry_t &GetPart(idx_t part_idx) const {
		D_ASSERT(part_idx < count);
		return GetPartData()[part_idx];
	}

	//! Copy the `GetDims()` coordinates of vertex `vert_idx` into `out`
	void GetVertex(idx_t vert_idx, double *out) const {
		D_ASSERT(vert_idx < count);
		memcpy(out, GetVertexData() + vert_idx * GetDims(), GetVertexWidth());
	}

	void SetVertices(double *verts, uint32_t vert_count) {
		D_ASSERT(IsLeaf());
		D_ASSERT(part_type != GeometryType::POINT || vert_count <= 1);
		D_ASSERT(vert_count == 0 || PointerIsAligned<double>(const_data_ptr_cast(verts)));
		count = vert_count;
		// Empty always means a null pointer, so nothing downstream has to carry a dangling one
		array = vert_count == 0 ? nullptr : verts;
	}
	void SetParts(geometry_t *parts, uint32_t part_count) {
		D_ASSERT(!IsLeaf() && !IsNull());
		D_ASSERT(part_count == 0 || PointerIsAligned<geometry_t>(const_data_ptr_cast(parts)));
		count = part_count;
		array = part_count == 0 ? nullptr : parts;
	}

	//! The raw pointer to the part/vertex array. Only for heap bookkeeping.
	data_ptr_t GetDataPointer() const {
		return data_ptr_cast(array);
	}

public:
	//! Recursively check the type invariants, alignment and nesting depth. Debug builds only.
	void Verify() const;

	//! Structural equality: same type, same vertex type, same coordinates
	bool operator==(const geometry_t &other) const;
	bool operator!=(const geometry_t &other) const {
		return !(*this == other);
	}
	//! Structural ordering: by geometry type, then vertex type, then count, then coordinate bytes.
	//! Deliberately not the WKB byte order - geometry ordering is not a stable contract.
	bool operator<(const geometry_t &other) const;
	bool operator<=(const geometry_t &other) const {
		return !(other < *this);
	}
	bool operator>(const geometry_t &other) const {
		return other < *this;
	}
	bool operator>=(const geometry_t &other) const {
		return !(*this < other);
	}

public:
	//===------------------------------------------------------------------===//
	// Allocation
	//===------------------------------------------------------------------===//
	//! The only two allocation entry points for geometry data. Both guarantee 8-byte alignment.
	static double *AllocateVerts(ArenaAllocator &arena, idx_t vert_count, idx_t dims);
	static geometry_t *AllocateParts(ArenaAllocator &arena, idx_t part_count);

	//! Deep copy into `arena`
	static geometry_t Copy(const geometry_t &geom, ArenaAllocator &arena);

	//===------------------------------------------------------------------===//
	// Heap format: the native structs, laid out contiguously with real pointers
	//===------------------------------------------------------------------===//
	//! Bytes CopyToHeap needs, excluding the root node itself. Always a multiple of 8.
	static idx_t GetHeapSize(const geometry_t &geom);
	//! Copy into a pre-allocated, 8-byte-aligned heap block of at least GetHeapSize() bytes
	static geometry_t CopyToHeap(const geometry_t &geom, data_ptr_t heap_ptr, idx_t heap_size);
	//! Fix up the pointers after the heap block moved
	static void RecomputeHeapPointers(geometry_t &geom, const_data_ptr_t old_heap, data_ptr_t new_heap);

	//===------------------------------------------------------------------===//
	// Native format: the wire format, and the sort key
	//===------------------------------------------------------------------===//
	//! Depth-first, 8-byte node header {part_type, vert_type, pad, count} followed by the payload
	static idx_t GetSerializedSize(const geometry_t &geom);
	static idx_t Serialize(const geometry_t &geom, data_ptr_t target);
	//! Writes into `result`'s string heap - `result` is a BLOB vector, not a GEOMETRY one
	static string_t Serialize(const geometry_t &geom, Vector &result);
	static geometry_t Deserialize(const string_t &blob, ArenaAllocator &arena);

	//===------------------------------------------------------------------===//
	// WKB format: on-disk (until the native storage format lands), ART keys, interop
	//===------------------------------------------------------------------===//
	//! Bytes SerializeWKB writes. Note a POINT always costs a full vertex - an empty point is
	//! encoded as NaN coordinates, matching the normalized WKB the rest of the system expects.
	static idx_t GetWKBSize(const geometry_t &geom);
	static idx_t SerializeWKB(const geometry_t &geom, data_ptr_t target);
	//! Writes into `result`'s string heap - `result` is a BLOB vector, not a GEOMETRY one
	static string_t SerializeWKB(const geometry_t &geom, Vector &result);
	static string_t SerializeWKB(const geometry_t &geom, StringHeap &heap);
	static void SerializeWKB(const geometry_t &geom, string &result);

	//! Parse *already normalized* WKB - our own SerializeWKB output, or bytes read back from
	//! storage. Use Geometry::FromBinary for arbitrary user WKB, which may be big-endian or EWKB.
	static geometry_t DeserializeWKB(const string_t &wkb, ArenaAllocator &arena);

private:
	geometry_t(GeometryType part_type_p, VertexType vert_type_p) : part_type(part_type_p), vert_type(vert_type_p) {
	}

private:
	GeometryType part_type = GeometryType::INVALID;
	VertexType vert_type = VertexType::XY;
	//! Reserved for future use. Must always be zero - Verify() checks it, and it is memcmp'd
	//! as part of the row layout.
	uint16_t flags = 0;
	//! Vertex count for leaves, part count for containers
	uint32_t count = 0;
	//! double[] for leaves, geometry_t[] for containers
	void *array = nullptr;
};

} // namespace duckdb

namespace std {
template <>
struct hash<duckdb::geometry_t> {
	size_t operator()(const duckdb::geometry_t &val) const {
		return duckdb::Hash(val);
	}
};
} // namespace std
