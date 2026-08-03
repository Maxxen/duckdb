#include "duckdb/common/types/geometry_type.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

// The layout is private, but a lot of the system depends on its size and alignment.
// Pin both down here so a change has to be deliberate.
static_assert(sizeof(geometry_t) == 16, "geometry_t must be 16 bytes");
static_assert(alignof(geometry_t) == 8, "geometry_t must be 8-byte aligned");

//----------------------------------------------------------------------------------------------------------------------
// Verify
//----------------------------------------------------------------------------------------------------------------------
#ifdef DEBUG
static void VerifyInternal(const geometry_t &geom, idx_t depth) {
	if (depth > geometry_t::MAX_RECURSION_DEPTH) {
		throw InternalException("geometry_t nested deeper than MAX_RECURSION_DEPTH");
	}

	switch (geom.GetVertexType()) {
	case VertexType::XY:
	case VertexType::XYZ:
	case VertexType::XYM:
	case VertexType::XYZM:
		break;
	default:
		throw InternalException("Invalid vertex type in geometry_t::Verify");
	}

	// Empty always means a null pointer - see SetVertices/SetParts
	D_ASSERT(!geom.IsEmpty() || geom.GetDataPointer() == nullptr);

	const auto type = geom.GetGeometryType();
	if (type == GeometryType::INVALID) {
		// The NULL geometry owns nothing
		D_ASSERT(geom.IsEmpty());
		return;
	}

	if (geom.IsLeaf()) {
		D_ASSERT(type != GeometryType::POINT || geom.GetVertexCount() <= 1);
		D_ASSERT(geom.IsEmpty() || PointerIsAligned<double>(geom.GetDataPointer()));
		return;
	}

	D_ASSERT(geom.IsEmpty() || PointerIsAligned<geometry_t>(geom.GetDataPointer()));

	const auto part_count = geom.GetPartCount();
	for (idx_t part_idx = 0; part_idx < part_count; part_idx++) {
		auto &part = geom.GetPart(part_idx);
		switch (type) {
		case GeometryType::POLYGON:
			// A polygon's parts are its rings
			D_ASSERT(part.GetGeometryType() == GeometryType::LINESTRING);
			break;
		case GeometryType::MULTIPOINT:
			D_ASSERT(part.GetGeometryType() == GeometryType::POINT);
			break;
		case GeometryType::MULTILINESTRING:
			D_ASSERT(part.GetGeometryType() == GeometryType::LINESTRING);
			break;
		case GeometryType::MULTIPOLYGON:
			D_ASSERT(part.GetGeometryType() == GeometryType::POLYGON);
			break;
		case GeometryType::GEOMETRYCOLLECTION:
			// Anything goes
			break;
		default:
			throw InternalException("Invalid geometry type in geometry_t::Verify");
		}
		// Every part carries the same dimensionality as its parent
		D_ASSERT(part.GetVertexType() == geom.GetVertexType());
		VerifyInternal(part, depth + 1);
	}
}
#endif

void geometry_t::Verify() const {
#ifdef DEBUG
	D_ASSERT(flags == 0);
	VerifyInternal(*this, 0);
#endif
}

//----------------------------------------------------------------------------------------------------------------------
// Comparison
//----------------------------------------------------------------------------------------------------------------------
bool geometry_t::operator==(const geometry_t &other) const {
	if (part_type != other.part_type || vert_type != other.vert_type || count != other.count) {
		return false;
	}
	if (count == 0) {
		return true;
	}
	if (IsLeaf()) {
		return memcmp(array, other.array, count * GetVertexWidth()) == 0;
	}
	const auto parts = GetPartData();
	const auto other_parts = other.GetPartData();
	for (idx_t part_idx = 0; part_idx < count; part_idx++) {
		if (parts[part_idx] != other_parts[part_idx]) {
			return false;
		}
	}
	return true;
}

bool geometry_t::operator<(const geometry_t &other) const {
	if (part_type != other.part_type) {
		return part_type < other.part_type;
	}
	if (vert_type != other.vert_type) {
		return vert_type < other.vert_type;
	}
	if (count != other.count) {
		return count < other.count;
	}
	if (count == 0) {
		return false;
	}
	if (IsLeaf()) {
		return memcmp(array, other.array, count * GetVertexWidth()) < 0;
	}
	const auto parts = GetPartData();
	const auto other_parts = other.GetPartData();
	for (idx_t part_idx = 0; part_idx < count; part_idx++) {
		if (parts[part_idx] < other_parts[part_idx]) {
			return true;
		}
		if (other_parts[part_idx] < parts[part_idx]) {
			return false;
		}
	}
	return false;
}

//----------------------------------------------------------------------------------------------------------------------
// Allocation
//----------------------------------------------------------------------------------------------------------------------
double *geometry_t::AllocateVerts(ArenaAllocator &arena, idx_t vert_count, idx_t dims) {
	if (vert_count == 0) {
		return nullptr;
	}
	return reinterpret_cast<double *>(arena.AllocateAligned(vert_count * dims * sizeof(double)));
}

geometry_t *geometry_t::AllocateParts(ArenaAllocator &arena, idx_t part_count) {
	if (part_count == 0) {
		return nullptr;
	}
	const auto ptr = arena.AllocateAligned(part_count * sizeof(geometry_t));
	// Default-construct so the reserved flags are zero even if a part is never filled in
	const auto parts = reinterpret_cast<geometry_t *>(ptr);
	for (idx_t part_idx = 0; part_idx < part_count; part_idx++) {
		parts[part_idx] = geometry_t::CreateNull();
	}
	return parts;
}

static geometry_t CopyInternal(const geometry_t &geom, ArenaAllocator &arena) {
	if (geom.IsNull() || geom.IsEmpty()) {
		return geom;
	}
	if (geom.IsLeaf()) {
		const auto vert_count = geom.GetVertexCount();
		const auto dims = geom.GetDims();
		const auto verts = geometry_t::AllocateVerts(arena, vert_count, dims);
		memcpy(verts, geom.GetVertexData(), vert_count * geom.GetVertexWidth());
		return geometry_t::CreateLeaf(geom.GetGeometryType(), geom.GetVertexType(), verts, vert_count);
	}
	const auto part_count = geom.GetPartCount();
	const auto parts = geometry_t::AllocateParts(arena, part_count);
	for (idx_t part_idx = 0; part_idx < part_count; part_idx++) {
		parts[part_idx] = CopyInternal(geom.GetPart(part_idx), arena);
	}
	return geometry_t::CreateContainer(geom.GetGeometryType(), geom.GetVertexType(), parts, part_count);
}

geometry_t geometry_t::Copy(const geometry_t &geom, ArenaAllocator &arena) {
	auto result = CopyInternal(geom, arena);
	result.Verify();
	return result;
}

//----------------------------------------------------------------------------------------------------------------------
// Heap format
//----------------------------------------------------------------------------------------------------------------------
// The native structs laid out contiguously in a caller-supplied block, with real pointers into it.
// Used by the row layout and by ColumnDataCollection, which both relocate the block and then fix
// the pointers up with RecomputeHeapPointers.

idx_t geometry_t::GetHeapSize(const geometry_t &geom) {
	// The root node itself lives in the row/vector, not in the heap
	if (geom.IsNull() || geom.IsEmpty()) {
		return 0;
	}
	if (geom.IsLeaf()) {
		return geom.GetVertexCount() * geom.GetVertexWidth();
	}
	const auto part_count = geom.GetPartCount();
	idx_t size = part_count * sizeof(geometry_t);
	for (idx_t part_idx = 0; part_idx < part_count; part_idx++) {
		size += GetHeapSize(geom.GetPart(part_idx));
	}
	return size;
}

static geometry_t CopyToHeapInternal(const geometry_t &geom, data_ptr_t &heap_ptr) {
	if (geom.IsNull() || geom.IsEmpty()) {
		// Nothing to write - notably this is the path a NULL row takes
		return geom;
	}
	if (geom.IsLeaf()) {
		const auto vert_count = geom.GetVertexCount();
		const auto byte_size = vert_count * geom.GetVertexWidth();
		D_ASSERT(PointerIsAligned<double>(heap_ptr));

		const auto verts = reinterpret_cast<double *>(heap_ptr);
		memcpy(verts, geom.GetVertexData(), byte_size);
		heap_ptr += byte_size;

		return geometry_t::CreateLeaf(geom.GetGeometryType(), geom.GetVertexType(), verts, vert_count);
	}

	const auto part_count = geom.GetPartCount();
	D_ASSERT(PointerIsAligned<geometry_t>(heap_ptr));

	// Reserve the part array first, then append the children after it
	const auto parts = reinterpret_cast<geometry_t *>(heap_ptr);
	heap_ptr += part_count * sizeof(geometry_t);
	for (idx_t part_idx = 0; part_idx < part_count; part_idx++) {
		parts[part_idx] = CopyToHeapInternal(geom.GetPart(part_idx), heap_ptr);
	}
	return geometry_t::CreateContainer(geom.GetGeometryType(), geom.GetVertexType(), parts, part_count);
}

geometry_t geometry_t::CopyToHeap(const geometry_t &geom, data_ptr_t heap_ptr, idx_t heap_size) {
	D_ASSERT(PointerIsAligned<geometry_t>(heap_ptr));
	auto write_ptr = heap_ptr;
	auto result = CopyToHeapInternal(geom, write_ptr);
	D_ASSERT(NumericCast<idx_t>(write_ptr - heap_ptr) == heap_size);
	return result;
}

void geometry_t::RecomputeHeapPointers(geometry_t &geom, const_data_ptr_t old_heap, data_ptr_t new_heap) {
	if (geom.IsNull() || geom.IsEmpty()) {
		// Points at nothing, so there is nothing to fix up
		return;
	}
	const auto offset = geom.GetDataPointer() - old_heap;
	D_ASSERT(offset >= 0);
	const auto new_data = new_heap + offset;

	if (geom.IsLeaf()) {
		geom.SetVertices(reinterpret_cast<double *>(new_data), geom.GetVertexCount());
		return;
	}

	const auto part_count = geom.GetPartCount();
	geom.SetParts(reinterpret_cast<geometry_t *>(new_data), part_count);

	const auto parts = geom.GetPartData();
	for (idx_t part_idx = 0; part_idx < part_count; part_idx++) {
		RecomputeHeapPointers(parts[part_idx], old_heap, new_heap);
	}
}

} // namespace duckdb
