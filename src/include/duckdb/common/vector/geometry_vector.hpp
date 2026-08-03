//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/geometry_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/geometry_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

//! Owns the memory a GEOMETRY vector's parts and coordinates point into. Lives in the buffer's
//! auxiliary data rather than the buffer itself, so that Vector::AddHeapReference - which shares the
//! whole AuxiliaryDataSet - lets one vector reference another's geometries without copying.
struct GeometryArenaHolder : AuxiliaryDataHolder {
	explicit GeometryArenaHolder(Allocator &allocator) : arena(allocator) {
	}

	idx_t GetAllocationSize() const override;

	ArenaAllocator arena;
};

class VectorGeometryBuffer : public StandardVectorBuffer {
public:
	VectorGeometryBuffer();
	explicit VectorGeometryBuffer(Allocator &allocator);
	VectorGeometryBuffer(Allocator &allocator, capacity_t capacity);
	explicit VectorGeometryBuffer(capacity_t capacity);
	explicit VectorGeometryBuffer(data_ptr_t data_ptr_p, count_t count);
	explicit VectorGeometryBuffer(AllocatedData &&data_p, count_t count);
	VectorGeometryBuffer(AllocatedData &&data_p, count_t count, const VectorGeometryBuffer &other);

public:
	ArenaAllocator &GetArena() {
		if (arena) {
			return *arena;
		}
		auto allocator = GetAllocator();
		if (allocator) {
			return AllocateArena(*allocator);
		}
		return AllocateArena();
	}

	void ClearAuxiliaryData() override {
		StandardVectorBuffer::ClearAuxiliaryData();
		arena = nullptr;
	}

public:
	Value GetValue(const LogicalType &type, idx_t index) const override;
	void SetValue(const LogicalType &type, idx_t index, const Value &val) override;

protected:
	buffer_ptr<VectorBuffer> SliceInternal(const LogicalType &type, idx_t offset, idx_t end) override;
	buffer_ptr<VectorBuffer> ConstantSliceInternal(const LogicalType &type, count_t count) override;
	buffer_ptr<VectorBuffer> CreateBuffer(AllocatedData &&new_data, count_t count) const override;
	void CopyInternal(const Vector &source, const SelectionVector &source_sel, idx_t source_count, idx_t source_offset,
	                  idx_t target_offset, idx_t copy_count) override;
	buffer_ptr<VectorBuffer> FlattenSliceInternal(const LogicalType &type, const SelectionVector &sel,
	                                              idx_t count) const override;
	void VerifyInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;

private:
	ArenaAllocator &AllocateArena(Allocator &allocator);
	ArenaAllocator &AllocateArena();

private:
	optional_ptr<ArenaAllocator> arena;
};

struct GeometryVector {
	//! Returns a reference to the underlying VectorGeometryBuffer - throws if the vector is not GEOMETRY
	DUCKDB_API static VectorGeometryBuffer &GetGeometryBuffer(Vector &vector);
	//! The arena backing this vector's geometries. All geometry allocation goes through here.
	DUCKDB_API static ArenaAllocator &GetArena(Vector &vector);
	//! Deep copy a geometry into this vector's arena
	DUCKDB_API static geometry_t Add(Vector &vector, const geometry_t &geom);
	//! Add a reference from this vector to the auxiliary data
	DUCKDB_API static void AddAuxiliaryData(Vector &vector, unique_ptr<AuxiliaryDataHolder> data);
	//! Keep a pinned block alive for as long as this vector points into it
	DUCKDB_API static void AddHandle(Vector &vector, BufferHandle handle);
	//! Share another vector's geometry memory instead of copying it
	DUCKDB_API static void AddHeapReference(Vector &vector, const Vector &other);
};

} // namespace duckdb
