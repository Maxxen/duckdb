#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class ArenaAllocator;
class GeometryExtent;

struct geometry_t {
private:
	uint32_t type;
	uint32_t size;
	char *data;

public:
	geometry_t() = default;
	// geometry_t(char* ptr, uint32_t size) : size(size), type(0), data(ptr) {}
	geometry_t(uint32_t type, char *ptr, uint32_t size) : type(type), size(size), data(ptr) {
		D_ASSERT(uintptr_t(data) % alignof(geometry_t) == 0);
	}

	void Verify() const;

	uint32_t GetType() const {
		return type;
	}
	uint32_t GetCount() const {
		return size;
	}

	geometry_t *GetChildren() {
		D_ASSERT(uintptr_t(data) % alignof(geometry_t) == 0);
		return reinterpret_cast<geometry_t *>(data);
	}

	const geometry_t *GetChildren() const {
		D_ASSERT(uintptr_t(data) % alignof(geometry_t) == 0);
		return reinterpret_cast<const geometry_t *>(data);
	}

	geometry_t &GetChild(uint32_t index) {
		D_ASSERT(index < size);
		return GetChildren()[index];
	}

	const geometry_t &GetChild(uint32_t index) const {
		D_ASSERT(index < size);
		return GetChildren()[index];
	}

	double *GetCoords() {
		D_ASSERT(uintptr_t(data) % alignof(double) == 0);
		return reinterpret_cast<double *>(data);
	}

	const double *GetCoords() const {
		D_ASSERT(uintptr_t(data) % alignof(double) == 0);
		return reinterpret_cast<const double *>(data);
	}

	uint32_t GetTotalByteSize() const;
	void Serialize(char *ptr, uint32_t len) const;
	string Serialize() const;

	static idx_t SerializeToHeapSize(const geometry_t &geom);
	static geometry_t SerializeToHeap(const geometry_t &geom, data_ptr_t heap_location, idx_t heap_size);
	void Unswizzle();

	static geometry_t Deserialize(Vector &vec, const char *ptr, uint32_t len);
	static geometry_t Deserialize(Vector &vec, string &data);

	geometry_t DeepCopy(ArenaAllocator &arena) const;
	geometry_t DeepCopy(Vector &result) const;

	void SetDataPtrUnsafe(char *ptr) {
		D_ASSERT(uintptr_t(ptr) % alignof(geometry_t) == 0);
		data = ptr;
	}
	data_ptr_t GetDataPtrUnsafe() const {
		D_ASSERT(uintptr_t(data) % alignof(geometry_t) == 0);
		return data_ptr_cast(data);
	}

	uint32_t GetExtent(GeometryExtent &extent) const;
};

struct VectorGeometryBuffer;

} // namespace duckdb
