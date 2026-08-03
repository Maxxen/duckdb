#include "duckdb/common/vector/geometry_vector.hpp"

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

idx_t GeometryArenaHolder::GetAllocationSize() const {
	return arena.AllocationSize();
}

VectorGeometryBuffer::VectorGeometryBuffer() : StandardVectorBuffer(capacity_t(0), sizeof(geometry_t)) {
	buffer_type = VectorBufferType::GEOMETRY_BUFFER;
}

VectorGeometryBuffer::VectorGeometryBuffer(Allocator &allocator)
    : StandardVectorBuffer(allocator, capacity_t(0), sizeof(geometry_t)), arena(AllocateArena(allocator)) {
	buffer_type = VectorBufferType::GEOMETRY_BUFFER;
}

VectorGeometryBuffer::VectorGeometryBuffer(Allocator &allocator, capacity_t capacity)
    : StandardVectorBuffer(allocator, capacity, sizeof(geometry_t)) {
	buffer_type = VectorBufferType::GEOMETRY_BUFFER;
}

VectorGeometryBuffer::VectorGeometryBuffer(capacity_t capacity)
    : StandardVectorBuffer(capacity, sizeof(geometry_t)) {
	buffer_type = VectorBufferType::GEOMETRY_BUFFER;
}

VectorGeometryBuffer::VectorGeometryBuffer(data_ptr_t data_ptr_p, count_t count)
    : StandardVectorBuffer(data_ptr_p, count, sizeof(geometry_t)) {
	buffer_type = VectorBufferType::GEOMETRY_BUFFER;
}

VectorGeometryBuffer::VectorGeometryBuffer(AllocatedData &&data_p, count_t count)
    : StandardVectorBuffer(std::move(data_p), count, sizeof(geometry_t)), arena(AllocateArena()) {
	buffer_type = VectorBufferType::GEOMETRY_BUFFER;
}

VectorGeometryBuffer::VectorGeometryBuffer(AllocatedData &&data_p, count_t count, const VectorGeometryBuffer &other)
    : StandardVectorBuffer(std::move(data_p), count, sizeof(geometry_t)) {
	auto auxiliary_data = other.GetAuxiliaryData();
	if (auxiliary_data) {
		AddAuxiliaryData(make_uniq<AuxiliaryDataSetHolder>(std::move(auxiliary_data)));
	}
	buffer_type = VectorBufferType::GEOMETRY_BUFFER;
}

ArenaAllocator &VectorGeometryBuffer::AllocateArena(Allocator &allocator) {
	auto data = make_uniq<GeometryArenaHolder>(allocator);
	auto &result = data->arena;
	AddAuxiliaryData(std::move(data));
	return result;
}

ArenaAllocator &VectorGeometryBuffer::AllocateArena() {
	return AllocateArena(Allocator::DefaultAllocator());
}

void VectorGeometryBuffer::CopyInternal(const Vector &source, const SelectionVector &source_sel, idx_t source_count,
                                        idx_t source_offset, idx_t target_offset, idx_t copy_count) {
	auto ldata = FlatVector::GetData<geometry_t>(source);
	auto tdata = reinterpret_cast<geometry_t *>(data_ptr);
	auto &target_arena = GetArena();
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = source_sel.get_index(source_offset + i);
		auto target_idx = target_offset + i;
		if (!validity.RowIsValid(target_idx)) {
			continue;
		}
		tdata[target_idx] = geometry_t::Copy(ldata[source_idx], target_arena);
	}
}

Value VectorGeometryBuffer::GetValue(const LogicalType &type, idx_t index) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		index = 0;
	}
	if (!validity.RowIsValid(index)) {
		return Value(type);
	}
	D_ASSERT(type.id() == LogicalTypeId::GEOMETRY);

	// TODO(geometry-physical-type): Value still holds WKB - once it holds a geometry_t, hand the
	// geometry over directly instead of serializing here.
	auto &geom = reinterpret_cast<const geometry_t *>(data_ptr)[index];
	string wkb;
	geometry_t::SerializeWKB(geom, wkb);

	if (GeoType::HasCRS(type)) {
		return Value::GEOMETRY(const_data_ptr_cast(wkb.c_str()), wkb.size(), GeoType::GetCRS(type));
	}
	return Value::GEOMETRY(const_data_ptr_cast(wkb.c_str()), wkb.size());
}

void VectorGeometryBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	if (!val.IsNull() && val.type() != type) {
		SetValue(type, index, val.DefaultCastAs(type));
		return;
	}
	validity.Set(index, !val.IsNull());
	if (val.IsNull()) {
		return;
	}
	// TODO(geometry-physical-type): see GetValue - this parse goes away once Value holds a geometry_t.
	auto &wkb = StringValue::Get(val);
	reinterpret_cast<geometry_t *>(data_ptr)[index] = geometry_t::DeserializeWKB(string_t(wkb), GetArena());
}

void VectorGeometryBuffer::VerifyInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	StandardVectorBuffer::VerifyInternal(type, sel, count);

	D_ASSERT(type.InternalType() == PhysicalType::GEOMETRY);
	auto geoms = reinterpret_cast<const geometry_t *>(data_ptr);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		if (!validity.RowIsValid(idx)) {
			continue;
		}
		geoms[idx].Verify();
	}
}

buffer_ptr<VectorBuffer> VectorGeometryBuffer::SliceInternal(const LogicalType &type, idx_t offset, idx_t end) {
	auto type_size = GetTypeIdSize(type.InternalType());
	auto offset_ptr = data_ptr + type_size * offset;
	auto count = count_t(end - offset);
	auto result = make_buffer<VectorGeometryBuffer>(offset_ptr, count);
	result->GetValidityMask().Slice(validity, offset, count);
	result->AddAuxiliaryData(make_uniq<VectorBufferHolder>(shared_from_this()));
	return result;
}

buffer_ptr<VectorBuffer> VectorGeometryBuffer::ConstantSliceInternal(const LogicalType &type, count_t count) {
	auto result = make_buffer<VectorGeometryBuffer>(data_ptr, count);
	result->GetValidityMask().Set(0, validity.RowIsValid(0));
	result->SetVectorType(VectorType::CONSTANT_VECTOR);
	result->AddAuxiliaryData(make_uniq<VectorBufferHolder>(shared_from_this()));
	return result;
}

buffer_ptr<VectorBuffer> VectorGeometryBuffer::CreateBuffer(AllocatedData &&new_data, count_t count) const {
	return make_buffer<VectorGeometryBuffer>(std::move(new_data), count, *this);
}

buffer_ptr<VectorBuffer> VectorGeometryBuffer::FlattenSliceInternal(const LogicalType &type,
                                                                    const SelectionVector &sel, idx_t count) const {
	auto result = StandardVectorBuffer::FlattenSliceInternal(type, sel, count);
	if (!result) {
		// already flat - bail
		return nullptr;
	}
	// the flattened geometries still point into our arena, so keep it alive
	if (auxiliary_data) {
		result->AddAuxiliaryData(make_uniq<AuxiliaryDataSetHolder>(auxiliary_data));
	}
	return result;
}

VectorGeometryBuffer &GeometryVector::GetGeometryBuffer(Vector &vector) {
	if (vector.GetType().InternalType() != PhysicalType::GEOMETRY) {
		throw InternalException(
		    "GeometryVector::GetGeometryBuffer - vector is not of internal type GEOMETRY but of type %s",
		    vector.GetType());
	}
	if (!vector.GetBufferRef()) {
		vector.SetBuffer(make_buffer<VectorGeometryBuffer>(nullptr, count_t(0)));
	}
	if (vector.Buffer().GetBufferType() != VectorBufferType::GEOMETRY_BUFFER) {
		throw InternalException(
		    "GeometryVector::GetGeometryBuffer called on a vector - but that vector does NOT have a geometry buffer");
	}
	return vector.BufferMutable().Cast<VectorGeometryBuffer>();
}

ArenaAllocator &GeometryVector::GetArena(Vector &vector) {
	return GetGeometryBuffer(vector).GetArena();
}

geometry_t GeometryVector::Add(Vector &vector, const geometry_t &geom) {
	return geometry_t::Copy(geom, GetArena(vector));
}

void GeometryVector::AddAuxiliaryData(Vector &vector, unique_ptr<AuxiliaryDataHolder> data) {
	vector.AddAuxiliaryData(std::move(data));
}

void GeometryVector::AddHandle(Vector &vector, BufferHandle handle) {
	AddAuxiliaryData(vector, make_uniq<PinnedBufferHolder>(std::move(handle)));
}

void GeometryVector::AddHeapReference(Vector &vector, const Vector &other) {
	vector.AddHeapReference(other);
}

} // namespace duckdb
