#include "duckdb/common/types/geometry_type.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/geometry_vector.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Uncompressed geometry storage
//===--------------------------------------------------------------------===//
// A thin adapter over the uncompressed *string* storage: geometries are serialized to WKB on the
// way in and rebuilt on the way out, so the bytes on disk stay exactly what DuckDB v1.5 wrote.
// That keeps every persistence and backwards-compatibility test meaningful across the physical
// type change, and it means overflow blocks, the dictionary and geometry statistics all keep
// working untouched.
//
// The aligned, native-format segment layout replaces this later; until then the cost is one
// serialize on append and one parse on scan.

namespace {

//! Scan state carrying a scratch BLOB chunk. A DataChunk rather than a bare Vector so that
//! Reset() drops the string heap and any pinned overflow-block handles between scans.
struct GeometryScanState : public StringScanState {
	GeometryScanState() {
		blob_chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::BLOB});
	}

	Vector &Blob() {
		return blob_chunk.data[0];
	}
	void ResetBlob() {
		blob_chunk.Reset();
	}

	DataChunk blob_chunk;
};

struct GeometryAppendState : public CompressionAppendState {
	explicit GeometryAppendState(BufferHandle handle_p) : CompressionAppendState(std::move(handle_p)) {
		blob_chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::BLOB});
	}

	Vector &Blob() {
		return blob_chunk.data[0];
	}
	void ResetBlob() {
		blob_chunk.Reset();
	}

	DataChunk blob_chunk;
};

//! Serialize `count` geometries starting at `offset` into a flat BLOB vector.
//! Invalid rows are left untouched - the string layer writes its own placeholder for them, and
//! a NULL geometry has no WKB representation.
void SerializeToBlob(const UnifiedVectorFormat &source, idx_t offset, idx_t count, Vector &blob) {
	const auto geoms = UnifiedVectorFormat::GetData<geometry_t>(source);
	const auto blob_data = FlatVector::GetDataMutable<string_t>(blob);
	auto &blob_validity = FlatVector::ValidityMutable(blob);

	for (idx_t i = 0; i < count; i++) {
		const auto source_idx = source.sel->get_index(offset + i);
		if (!source.validity.RowIsValid(source_idx)) {
			blob_validity.SetInvalid(i);
			blob_data[i] = string_t();
			continue;
		}
		blob_data[i] = geometry_t::SerializeWKB(geoms[source_idx], blob);
	}
	FlatVector::SetSize(blob, count_t(count));
}

//! Rebuild geometries from WKB read back out of a segment.
//! NULL rows are stored as a zero-length entry (the string dictionary repeats the previous offset),
//! which is not parseable WKB - the minimum is 5 bytes - so treat empty as NULL. Validity itself
//! comes from the separate validity column, exactly as it does for strings.
void DeserializeFromBlob(Vector &blob, Vector &result, idx_t count, idx_t result_offset) {
	const auto blob_data = FlatVector::GetData<string_t>(blob);
	const auto result_data = FlatVector::GetDataMutable<geometry_t>(result);
	auto &arena = GeometryVector::GetArena(result);

	for (idx_t i = 0; i < count; i++) {
		auto &wkb = blob_data[i];
		if (wkb.GetSize() == 0) {
			result_data[result_offset + i] = geometry_t::CreateNull();
			continue;
		}
		result_data[result_offset + i] = geometry_t::DeserializeWKB(wkb, arena);
	}
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
bool GeometryAnalyze(AnalyzeState &state, const Vector &input) {
	const auto count = input.size();

	// TODO(geometry-physical-type): this allocates a scratch vector per analyzed vector. It goes
	// away with the native segment format, which sizes geometries without serializing them.
	Vector blob(LogicalType::BLOB, count);
	geometry_t::SerializeWKB(input, blob, count);
	FlatVector::SetSize(blob, count_t(count));

	return UncompressedStringStorage::StringAnalyze(state, blob);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> GeometryInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto result = make_uniq<GeometryScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	result->handle = buffer_manager.Pin(segment.GetBlockHandle());
	return std::move(result);
}

void GeometryScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                         idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<GeometryScanState>();
	scan_state.ResetBlob();

	UncompressedStringStorage::StringScanPartial(segment, state, scan_count, scan_state.Blob(), 0);
	DeserializeFromBlob(scan_state.Blob(), result, scan_count, result_offset);
}

void GeometryScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	GeometryScanPartial(segment, state, scan_count, result, 0);
}

void GeometrySelect(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                    const SelectionVector &sel, idx_t sel_count) {
	auto &scan_state = state.scan_state->Cast<GeometryScanState>();
	scan_state.ResetBlob();

	UncompressedStringStorage::Select(segment, state, vector_count, scan_state.Blob(), sel, sel_count);
	DeserializeFromBlob(scan_state.Blob(), result, sel_count, 0);
}

void GeometryFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	Vector blob(LogicalType::BLOB, 1);
	UncompressedStringStorage::StringFetchRow(segment, state, row_id, blob, 0);
	DeserializeFromBlob(blob, result, 1, result_idx);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
unique_ptr<CompressionAppendState> GeometryInitAppend(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(segment.GetBlockHandle());
	return make_uniq<GeometryAppendState>(std::move(handle));
}

idx_t GeometryAppend(CompressionAppendState &append_state, ColumnSegment &segment, BaseStatistics &stats,
                     UnifiedVectorFormat &data, idx_t offset, idx_t count) {
	auto &state = append_state.Cast<GeometryAppendState>();
	state.ResetBlob();
	SerializeToBlob(data, offset, count, state.Blob());

	UnifiedVectorFormat blob_data;
	state.Blob().ToUnifiedFormat(blob_data);

	// The string layer computes geometry statistics from the WKB we just handed it, via
	// StatsWriter<string_t>'s geometry branch - so stats need no special handling here.
	return UncompressedStringStorage::StringAppendBase(state.handle, segment, stats, blob_data, 0, count);
}

} // namespace

CompressionFunction GeometryUncompressed::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::GEOMETRY);
	return CompressionFunction(
	    CompressionType::COMPRESSION_UNCOMPRESSED, data_type, UncompressedStringStorage::StringInitAnalyze,
	    GeometryAnalyze, UncompressedStringStorage::StringFinalAnalyze, UncompressedFunctions::InitCompression,
	    UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress, GeometryInitScan, GeometryScan,
	    GeometryScanPartial, GeometryFetchRow, UncompressedFunctions::EmptySkip,
	    UncompressedStringStorage::StringInitSegment, GeometryInitAppend, GeometryAppend,
	    UncompressedStringStorage::FinalizeAppend, UncompressedStringStorage::StringRevertAppend,
	    UncompressedStringStorage::SerializeState, UncompressedStringStorage::DeserializeState,
	    UncompressedStringStorage::VisitBlockIds, UncompressedStringInitPrefetch, GeometrySelect);
}

} // namespace duckdb
