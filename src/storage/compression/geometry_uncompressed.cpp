#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include "duckdb/storage/geometry_uncompressed.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Geometry Uncompressed
//===--------------------------------------------------------------------===//

namespace {

//! Dictionary header size at the beginning of the string segment (offset + length)
static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(uint32_t) + sizeof(uint32_t);
//! Marker used in length field to indicate the presence of a big string
static constexpr uint16_t BIG_GEOMETRY_MARKER = (uint16_t)-1;
//! Base size of big string marker (block id + offset)
static constexpr idx_t BIG_GEOMETRY_MARKER_BASE_SIZE = sizeof(block_id_t) + sizeof(int32_t);
//! The marker size of the big string
static constexpr idx_t BIG_GEOMETRY_MARKER_SIZE = BIG_GEOMETRY_MARKER_BASE_SIZE;

static constexpr idx_t DEFAULT_GEOMETRY_BLOCK_LIMIT = 4096;

idx_t GetGeometryBlockLimit(const idx_t block_size) {
	return MinValue(AlignValueFloor(block_size / 4), DEFAULT_GEOMETRY_BLOCK_LIMIT);
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

using UncompressedGeometrySegmentState = UncompressedStringSegmentState;

struct GeometryDictionary {
	//! The size of the dictionary
	uint32_t size;
	//! The end of the dictionary, which defaults to the block size.
	uint32_t end;

	void Verify(const idx_t block_size) {
		D_ASSERT(size <= block_size);
		D_ASSERT(end <= block_size);
		D_ASSERT(size <= end);
	}
};

void SetDictionary(ColumnSegment &segment, BufferHandle &handle, GeometryDictionary container) {
	auto startptr = handle.Ptr() + segment.GetBlockOffset();
	Store<uint32_t>(container.size, startptr);
	Store<uint32_t>(container.end, startptr + sizeof(uint32_t));
}

GeometryDictionary GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto startptr = handle.Ptr() + segment.GetBlockOffset();
	GeometryDictionary container;
	container.size = Load<uint32_t>(startptr);
	container.end = Load<uint32_t>(startptr + sizeof(uint32_t));
	return container;
}

uint32_t GetDictionaryEnd(ColumnSegment &segment, BufferHandle &handle) {
	auto startptr = handle.Ptr() + segment.GetBlockOffset();
	return Load<uint32_t>(startptr + sizeof(uint32_t));
}

idx_t RemainingSpace(ColumnSegment &segment, BufferHandle &handle) {
	auto dictionary = GetDictionary(segment, handle);
	D_ASSERT(dictionary.end == segment.SegmentSize());
	idx_t used_space = dictionary.size + segment.count * sizeof(int32_t) + DICTIONARY_HEADER_SIZE;
	D_ASSERT(segment.SegmentSize() >= used_space);
	return segment.SegmentSize() - used_space;
}

geometry_t ReadGeometry(data_ptr_t target, int32_t offset, uint32_t string_length, Vector &result) {
	auto ptr = target + offset;
	auto str_ptr = char_ptr_cast(ptr);

	return geometry_t::Deserialize(result, str_ptr, string_length);
}

geometry_t ReadGeometryWithLength(data_ptr_t target, int32_t offset, Vector &result) {
	auto ptr = target + offset;
	auto str_length = Load<uint32_t>(ptr);
	auto str_ptr = char_ptr_cast(ptr + sizeof(uint32_t));
	return geometry_t::Deserialize(result, str_ptr, str_length);
}

void WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset) {
	memcpy(target, &block_id, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(target, &offset, sizeof(int32_t));
}

void ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset) {
	memcpy(&block_id, target, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(&offset, target, sizeof(int32_t));
}

void WriteGeometryMemory(ColumnSegment &segment, const geometry_t &geom, block_id_t &result_block,
                         int32_t &result_offset) {
	// TODO: pass this down, dont recompute here
	const auto serialized_size = geom.GetTotalByteSize();
	;
	auto total_length = UnsafeNumericCast<uint32_t>(serialized_size + sizeof(uint32_t));

	shared_ptr<BlockHandle> block;
	BufferHandle handle;

	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto &state = segment.GetSegmentState()->Cast<UncompressedGeometrySegmentState>();
	// check if the string fits in the current block
	if (!state.head || state.head->offset + total_length >= state.head->size) {
		// string does not fit, allocate space for it
		// create a new string block
		auto alloc_size = MaxValue<idx_t>(total_length, segment.GetBlockManager().GetBlockSize());
		auto new_block = make_uniq<StringBlock>();
		new_block->offset = 0;
		new_block->size = alloc_size;
		// allocate an in-memory buffer for it
		handle = buffer_manager.Allocate(MemoryTag::OVERFLOW_STRINGS, alloc_size, false);
		block = handle.GetBlockHandle();
		state.overflow_blocks.insert(make_pair(block->BlockId(), reference<StringBlock>(*new_block)));
		new_block->block = std::move(block);
		new_block->next = std::move(state.head);
		state.head = std::move(new_block);
	} else {
		// string fits, copy it into the current block
		handle = buffer_manager.Pin(state.head->block);
	}

	result_block = state.head->block->BlockId();
	result_offset = UnsafeNumericCast<int32_t>(state.head->offset);

	// copy the string and the length there
	auto ptr = handle.Ptr() + state.head->offset;
	Store<uint32_t>(UnsafeNumericCast<uint32_t>(serialized_size), ptr);
	ptr += sizeof(uint32_t);

	// serialize the data into the buffer
	geom.Serialize(char_ptr_cast(ptr), serialized_size);

	state.head->offset += total_length;
}

void WriteGeometry(ColumnSegment &segment, const geometry_t &geom, block_id_t &result_block, int32_t &result_offset) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedGeometrySegmentState>();
	if (state.overflow_writer) {
		// overflow writer is set: write string there
		throw NotImplementedException("Geometry overflow writer is not implemented yet.");
		// state.overflow_writer->WriteString(state, geom, result_block, result_offset);
	} else {
		// default overflow behavior: use in-memory buffer to store the overflow string
		WriteGeometryMemory(segment, geom, result_block, result_offset);
	}
}

geometry_t ReadOverflowString(ColumnSegment &segment, Vector &result, block_id_t block, int32_t offset) {
	auto &block_manager = segment.GetBlockManager();
	auto &buffer_manager = block_manager.buffer_manager;
	auto &state = segment.GetSegmentState()->Cast<UncompressedGeometrySegmentState>();

	D_ASSERT(block != INVALID_BLOCK);
	D_ASSERT(offset < NumericCast<int32_t>(block_manager.GetBlockSize()));

	if (block >= MAXIMUM_BLOCK) {
		// read the overflow string from memory
		// first pin the handle, if it is not pinned yet
		auto entry = state.overflow_blocks.find(block);
		D_ASSERT(entry != state.overflow_blocks.end());
		auto handle = buffer_manager.Pin(entry->second.get().block);
		auto final_buffer = handle.Ptr();
		GeometryVector::AddHandle(result, std::move(handle));
		return ReadGeometryWithLength(final_buffer, offset, result);
	}

	// read the overflow string from disk
	// pin the initial handle and read the length
	auto block_handle = state.GetHandle(block_manager, block);
	auto handle = buffer_manager.Pin(block_handle);

	// read header
	uint32_t length = Load<uint32_t>(handle.Ptr() + offset);
	uint32_t remaining = length;
	offset += sizeof(uint32_t);

	BufferHandle target_handle;

	// geometry_t overflow_string;

	data_ptr_t target_ptr;
	bool allocate_block = length >= block_manager.GetBlockSize();
	if (allocate_block) {
		// overflow string is bigger than a block - allocate a temporary buffer for it
		target_handle = buffer_manager.Allocate(MemoryTag::OVERFLOW_STRINGS, length);
		target_ptr = target_handle.Ptr();
	} else {
		// overflow string is smaller than a block - add it to the vector directly
		// overflow_string = StringVector::EmptyString(result, length);
		target_ptr = GeometryVector::GetArena(result).AllocateAligned(length);
		// target_ptr = data_ptr_cast(overflow_string.GetDataWriteable());
	}

	// now append the string to the single buffer
	while (remaining > 0) {
		idx_t to_write = MinValue<idx_t>(remaining, block_manager.GetBlockSize() - sizeof(block_id_t) -
		                                                UnsafeNumericCast<idx_t>(offset));
		memcpy(target_ptr, handle.Ptr() + offset, to_write);
		remaining -= to_write;
		offset += UnsafeNumericCast<int32_t>(to_write);
		target_ptr += to_write;
		if (remaining > 0) {
			// read the next block
			block_id_t next_block = Load<block_id_t>(handle.Ptr() + offset);
			block_handle = state.GetHandle(block_manager, next_block);
			handle = buffer_manager.Pin(block_handle);
			offset = 0;
		}
	}
	if (allocate_block) {
		auto final_buffer = target_handle.Ptr();
		GeometryVector::AddHandle(result, std::move(target_handle));
		return ReadGeometry(final_buffer, 0, length, result);
	} else {
		// TODO: We should de-swizzle here in the future, right now we're copying twice
		return geometry_t::Deserialize(result, char_ptr_cast(target_ptr), length);
		// overflow_string.Finalize();
		// return overflow_string;
	}
}

inline static geometry_t FetchGeometryFromDict(ColumnSegment &segment, uint32_t dict_end_offset, Vector &result,
                                               data_ptr_t base_ptr, int32_t dict_offset, uint32_t string_length) {
	D_ASSERT(dict_offset <= NumericCast<int32_t>(segment.GetBlockManager().GetBlockSize()));
	if (DUCKDB_LIKELY(dict_offset >= 0)) {
		// regular string - fetch from dictionary
		auto dict_end = base_ptr + dict_end_offset;
		auto dict_pos = dict_end - dict_offset;

		auto str_ptr = char_ptr_cast(dict_pos);
		return ReadGeometry(dict_pos, 0, string_length, result);

	} else {
		// read overflow string
		block_id_t block_id;
		int32_t offset;
		ReadStringMarker(base_ptr + dict_end_offset - AbsValue<int32_t>(dict_offset), block_id, offset);

		return ReadOverflowString(segment, result, block_id, offset);
	}
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//

struct GeometryAnalyzeState : public AnalyzeState {
	explicit GeometryAnalyzeState(const CompressionInfo &info) : AnalyzeState(info) {
	}

	idx_t count;
	idx_t total_string_size;
	idx_t overflow_strings;
};

unique_ptr<AnalyzeState> GeometryInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager());
	return make_uniq<GeometryAnalyzeState>(info);
}

bool GeometryAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<GeometryAnalyzeState>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	state.count += count;
	auto data = UnifiedVectorFormat::GetData<geometry_t>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			// Get total serialized size
			auto string_size = data[idx].GetTotalByteSize();

			state.total_string_size += string_size;
			if (string_size >= GetGeometryBlockLimit(state.info.GetBlockSize())) {
				state.overflow_strings++;
			}
		}
	}

	return true;
}

idx_t GeometryFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<GeometryAnalyzeState>();
	return state.count * sizeof(int32_t) + state.total_string_size + state.overflow_strings * BIG_GEOMETRY_MARKER_SIZE;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct GeometryScanState final : public SegmentScanState {
	BufferHandle handle;
};

void GeometryInitPrefetch(ColumnSegment &segment, PrefetchState &prefetch_state) {
	prefetch_state.AddBlock(segment.block);
	auto segment_state = segment.GetSegmentState();
	if (segment_state) {
		auto &state = segment_state->Cast<UncompressedGeometrySegmentState>();
		auto &block_manager = segment.GetBlockManager();
		for (auto &block_id : state.on_disk_blocks) {
			auto block_handle = state.GetHandle(block_manager, block_id);
			prefetch_state.AddBlock(block_handle);
		}
	}
}

unique_ptr<SegmentScanState> GeometryInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto result = make_uniq<GeometryScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void GeometryScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                         idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<GeometryScanState>();
	auto start = state.GetPositionInSegment();

	auto baseptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto dict_end = GetDictionaryEnd(segment, scan_state.handle);
	auto base_data = reinterpret_cast<int32_t *>(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<geometry_t>(result);

	int32_t previous_offset = start > 0 ? base_data[start - 1] : 0;

	for (idx_t i = 0; i < scan_count; i++) {
		// std::abs used since offsets can be negative to indicate big strings
		auto current_offset = base_data[start + i];
		auto string_length = UnsafeNumericCast<uint32_t>(std::abs(current_offset) - std::abs(previous_offset));
		result_data[result_offset + i] =
		    FetchGeometryFromDict(segment, dict_end, result, baseptr, current_offset, string_length);
		previous_offset = base_data[start + i];
	}
}

void GeometryScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	GeometryScanPartial(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
void GeometrySelect(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                    const SelectionVector &sel, idx_t sel_count) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<GeometryScanState>();
	auto start = state.GetPositionInSegment();

	auto baseptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto dict_end = GetDictionaryEnd(segment, scan_state.handle);
	auto base_data = reinterpret_cast<int32_t *>(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<geometry_t>(result);

	for (idx_t i = 0; i < sel_count; i++) {
		idx_t index = start + sel.get_index(i);
		auto current_offset = base_data[index];
		auto prev_offset = index > 0 ? base_data[index - 1] : 0;
		auto string_length = UnsafeNumericCast<uint32_t>(std::abs(current_offset) - std::abs(prev_offset));
		result_data[i] = FetchGeometryFromDict(segment, dict_end, result, baseptr, current_offset, string_length);
	}
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void GeometryFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	// first pin the main buffer if it is not already pinned
	auto &handle = state.GetOrInsertHandle(segment);

	auto baseptr = handle.Ptr() + segment.GetBlockOffset();
	auto dict_end = GetDictionaryEnd(segment, handle);
	auto base_data = reinterpret_cast<int32_t *>(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<geometry_t>(result);

	auto dict_offset = base_data[row_id];
	uint32_t string_length;
	if (DUCKDB_UNLIKELY(row_id == 0LL)) {
		// edge case where this is the first string in the dict
		string_length = NumericCast<uint32_t>(std::abs(dict_offset));
	} else {
		string_length = NumericCast<uint32_t>(std::abs(dict_offset) - std::abs(base_data[row_id - 1]));
	}
	result_data[result_idx] = FetchGeometryFromDict(segment, dict_end, result, baseptr, dict_offset, string_length);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//

unique_ptr<CompressedSegmentState> GeometryInitSegment(ColumnSegment &segment, block_id_t block_id,
                                                       optional_ptr<ColumnSegmentState> segment_state) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	if (block_id == INVALID_BLOCK) {
		auto handle = buffer_manager.Pin(segment.block);
		GeometryDictionary dictionary;
		dictionary.size = 0;
		dictionary.end = UnsafeNumericCast<uint32_t>(segment.SegmentSize());
		SetDictionary(segment, handle, dictionary);
	}
	auto result = make_uniq<UncompressedGeometrySegmentState>();
	if (segment_state) {
		auto &serialized_state = segment_state->Cast<SerializedStringSegmentState>();
		result->on_disk_blocks = std::move(serialized_state.blocks);
	}
	return std::move(result);
}

unique_ptr<CompressionAppendState> GeometryInitAppend(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	// This block was initialized in StringInitSegment
	auto handle = buffer_manager.Pin(segment.block);
	return make_uniq<CompressionAppendState>(std::move(handle));
}

idx_t GeometryAppendBase(BufferHandle &handle, ColumnSegment &segment, SegmentStatistics &stats,
                         UnifiedVectorFormat &data, idx_t offset, idx_t count) {
	D_ASSERT(segment.GetBlockOffset() == 0);
	auto handle_ptr = handle.Ptr();
	auto source_data = UnifiedVectorFormat::GetData<geometry_t>(data);
	auto result_data = reinterpret_cast<int32_t *>(handle_ptr + DICTIONARY_HEADER_SIZE);
	auto dictionary_size = reinterpret_cast<uint32_t *>(handle_ptr);
	auto dictionary_end = reinterpret_cast<uint32_t *>(handle_ptr + sizeof(uint32_t));

	idx_t remaining_space = RemainingSpace(segment, handle);
	auto base_count = segment.count.load();
	for (idx_t i = 0; i < count; i++) {
		auto source_idx = data.sel->get_index(offset + i);
		auto target_idx = base_count + i;
		if (remaining_space < sizeof(int32_t)) {
			// string index does not fit in the block at all
			segment.count += i;
			return i;
		}
		remaining_space -= sizeof(int32_t);
		const bool is_null = !data.validity.RowIsValid(source_idx);
		if (is_null) {
			stats.statistics.SetHasNullFast();
			// null value is stored as a copy of the last value, this is done to be able to efficiently do the
			// string_length calculation
			if (target_idx > 0) {
				result_data[target_idx] = result_data[target_idx - 1];
			} else {
				result_data[target_idx] = 0;
			}
			continue;
		}
		auto end = handle.Ptr() + *dictionary_end;

#ifdef DEBUG
		GetDictionary(segment, handle).Verify(segment.GetBlockManager().GetBlockSize());
#endif
		// Unknown string, continue
		// non-null value, check if we can fit it within the block
		idx_t string_length = source_data[source_idx].GetTotalByteSize();

		// determine whether or not we have space in the block for this string
		bool use_overflow_block = false;
		idx_t required_space = string_length;
		if (DUCKDB_UNLIKELY(required_space >= GetGeometryBlockLimit(segment.GetBlockManager().GetBlockSize()))) {
			// string exceeds block limit, store in overflow block and only write a marker here
			required_space = BIG_GEOMETRY_MARKER_SIZE;
			use_overflow_block = true;
		}
		if (DUCKDB_UNLIKELY(required_space > remaining_space)) {
			// no space remaining: return how many tuples we ended up writing
			segment.count += i;
			return i;
		}

		// we have space: write the string
		// TODO: Write stats
		// UpdateStringStats(stats, source_data[source_idx]);

		if (DUCKDB_UNLIKELY(use_overflow_block)) {
			// write to overflow blocks
			block_id_t block;
			int32_t current_offset;
			// write the geometry into the current string block
			WriteGeometry(segment, source_data[source_idx], block, current_offset);
			*dictionary_size += BIG_GEOMETRY_MARKER_SIZE;
			remaining_space -= BIG_GEOMETRY_MARKER_SIZE;
			auto dict_pos = end - *dictionary_size;

			// write a big geometry marker into the dictionary
			WriteStringMarker(dict_pos, block, current_offset);

			// place the dictionary offset into the set of vectors
			// note: for overflow strings we write negative value

			// dictionary_size is an uint32_t value, so we can cast up.
			D_ASSERT(NumericCast<idx_t>(*dictionary_size) <= segment.GetBlockManager().GetBlockSize());
			result_data[target_idx] = -NumericCast<int32_t>((*dictionary_size));
		} else {
			// string fits in block, append to dictionary and increment dictionary position
			D_ASSERT(string_length < NumericLimits<uint16_t>::Maximum());
			*dictionary_size += required_space;
			remaining_space -= required_space;
			auto dict_pos = end - *dictionary_size;

			// now write the actual string data into the dictionary
			source_data[source_idx].Serialize(char_ptr_cast(dict_pos), string_length);

			// dictionary_size is an uint32_t value, so we can cast up.
			D_ASSERT(NumericCast<idx_t>(*dictionary_size) <= segment.GetBlockManager().GetBlockSize());
			// Place the dictionary offset into the set of vectors.
			result_data[target_idx] = NumericCast<int32_t>(*dictionary_size);
		}
		D_ASSERT(RemainingSpace(segment, handle) <= segment.GetBlockManager().GetBlockSize());
#ifdef DEBUG
		GetDictionary(segment, handle).Verify(segment.GetBlockManager().GetBlockSize());
#endif
	}
	segment.count += count;
	return count;
}

idx_t FinalizeAppend(ColumnSegment &segment, SegmentStatistics &) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto dict = GetDictionary(segment, handle);
	D_ASSERT(dict.end == segment.SegmentSize());
	// compute the total size required to store this segment
	auto offset_size = DICTIONARY_HEADER_SIZE + segment.count * sizeof(int32_t);
	auto total_size = offset_size + dict.size;

	CompressionInfo info(segment.GetBlockManager());
	if (total_size >= info.GetCompactionFlushLimit()) {
		// the block is full enough, don't bother moving around the dictionary
		return segment.SegmentSize();
	}

	// the block has space left: figure out how much space we can save
	auto move_amount = segment.SegmentSize() - total_size;
	// move the dictionary so it lines up exactly with the offsets
	auto dataptr = handle.Ptr();
	memmove(dataptr + offset_size, dataptr + dict.end - dict.size, dict.size);
	dict.end -= move_amount;
	D_ASSERT(dict.end == total_size);
	// write the new dictionary (with the updated "end")
	SetDictionary(segment, handle, dict);
	return total_size;
}

idx_t GeometryAppendBase(ColumnSegment &segment, SegmentStatistics &stats, UnifiedVectorFormat &data, idx_t offset,
                         idx_t count) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	return GeometryAppendBase(handle, segment, stats, data, offset, count);
}

idx_t GeometryAppend(CompressionAppendState &append_state, ColumnSegment &segment, SegmentStatistics &stats,
                     UnifiedVectorFormat &data, idx_t offset, idx_t count) {
	return GeometryAppendBase(append_state.handle, segment, stats, data, offset, count);
}

} // namespace

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction GeometryUncompressed::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::GEOMETRY);
	return CompressionFunction(
	    CompressionType::COMPRESSION_UNCOMPRESSED, data_type, GeometryInitAnalyze, GeometryAnalyze,
	    GeometryFinalAnalyze, UncompressedFunctions::InitCompression, UncompressedFunctions::Compress,
	    UncompressedFunctions::FinalizeCompress, GeometryInitScan, GeometryScan, GeometryScanPartial, GeometryFetchRow,
	    UncompressedFunctions::EmptySkip, GeometryInitSegment, GeometryInitAppend, GeometryAppend, FinalizeAppend,
	    nullptr, UncompressedStringStorage::SerializeState, UncompressedStringStorage::DeserializeState,
	    UncompressedStringStorage::VisitBlockIds, GeometryInitPrefetch, GeometrySelect);
}

} // namespace duckdb
