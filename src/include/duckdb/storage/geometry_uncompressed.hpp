#pragma once

namespace duckdb {

/*
struct SerializedGeometrySegmentState : public ColumnSegmentState {
public:
  SerializedGeometrySegmentState();
  explicit SerializedGeometrySegmentState(vector<block_id_t> blocks_p);
public:
  void Serialize(Serializer &serializer) const override;
};

struct UncompressedGeometryStorage {
  //! Dictionary header size at the beginning of the string segment (offset + length)
  static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(uint32_t) + sizeof(uint32_t);
  //! Marker used in length field to indicate the presence of a big string
  static constexpr uint16_t BIG_GEOMETRY_MARKER = (uint16_t)-1;
  //! Base size of big string marker (block id + offset)
  static constexpr idx_t BIG_GEOMETRY_MARKER_BASE_SIZE = sizeof(block_id_t) + sizeof(int32_t);
  //! The marker size of the big string
  static constexpr idx_t BIG_GEOMETRY_MARKER_SIZE = BIG_GEOMETRY_MARKER_BASE_SIZE;

public:


  //===--------------------------------------------------------------------===//
  // Analyze
  //===--------------------------------------------------------------------===/

  static unique_ptr<AnalyzeState> GeometryInitAnalyze(ColumnData &col_data, PhysicalType type);
  static bool GeometryAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
  static idx_t GeometryFinalAnalyze(AnalyzeState &state_p);


  //===--------------------------------------------------------------------===//
  // Append
  //===--------------------------------------------------------------------===/

  static idx_t GeometryAppendBase(BufferHandle &handle, ColumnSegment &segment, SegmentStatistics &stats,
                                  UnifiedVectorFormat &data, idx_t offset, idx_t count);

  static idx_t GeometryAppend(CompressionAppendState &append_state, ColumnSegment &segment,
                            SegmentStatistics &stats, UnifiedVectorFormat &data, idx_t offset, idx_t count);

  static idx_t GeometryAppendBase(ColumnSegment &segment, SegmentStatistics &stats,
                              UnifiedVectorFormat &data, idx_t offset, idx_t count);

  static idx_t FinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats);

};
*/

} // namespace duckdb
