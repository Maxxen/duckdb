//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/string_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "reader/templated_column_reader.hpp"

namespace duckdb {

class GeoColumnReader final : public StringColumnReader {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::VARCHAR;

	GeoColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema) : StringColumnReader(reader, schema) {
		D_ASSERT(schema.parquet_type == Type::BYTE_ARRAY);
	}

	void Verify(const char *str_data, uint32_t str_len) override;

	void Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values, idx_t result_offset,
	           Vector &result) override;
	void PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values) override;
	void PlainSelect(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values, Vector &result,
	                 const SelectionVector &sel, idx_t count) override;
};

} // namespace duckdb
