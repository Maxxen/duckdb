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

class StringColumnReader : public ColumnReader {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::VARCHAR;

public:
	StringColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema);
	idx_t fixed_width_string_length;

public:
	static void VerifyString(const char *str_data, uint32_t str_len);
	static void ReferenceBlock(Vector &result, shared_ptr<ResizeableBuffer> &block);

	virtual void Verify(const char *str_data, uint32_t str_len);

	// Verify, reference or transform the string data if necessary. Return true if we need to reference the block
	virtual bool FromRawData(const char *data, uint32_t length, string_t &result) {
		Verify(data, length);
		result = string_t(data, length);
		return true;
	}

protected:
	void Plain(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values, idx_t result_offset,
	           Vector &result) override {
		throw NotImplementedException("StringColumnReader can only read plain data from a shared buffer");
	}
	void Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values, idx_t result_offset,
	           Vector &result) override;
	void PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values) override;
	void PlainSelect(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values, Vector &result,
	                 const SelectionVector &sel, idx_t count) override;

	bool SupportsDirectFilter() const override {
		return true;
	}
	bool SupportsDirectSelect() const override {
		return true;
	}
};

} // namespace duckdb
