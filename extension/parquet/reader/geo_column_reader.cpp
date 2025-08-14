#include "reader/geo_column_reader.hpp"
#include "parquet_reader.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// StringParquetValueConversion
//===--------------------------------------------------------------------===//
struct GeometryParquetValueConversion {

	template <bool CHECKED>
	static string_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		auto &scr = reader.Cast<GeoColumnReader>();

		const auto wkb_len = plain_data.read<uint32_t>();
		plain_data.available(wkb_len);

		const auto wkb_ptr = char_ptr_cast(plain_data.ptr);
		scr.Verify(wkb_ptr, wkb_len);

		const auto ret_str = string_t(wkb_ptr, wkb_len);
		plain_data.inc(wkb_len);
		return ret_str;
	}

	template <bool CHECKED>
	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		const auto wkb_len = plain_data.read<uint32_t>();
		plain_data.inc(wkb_len);
	}

	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
		return false;
	}

	static idx_t PlainConstantSize() {
		return 0;
	}
};

//===--------------------------------------------------------------------===//
// Geo Column Reader
//===--------------------------------------------------------------------===//
void GeoColumnReader::Verify(const char *str_data, uint32_t str_len) {
	// Verify that this is valid WKB!
}

void GeoColumnReader::Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
                            idx_t result_offset, Vector &result) {
	ReferenceBlock(result, plain_data);
	PlainTemplated<string_t, GeometryParquetValueConversion>(*plain_data, defines, num_values, result_offset, result);
}

void GeoColumnReader::PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values) {
	PlainSkipTemplated<GeometryParquetValueConversion>(plain_data, defines, num_values);
}

void GeoColumnReader::PlainSelect(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
                                  Vector &result, const SelectionVector &sel, idx_t count) {
	ReferenceBlock(result, plain_data);
	PlainSelectTemplated<string_t, GeometryParquetValueConversion>(*plain_data, defines, num_values, result, sel,
	                                                               count);
}

} // namespace duckdb
