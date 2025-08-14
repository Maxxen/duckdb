#include "reader/string_column_reader.hpp"
#include "utf8proc_wrapper.hpp"
#include "parquet_reader.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// StringParquetValueConversion
//===--------------------------------------------------------------------===//
struct StringParquetValueConversion {
	template <bool CHECKED>
	static string_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		auto &scr = reader.Cast<StringColumnReader>();
		uint32_t str_len =
		    scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() : scr.fixed_width_string_length;
		plain_data.available(str_len);
		auto plain_str = char_ptr_cast(plain_data.ptr);
		scr.Verify(plain_str, str_len);
		auto ret_str = string_t(plain_str, str_len);
		plain_data.inc(str_len);
		return ret_str;
	}
	template <bool CHECKED>
	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		auto &scr = reader.Cast<StringColumnReader>();
		uint32_t str_len =
		    scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() : scr.fixed_width_string_length;
		plain_data.inc(str_len);
	}
	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
		return false;
	}

	static idx_t PlainConstantSize() {
		return 0;
	}
};

//===--------------------------------------------------------------------===//
// String Column Reader
//===--------------------------------------------------------------------===//
StringColumnReader::StringColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema)
    : ColumnReader(reader, schema) {
	fixed_width_string_length = 0;
	if (schema.parquet_type == Type::FIXED_LEN_BYTE_ARRAY) {
		fixed_width_string_length = schema.type_length;
	}
}

void StringColumnReader::VerifyString(const char *str_data, uint32_t str_len) {
	// verify if a string is actually UTF8, and if there are no null bytes in the middle of the string
	// technically Parquet should guarantee this, but reality is often disappointing
	UnicodeInvalidReason reason;
	size_t pos;
	auto utf_type = Utf8Proc::Analyze(str_data, str_len, &reason, &pos);
	if (utf_type == UnicodeType::INVALID) {
		throw InvalidInputException("Invalid string encoding found in Parquet file: value \"" +
		                            Blob::ToString(string_t(str_data, str_len)) + "\" is not valid UTF8!");
	}
}

void StringColumnReader::Verify(const char *str_data, uint32_t str_len) {
	if (Type().id() == LogicalTypeId::VARCHAR) {
		VerifyString(str_data, str_len);
	}
}

class ParquetStringVectorBuffer : public VectorBuffer {
public:
	explicit ParquetStringVectorBuffer(shared_ptr<ResizeableBuffer> buffer_p)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), buffer(std::move(buffer_p)) {
	}

private:
	shared_ptr<ResizeableBuffer> buffer;
};

void StringColumnReader::ReferenceBlock(Vector &result, shared_ptr<ResizeableBuffer> &block) {
	StringVector::AddBuffer(result, make_buffer<ParquetStringVectorBuffer>(block));
}

void StringColumnReader::Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
                               idx_t result_offset, Vector &result) {
	ReferenceBlock(result, plain_data);
	PlainTemplated<string_t, StringParquetValueConversion>(*plain_data, defines, num_values, result_offset, result);
}

void StringColumnReader::PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values) {
	PlainSkipTemplated<StringParquetValueConversion>(plain_data, defines, num_values);
}

void StringColumnReader::PlainSelect(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
                                     Vector &result, const SelectionVector &sel, idx_t count) {
	ReferenceBlock(result, plain_data);
	PlainSelectTemplated<string_t, StringParquetValueConversion>(*plain_data, defines, num_values, result, sel, count);
}

} // namespace duckdb
