#include "capi_v2_internal.hpp"

#include "duckdb/common/sql_identifier.hpp"

#include <cstdlib>
#include <cstring>

DUCKDB_V2_API_CALL_t duckdb_v2_identifier_render_quoted(duckdb_v2_identifier_t name, char **out_text,
                                                        duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		// A {NULL, 0} view is a valid (empty) name; only a null pointer with a
		// nonzero length is malformed.
		if ((!name.ptr && name.len > 0) || !out_text) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_identifier_render_quoted");
		}
		*out_text = nullptr;
		auto rendered = duckdb::SQLIdentifier::ToString(duckdb::ToString(name));
		auto *buf = static_cast<char *>(std::malloc(rendered.size() + 1));
		if (!buf) {
			throw duckdb::OutOfMemoryException("malloc failed in duckdb_v2_identifier_render_quoted");
		}
		std::memcpy(buf, rendered.data(), rendered.size());
		buf[rendered.size()] = '\0';
		*out_text = buf;
	});
}
