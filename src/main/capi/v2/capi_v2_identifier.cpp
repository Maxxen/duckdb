#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/common/sql_identifier.hpp"

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_identifier_render_quoted(const duckdb_v2_identifier_t *name, duckdb_v2_bytebuf *out_text,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_text);
	ResetBytebuf(*out_text);
	DUCKDB_CHECK_ARG(name);
	return WithErrorHandler(err, [&]() {
		SetBytebufText(*out_text, duckdb::SQLIdentifier::ToString(duckdb::string(ConvertIdentifierName(name))));
	});
}
