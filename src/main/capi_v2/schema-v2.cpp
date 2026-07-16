#include "capi_v2_internal.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_schema_get_count(duckdb_v2_schema_handle schema, idx_t *out_count,
                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!schema || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_schema_get_count");
		}
		*out_count = duckdb::ToSchema(schema)->fields.size();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_schema_get_field(duckdb_v2_schema_handle schema, idx_t index,
                                                duckdb_v2_identifier_t *out_name,
                                                duckdb_v2_logical_type_handle *out_type,
                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!schema || !out_name || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_schema_get_field");
		}
		*out_name = duckdb_v2_identifier_t {nullptr, 0};
		*out_type = nullptr;
		auto &wrapper = *duckdb::ToSchema(schema);
		if (index >= wrapper.fields.size()) {
			throw duckdb::InvalidInputException("index out of range in duckdb_v2_schema_get_field");
		}
		auto &field = wrapper.fields[index];
		// Both borrowed, valid until the schema is destroyed; out_type aliases the
		// wrapper-owned LogicalType and must not be destroyed by the caller.
		*out_name = duckdb::ToStr(field.name);
		*out_type = reinterpret_cast<duckdb_v2_logical_type_handle>(&field.type);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_schema_destroy(duckdb_v2_schema_handle *schema) {
	if (!schema || !*schema) {
		return static_cast<DUCKDB_V2_API_CALL_t>(DUCKDB_V2_ERROR_NONE);
	}
	delete duckdb::ToSchema(*schema);
	*schema = nullptr;
	return static_cast<DUCKDB_V2_API_CALL_t>(DUCKDB_V2_ERROR_NONE);
}
