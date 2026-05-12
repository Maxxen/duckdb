#include "capi_v2_internal.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_error_info_get_message(duckdb_v2_error_info_ptr info, const char **out_message,
                                                      duckdb_v2_error_info_ptr *err) {
	if (!info || !out_message) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_error_info_get_message");
	}
	auto *ei = static_cast<duckdb::ErrorInfoV2 *>(info);
	*out_message = ei->message.c_str();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_error_info_destroy(duckdb_v2_error_info_ptr *info, duckdb_v2_error_info_ptr *err) {
	// Null-safe on both parameters. Destroying a freshly-zeroed or
	// already-destroyed info is a no-op.
	duckdb::DestroyErrorInfoSlot(info);
	return duckdb::ClearErrorInfo(err);
}
