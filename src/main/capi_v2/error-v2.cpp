#include "capi_v2_internal.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_error_info_get_text(duckdb_v2_error_info_handle info, duckdb_v2_str *out_text) {
	if (!info || !out_text) {
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	}
	const auto *ei = reinterpret_cast<duckdb::ErrorInfoV2 *>(info);
	*out_text = duckdb::ToStr(ei->message);
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_API_CALL_t duckdb_v2_error_info_set_text(duckdb_v2_error_info_handle info, duckdb_v2_str text) {
	if (!info || (!text.ptr && text.len > 0)) {
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	}
	auto *ei = reinterpret_cast<duckdb::ErrorInfoV2 *>(info);
	ei->message = duckdb::ToString(text);
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_API_CALL_t duckdb_v2_error_info_get_code(duckdb_v2_error_info_handle info, duckdb_v2_error_code_t *out_code) {
	if (!info || !out_code) {
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	}
	const auto *ei = reinterpret_cast<duckdb::ErrorInfoV2 *>(info);
	*out_code = ei->code;
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_API_CALL_t duckdb_v2_error_info_set_code(duckdb_v2_error_info_handle info, duckdb_v2_error_code_t code) {
	if (!info) {
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	}
	auto *ei = reinterpret_cast<duckdb::ErrorInfoV2 *>(info);
	ei->code = code;
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_API_CALL_t duckdb_v2_error_info_destroy(duckdb_v2_error_info_handle *info) {
	if (info && *info) {
		delete reinterpret_cast<duckdb::ErrorInfoV2 *>(*info);
		*info = nullptr;
	}
	return DUCKDB_V2_ERROR_NONE;
}
