//===----------------------------------------------------------------------===//
//                         DuckDB
//
// capi_v2_internal.hpp
//
// Internal header for V2 C API bridge implementations.
//
//===----------------------------------------------------------------------===//

#pragma once

// V2 C API header -- all types use duckdb_v2_ prefix, no collision with V1.
#include "duckdb_v2.h"

#include <string>
#include <vector>

namespace duckdb {

// Backing struct for the opaque duckdb_v2_option_ptr handle. Owns all
// strings; borrowed pointers returned by accessors are valid until the
// option is destroyed. Created via duckdb_v2_option_create with just
// (name, setting); the metadata fields are populated by the eventual
// database/connection get path (added in a follow-up).
struct OptionWrapperV2 {
	std::string name;
	std::string setting;
	std::string default_setting;
	std::string description;
	DUCKDB_V2_OPTION_TARGET_SCOPE target_scope = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	std::vector<std::string> aliases;
};

// Backing struct for the opaque duckdb_v2_error_info_ptr handle. Allocated
// only on failure paths and only when the caller requested detail (i.e.
// passed a non-null err out-parameter).
struct ErrorInfoV2 {
	std::string message;
};

// Destroy a previously-allocated error info (if any) and null the slot.
inline void DestroyErrorInfoSlot(duckdb_v2_error_info_ptr *err) {
	if (err && *err) {
		delete static_cast<ErrorInfoV2 *>(*err);
		*err = nullptr;
	}
}

// Failure path. Allocate a fresh ErrorInfoV2 and store its pointer in *err.
// If *err was already non-null, the previous info is destroyed first. Safe
// to call with err == nullptr (caller opted out of detail). Always returns
// the supplied code; the return value is authoritative regardless of err.
inline DUCKDB_V2_API_CALL_t SetErrorInfo(duckdb_v2_error_info_ptr *err, DUCKDB_V2_API_CALL_t code, const char *msg) {
	if (err) {
		DestroyErrorInfoSlot(err);
		auto *info = new ErrorInfoV2();
		info->message = msg ? msg : "";
		*err = static_cast<duckdb_v2_error_info_ptr>(info);
	}
	return code;
}

// Success path. Ensure *err is nullptr, destroying any prior info. Safe to
// call with err == nullptr.
inline DUCKDB_V2_API_CALL_t ClearErrorInfo(duckdb_v2_error_info_ptr *err) {
	DestroyErrorInfoSlot(err);
	return DUCKDB_V2_ERROR_NONE;
}

} // namespace duckdb
