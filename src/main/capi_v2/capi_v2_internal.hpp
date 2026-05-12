//===----------------------------------------------------------------------===//
//                         DuckDB
//
// capi_v2_internal.hpp
//
// Internal header for V2 C API bridge implementations.
//
//===----------------------------------------------------------------------===//

#pragma once

// DuckDB C++ internals (also pulls in duckdb.h which defines idx_t, etc.)
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"
#include "duckdb/main/db_instance_cache.hpp"

// V2 C API header -- all types use duckdb_v2_ prefix, no collision with V1.
#include "duckdb_v2.h"

#include <cstring>
#include <string>
#include <vector>

#ifdef _WIN32
#ifndef strdup
#define strdup _strdup
#endif
#endif

namespace duckdb {

struct DatabaseWrapperV2 {
	shared_ptr<DuckDB> database;
};

struct ConnectionWrapperV2 {
	shared_ptr<Connection> connection;
};

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

struct PreparedStatementWrapperV2 {
	case_insensitive_map_t<BoundParameterData> values;
	unique_ptr<PreparedStatement> statement;
	bool success = true;
	ErrorData error_data;
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
