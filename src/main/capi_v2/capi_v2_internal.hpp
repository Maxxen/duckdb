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

// DuckDB internals used by the option set/get bridge.
#include "duckdb/main/setting_info.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/main/database.hpp"

// V2 C API header -- all types use duckdb_v2_ prefix, no collision with V1.
#include "duckdb_v2.h"

#include <atomic>
#include <cstring>
#include <string>
#include <vector>

#ifdef _WIN32
#ifndef strdup
#define strdup _strdup
#endif
#endif

namespace duckdb {

// Forward declarations.
struct EnvironmentWrapperV2;
struct DatabaseWrapperV2;
struct ConnectionWrapperV2;

// Backing struct for the opaque duckdb_v2_environment_ptr handle. Owns
// the DBInstanceCache used to share the path manager across all
// databases opened through it (so the same file opened twice is
// rejected with RESOURCE_IN_USE). Cache behavior at open time is
// NEVER_CACHE: the cache participates in path-manager coordination but
// never memoizes DatabaseInstances; every open produces a fresh handle.
//
// open_database_count is incremented at open time and decremented in
// duckdb_v2_close. duckdb_v2_destroy_environment refuses while the
// counter is > 0 (the only handle in V2 with strict refuse-on-destroy
// semantics).
struct EnvironmentWrapperV2 {
	unique_ptr<DBInstanceCache> cache;
	std::atomic<uint64_t> open_database_count {0};
};

struct DatabaseWrapperV2 {
	// Back-pointer used by duckdb_v2_close to decrement the
	// environment's open-database counter. Guaranteed valid for the
	// database's lifetime: destroy_environment refuses while this
	// database is still alive, so the env outlives the db.
	EnvironmentWrapperV2 *environment = nullptr;
	shared_ptr<DuckDB> database;
	// Internal connection used by database-scope option reads to drive
	// the legacy get_setting cascade (which requires a ClientContext).
	// Created at open and destroyed with the wrapper.
	unique_ptr<Connection> admin_connection;
};

struct ConnectionWrapperV2 {
	shared_ptr<Connection> connection;
};

// Backing struct for the opaque duckdb_v2_option_ptr handle. Owns all
// strings; borrowed pointers returned by accessors are valid until the
// option is destroyed. Created via duckdb_v2_option_create with just
// (name, setting); metadata fields (description, default setting,
// target scope, aliases) are populated by the database/connection get
// paths.
struct OptionWrapperV2 {
	std::string name;
	std::string setting;
	std::string default_setting;
	std::string description;
	DUCKDB_V2_OPTION_TARGET_SCOPE target_scope = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	std::vector<std::string> aliases;
};

// Opaque-handle casts used across the bridge. Inline so the unity build
// doesn't see duplicate definitions if two TUs are concatenated.
inline EnvironmentWrapperV2 *ToEnv(duckdb_v2_environment_ptr ptr) {
	return static_cast<EnvironmentWrapperV2 *>(ptr);
}
inline DatabaseWrapperV2 *ToDb(duckdb_v2_database_ptr ptr) {
	return static_cast<DatabaseWrapperV2 *>(ptr);
}
inline ConnectionWrapperV2 *ToConn(duckdb_v2_connection_ptr ptr) {
	return static_cast<ConnectionWrapperV2 *>(ptr);
}
inline OptionWrapperV2 *ToOption(duckdb_v2_option_ptr ptr) {
	return static_cast<OptionWrapperV2 *>(ptr);
}
// The logical_type handle is not wrapped — the underlying duckdb::LogicalType
// is heap-allocated directly. The V2 test suite relies on this layout to
// share fixtures with V1 (V1-built composites are reinterpret-cast to V2
// handles); if a wrapper is added later, those tests must change too.
inline LogicalType *ToLogicalType(duckdb_v2_logical_type_ptr ptr) {
	return static_cast<LogicalType *>(ptr);
}

// Map DuckDB's SettingScopeTarget to the V2 enum. Legacy options
// (declared via DUCKDB_GLOBAL / DUCKDB_LOCAL / DUCKDB_GLOBAL_LOCAL)
// carry SettingScopeTarget::INVALID; we surface that as UNKNOWN so V2
// callers can distinguish "unconstrained legacy" from a declared scope.
inline DUCKDB_V2_OPTION_TARGET_SCOPE MapScopeTarget(SettingScopeTarget s) {
	switch (s) {
	case SettingScopeTarget::GLOBAL_ONLY:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_ONLY;
	case SettingScopeTarget::LOCAL_ONLY:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_ONLY;
	case SettingScopeTarget::GLOBAL_DEFAULT:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_DEFAULT;
	case SettingScopeTarget::LOCAL_DEFAULT:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_DEFAULT;
	default:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	}
}

// Map V2's user-facing scope choice to DuckDB's SetScope.
inline SetScope MapSettingScopeV2(DUCKDB_V2_SETTING_SCOPE s) {
	switch (s) {
	case DUCKDB_V2_SETTING_SCOPE_GLOBAL:
		return SetScope::GLOBAL;
	case DUCKDB_V2_SETTING_SCOPE_LOCAL:
		return SetScope::SESSION;
	default:
		return SetScope::AUTOMATIC;
	}
}

// Scan setting_aliases[] for entries pointing at the same canonical
// option (matched by name) and append their alias names.
inline void PopulateOptionAliases(OptionWrapperV2 &out, const char *canonical_name) {
	auto alias_count = DBConfig::GetAliasCount();
	for (idx_t i = 0; i < alias_count; i++) {
		auto alias = DBConfig::GetAliasByIndex(i);
		if (!alias) {
			continue;
		}
		auto aliased = DBConfig::GetOptionByIndex(alias->option_index);
		if (aliased && std::strcmp(aliased->name, canonical_name) == 0) {
			out.aliases.emplace_back(alias->alias);
		}
	}
}

// Read the effective setting for `name` through `client`'s setting
// cascade. For a database admin client (no LOCAL overrides) this
// returns GLOBAL → static default; for a connection client it returns
// LOCAL → GLOBAL → static default. Falls back to `fallback_default` if
// the cascade returned NULL.
inline std::string ReadEffectiveSetting(ClientContext &client, const std::string &name,
                                        const std::string &fallback_default) {
	Value result;
	if (client.TryGetCurrentSetting(name, result) && !result.IsNull()) {
		return result.ToString();
	}
	return fallback_default;
}

// Populate `out` from a core ConfigurationOption resolved by name. The
// canonical name is taken from the option (not the input), so passing
// an alias resolves to the canonical with the alias listed in
// out.aliases.
inline void PopulateOptionFromCore(OptionWrapperV2 &out, const ConfigurationOption &option, ClientContext &client) {
	out.name = option.name ? option.name : "";
	out.description = option.description ? option.description : "";
	out.target_scope = MapScopeTarget(option.scope);
	out.default_setting = option.default_value ? option.default_value : "";
	out.aliases.clear();
	PopulateOptionAliases(out, out.name.c_str());
	out.setting = ReadEffectiveSetting(client, out.name, out.default_setting);
}

// Populate `out` from an extension option. Extension options carry no
// SettingScopeTarget (the V2 enum reports UNKNOWN) and no aliases.
inline void PopulateOptionFromExtension(OptionWrapperV2 &out, const std::string &name,
                                        const ExtensionOption &ext_option, ClientContext &client) {
	out.name = name;
	out.description = ext_option.description;
	out.target_scope = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	out.default_setting = ext_option.default_value.IsNull() ? std::string() : ext_option.default_value.ToString();
	out.aliases.clear();
	out.setting = ReadEffectiveSetting(client, name, out.default_setting);
}

// Resolve `name` to either a core option or an extension option and
// populate `out`. Throws InvalidInputException if neither matches. The
// bridge layer is responsible for translating the exception into a V2
// error code (matches the pattern PhysicalSet uses internally).
inline void BuildOptionByName(OptionWrapperV2 &out, ClientContext &client, DBConfig &config, const std::string &name) {
	auto option = DBConfig::GetOptionByName(name);
	if (option) {
		PopulateOptionFromCore(out, *option, client);
		return;
	}
	ExtensionOption ext_option;
	if (config.TryGetExtensionOption(name, ext_option)) {
		PopulateOptionFromExtension(out, name, ext_option, client);
		return;
	}
	throw InvalidInputException("unknown configuration option: %s", name);
}

// Populate `out` from the option at `index` in the {core ∪ extension}
// space. Indices [0, core_count) hit core options;
// [core_count, total) hit extension options. Throws
// InvalidInputException for an out-of-range index.
inline void BuildOptionByIndex(OptionWrapperV2 &out, ClientContext &client, DBConfig &config, idx_t index) {
	auto core_count = DBConfig::GetOptionCount();
	if (index < core_count) {
		auto option = DBConfig::GetOptionByIndex(index);
		if (!option) {
			throw InvalidInputException("core option not found at given index");
		}
		PopulateOptionFromCore(out, *option, client);
		return;
	}
	idx_t ext_rel = index - core_count;
	auto ext_settings = config.GetExtensionSettings();
	if (ext_rel >= ext_settings.size()) {
		throw InvalidInputException("option index out of range");
	}
	idx_t i = 0;
	for (auto &kv : ext_settings) {
		if (i == ext_rel) {
			PopulateOptionFromExtension(out, kv.first, kv.second, client);
			return;
		}
		++i;
	}
	throw InvalidInputException("option index out of range");
}

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
	DUCKDB_V2_API_CALL_t code;
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

inline DUCKDB_V2_API_CALL_t GetErrorCodeFromExceptionType(ExceptionType type) {
	switch (type) {
	// Invalid Input
	case ExceptionType::INVALID_INPUT:
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	case ExceptionType::OUT_OF_RANGE:
		return DUCKDB_V2_ERROR_OUT_OF_RANGE;
	case ExceptionType::OBJECT_SIZE:
		return DUCKDB_V2_ERROR_OBJECT_SIZE;
	// IO
	case ExceptionType::IO:
		return DUCKDB_V2_ERROR_IO_GENERAL;
	case ExceptionType::NETWORK:
		return DUCKDB_V2_ERROR_IO_NETWORK;
	case ExceptionType::HTTP:
		return DUCKDB_V2_ERROR_IO_HTTP;
	// Resource
	case ExceptionType::OUT_OF_MEMORY:
		return DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY;
	case ExceptionType::CONNECTION:
		return DUCKDB_V2_ERROR_RESOURCE_CONNECTION;
	case ExceptionType::DEPENDENCY:
		return DUCKDB_V2_ERROR_RESOURCE_DEPENDENCY;
	case ExceptionType::MISSING_EXTENSION:
		return DUCKDB_V2_ERROR_RESOURCE_MISSING_EXTENSION;
	case ExceptionType::AUTOLOAD:
		return DUCKDB_V2_ERROR_RESOURCE_AUTOLOAD;
	// Type
	case ExceptionType::CONVERSION:
		return DUCKDB_V2_ERROR_TYPE_CONVERSION;
	case ExceptionType::UNKNOWN_TYPE:
		return DUCKDB_V2_ERROR_TYPE_UNKNOWN;
	case ExceptionType::INVALID_TYPE:
		return DUCKDB_V2_ERROR_TYPE_INVALID;
	case ExceptionType::MISMATCH_TYPE:
		return DUCKDB_V2_ERROR_TYPE_MISMATCH;
	case ExceptionType::DECIMAL:
		return DUCKDB_V2_ERROR_TYPE_DECIMAL;
	case ExceptionType::DIVIDE_BY_ZERO:
		return DUCKDB_V2_ERROR_TYPE_DIVIDE_BY_ZERO;
	// Query
	case ExceptionType::PARSER:
		return DUCKDB_V2_ERROR_QUERY_PARSER;
	case ExceptionType::SYNTAX:
		return DUCKDB_V2_ERROR_QUERY_SYNTAX;
	case ExceptionType::BINDER:
		return DUCKDB_V2_ERROR_QUERY_BINDER;
	case ExceptionType::PLANNER:
		return DUCKDB_V2_ERROR_QUERY_PLANNER;
	case ExceptionType::OPTIMIZER:
		return DUCKDB_V2_ERROR_QUERY_OPTIMIZER;
	case ExceptionType::EXPRESSION:
		return DUCKDB_V2_ERROR_QUERY_EXPRESSION;
	case ExceptionType::EXECUTOR:
		return DUCKDB_V2_ERROR_QUERY_EXECUTOR;
	case ExceptionType::SCHEDULER:
		return DUCKDB_V2_ERROR_QUERY_SCHEDULER;
	case ExceptionType::NOT_IMPLEMENTED:
		return DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED;
	case ExceptionType::PARAMETER_NOT_RESOLVED:
		return DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED;
	case ExceptionType::PARAMETER_NOT_ALLOWED:
		return DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_ALLOWED;
	// Database
	case ExceptionType::CATALOG:
		return DUCKDB_V2_ERROR_DATABASE_CATALOG;
	case ExceptionType::TRANSACTION:
		return DUCKDB_V2_ERROR_DATABASE_TRANSACTION;
	case ExceptionType::CONSTRAINT:
		return DUCKDB_V2_ERROR_DATABASE_CONSTRAINT;
	case ExceptionType::INDEX:
		return DUCKDB_V2_ERROR_DATABASE_INDEX;
	case ExceptionType::SEQUENCE:
		return DUCKDB_V2_ERROR_DATABASE_SEQUENCE;
	case ExceptionType::STAT:
		return DUCKDB_V2_ERROR_DATABASE_STATISTICS;
	case ExceptionType::SERIALIZATION:
		return DUCKDB_V2_ERROR_DATABASE_SERIALIZATION;
	// Configuration
	case ExceptionType::SETTINGS:
		return DUCKDB_V2_ERROR_CONFIGURATION_SETTINGS;
	case ExceptionType::INVALID_CONFIGURATION:
		return DUCKDB_V2_ERROR_CONFIGURATION_INVALID;
	case ExceptionType::PERMISSION:
		return DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION;
	// Runtime
	case ExceptionType::INTERNAL:
		return DUCKDB_V2_ERROR_RUNTIME_INTERNAL;
	case ExceptionType::FATAL:
		return DUCKDB_V2_ERROR_RUNTIME_FATAL;
	case ExceptionType::INTERRUPT:
		return DUCKDB_V2_ERROR_RUNTIME_INTERRUPT;
	case ExceptionType::NULL_POINTER:
		return DUCKDB_V2_ERROR_RUNTIME_NULL_POINTER;
	default:
		return DUCKDB_V2_API_ERROR;
	}
}

template <class T>
DUCKDB_V2_API_CALL_t WithErrorHandler(duckdb_v2_error_info_ptr err, T callback) {
	auto code = static_cast<DUCKDB_V2_API_CALL_t>(DUCKDB_V2_ERROR_NONE);
	auto text = string();

	try {
		// Invoke the callback
		callback();
	} catch (const duckdb::Exception &ex) {
		ErrorData error_data(ex);
		code = GetErrorCodeFromExceptionType(error_data.Type());
		text = error_data.Message();
	} catch (const std::exception &ex) {
		code = DUCKDB_V2_API_ERROR;
		text = ex.what();
	} catch (...) {
		code = DUCKDB_V2_API_ERROR;
		text = "An unknown error occurred.";
	}

	// Pass up to the caller via the out-parameter if they provided one; otherwise swallow.
	if (err) {
		auto &out = *static_cast<ErrorInfoV2 *>(err);
		out.code = code;
		out.message = std::move(text);
	}

	return code;
}

} // namespace duckdb
