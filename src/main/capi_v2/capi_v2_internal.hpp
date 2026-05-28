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
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/bignum.hpp"
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
// Same pattern as ToLogicalType: a duckdb_v2_value_ptr is a heap-allocated
// duckdb::Value with no wrapper. Lets V2 tests adopt V1-built values the
// same way the logical_type bridge reuses V1 fixtures. Keep it identity —
// do not wrap.
inline Value *ToValue(duckdb_v2_value_ptr ptr) {
	return static_cast<Value *>(ptr);
}
// duckdb_v2_result_ptr is a heap-allocated duckdb::MaterializedQueryResult
// with no wrapper. The V2 result surface today is materialized-only, by
// construction: connection_query is the sole producer and uses
// Connection::Query(const string &) which returns
// unique_ptr<MaterializedQueryResult>. If a future entrypoint produces a
// streaming or pending QueryResult, this cast must change — the bridge
// should either gain a wrapper that tags the variant, or split into
// per-shape handle types.
inline MaterializedQueryResult *ToResult(duckdb_v2_result_ptr ptr) {
	return static_cast<MaterializedQueryResult *>(ptr);
}
// duckdb_v2_data_chunk_ptr is a heap-allocated duckdb::DataChunk with no
// wrapper. Cardinality flows through the API explicitly; no per-chunk
// state needs caching. duckdb_v2_data_chunk_destroy deletes through this
// cast.
inline DataChunk *ToDataChunk(duckdb_v2_data_chunk_ptr ptr) {
	return static_cast<DataChunk *>(ptr);
}
// duckdb_v2_vector_ptr is a borrowed duckdb::Vector. Top-level vectors
// point into the owning DataChunk's data[]; nested children point into
// core's ListVector / ArrayVector / StructVector / etc storage. No
// wrapper — vector_get_view extracts (data, validity, sel) directly via
// the matching FlatVector / ConstantVector / DictionaryVector core
// helpers, without caching a UnifiedVectorFormat.
inline Vector *ToVector(duckdb_v2_vector_ptr ptr) {
	return static_cast<Vector *>(ptr);
}

// Map core's VectorType to the V2 surface. FSST / SEQUENCE / SHREDDED
// collapse into OTHER — V2's untyped view rejects those kinds and
// requires an explicit duckdb_v2_vector_flatten first.
inline DUCKDB_V2_VECTOR_TYPE MapVectorType(VectorType vt) {
	switch (vt) {
	case VectorType::FLAT_VECTOR:
		return DUCKDB_V2_VECTOR_TYPE_FLAT;
	case VectorType::CONSTANT_VECTOR:
		return DUCKDB_V2_VECTOR_TYPE_CONSTANT;
	case VectorType::DICTIONARY_VECTOR:
		return DUCKDB_V2_VECTOR_TYPE_DICTIONARY;
	default:
		return DUCKDB_V2_VECTOR_TYPE_OTHER;
	}
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
	DUCKDB_V2_API_CALL_t code = DUCKDB_V2_ERROR_NONE;
	std::string message;
};

// Failure path. Ensure *err holds an ErrorInfoV2 (lazy-allocate if the slot
// is empty), then write code + message in place. Reuses any pre-existing
// slot content — which may be heap-allocated by a prior call OR caller-
// owned / trampoline-owned stack memory. Never destroys the slot. Safe to
// call with err == nullptr (caller opted out of detail). Returns the
// supplied code; the return value is authoritative regardless of err.
//
// Slot cleanup is the caller's job. External callers destroy via
// `error_info_destroy` on slots they own; callbacks must NEVER destroy the
// err slot handed to them by the library (the slot's storage may live on
// the library's stack).
inline DUCKDB_V2_API_CALL_t SetErrorInfo(duckdb_v2_error_info_ptr *err, DUCKDB_V2_API_CALL_t code, const char *msg) {
	if (err) {
		if (!*err) {
			*err = static_cast<duckdb_v2_error_info_ptr>(new ErrorInfoV2());
		}
		auto &info = *static_cast<ErrorInfoV2 *>(*err);
		info.code = code;
		info.message = msg ? msg : "";
	}
	return code;
}

// Shared BIGNUM magnitude/sign decoder. Used by both
// duckdb_v2_value_get_bignum (PR2 value-side) and duckdb_v2_bignum_decode
// (PR4 vector-side) so the magnitude reconstruction + sign extraction
// lives in exactly one place. Allocates an owned buffer with malloc
// (caller frees with free()).
inline DUCKDB_V2_API_CALL_t DecodeBignumStringT(const string_t &storage, uint8_t **out_data, idx_t *out_length,
                                                bool *out_is_negative, const char *function_name,
                                                duckdb_v2_error_info_ptr *err) {
	*out_data = nullptr;
	*out_length = 0;
	*out_is_negative = false;
	try {
		vector<uint8_t> magnitude;
		bool is_negative = false;
		Bignum::GetByteArray(magnitude, is_negative, storage);
		auto alloc = magnitude.empty() ? size_t {1} : magnitude.size();
		auto *buf = static_cast<uint8_t *>(std::malloc(alloc));
		if (!buf) {
			std::string msg = std::string(function_name) + ": malloc failed";
			return SetErrorInfo(err, DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY, msg.c_str());
		}
		if (!magnitude.empty()) {
			std::memcpy(buf, magnitude.data(), magnitude.size());
		}
		*out_data = buf;
		*out_length = magnitude.size();
		*out_is_negative = is_negative;
		return DUCKDB_V2_ERROR_NONE;
	} catch (std::exception &e) {
		return SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		std::string msg = std::string("unknown error in ") + function_name;
		return SetErrorInfo(err, DUCKDB_V2_API_ERROR, msg.c_str());
	}
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
DUCKDB_V2_API_CALL_t WithErrorHandler(duckdb_v2_error_info_ptr *err, T callback) {
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

	// Success leaves the slot untouched: the return code is authoritative, and
	// writing on every successful call (allocating one on first use) is pure
	// overhead. A stale info from an earlier failure may therefore survive a
	// later successful call — that is fine, because the caller keys off the
	// return code, not the slot, and reads *err only after a failing return.
	if (code == DUCKDB_V2_ERROR_NONE) {
		return code;
	}

	// Failure: report detail through the slot if the caller provided one.
	if (err) {
		if (!*err) {
			*err = static_cast<duckdb_v2_error_info_ptr>(new ErrorInfoV2());
		}
		auto &out = *static_cast<ErrorInfoV2 *>(*err);
		out.code = code;
		out.message = std::move(text);
	}

	return code;
}

} // namespace duckdb
