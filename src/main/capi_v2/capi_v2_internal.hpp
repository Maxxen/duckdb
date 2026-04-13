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

struct PreparedStatementWrapperV2 {
	case_insensitive_map_t<BoundParameterData> values;
	unique_ptr<PreparedStatement> statement;
	bool success = true;
	ErrorData error_data;
};

} // namespace duckdb
