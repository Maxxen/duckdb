#include "capi_v2_internal.hpp"

#include "duckdb/main/materialized_query_result.hpp"

// Exception policy:
//   - connection_query wraps Connection::Query in try/catch. The public
//     Query(const string &) typically catches internally and folds errors
//     into the returned MaterializedQueryResult, but the allocation /
//     unique_ptr::release path can still std::bad_alloc.
//   - result_column_logical_type wraps `new LogicalType(...)`: the copy
//     ctor itself just bumps a shared_ptr on ExtraTypeInfo, but the `new`
//     is the throw source.
//   - result_rows_changed wraps materialized->GetValue(0, 0): on the
//     CHANGED_ROWS shape today this is a uint64 read that can't throw,
//     but it goes through ColumnDataCollection scan-state plumbing that
//     can theoretically allocate.
//   - Other accessors read pre-validated state on a MaterializedQueryResult
//     that was successfully constructed; they don't allocate and aren't
//     wrapped.
//
// Error category mapping in connection_query:
//   Every query failure today funnels into DUCKDB_V2_ERROR_INVALID_INPUT
//   regardless of whether it is a parser, binder, planner, catalog, or
//   extension-raised InternalException. A finer mapping
//   (ExceptionType -> V2 error code) is deferred until the V2 error-code
//   table grows the additional categories; once it does, this site
//   should funnel through that lookup using
//   result->GetErrorObject().Type().

namespace duckdb {
namespace {

// Map duckdb::StatementReturnType to DUCKDB_V2_RESULT_TYPE. Values are
// numerically identical by §4 of the V2 conventions (numeric enum-id
// round-trip); the switch is the explicit mapping that gives -Wswitch
// teeth and asserts in debug if core adds a variant the V2 enum does
// not yet surface.
DUCKDB_V2_RESULT_TYPE MapResultType(StatementReturnType t) {
	switch (t) {
	case StatementReturnType::QUERY_RESULT:
		return DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
	case StatementReturnType::CHANGED_ROWS:
		return DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS;
	case StatementReturnType::NOTHING:
		return DUCKDB_V2_RESULT_TYPE_NOTHING;
	}
	D_ASSERT(false); // unmapped StatementReturnType variant
	return DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
}

} // anonymous namespace
} // namespace duckdb

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_connection_query(duckdb_v2_connection_ptr conn, const char *sql,
                                                duckdb_v2_result_ptr *out_result, duckdb_v2_error_info_ptr *err) {
	if (!conn || !sql || !out_result) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_connection_query");
	}
	*out_result = nullptr;
	auto *conn_wrapper = duckdb::ToConn(conn);
	try {
		auto result = conn_wrapper->connection->Query(std::string(sql));
		if (result->HasError()) {
			return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, result->GetError().c_str());
		}
		*out_result = static_cast<duckdb_v2_result_ptr>(result.release());
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_connection_query");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_destroy(duckdb_v2_result_ptr *result, duckdb_v2_error_info_ptr *err) {
	if (!result) {
		return duckdb::ClearErrorInfo(err);
	}
	if (*result) {
		delete duckdb::ToResult(*result);
		*result = nullptr;
	}
	return duckdb::ClearErrorInfo(err);
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_result_get_result_type(duckdb_v2_result_ptr result, DUCKDB_V2_RESULT_TYPE *out_type,
                                                      duckdb_v2_error_info_ptr *err) {
	if (!result || !out_type) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_result_get_result_type");
	}
	auto *r = duckdb::ToResult(result);
	*out_type = duckdb::MapResultType(r->properties.return_type);
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_get_statement_type(duckdb_v2_result_ptr result,
                                                         DUCKDB_V2_STATEMENT_TYPE *out_type,
                                                         duckdb_v2_error_info_ptr *err) {
	if (!result || !out_type) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_result_get_statement_type");
	}
	auto *r = duckdb::ToResult(result);
	*out_type = static_cast<DUCKDB_V2_STATEMENT_TYPE>(r->statement_type);
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_column_count(duckdb_v2_result_ptr result, idx_t *out_count,
                                                   duckdb_v2_error_info_ptr *err) {
	if (!result || !out_count) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_result_column_count");
	}
	auto *r = duckdb::ToResult(result);
	*out_count = r->types.size();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_column_name(duckdb_v2_result_ptr result, idx_t index, const char **out_name,
                                                  idx_t *out_length, duckdb_v2_error_info_ptr *err) {
	if (!result || !out_name || !out_length) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_result_column_name");
	}
	auto *r = duckdb::ToResult(result);
	if (index >= r->names.size()) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "result column index out of range");
	}
	auto &name = r->names[index];
	*out_name = name.c_str();
	*out_length = name.size();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_column_logical_type(duckdb_v2_result_ptr result, idx_t index,
                                                          duckdb_v2_logical_type_ptr *out_type,
                                                          duckdb_v2_error_info_ptr *err) {
	if (!result || !out_type) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_result_column_logical_type");
	}
	*out_type = nullptr;
	auto *r = duckdb::ToResult(result);
	if (index >= r->types.size()) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "result column index out of range");
	}
	try {
		auto *lt = new duckdb::LogicalType(r->types[index]);
		*out_type = static_cast<duckdb_v2_logical_type_ptr>(lt);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error allocating column logical_type");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_rows_changed(duckdb_v2_result_ptr result, idx_t *out_count,
                                                   duckdb_v2_error_info_ptr *err) {
	if (!result || !out_count) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_result_rows_changed");
	}
	auto *r = duckdb::ToResult(result);
	if (r->properties.return_type != duckdb::StatementReturnType::CHANGED_ROWS) {
		*out_count = 0;
		return duckdb::ClearErrorInfo(err);
	}
	// CHANGED_ROWS results are constructed by core as a single BIGINT
	// "Count" column with one row. The runtime guard catches a future
	// shape divergence in release; debug builds trip the assert first.
	D_ASSERT(r->RowCount() == 1 && r->types.size() == 1);
	if (r->RowCount() != 1 || r->types.size() != 1) {
		*out_count = 0;
		return duckdb::ClearErrorInfo(err);
	}
	try {
		*out_count = static_cast<idx_t>(r->GetValue(0, 0).GetValue<int64_t>());
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_result_rows_changed");
	}
}
