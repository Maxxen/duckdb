#include "capi_v2_internal.hpp"

#include "duckdb/main/materialized_query_result.hpp"

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

DUCKDB_V2_API_CALL_t duckdb_v2_connection_query(duckdb_v2_connection_handle conn, duckdb_v2_str sql,
                                                duckdb_v2_result_handle *out_result, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || (!sql.ptr && sql.len > 0) || !out_result) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_query");
		}
		*out_result = nullptr;
		auto *conn_wrapper = duckdb::ToConn(conn);
		auto result = conn_wrapper->connection->Query(duckdb::ToString(sql));
		if (result->HasError()) {
			// Re-throw the typed ErrorData so the exception's ExceptionType
			// is preserved and routed through GetErrorCodeFromExceptionType.
			result->GetErrorObject().Throw();
		}
		*out_result = reinterpret_cast<_duckdb_v2_result *>(result.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_destroy(duckdb_v2_result_handle *result) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!result) {
			return;
		}
		if (*result) {
			delete duckdb::ToResult(*result);
			*result = nullptr;
		}
	});
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_result_get_result_type(duckdb_v2_result_handle result, DUCKDB_V2_RESULT_TYPE *out_type,
                                                      duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!result || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_result_get_result_type");
		}
		auto *r = duckdb::ToResult(result);
		*out_type = duckdb::MapResultType(r->properties.return_type);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_get_statement_type(duckdb_v2_result_handle result,
                                                         DUCKDB_V2_STATEMENT_TYPE *out_type,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!result || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_result_get_statement_type");
		}
		auto *r = duckdb::ToResult(result);
		*out_type = static_cast<DUCKDB_V2_STATEMENT_TYPE>(r->statement_type);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_column_count(duckdb_v2_result_handle result, idx_t *out_count,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!result || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_result_column_count");
		}
		auto *r = duckdb::ToResult(result);
		*out_count = r->types.size();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_column_name(duckdb_v2_result_handle result, idx_t index, duckdb_v2_str *out_name,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!result || !out_name) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_result_column_name");
		}
		auto *r = duckdb::ToResult(result);
		if (index >= r->names.size()) {
			throw duckdb::InvalidInputException("result column index out of range");
		}
		*out_name = duckdb::ToStr(r->names[index]);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_column_logical_type(duckdb_v2_result_handle result, idx_t index,
                                                          duckdb_v2_logical_type_handle *out_type,
                                                          duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!result || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_result_column_logical_type");
		}
		*out_type = nullptr;
		auto *r = duckdb::ToResult(result);
		if (index >= r->types.size()) {
			throw duckdb::InvalidInputException("result column index out of range");
		}
		auto *lt = new duckdb::LogicalType(r->types[index]);
		*out_type = reinterpret_cast<_duckdb_v2_logical_type *>(lt);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_rows_changed(duckdb_v2_result_handle result, idx_t *out_count,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!result || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_result_rows_changed");
		}
		auto *r = duckdb::ToResult(result);
		if (r->properties.return_type != duckdb::StatementReturnType::CHANGED_ROWS) {
			*out_count = 0;
			return;
		}
		// CHANGED_ROWS results are constructed by core as a single BIGINT
		// "Count" column with one row. The runtime guard catches a future
		// shape divergence in release; debug builds trip the assert first.
		D_ASSERT(r->RowCount() == 1 && r->types.size() == 1);
		if (r->RowCount() != 1 || r->types.size() != 1) {
			*out_count = 0;
			return;
		}
		*out_count = static_cast<idx_t>(r->GetValue(0, 0).GetValue<int64_t>());
	});
}
