#include "capi_v2_internal.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_statement_prepare(duckdb_v2_connection_handle conn,
                                                 duckdb_v2_sql_statement_handle statement, bool require_cacheable,
                                                 duckdb_v2_prepared_statement_handle *out_prepared,
                                                 duckdb_v2_error_info_handle *err) {
	if (out_prepared) {
		*out_prepared = nullptr;
	}
	if (!conn || !statement || !out_prepared) {
		return duckdb::WithErrorHandler(
		    err, [&]() { throw duckdb::InvalidInputException("null argument to duckdb_v2_statement_prepare"); });
	}
	auto *connection = duckdb::ToConn(conn);
	// One live result per connection: Prepare runs InitialCleanup, which would
	// cancel a live stream, so refuse instead (matching statement_execute). Prepare
	// produces no result, so this only checks the slot is free, it does not claim
	// it. Manual return path: no ExceptionType maps to RESOURCE_IN_USE.
	if (duckdb::GetBusySlot(*connection->context)->owner.load() != nullptr) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_RESOURCE_IN_USE,
		                            "connection has a live result; drain, destroy, or interrupt it before preparing "
		                            "a statement (or open another connection)");
	}
	return duckdb::WithErrorHandler(err, [&]() {
		// Borrowed, not consumed: prepare a copy so the caller keeps the original.
		auto stmt = duckdb::ToSqlStatement(statement)->Copy();
		// Prepare returns an error-carrying PreparedStatement rather than throwing;
		// re-throw the typed ErrorData so its ExceptionType routes through
		// GetErrorCodeFromExceptionType.
		auto prepared = connection->context->Prepare(std::move(stmt));
		if (prepared->HasError()) {
			prepared->GetErrorObject().Throw();
		}
		if (require_cacheable && !duckdb::PreparedReusesPlan(prepared->GetStatementProperties())) {
			throw duckdb::InvalidInputException(
			    "statement_prepare(require_cacheable): the prepared plan is not reused across executions (unanchored "
			    "parameter or a table scan forces a re-bind each time); prepare without require_cacheable to accept "
			    "it");
		}
		auto wrapper = duckdb::make_uniq<duckdb::PreparedStatementWrapperV2>();
		wrapper->prepared = std::move(prepared);
		*out_prepared = reinterpret_cast<_duckdb_v2_prepared_statement *>(wrapper.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_prepared_execute(duckdb_v2_prepared_statement_handle prepared,
                                                const duckdb_v2_str *parameter_names,
                                                const duckdb_v2_value_handle *parameter_values, idx_t parameter_count,
                                                duckdb_v2_result_handle *out_result, duckdb_v2_error_info_handle *err) {
	if (out_result) {
		*out_result = nullptr;
	}
	// The refusals below never reach the engine and leave the prepared handle intact.
	if (!prepared || !out_result || (parameter_count > 0 && !parameter_values)) {
		return duckdb::WithErrorHandler(
		    err, [&]() { throw duckdb::InvalidInputException("null argument to duckdb_v2_prepared_execute"); });
	}
	auto *wrapper = duckdb::ToPreparedStatement(prepared);
	auto &context = *wrapper->prepared->context;
	auto result = duckdb::make_uniq<duckdb::ResultWrapperV2>();
	// One live result per connection: PendingQuery's InitialCleanup would cancel a
	// live stream, so the busy slot guards it as statement_execute does. Manual
	// return path: no ExceptionType maps to RESOURCE_IN_USE.
	auto busy_slot = duckdb::GetBusySlot(context);
	void *expected = nullptr;
	if (!busy_slot->owner.compare_exchange_strong(expected, result.get())) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_RESOURCE_IN_USE,
		                            "connection has a live result; drain, destroy, or interrupt it before starting "
		                            "a new query (or open another connection)");
	}
	// On any failure below, the result wrapper's destructor releases the slot.
	result->busy_slot = std::move(busy_slot);
	result->busy_slot->cancel_requested.store(false, std::memory_order_relaxed);
	return duckdb::WithErrorHandler(err, [&]() {
		// Fold parameter values in as constants, keyed by name when parameter_names
		// supplies one and positionally ("1".."N") otherwise.
		duckdb::identifier_map_t<duckdb::BoundParameterData> values;
		duckdb::BuildParameterMap(parameter_names, parameter_values, parameter_count, "duckdb_v2_prepared_execute",
		                          values);
		// A prepared statement is a single engine statement: no expansion, no
		// fragment group, no bridge-injected transaction. It reaches the shared
		// BeginPending seam directly, always principal (fragments stays empty;
		// fragment_count is 1 for metadata symmetry only). The engine's own
		// per-query transaction manages commit/rollback, so owns_wrapping_transaction
		// stays false and RollbackIncompleteGroup is a no-op. The execute-prepared
		// path gives catalog-version / type-change re-bind for free.
		result->context = wrapper->prepared->context;
		result->fragment_count = 1;
		result->BeginPending(wrapper->prepared->PendingQuery(values, /*allow_stream_result=*/true), true);
		*out_result = reinterpret_cast<_duckdb_v2_result *>(result.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_prepared_reuses_plan(duckdb_v2_prepared_statement_handle prepared, bool *out_reuses,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!prepared || !out_reuses) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_prepared_reuses_plan");
		}
		auto *wrapper = duckdb::ToPreparedStatement(prepared);
		*out_reuses = duckdb::PreparedReusesPlan(wrapper->prepared->GetStatementProperties());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_prepared_statement_destroy(duckdb_v2_prepared_statement_handle *prepared) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!prepared) {
			return;
		}
		if (*prepared) {
			delete duckdb::ToPreparedStatement(*prepared);
			*prepared = nullptr;
		}
	});
}
