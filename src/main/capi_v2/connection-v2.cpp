#include "capi_v2_internal.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_connect(duckdb_v2_database_handle db, duckdb_v2_connection_handle *out_conn,
                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!db || !out_conn) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connect");
		}
		*out_conn = nullptr;
		auto *db_wrapper = duckdb::ToDb(db);
		// Bare Connection, same allocation shape as V1's duckdb_connect, so the
		// handle is interchangeable with a V1 duckdb_connection for operations.
		auto connection = duckdb::make_uniq<duckdb::Connection>(*db_wrapper->database);
		*out_conn = reinterpret_cast<_duckdb_v2_connection *>(connection.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_disconnect(duckdb_v2_connection_handle *conn) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!conn) {
			return;
		}
		if (*conn) {
			delete duckdb::ToConn(*conn);
			*conn = nullptr;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_set(duckdb_v2_connection_handle conn, duckdb_v2_option_handle option,
                                                     DUCKDB_V2_SETTING_SCOPE scope, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || !option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_set");
		}
		auto *opt = duckdb::ToOption(option);
		auto &client = *duckdb::ToConn(conn)->context;
		duckdb::PhysicalSet::ApplyVariable(client, opt->name, duckdb::MapSettingScopeV2(scope),
		                                   duckdb::Value(opt->setting));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_get(duckdb_v2_connection_handle conn, duckdb_v2_identifier_t name,
                                                     duckdb_v2_option_handle *out_option,
                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || (!name.ptr && name.len > 0) || !out_option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_get");
		}
		*out_option = nullptr;
		auto &client = *duckdb::ToConn(conn)->context;
		auto &config = duckdb::DBConfig::GetConfig(client);
		auto wrapper = duckdb::make_uniq<duckdb::OptionWrapperV2>();
		duckdb::BuildOptionByName(*wrapper, client, config, duckdb::ToString(name));
		*out_option = reinterpret_cast<_duckdb_v2_option *>(wrapper.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_get_count(duckdb_v2_connection_handle conn, idx_t *out_count,
                                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_get_count");
		}
		auto &client = *duckdb::ToConn(conn)->context;
		auto &config = duckdb::DBConfig::GetConfig(client);
		*out_count = duckdb::DBConfig::GetOptionCount() + config.GetExtensionSettings().size();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_get_by_index(duckdb_v2_connection_handle conn, idx_t index,
                                                              duckdb_v2_option_handle *out_option,
                                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || !out_option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_get_by_index");
		}
		*out_option = nullptr;
		auto &client = *duckdb::ToConn(conn)->context;
		auto &config = duckdb::DBConfig::GetConfig(client);
		auto wrapper = duckdb::make_uniq<duckdb::OptionWrapperV2>();
		duckdb::BuildOptionByIndex(*wrapper, client, config, index);
		*out_option = reinterpret_cast<_duckdb_v2_option *>(wrapper.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_execute_with_context(duckdb_v2_connection_handle conn,
                                                               duckdb_v2_connection_callback_fn callback,
                                                               void *user_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn) {
			throw duckdb::InvalidInputException("Connection pointer cannot be null.");
		}
		if (!callback) {
			throw duckdb::InvalidInputException("Callback pointer cannot be null.");
		}

		auto &ctx = *duckdb::ToConn(conn)->context;

		ctx.RunFunctionInTransaction([&]() {
			auto cb_ctx_handle = reinterpret_cast<_duckdb_v2_context *>(&ctx);
			// Hand the callback a stack-backed slot per the uniform err model:
			// a code set by the callback becomes a thrown engine exception, so
			// the transaction rolls back and the overall call fails with it.
			duckdb::InvokeWithErrorSlot<duckdb::InvalidInputException>(
			    [&](duckdb_v2_error_info_handle *slot) { callback(cb_ctx_handle, user_data, slot); });
		});
	});
}

// ---------------------------------------------------------------------------
// Query process management
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_connection_interrupt(duckdb_v2_connection_handle conn,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_interrupt");
		}
		// ClientContext::Interrupt is an atomic store; safe to call from any
		// thread, including while another thread steps this connection's
		// result. A no-op when no query is active.
		auto &context = *duckdb::ToConn(conn)->context;
		// Record that the cancellation was consumer-initiated.
		duckdb::GetBusySlot(context)->cancel_requested.store(true, std::memory_order_relaxed);
		context.Interrupt();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_query_progress(duckdb_v2_connection_handle conn,
                                                         duckdb_v2_query_progress_handle *out_progress,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || !out_progress) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_query_progress");
		}
		*out_progress = nullptr;
		auto progress = duckdb::ToConn(conn)->context->GetQueryProgress();
		auto wrapper = duckdb::make_uniq<duckdb::QueryProgressWrapperV2>();
		wrapper->percentage = progress.GetPercentage();
		wrapper->rows_processed = progress.GetRowsProcessed();
		wrapper->total_rows_to_process = progress.GetTotalRowsToProcess();
		*out_progress = reinterpret_cast<_duckdb_v2_query_progress *>(wrapper.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_query_progress_get_percentage(duckdb_v2_query_progress_handle progress,
                                                             double *out_percentage, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!progress || !out_percentage) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_query_progress_get_percentage");
		}
		*out_percentage = duckdb::ToQueryProgress(progress)->percentage;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_query_progress_get_rows_processed(duckdb_v2_query_progress_handle progress,
                                                                 uint64_t *out_rows_processed,
                                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!progress || !out_rows_processed) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_query_progress_get_rows_processed");
		}
		*out_rows_processed = duckdb::ToQueryProgress(progress)->rows_processed;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_query_progress_get_total_rows_to_process(duckdb_v2_query_progress_handle progress,
                                                                        uint64_t *out_total_rows_to_process,
                                                                        duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!progress || !out_total_rows_to_process) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_query_progress_get_total_rows_to_process");
		}
		*out_total_rows_to_process = duckdb::ToQueryProgress(progress)->total_rows_to_process;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_query_progress_destroy(duckdb_v2_query_progress_handle *progress) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!progress) {
			return;
		}
		if (*progress) {
			delete duckdb::ToQueryProgress(*progress);
			*progress = nullptr;
		}
	});
}
