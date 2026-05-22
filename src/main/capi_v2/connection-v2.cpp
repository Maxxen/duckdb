#include "capi_v2_internal.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_connect(duckdb_v2_database_ptr db, duckdb_v2_connection_ptr *out_conn,
                                       duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!db || !out_conn) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connect");
		}
		*out_conn = nullptr;
		auto *db_wrapper = duckdb::ToDb(db);
		auto conn_wrapper = duckdb::make_uniq<duckdb::ConnectionWrapperV2>();
		conn_wrapper->connection = duckdb::make_shared_ptr<duckdb::Connection>(*db_wrapper->database);
		*out_conn = static_cast<duckdb_v2_connection_ptr>(conn_wrapper.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_disconnect(duckdb_v2_connection_ptr *conn, duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn) {
			return;
		}
		if (*conn) {
			delete duckdb::ToConn(*conn);
			*conn = nullptr;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_set(duckdb_v2_connection_ptr conn, duckdb_v2_option_ptr option,
                                                     DUCKDB_V2_SETTING_SCOPE scope, duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || !option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_set");
		}
		auto *conn_wrapper = duckdb::ToConn(conn);
		auto *opt = duckdb::ToOption(option);
		auto &client = *conn_wrapper->connection->context;
		duckdb::PhysicalSet::ApplyVariable(client, opt->name, duckdb::MapSettingScopeV2(scope),
		                                   duckdb::Value(opt->setting));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_get(duckdb_v2_connection_ptr conn, const char *name,
                                                     duckdb_v2_option_ptr *out_option, duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || !name || !out_option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_get");
		}
		*out_option = nullptr;
		auto *conn_wrapper = duckdb::ToConn(conn);
		auto &client = *conn_wrapper->connection->context;
		auto &config = duckdb::DBConfig::GetConfig(client);
		auto wrapper = duckdb::make_uniq<duckdb::OptionWrapperV2>();
		duckdb::BuildOptionByName(*wrapper, client, config, std::string(name));
		*out_option = static_cast<duckdb_v2_option_ptr>(wrapper.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_get_count(duckdb_v2_connection_ptr conn, idx_t *out_count,
                                                           duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_get_count");
		}
		auto *conn_wrapper = duckdb::ToConn(conn);
		auto &client = *conn_wrapper->connection->context;
		auto &config = duckdb::DBConfig::GetConfig(client);
		*out_count = duckdb::DBConfig::GetOptionCount() + config.GetExtensionSettings().size();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_get_by_index(duckdb_v2_connection_ptr conn, idx_t index,
                                                              duckdb_v2_option_ptr *out_option,
                                                              duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || !out_option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_connection_option_get_by_index");
		}
		*out_option = nullptr;
		auto *conn_wrapper = duckdb::ToConn(conn);
		auto &client = *conn_wrapper->connection->context;
		auto &config = duckdb::DBConfig::GetConfig(client);
		auto wrapper = duckdb::make_uniq<duckdb::OptionWrapperV2>();
		duckdb::BuildOptionByIndex(*wrapper, client, config, index);
		*out_option = static_cast<duckdb_v2_option_ptr>(wrapper.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_execute_with_context(duckdb_v2_connection_ptr conn,
                                                               duckdb_v2_connection_callback_cb callback,
                                                               void *user_data, duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn) {
			throw duckdb::InvalidInputException("Connection pointer cannot be null.");
		}
		if (!callback) {
			throw duckdb::InvalidInputException("Callback pointer cannot be null.");
		}

		auto *conn_wrapper = duckdb::ToConn(conn);
		auto &ctx = *conn_wrapper->connection->context;

		ctx.RunFunctionInTransaction([&]() {
			auto cb_ctx_ptr = static_cast<duckdb_v2_context_ptr>(&ctx);
			callback(cb_ctx_ptr, user_data, err);
		});
	});
}
