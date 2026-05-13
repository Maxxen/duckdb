#include "capi_v2_internal.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_connect(duckdb_v2_database_ptr db, duckdb_v2_connection_ptr *out_conn,
                                       duckdb_v2_error_info_ptr *err) {
	if (!db || !out_conn) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_connect");
	}
	*out_conn = nullptr;
	auto *db_wrapper = duckdb::ToDb(db);
	auto conn_wrapper = duckdb::make_uniq<duckdb::ConnectionWrapperV2>();
	try {
		conn_wrapper->connection = duckdb::make_shared_ptr<duckdb::Connection>(*db_wrapper->database);
		*out_conn = static_cast<duckdb_v2_connection_ptr>(conn_wrapper.release());
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_connect");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_disconnect(duckdb_v2_connection_ptr *conn, duckdb_v2_error_info_ptr *err) {
	if (!conn) {
		return duckdb::ClearErrorInfo(err);
	}
	if (*conn) {
		delete duckdb::ToConn(*conn);
		*conn = nullptr;
	}
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_set(duckdb_v2_connection_ptr conn, duckdb_v2_option_ptr option,
                                                     DUCKDB_V2_SETTING_SCOPE scope, duckdb_v2_error_info_ptr *err) {
	if (!conn || !option) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_connection_option_set");
	}
	auto *conn_wrapper = duckdb::ToConn(conn);
	auto *opt = duckdb::ToOption(option);
	auto &client = *conn_wrapper->connection->context;
	try {
		duckdb::PhysicalSet::ApplyVariable(client, opt->name, duckdb::MapSettingScopeV2(scope),
		                                   duckdb::Value(opt->setting));
		return duckdb::ClearErrorInfo(err);
	} catch (duckdb::InvalidInputException &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, e.what());
	} catch (duckdb::CatalogException &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, e.what());
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_connection_option_set");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_get(duckdb_v2_connection_ptr conn, const char *name,
                                                     duckdb_v2_option_ptr *out_option, duckdb_v2_error_info_ptr *err) {
	if (!conn || !name || !out_option) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_connection_option_get");
	}
	*out_option = nullptr;
	auto *conn_wrapper = duckdb::ToConn(conn);
	auto &client = *conn_wrapper->connection->context;
	auto &config = duckdb::DBConfig::GetConfig(client);
	auto wrapper = duckdb::make_uniq<duckdb::OptionWrapperV2>();
	try {
		duckdb::BuildOptionByName(*wrapper, client, config, std::string(name));
		*out_option = static_cast<duckdb_v2_option_ptr>(wrapper.release());
		return duckdb::ClearErrorInfo(err);
	} catch (duckdb::InvalidInputException &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, e.what());
	} catch (duckdb::CatalogException &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, e.what());
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_connection_option_get");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_get_count(duckdb_v2_connection_ptr conn, idx_t *out_count,
                                                           duckdb_v2_error_info_ptr *err) {
	if (!conn || !out_count) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_connection_option_get_count");
	}
	auto *conn_wrapper = duckdb::ToConn(conn);
	auto &client = *conn_wrapper->connection->context;
	auto &config = duckdb::DBConfig::GetConfig(client);
	*out_count = duckdb::DBConfig::GetOptionCount() + config.GetExtensionSettings().size();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_connection_option_get_by_index(duckdb_v2_connection_ptr conn, idx_t index,
                                                              duckdb_v2_option_ptr *out_option,
                                                              duckdb_v2_error_info_ptr *err) {
	if (!conn || !out_option) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_connection_option_get_by_index");
	}
	*out_option = nullptr;
	auto *conn_wrapper = duckdb::ToConn(conn);
	auto &client = *conn_wrapper->connection->context;
	auto &config = duckdb::DBConfig::GetConfig(client);
	auto wrapper = duckdb::make_uniq<duckdb::OptionWrapperV2>();
	try {
		duckdb::BuildOptionByIndex(*wrapper, client, config, index);
		*out_option = static_cast<duckdb_v2_option_ptr>(wrapper.release());
		return duckdb::ClearErrorInfo(err);
	} catch (duckdb::InvalidInputException &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, e.what());
	} catch (duckdb::CatalogException &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, e.what());
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR,
		                            "unknown error in duckdb_v2_connection_option_get_by_index");
	}
}
