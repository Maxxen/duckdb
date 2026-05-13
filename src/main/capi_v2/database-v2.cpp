#include "capi_v2_internal.hpp"

#include "duckdb/common/types/value.hpp"

namespace {

// Identifies DuckDB's "this file is already open" conflict so we can
// map it to DUCKDB_V2_ERROR_RESOURCE_IN_USE. The message is raised from
// DatabaseFilePathManager::InsertDatabasePath.
bool IsUniqueFileHandleConflict(const char *what) {
	return what != nullptr && std::strstr(what, "Unique file handle conflict") != nullptr;
}

} // namespace

DUCKDB_V2_API_CALL_t duckdb_v2_open(duckdb_v2_environment_ptr env, const char *path, duckdb_v2_option_ptr *options,
                                    idx_t option_count, duckdb_v2_database_ptr *out_db, duckdb_v2_error_info_ptr *err) {
	if (!env || !out_db) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_open");
	}
	if (option_count > 0 && !options) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "option_count > 0 but options is null in duckdb_v2_open");
	}
	*out_db = nullptr;
	auto *env_wrapper = duckdb::ToEnv(env);
	auto wrapper = duckdb::make_uniq<duckdb::DatabaseWrapperV2>();
	try {
		auto config = duckdb::make_uniq<duckdb::DBConfig>();
		for (idx_t i = 0; i < option_count; i++) {
			auto *opt = duckdb::ToOption(options[i]);
			if (!opt) {
				return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
				                            "null option handle in options array passed to duckdb_v2_open");
			}
			config->SetOptionByName(opt->name, duckdb::Value(opt->setting));
		}
		// Route through the env's DBInstanceCache with NEVER_CACHE so the
		// path manager is shared across all opens (file conflicts get
		// detected) but no instance is memoized — every open produces a
		// fresh DatabaseInstance.
		std::string path_str = path ? std::string(path) : std::string();
		wrapper->database =
		    env_wrapper->cache->GetOrCreateInstance(path_str, *config, duckdb::CacheBehavior::NEVER_CACHE);
		wrapper->admin_connection = duckdb::make_uniq<duckdb::Connection>(*wrapper->database);
		wrapper->environment = env_wrapper;
		env_wrapper->open_database_count.fetch_add(1, std::memory_order_release);
		*out_db = static_cast<duckdb_v2_database_ptr>(wrapper.release());
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		if (IsUniqueFileHandleConflict(e.what())) {
			return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_RESOURCE_IN_USE, e.what());
		}
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_open");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_close(duckdb_v2_database_ptr *db, duckdb_v2_error_info_ptr *err) {
	if (!db) {
		return duckdb::ClearErrorInfo(err);
	}
	if (*db) {
		auto *wrapper = duckdb::ToDb(*db);
		auto *env = wrapper->environment;
		delete wrapper;
		if (env) {
			env->open_database_count.fetch_sub(1, std::memory_order_release);
		}
		*db = nullptr;
	}
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_database_option_set(duckdb_v2_database_ptr db, duckdb_v2_option_ptr option,
                                                   duckdb_v2_error_info_ptr *err) {
	if (!db || !option) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_database_option_set");
	}
	auto *db_wrapper = duckdb::ToDb(db);
	auto *opt = duckdb::ToOption(option);
	// Use the database's admin ClientContext as the bridge into
	// PhysicalSet::ApplyVariable. Force GLOBAL scope: the admin context
	// has no LOCAL settings of its own, and database-scoped writes only
	// make sense at GLOBAL anyway.
	auto &client = *db_wrapper->admin_connection->context;
	try {
		duckdb::PhysicalSet::ApplyVariable(client, opt->name, duckdb::SetScope::GLOBAL, duckdb::Value(opt->setting));
		return duckdb::ClearErrorInfo(err);
	} catch (duckdb::InvalidInputException &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, e.what());
	} catch (duckdb::CatalogException &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, e.what());
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_database_option_set");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_database_option_get(duckdb_v2_database_ptr db, const char *name,
                                                   duckdb_v2_option_ptr *out_option, duckdb_v2_error_info_ptr *err) {
	if (!db || !name || !out_option) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_database_option_get");
	}
	*out_option = nullptr;
	auto *db_wrapper = duckdb::ToDb(db);
	auto &client = *db_wrapper->admin_connection->context;
	auto &config = db_wrapper->database->instance->config;
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
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_database_option_get");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_database_option_get_count(duckdb_v2_database_ptr db, idx_t *out_count,
                                                         duckdb_v2_error_info_ptr *err) {
	if (!db || !out_count) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_database_option_get_count");
	}
	auto *db_wrapper = duckdb::ToDb(db);
	auto &config = db_wrapper->database->instance->config;
	*out_count = duckdb::DBConfig::GetOptionCount() + config.GetExtensionSettings().size();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_database_option_get_by_index(duckdb_v2_database_ptr db, idx_t index,
                                                            duckdb_v2_option_ptr *out_option,
                                                            duckdb_v2_error_info_ptr *err) {
	if (!db || !out_option) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_database_option_get_by_index");
	}
	*out_option = nullptr;
	auto *db_wrapper = duckdb::ToDb(db);
	auto &client = *db_wrapper->admin_connection->context;
	auto &config = db_wrapper->database->instance->config;
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
		                            "unknown error in duckdb_v2_database_option_get_by_index");
	}
}
