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

// duckdb_v2_open keeps a manual try/catch (rather than WithErrorHandler) so
// it can identify the unique-file-handle conflict by message and surface it
// as RESOURCE_IN_USE. The default exception->code mapping in WithErrorHandler
// would route this through IO_GENERAL, which loses the distinction the V2
// surface wants for "this file is already open in this environment."
DUCKDB_V2_API_CALL_t duckdb_v2_open(duckdb_v2_environment_handle env, duckdb_v2_str path,
                                    duckdb_v2_option_handle *options, idx_t option_count,
                                    duckdb_v2_database_handle *out_db, duckdb_v2_error_info_handle *err) {
	if (!env || !out_db || (!path.ptr && path.len > 0)) {
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
		std::string path_str = duckdb::ToString(path);
		wrapper->database =
		    env_wrapper->cache->GetOrCreateInstance(path_str, *config, duckdb::CacheBehavior::NEVER_CACHE);
		wrapper->admin_connection = duckdb::make_uniq<duckdb::Connection>(*wrapper->database);
		wrapper->environment = env_wrapper;
		env_wrapper->open_database_count.fetch_add(1, std::memory_order_release);
		*out_db = reinterpret_cast<_duckdb_v2_database *>(wrapper.release());
		return DUCKDB_V2_ERROR_NONE;
	} catch (std::exception &e) {
		if (IsUniqueFileHandleConflict(e.what())) {
			return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_RESOURCE_IN_USE, e.what());
		}
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_open");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_close(duckdb_v2_database_handle *db) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!db) {
			return;
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
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_database_option_set(duckdb_v2_database_handle db, duckdb_v2_option_handle option,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!db || !option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_database_option_set");
		}
		auto *db_wrapper = duckdb::ToDb(db);
		auto *opt = duckdb::ToOption(option);
		// Use the database's admin ClientContext as the bridge into
		// PhysicalSet::ApplyVariable. Force GLOBAL scope: the admin context
		// has no LOCAL settings of its own, and database-scoped writes only
		// make sense at GLOBAL anyway.
		auto &client = *db_wrapper->admin_connection->context;
		duckdb::PhysicalSet::ApplyVariable(client, opt->name, duckdb::SetScope::GLOBAL, duckdb::Value(opt->setting));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_database_option_get(duckdb_v2_database_handle db, duckdb_v2_str name,
                                                   duckdb_v2_option_handle *out_option,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!db || (!name.ptr && name.len > 0) || !out_option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_database_option_get");
		}
		*out_option = nullptr;
		auto *db_wrapper = duckdb::ToDb(db);
		auto &client = *db_wrapper->admin_connection->context;
		auto &config = db_wrapper->database->instance->config;
		auto wrapper = duckdb::make_uniq<duckdb::OptionWrapperV2>();
		duckdb::BuildOptionByName(*wrapper, client, config, duckdb::ToString(name));
		*out_option = reinterpret_cast<_duckdb_v2_option *>(wrapper.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_database_option_get_count(duckdb_v2_database_handle db, idx_t *out_count,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!db || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_database_option_get_count");
		}
		auto *db_wrapper = duckdb::ToDb(db);
		auto &config = db_wrapper->database->instance->config;
		*out_count = duckdb::DBConfig::GetOptionCount() + config.GetExtensionSettings().size();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_database_option_get_by_index(duckdb_v2_database_handle db, idx_t index,
                                                            duckdb_v2_option_handle *out_option,
                                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!db || !out_option) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_database_option_get_by_index");
		}
		*out_option = nullptr;
		auto *db_wrapper = duckdb::ToDb(db);
		auto &client = *db_wrapper->admin_connection->context;
		auto &config = db_wrapper->database->instance->config;
		auto wrapper = duckdb::make_uniq<duckdb::OptionWrapperV2>();
		duckdb::BuildOptionByIndex(*wrapper, client, config, index);
		*out_option = reinterpret_cast<_duckdb_v2_option *>(wrapper.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_library_version(char **out_version, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_version) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_library_version");
		}
		*out_version = nullptr;
		const auto *version = duckdb::DuckDB::LibraryVersion();
		const auto len = std::strlen(version);
		auto *buf = static_cast<char *>(std::malloc(len + 1));
		if (!buf) {
			throw duckdb::OutOfMemoryException("malloc failed in duckdb_v2_library_version");
		}
		std::memcpy(buf, version, len + 1);
		*out_version = buf;
	});
}
