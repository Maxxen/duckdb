#include "capi_v2_internal.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_create_environment(duckdb_v2_environment_ptr *out_env, duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_env) {
			throw duckdb::InvalidInputException("null out_env in duckdb_v2_create_environment");
		}
		*out_env = nullptr;
		auto wrapper = duckdb::make_uniq<duckdb::EnvironmentWrapperV2>();
		wrapper->cache = duckdb::make_uniq<duckdb::DBInstanceCache>();
		*out_env = static_cast<duckdb_v2_environment_ptr>(wrapper.release());
	});
}

// destroy_environment keeps a manual return path so the open-databases case
// can surface as RESOURCE_IN_USE — there is no ExceptionType that maps to
// that V2 code, so routing it through WithErrorHandler would degrade it.
DUCKDB_V2_API_CALL_t duckdb_v2_destroy_environment(duckdb_v2_environment_ptr *env, duckdb_v2_error_info_ptr *err) {
	if (!env) {
		return duckdb::ClearErrorInfo(err);
	}
	if (!*env) {
		return duckdb::ClearErrorInfo(err);
	}
	auto *wrapper = duckdb::ToEnv(*env);
	auto count = wrapper->open_database_count.load(std::memory_order_acquire);
	if (count != 0) {
		auto msg = "cannot destroy environment: " + std::to_string(count) + " database(s) still open";
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_RESOURCE_IN_USE, msg.c_str());
	}
	delete wrapper;
	*env = nullptr;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_environment_database_count(duckdb_v2_environment_ptr env, idx_t *out_count,
                                                          duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!env || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_environment_database_count");
		}
		auto *wrapper = duckdb::ToEnv(env);
		*out_count = static_cast<idx_t>(wrapper->open_database_count.load(std::memory_order_acquire));
	});
}
