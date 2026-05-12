#include "capi_v2_internal.hpp"

namespace {

duckdb::OptionWrapperV2 *ToOption(duckdb_v2_option_ptr ptr) {
	return static_cast<duckdb::OptionWrapperV2 *>(ptr);
}

} // namespace

DUCKDB_V2_API_CALL_t duckdb_v2_option_create(const char *name, const char *setting, duckdb_v2_option_ptr *out_option,
                                             duckdb_v2_error_info_ptr *err) {
	if (!name || !setting || !out_option) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_option_create");
	}
	*out_option = nullptr;
	try {
		auto *wrapper = new duckdb::OptionWrapperV2();
		wrapper->name = name;
		wrapper->setting = setting;
		*out_option = static_cast<duckdb_v2_option_ptr>(wrapper);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_option_create");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_option_destroy(duckdb_v2_option_ptr *option, duckdb_v2_error_info_ptr *err) {
	if (!option) {
		return duckdb::ClearErrorInfo(err);
	}
	if (*option) {
		delete ToOption(*option);
		*option = nullptr;
	}
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_option_get_name(duckdb_v2_option_ptr option, const char **out_name,
                                               duckdb_v2_error_info_ptr *err) {
	if (!option || !out_name) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_option_get_name");
	}
	*out_name = ToOption(option)->name.c_str();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_option_get_setting(duckdb_v2_option_ptr option, const char **out_setting,
                                                  duckdb_v2_error_info_ptr *err) {
	if (!option || !out_setting) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_option_get_setting");
	}
	*out_setting = ToOption(option)->setting.c_str();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_option_get_default_setting(duckdb_v2_option_ptr option, const char **out_default_setting,
                                                          duckdb_v2_error_info_ptr *err) {
	if (!option || !out_default_setting) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_option_get_default_setting");
	}
	*out_default_setting = ToOption(option)->default_setting.c_str();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_option_get_description(duckdb_v2_option_ptr option, const char **out_description,
                                                      duckdb_v2_error_info_ptr *err) {
	if (!option || !out_description) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_option_get_description");
	}
	*out_description = ToOption(option)->description.c_str();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_option_get_target_scope(duckdb_v2_option_ptr option,
                                                       DUCKDB_V2_OPTION_TARGET_SCOPE *out_target_scope,
                                                       duckdb_v2_error_info_ptr *err) {
	if (!option || !out_target_scope) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_option_get_target_scope");
	}
	*out_target_scope = ToOption(option)->target_scope;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_option_get_alias_count(duckdb_v2_option_ptr option, idx_t *out_count,
                                                      duckdb_v2_error_info_ptr *err) {
	if (!option || !out_count) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_option_get_alias_count");
	}
	*out_count = static_cast<idx_t>(ToOption(option)->aliases.size());
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_option_get_alias(duckdb_v2_option_ptr option, idx_t index, const char **out_alias,
                                                duckdb_v2_error_info_ptr *err) {
	if (!option || !out_alias) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_option_get_alias");
	}
	auto *wrapper = ToOption(option);
	if (index >= wrapper->aliases.size()) {
		*out_alias = nullptr;
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "alias index out of range in duckdb_v2_option_get_alias");
	}
	*out_alias = wrapper->aliases[index].c_str();
	return duckdb::ClearErrorInfo(err);
}
