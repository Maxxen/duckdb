#include "capi_v2_internal.hpp"

#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

// Trampoline runs in the binder; it is not WithErrorHandler-wrapped, so throws surface as normal query errors.

namespace duckdb {
namespace {

// Registration payload: user callback + user data. Destroyed (running the user data's destroy) when the DB closes.
struct ReplacementScanDataV2 final : public ReplacementScanData {
	duckdb_v2_replacement_scan_callback_fn callback = nullptr;
	OpaqueDataHandle user_data;
};

// Per-callback stack state: the unresolved name plus the claim. Empty function_name = decline.
struct ReplacementScanInfoV2 {
	ReplacementScanInput *input = nullptr;
	ReplacementScanDataV2 *data = nullptr;
	Identifier function_name;
	vector<Value> parameters;
	vector<std::pair<Identifier, Value>> named_parameters;
};

inline ReplacementScanInfoV2 *ToReplacementScanInfo(duckdb_v2_replacement_scan_info_handle ptr) {
	return reinterpret_cast<ReplacementScanInfoV2 *>(ptr);
}

// Registered into DBConfig::replacement_scans; nullptr return = decline.
unique_ptr<TableRef> ReplacementScanTrampolineV2(ClientContext &context, ReplacementScanInput &input,
                                                 optional_ptr<ReplacementScanData> data) {
	auto &scan_data = data->Cast<ReplacementScanDataV2>();

	ReplacementScanInfoV2 info;
	info.input = &input;
	info.data = &scan_data;

	auto info_handle = reinterpret_cast<duckdb_v2_replacement_scan_info_handle>(&info);
	auto ctx_handle = reinterpret_cast<duckdb_v2_context_handle>(&context);
	// BinderException: phase-default fallback for unmapped codes (we run in the binder).
	InvokeWithErrorSlot<BinderException>(
	    [&](duckdb_v2_error_info_handle *err_ptr) { scan_data.callback(info_handle, ctx_handle, err_ptr); });

	if (info.function_name.empty()) {
		return nullptr; // decline
	}
	vector<unique_ptr<ParsedExpression>> children;
	for (auto &param : info.parameters) {
		children.push_back(make_uniq<ConstantExpression>(std::move(param)));
	}
	for (auto &named_param : info.named_parameters) {
		// named parameter: a child whose alias is the name (`name => expr`)
		auto child = make_uniq<ConstantExpression>(std::move(named_param.second));
		child->SetAlias(named_param.first);
		children.push_back(std::move(child));
	}
	auto table_function = make_uniq<TableFunctionRef>();
	table_function->function = make_uniq<FunctionExpression>(info.function_name, std::move(children));
	return std::move(table_function);
}

// Absent catalog/schema maps to the canonical empty view {NULL, 0}, never {ptr, 0}.
duckdb_v2_identifier_t BorrowReplacementScanNamePart(const string &part) {
	if (part.empty()) {
		return duckdb_v2_identifier_t {nullptr, 0};
	}
	return ToStr(part);
}

} // namespace
} // namespace duckdb

static void RegisterReplacementScanV2(duckdb::DBConfig &config, duckdb_v2_replacement_scan_callback_fn callback,
                                      duckdb::OpaqueDataHandle owned) {
	auto data = duckdb::make_uniq<duckdb::ReplacementScanDataV2>();
	data->callback = callback;
	data->user_data = std::move(owned);
	config.replacement_scans.push_back(duckdb::ReplacementScan(duckdb::ReplacementScanTrampolineV2, std::move(data)));
}

DUCKDB_V2_ERROR duckdb_v2_replacement_scan_register_with_database(duckdb_v2_database_handle db,
                                                                  duckdb_v2_replacement_scan_callback_fn callback,
                                                                  duckdb_v2_opaque user_data,
                                                                  duckdb_v2_error_info_handle *err) {
	duckdb::OpaqueDataHandle owned(user_data.ptr, user_data.destroy, user_data.equals);
	return duckdb::WithErrorHandler(err, [&]() {
		if (!db || !callback) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_replacement_scan_register_with_database");
		}
		auto *wrapper = duckdb::ToDb(db);
		auto &config = duckdb::DBConfig::GetConfig(*wrapper->database->instance);
		RegisterReplacementScanV2(config, callback, std::move(owned));
	});
}

DUCKDB_V2_ERROR duckdb_v2_replacement_scan_register_with_extension(duckdb_v2_extension_handle extension,
                                                                   duckdb_v2_replacement_scan_callback_fn callback,
                                                                   duckdb_v2_opaque user_data,
                                                                   duckdb_v2_error_info_handle *err) {
	duckdb::OpaqueDataHandle owned(user_data.ptr, user_data.destroy, user_data.equals);
	return duckdb::WithErrorHandler(err, [&]() {
		if (!extension || !callback) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_replacement_scan_register_with_extension");
		}
		auto &config = duckdb::DBConfig::GetConfig(duckdb::ToExtension(extension)->GetDatabaseInstance());
		RegisterReplacementScanV2(config, callback, std::move(owned));
	});
}

DUCKDB_V2_ERROR duckdb_v2_replacement_scan_get_catalog_name(duckdb_v2_replacement_scan_info_handle info,
                                                            duckdb_v2_identifier_t *out_name,
                                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (out_name) {
			*out_name = duckdb_v2_identifier_t {nullptr, 0};
		}
		if (!info || !out_name) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_replacement_scan_get_catalog_name");
		}
		*out_name = duckdb::BorrowReplacementScanNamePart(duckdb::ToReplacementScanInfo(info)->input->catalog_name);
	});
}

DUCKDB_V2_ERROR duckdb_v2_replacement_scan_get_schema_name(duckdb_v2_replacement_scan_info_handle info,
                                                           duckdb_v2_identifier_t *out_name,
                                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (out_name) {
			*out_name = duckdb_v2_identifier_t {nullptr, 0};
		}
		if (!info || !out_name) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_replacement_scan_get_schema_name");
		}
		*out_name = duckdb::BorrowReplacementScanNamePart(duckdb::ToReplacementScanInfo(info)->input->schema_name);
	});
}

DUCKDB_V2_ERROR duckdb_v2_replacement_scan_get_table_name(duckdb_v2_replacement_scan_info_handle info,
                                                          duckdb_v2_identifier_t *out_name,
                                                          duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (out_name) {
			*out_name = duckdb_v2_identifier_t {nullptr, 0};
		}
		if (!info || !out_name) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_replacement_scan_get_table_name");
		}
		*out_name = duckdb::BorrowReplacementScanNamePart(duckdb::ToReplacementScanInfo(info)->input->table_name);
	});
}

DUCKDB_V2_ERROR duckdb_v2_replacement_scan_get_user_data(duckdb_v2_replacement_scan_info_handle info, void **out_data,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (out_data) {
			*out_data = nullptr;
		}
		if (!info || !out_data) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_replacement_scan_get_user_data");
		}
		*out_data = duckdb::ToReplacementScanInfo(info)->data->user_data.GetData();
	});
}

DUCKDB_V2_ERROR duckdb_v2_replacement_scan_set_function_name(duckdb_v2_replacement_scan_info_handle info,
                                                             duckdb_v2_identifier_t name,
                                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info || (!name.ptr && name.len > 0)) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_replacement_scan_set_function_name");
		}
		duckdb::ToReplacementScanInfo(info)->function_name = duckdb::ToIdentifier(name);
	});
}

DUCKDB_V2_ERROR duckdb_v2_replacement_scan_add_parameter(duckdb_v2_replacement_scan_info_handle info,
                                                         duckdb_v2_value_handle value,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info || !value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_replacement_scan_add_parameter");
		}
		duckdb::ToReplacementScanInfo(info)->parameters.push_back(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_ERROR duckdb_v2_replacement_scan_add_named_parameter(duckdb_v2_replacement_scan_info_handle info,
                                                               duckdb_v2_identifier_t name,
                                                               duckdb_v2_value_handle value,
                                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info || (!name.ptr && name.len > 0) || !value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_replacement_scan_add_named_parameter");
		}
		duckdb::ToReplacementScanInfo(info)->named_parameters.emplace_back(duckdb::ToIdentifier(name),
		                                                                   *duckdb::ToValue(value));
	});
}
