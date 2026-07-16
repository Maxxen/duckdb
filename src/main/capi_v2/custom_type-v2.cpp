#include "capi_v2_internal.hpp"

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {
namespace {

// TODO: This will hold much more stuff in the future
class CustomTypeBuilderV2 {
public:
	string name;
	LogicalType base_type;
};

} // namespace
} // namespace duckdb

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_create_with_alias(duckdb_v2_logical_type_handle base_type,
                                                              duckdb_v2_identifier_t alias_name,
                                                              duckdb_v2_logical_type_handle *out_type,
                                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!base_type || (!alias_name.ptr && alias_name.len > 0) || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_logical_type_create_with_alias");
		}
		if (alias_name.len == 0) {
			throw duckdb::InvalidInputException(
			    "Alias name cannot be empty in duckdb_v2_logical_type_create_with_alias");
		}
		auto copy = *duckdb::ToLogicalType(base_type);

		copy.SetAlias(duckdb::ToString(alias_name));

		*out_type = reinterpret_cast<duckdb_v2_logical_type_handle>(new duckdb::LogicalType(std::move(copy)));
	});
}

//-------------------------------------------------------------------------------------------------
// Custom Type Builder
//-------------------------------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_custom_type_builder_create(duckdb_v2_context_handle context,
                                                          duckdb_v2_custom_type_builder_handle *out_builder,
                                                          duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context || !out_builder) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_custom_type_builder_create");
		}
		*out_builder = nullptr;
		auto builder = new duckdb::CustomTypeBuilderV2();
		*out_builder = reinterpret_cast<duckdb_v2_custom_type_builder_handle>(builder);
	});
}

void duckdb_v2_custom_type_builder_destroy(duckdb_v2_custom_type_builder_handle *builder) {
	duckdb::WithErrorHandler(nullptr, [&]() {
		if (builder && *builder) {
			delete reinterpret_cast<duckdb::CustomTypeBuilderV2 *>(*builder);
			*builder = nullptr;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_custom_type_builder_register(duckdb_v2_context_handle context,
                                                            duckdb_v2_custom_type_builder_handle builder,
                                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("null context argument to duckdb_v2_custom_type_builder_register");
		}
		if (!builder) {
			throw duckdb::InvalidInputException("null builder argument to duckdb_v2_custom_type_builder_register");
		}
		auto &ctx = *reinterpret_cast<duckdb::ClientContext *>(context);
		auto &b = *reinterpret_cast<duckdb::CustomTypeBuilderV2 *>(builder);

		if (b.name.empty()) {
			throw duckdb::InvalidInputException("Custom type name cannot be empty.");
		}

		if (b.base_type.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Custom type base type must be set.");
		}
		// ANY is a signature wildcard; a registered type carries data, so reject it
		// (an ANY reached during execution throws InternalException).
		if (duckdb::TypeVisitor::Contains(b.base_type, duckdb::LogicalTypeId::ANY)) {
			throw duckdb::InvalidInputException("Custom type base type cannot be ANY.");
		}

		auto &catalog = duckdb::Catalog::GetSystemCatalog(ctx);
		auto base_type = b.base_type;
		base_type.SetAlias(b.name);

		duckdb::CreateTypeInfo info(base_type.GetAlias(), base_type);
		info.temporary = true;
		info.internal = true;
		catalog.CreateType(ctx, info);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_custom_type_builder_set_name(duckdb_v2_custom_type_builder_handle builder,
                                                            duckdb_v2_identifier_t name,
                                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_custom_type_builder_set_name");
		}

		auto *b = reinterpret_cast<duckdb::CustomTypeBuilderV2 *>(builder);
		b->name = duckdb::ToString(name);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_custom_type_builder_set_base_type(duckdb_v2_custom_type_builder_handle builder,
                                                                 duckdb_v2_logical_type_handle base_type,
                                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder || !base_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_custom_type_builder_set_base_type");
		}
		auto *b = reinterpret_cast<duckdb::CustomTypeBuilderV2 *>(builder);
		b->base_type = *duckdb::ToLogicalType(base_type);
	});
}
