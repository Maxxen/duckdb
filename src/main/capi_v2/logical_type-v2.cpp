#include "capi_v2_internal.hpp"

#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

#include <cstdlib>
#include <cstring>

namespace duckdb {
namespace {

// The set of LogicalTypeIds accepted by duckdb_v2_logical_type_create_from_id.
// Includes ANY, the function-signature wildcard: it is constructible so it can
// be passed to function parameter / varargs setters, while data-creating
// surfaces reject it. Excludes:
//  - INVALID (sentinel),
//  - the remaining bind-time-only ids (SQLNULL, UNKNOWN) which only exist
//    inside the planner / UDF binding paths,
//  - parameterised types (DECIMAL, LIST, STRUCT, TUPLE, MAP, ARRAY, UNION,
//    ENUM, VARIANT, GEOMETRY).
bool IsPrimitiveCreatable(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::ANY:
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
	case LogicalTypeId::BIGNUM:
	case LogicalTypeId::UUID:
		return true;
	default:
		return false;
	}
}

bool IsValidTypeEntry(optional_ptr<CatalogEntry> entry) {
	if (!entry) {
		return false;
	}
	return entry->Cast<TypeCatalogEntry>().user_type.id() != LogicalTypeId::INVALID;
}

// Hand-mirrors ExpressionBinder::BindExpression(TypeExpression&) in
// src/planner/binder/expression/bind_type_expression.cpp: search path
// first, then the system catalog (where custom_type_builder registers).
// Intentional omissions vs the mirrored source, per the unqualified-only
// contract: qualified names (catalog/schema splitting and the 4-step
// lookup), expression-valued params (values arrive pre-folded here), and
// query-location error context. If engine type binding changes, this
// mirror must follow. Runs inside a transaction; the caller provides it.
LogicalType BindTypeByNameV2(ClientContext &context, const string &name, const vector<TypeArgument> &args) {
	EntryLookupInfo lookup(CatalogType::TYPE_ENTRY, QualifiedName(Identifier(name)));
	CatalogEntryRetriever retriever(context);
	optional_ptr<CatalogEntry> entry;
	if (!DatabaseManager::Get(context).HasDefaultDatabase()) {
		entry = retriever.GetEntry(
		    EntryLookupInfo(lookup, QualifiedName(Identifier::SystemCatalog(), Identifier::InvalidSchema(),
		                                          lookup.GetEntryIdentifier())));
	} else {
		entry = retriever.GetEntry(lookup, OnEntryNotFound::RETURN_NULL);
		if (!IsValidTypeEntry(entry)) {
			entry = retriever.GetEntry(
			    EntryLookupInfo(lookup, QualifiedName(Identifier::SystemCatalog(), Identifier::DefaultSchema(),
			                                          lookup.GetEntryIdentifier())),
			    OnEntryNotFound::THROW_EXCEPTION);
		}
	}
	auto &type_entry = entry->Cast<TypeCatalogEntry>();
	if (!type_entry.bind_function) {
		if (!args.empty()) {
			throw BinderException("Type '%s' does not take any type parameters", name);
		}
		return type_entry.user_type;
	}
	BindLogicalTypeInput input {context, type_entry.user_type, args};
	return type_entry.bind_function(input);
}

// The value-parameter view of a bound type: the exact dual of
// logical_type_create. Kinds without retained parameters report 0.
idx_t TypeParamCount(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::MAP:
		return 2;
	case LogicalTypeId::LIST:
		return 1;
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE:
		return StructType::GetChildCount(type);
	case LogicalTypeId::UNION:
		return UnionType::GetMemberCount(type);
	case LogicalTypeId::ENUM:
		return EnumType::GetSize(type);
	case LogicalTypeId::VARCHAR:
		return StringType::GetCollation(type).empty() ? 0 : 1;
	case LogicalTypeId::GEOMETRY:
		return GeoType::HasCRS(type) ? 1 : 0;
	default:
		return 0;
	}
}

// Produces param `index` of `type`: the owned value is returned, the
// borrowed name (off the type, or a static literal) goes into out_name,
// which arrives pre-set to the positional {NULL, 0}. The caller has
// bounds-checked index against TypeParamCount.
Value TypeParamValue(const LogicalType &type, idx_t index, duckdb_v2_identifier_t &out_name) {
	switch (type.id()) {
	case LogicalTypeId::DECIMAL:
		return index == 0 ? Value::UTINYINT(DecimalType::GetWidth(type)) : Value::UTINYINT(DecimalType::GetScale(type));
	case LogicalTypeId::LIST:
		return Value::TYPE(ListType::GetChildType(type));
	case LogicalTypeId::ARRAY:
		return index == 0 ? Value::TYPE(ArrayType::GetChildType(type))
		                  : Value::BIGINT(NumericCast<int64_t>(ArrayType::GetSize(type)));
	case LogicalTypeId::MAP:
		return Value::TYPE(index == 0 ? MapType::KeyType(type) : MapType::ValueType(type));
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE:
		if (!StructType::IsUnnamed(type)) {
			out_name = ToStr(StructType::GetChildName(type, index));
		}
		return Value::TYPE(StructType::GetChildType(type, index));
	case LogicalTypeId::UNION:
		out_name = ToStr(UnionType::GetMemberName(type, index));
		return Value::TYPE(UnionType::GetMemberType(type, index));
	case LogicalTypeId::ENUM:
		return Value(EnumType::GetString(type, index).GetString());
	case LogicalTypeId::VARCHAR:
		out_name = duckdb_v2_identifier_t {"collation", 9};
		return Value(StringType::GetCollation(type));
	case LogicalTypeId::GEOMETRY:
		return Value(GeoType::GetCRS(type).GetDefinition());
	default:
		throw InternalException("TypeParamValue called for a kind without parameters");
	}
}

} // anonymous namespace
} // namespace duckdb

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID type_id,
                                                           duckdb_v2_logical_type_handle *out_type,
                                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_type) {
			throw duckdb::InvalidInputException("null out_type in duckdb_v2_logical_type_create_from_id");
		}
		*out_type = nullptr;
		auto id = static_cast<duckdb::LogicalTypeId>(type_id);
		if (!duckdb::IsPrimitiveCreatable(id)) {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_logical_type_create_from_id only accepts primitive type ids");
		}
		auto *lt = new duckdb::LogicalType(id);
		*out_type = reinterpret_cast<_duckdb_v2_logical_type *>(lt);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_create_from_text(duckdb_v2_context_handle ctx, duckdb_v2_str text,
                                                             duckdb_v2_logical_type_handle *out_type,
                                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!ctx || !out_type || (!text.ptr && text.len > 0)) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_logical_type_create_from_text");
		}
		*out_type = nullptr;
		// A context arrives with the lock held and a transaction active, so no
		// lock or transaction management here. Parse and bind errors propagate.
		auto &context = *duckdb::ToContext(ctx);
		auto parsed = duckdb::TransformStringToLogicalType(duckdb::ToString(text), context);
		*out_type = reinterpret_cast<_duckdb_v2_logical_type *>(new duckdb::LogicalType(std::move(parsed)));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_create(duckdb_v2_context_handle ctx, duckdb_v2_identifier_t name,
                                                   const duckdb_v2_identifier_t *param_names,
                                                   const duckdb_v2_value_handle *param_values, idx_t param_count,
                                                   duckdb_v2_logical_type_handle *out_type,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!ctx || !out_type || (!name.ptr && name.len > 0) || (param_count > 0 && !param_values)) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_logical_type_create");
		}
		*out_type = nullptr;
		duckdb::vector<duckdb::TypeArgument> args;
		args.reserve(param_count);
		for (idx_t i = 0; i < param_count; i++) {
			auto param_name = param_names ? param_names[i] : duckdb_v2_str {nullptr, 0};
			if ((!param_name.ptr && param_name.len > 0) || !param_values[i]) {
				throw duckdb::InvalidInputException("null parameter in duckdb_v2_logical_type_create");
			}
			args.emplace_back(duckdb::ToString(param_name), *duckdb::ToValue(param_values[i]));
		}
		// A context arrives with the lock held and a transaction active, so no
		// lock or transaction management here. Bind errors propagate.
		auto &context = *duckdb::ToContext(ctx);
		auto bound = duckdb::BindTypeByNameV2(context, duckdb::ToString(name), args);
		*out_type = reinterpret_cast<_duckdb_v2_logical_type *>(new duckdb::LogicalType(std::move(bound)));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_copy(duckdb_v2_logical_type_handle type,
                                                 duckdb_v2_logical_type_handle *out_type,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!type || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_logical_type_copy");
		}
		auto *lt = duckdb::ToLogicalType(type);
		auto *copy = new duckdb::LogicalType(*lt);
		*out_type = reinterpret_cast<duckdb_v2_logical_type_handle>(copy);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_destroy(duckdb_v2_logical_type_handle *type) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!type) {
			return;
		}
		if (*type) {
			delete duckdb::ToLogicalType(*type);
			*type = nullptr;
		}
	});
}

// ---------------------------------------------------------------------------
// Common introspection
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_is_equal(duckdb_v2_logical_type_handle left,
                                                     duckdb_v2_logical_type_handle right, bool *result,
                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!left || !right || !result) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_logical_type_is_equal");
		}

		const auto &left_lt = *duckdb::ToLogicalType(left);
		const auto &right_lt = *duckdb::ToLogicalType(right);

		*result = left_lt == right_lt;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_id(duckdb_v2_logical_type_handle type,
                                                   DUCKDB_V2_LOGICAL_TYPE_ID *out_id,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!type || !out_id) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_logical_type_get_id");
		}
		*out_id = static_cast<DUCKDB_V2_LOGICAL_TYPE_ID>(duckdb::ToLogicalType(type)->id());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_name(duckdb_v2_logical_type_handle type,
                                                     duckdb_v2_identifier_t *out_name,
                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!type || !out_name) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_logical_type_get_name");
		}
		auto *lt = duckdb::ToLogicalType(type);
		auto info = lt->AuxInfo();
		if (info && !info->alias.empty()) {
			*out_name = duckdb::ToStr(info->alias);
			return;
		}
		// Canonical fixed name of the id: static storage, so the borrowed
		// view outlives even the handle.
		const char *canonical = duckdb::EnumUtil::ToChars<duckdb::LogicalTypeId>(lt->id());
		*out_name = duckdb_v2_str {canonical, std::strlen(canonical)};
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_to_text(duckdb_v2_logical_type_handle type, char **out_text,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!type || !out_text) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_logical_type_to_text");
		}
		*out_text = nullptr;
		auto str = duckdb::ToLogicalType(type)->ToString();
		auto *buf = static_cast<char *>(std::malloc(str.size() + 1));
		if (!buf) {
			throw duckdb::OutOfMemoryException("malloc failed in duckdb_v2_logical_type_to_text");
		}
		std::memcpy(buf, str.data(), str.size());
		buf[str.size()] = '\0';
		*out_text = buf;
	});
}

// ---------------------------------------------------------------------------
// Generic parameter inspection
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_param_count(duckdb_v2_logical_type_handle type, idx_t *out_count,
                                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!type || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_logical_type_get_param_count");
		}
		*out_count = duckdb::TypeParamCount(*duckdb::ToLogicalType(type));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_param(duckdb_v2_logical_type_handle type, idx_t index,
                                                      duckdb_v2_identifier_t *out_name,
                                                      duckdb_v2_value_handle *out_value,
                                                      duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!type || !out_name || !out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_logical_type_get_param");
		}
		*out_name = duckdb_v2_identifier_t {nullptr, 0};
		*out_value = nullptr;
		auto &lt = *duckdb::ToLogicalType(type);
		if (index >= duckdb::TypeParamCount(lt)) {
			throw duckdb::InvalidInputException("parameter index out of range in duckdb_v2_logical_type_get_param");
		}
		duckdb_v2_identifier_t name {nullptr, 0};
		auto *value = new duckdb::Value(duckdb::TypeParamValue(lt, index, name));
		*out_name = name;
		*out_value = reinterpret_cast<_duckdb_v2_value *>(value);
	});
}
