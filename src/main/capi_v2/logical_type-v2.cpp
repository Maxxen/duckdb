#include "capi_v2_internal.hpp"

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"

// Exception policy:
//   - Allocating sites (create_from_id and the owned-out-param child
//     getters) wrap `new duckdb::LogicalType(...)` in try/catch. `new` is
//     the only throw source — the LogicalType copy ctor just bumps a
//     shared_ptr.
//   - Non-allocating accessors read pre-validated state. The DuckDB
//     internal accessors (DecimalType::GetWidth, EnumType::GetSize,
//     StructType::GetChildName, …) call ExtraTypeInfo::Cast<T>(), which
//     asserts the downcast in debug and reinterpret-casts (silent UB on
//     mismatch) in release. We treat the mismatch as unreachable because
//     every accessor first checks lt->id().
//   - get_enum_value additionally reads through FlatVector::GetData<T>,
//     which throws on a non-FLAT vector. Today every enum dictionary we
//     can observe is FLAT, but this accessor is wrapped in try/catch
//     anyway to keep the "no exceptions across the C ABI" contract.

namespace duckdb {
namespace {

// The set of LogicalTypeIds accepted by duckdb_v2_logical_type_create_from_id.
// Excludes:
//  - INVALID (sentinel),
//  - bind-time-only ids (SQLNULL, ANY, UNKNOWN) which only exist inside
//    the planner / UDF binding paths and have no use in PR1's read-only
//    surface,
//  - parameterised types (DECIMAL, LIST, STRUCT, MAP, ARRAY, UNION, ENUM,
//    VARIANT, GEOMETRY).
bool IsPrimitiveCreatable(LogicalTypeId id) {
	switch (id) {
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

// Map a PhysicalType used by DECIMAL storage to the matching logical id.
DUCKDB_V2_LOGICAL_TYPE_ID DecimalPhysicalToLogical(PhysicalType physical) {
	switch (physical) {
	case PhysicalType::INT16:
		return DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT;
	case PhysicalType::INT32:
		return DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER;
	case PhysicalType::INT64:
		return DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT;
	case PhysicalType::INT128:
		return DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT;
	default:
		return DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	}
}

// Map a PhysicalType used by ENUM index storage to the matching logical id.
DUCKDB_V2_LOGICAL_TYPE_ID EnumPhysicalToLogical(PhysicalType physical) {
	switch (physical) {
	case PhysicalType::UINT8:
		return DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT;
	case PhysicalType::UINT16:
		return DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT;
	case PhysicalType::UINT32:
		return DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER;
	default:
		return DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	}
}

} // anonymous namespace
} // namespace duckdb

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID type_id,
                                                           duckdb_v2_logical_type_ptr *out_type,
                                                           duckdb_v2_error_info_ptr *err) {
	if (!out_type) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null out_type in duckdb_v2_logical_type_create_from_id");
	}
	*out_type = nullptr;
	auto id = static_cast<duckdb::LogicalTypeId>(type_id);
	if (!duckdb::IsPrimitiveCreatable(id)) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "duckdb_v2_logical_type_create_from_id only accepts primitive type ids");
	}
	try {
		auto *lt = new duckdb::LogicalType(id);
		*out_type = static_cast<duckdb_v2_logical_type_ptr>(lt);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_logical_type_create_from_id");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_destroy(duckdb_v2_logical_type_ptr *type, duckdb_v2_error_info_ptr *err) {
	if (!type) {
		return duckdb::ClearErrorInfo(err);
	}
	if (*type) {
		delete duckdb::ToLogicalType(*type);
		*type = nullptr;
	}
	return duckdb::ClearErrorInfo(err);
}

// ---------------------------------------------------------------------------
// Common introspection
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_id(duckdb_v2_logical_type_ptr type, DUCKDB_V2_LOGICAL_TYPE_ID *out_id,
                                                   duckdb_v2_error_info_ptr *err) {
	if (!type || !out_id) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_id");
	}
	*out_id = static_cast<DUCKDB_V2_LOGICAL_TYPE_ID>(duckdb::ToLogicalType(type)->id());
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_alias(duckdb_v2_logical_type_ptr type, const char **out_alias,
                                                      duckdb_v2_error_info_ptr *err) {
	if (!type || !out_alias) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_alias");
	}
	auto info = duckdb::ToLogicalType(type)->AuxInfo();
	*out_alias = (info && !info->alias.empty()) ? info->alias.c_str() : nullptr;
	return duckdb::ClearErrorInfo(err);
}

// ---------------------------------------------------------------------------
// DECIMAL
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_decimal_width(duckdb_v2_logical_type_ptr type, uint8_t *out_width,
                                                              duckdb_v2_error_info_ptr *err) {
	if (!type || !out_width) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_decimal_width");
	}
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::DECIMAL) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a DECIMAL logical type");
	}
	*out_width = duckdb::DecimalType::GetWidth(*lt);
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_decimal_scale(duckdb_v2_logical_type_ptr type, uint8_t *out_scale,
                                                              duckdb_v2_error_info_ptr *err) {
	if (!type || !out_scale) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_decimal_scale");
	}
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::DECIMAL) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a DECIMAL logical type");
	}
	*out_scale = duckdb::DecimalType::GetScale(*lt);
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_decimal_internal_type_id(duckdb_v2_logical_type_ptr type,
                                                                         DUCKDB_V2_LOGICAL_TYPE_ID *out_id,
                                                                         duckdb_v2_error_info_ptr *err) {
	if (!type || !out_id) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_decimal_internal_type_id");
	}
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::DECIMAL) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a DECIMAL logical type");
	}
	auto mapped = duckdb::DecimalPhysicalToLogical(lt->InternalType());
	if (mapped == DUCKDB_V2_LOGICAL_TYPE_ID_INVALID) {
		std::string msg =
		    "DECIMAL has unexpected internal physical type: " + duckdb::TypeIdToString(lt->InternalType());
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, msg.c_str());
	}
	*out_id = mapped;
	return duckdb::ClearErrorInfo(err);
}

// ---------------------------------------------------------------------------
// ENUM
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_enum_size(duckdb_v2_logical_type_ptr type, idx_t *out_size,
                                                          duckdb_v2_error_info_ptr *err) {
	if (!type || !out_size) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_enum_size");
	}
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::ENUM) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not an ENUM logical type");
	}
	*out_size = duckdb::EnumType::GetSize(*lt);
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_enum_value(duckdb_v2_logical_type_ptr type, idx_t index,
                                                           const char **out_value, idx_t *out_length,
                                                           duckdb_v2_error_info_ptr *err) {
	if (!type || !out_value || !out_length) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_enum_value");
	}
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::ENUM) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not an ENUM logical type");
	}
	if (index >= duckdb::EnumType::GetSize(*lt)) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "enum index out of range");
	}
	try {
		// Borrow from the stored Vector directly: FlatVector::GetData<string_t>
		// hands back the buffer-resident string_t entries (not a copy), so the
		// data pointer stays valid for the lifetime of the EnumTypeInfo. The
		// call throws if the dictionary is not FLAT — unreachable today but the
		// catch keeps the C ABI clean.
		auto &dict = duckdb::EnumType::GetValuesInsertOrder(*lt);
		auto *entries = duckdb::FlatVector::GetData<duckdb::string_t>(dict);
		auto &entry = entries[index];
		*out_value = entry.GetData();
		*out_length = entry.GetSize();
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error reading enum dictionary");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_enum_internal_type_id(duckdb_v2_logical_type_ptr type,
                                                                      DUCKDB_V2_LOGICAL_TYPE_ID *out_id,
                                                                      duckdb_v2_error_info_ptr *err) {
	if (!type || !out_id) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_enum_internal_type_id");
	}
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::ENUM) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not an ENUM logical type");
	}
	auto enum_physical = duckdb::EnumType::GetPhysicalType(*lt);
	auto mapped = duckdb::EnumPhysicalToLogical(enum_physical);
	if (mapped == DUCKDB_V2_LOGICAL_TYPE_ID_INVALID) {
		std::string msg = "ENUM has unexpected internal physical type: " + duckdb::TypeIdToString(enum_physical);
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, msg.c_str());
	}
	*out_id = mapped;
	return duckdb::ClearErrorInfo(err);
}

// ---------------------------------------------------------------------------
// LIST / ARRAY
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_list_child_type(duckdb_v2_logical_type_ptr type,
                                                                duckdb_v2_logical_type_ptr *out_child,
                                                                duckdb_v2_error_info_ptr *err) {
	if (!type || !out_child) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_list_child_type");
	}
	*out_child = nullptr;
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::LIST) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a LIST logical type");
	}
	try {
		auto *child = new duckdb::LogicalType(duckdb::ListType::GetChildType(*lt));
		*out_child = static_cast<duckdb_v2_logical_type_ptr>(child);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error allocating child logical_type");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_array_child_type(duckdb_v2_logical_type_ptr type,
                                                                 duckdb_v2_logical_type_ptr *out_child,
                                                                 duckdb_v2_error_info_ptr *err) {
	if (!type || !out_child) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_array_child_type");
	}
	*out_child = nullptr;
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::ARRAY) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not an ARRAY logical type");
	}
	try {
		auto *child = new duckdb::LogicalType(duckdb::ArrayType::GetChildType(*lt));
		*out_child = static_cast<duckdb_v2_logical_type_ptr>(child);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error allocating child logical_type");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_array_size(duckdb_v2_logical_type_ptr type, idx_t *out_size,
                                                           duckdb_v2_error_info_ptr *err) {
	if (!type || !out_size) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_array_size");
	}
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::ARRAY) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not an ARRAY logical type");
	}
	*out_size = duckdb::ArrayType::GetSize(*lt);
	return duckdb::ClearErrorInfo(err);
}

// ---------------------------------------------------------------------------
// MAP
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_map_key_type(duckdb_v2_logical_type_ptr type,
                                                             duckdb_v2_logical_type_ptr *out_key,
                                                             duckdb_v2_error_info_ptr *err) {
	if (!type || !out_key) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_map_key_type");
	}
	*out_key = nullptr;
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::MAP) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a MAP logical type");
	}
	try {
		auto *key = new duckdb::LogicalType(duckdb::MapType::KeyType(*lt));
		*out_key = static_cast<duckdb_v2_logical_type_ptr>(key);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error allocating child logical_type");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_map_value_type(duckdb_v2_logical_type_ptr type,
                                                               duckdb_v2_logical_type_ptr *out_value,
                                                               duckdb_v2_error_info_ptr *err) {
	if (!type || !out_value) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_map_value_type");
	}
	*out_value = nullptr;
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::MAP) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a MAP logical type");
	}
	try {
		auto *val = new duckdb::LogicalType(duckdb::MapType::ValueType(*lt));
		*out_value = static_cast<duckdb_v2_logical_type_ptr>(val);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error allocating child logical_type");
	}
}

// ---------------------------------------------------------------------------
// STRUCT
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_struct_child_count(duckdb_v2_logical_type_ptr type, idx_t *out_count,
                                                                   duckdb_v2_error_info_ptr *err) {
	if (!type || !out_count) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_struct_child_count");
	}
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::STRUCT) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a STRUCT logical type");
	}
	*out_count = duckdb::StructType::GetChildCount(*lt);
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_struct_child_name(duckdb_v2_logical_type_ptr type, idx_t index,
                                                                  const char **out_name, idx_t *out_length,
                                                                  duckdb_v2_error_info_ptr *err) {
	if (!type || !out_name || !out_length) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_struct_child_name");
	}
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::STRUCT) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a STRUCT logical type");
	}
	if (index >= duckdb::StructType::GetChildCount(*lt)) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "struct child index out of range");
	}
	auto &name = duckdb::StructType::GetChildName(*lt, index);
	*out_name = name.c_str();
	*out_length = name.size();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_struct_child_type(duckdb_v2_logical_type_ptr type, idx_t index,
                                                                  duckdb_v2_logical_type_ptr *out_child,
                                                                  duckdb_v2_error_info_ptr *err) {
	if (!type || !out_child) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_struct_child_type");
	}
	*out_child = nullptr;
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::STRUCT) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a STRUCT logical type");
	}
	if (index >= duckdb::StructType::GetChildCount(*lt)) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "struct child index out of range");
	}
	try {
		auto *child = new duckdb::LogicalType(duckdb::StructType::GetChildType(*lt, index));
		*out_child = static_cast<duckdb_v2_logical_type_ptr>(child);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error allocating child logical_type");
	}
}

// ---------------------------------------------------------------------------
// UNION
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_union_member_count(duckdb_v2_logical_type_ptr type, idx_t *out_count,
                                                                   duckdb_v2_error_info_ptr *err) {
	if (!type || !out_count) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_union_member_count");
	}
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::UNION) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a UNION logical type");
	}
	*out_count = duckdb::UnionType::GetMemberCount(*lt);
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_union_member_name(duckdb_v2_logical_type_ptr type, idx_t index,
                                                                  const char **out_name, idx_t *out_length,
                                                                  duckdb_v2_error_info_ptr *err) {
	if (!type || !out_name || !out_length) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_union_member_name");
	}
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::UNION) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a UNION logical type");
	}
	if (index >= duckdb::UnionType::GetMemberCount(*lt)) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "union member index out of range");
	}
	auto &name = duckdb::UnionType::GetMemberName(*lt, index);
	*out_name = name.c_str();
	*out_length = name.size();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_type_get_union_member_type(duckdb_v2_logical_type_ptr type, idx_t index,
                                                                  duckdb_v2_logical_type_ptr *out_child,
                                                                  duckdb_v2_error_info_ptr *err) {
	if (!type || !out_child) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_logical_type_get_union_member_type");
	}
	*out_child = nullptr;
	auto *lt = duckdb::ToLogicalType(type);
	if (lt->id() != duckdb::LogicalTypeId::UNION) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "not a UNION logical type");
	}
	if (index >= duckdb::UnionType::GetMemberCount(*lt)) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "union member index out of range");
	}
	try {
		auto *child = new duckdb::LogicalType(duckdb::UnionType::GetMemberType(*lt, index));
		*out_child = static_cast<duckdb_v2_logical_type_ptr>(child);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error allocating child logical_type");
	}
}
