#include "capi_v2_internal.hpp"

#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/value.hpp"

#include <cstdlib>
#include <cstring>
#include <string>

// Out-param zeroing on failure:
//   - Pointer out-params (out_value, out_type, out_data) are set to nullptr
//     on every INVALID_INPUT path to keep dangling-pointer hazards out of
//     caller code.
//   - Scalar out-params (out_len, out_count, ...) are unspecified on
//     failure. Callers must check the return code before reading scalars.

namespace duckdb {
namespace {

// Common preflight for typed-payload getters: handle must be non-null, value
// id must match, and the value must not be NULL. Throws InvalidInputException
// on any precondition failure so the outer WithErrorHandler routes it.
void RequireTypedValue(duckdb_v2_value_handle value, LogicalTypeId expected, const char *function_name) {
	if (!value) {
		throw InvalidInputException(std::string("null argument to ") + function_name);
	}
	auto *v = ToValue(value);
	if (v->type().id() != expected) {
		throw InvalidInputException(std::string(function_name) + ": value is not of the expected type");
	}
	if (v->IsNull()) {
		throw InvalidInputException(std::string(function_name) + ": value is NULL");
	}
}
// The child-count view of a value: LIST/ARRAY/STRUCT elements or fields,
// MAP 2 x entries (children alternate key, value), UNION 2 (tag + active
// member). NULL values of any type, and non-composites, report 0.
idx_t CompositeChildCount(const Value &v) {
	if (v.IsNull()) {
		return 0;
	}
	switch (v.type().id()) {
	case LogicalTypeId::LIST:
		return ListValue::GetChildren(v).size();
	case LogicalTypeId::ARRAY:
		return ArrayValue::GetChildren(v).size();
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE:
		return StructValue::GetChildren(v).size();
	case LogicalTypeId::MAP:
		return MapValue::GetChildren(v).size() * 2;
	case LogicalTypeId::UNION:
		return 2;
	default:
		return 0;
	}
}

// Leaf payload codec. The committed physical layouts are contract text: the
// vector view layout table plus the DECIMAL / ENUM storage tier tables.
// Fixed-size kinds carry GetTypeIdSize(InternalType()) bytes; VARCHAR / BLOB /
// BIT carry their wire bytes. TYPE, BIGNUM (wire encoding not committed),
// GEOMETRY (no committed layout), composites, and bind-time ids are not
// leaf-data addressable.

bool IsWireBytesKind(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
		return true;
	default:
		return false;
	}
}

bool IsFixedLeafKind(LogicalTypeId id) {
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
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::UUID:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::ENUM:
		return true;
	default:
		return false;
	}
}

// Builds the leaf value from its payload. len is pre-validated against the
// layout size for fixed-size kinds. Layout-raw: no semantic validation
// beyond the gates documented on value_create_from_data.
Value LeafValueFromData(const LogicalType &lt, const_data_ptr_t data, idx_t len) {
	switch (lt.id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(Load<bool>(data));
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(Load<int8_t>(data));
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(Load<int16_t>(data));
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(Load<int32_t>(data));
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(Load<int64_t>(data));
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(Load<uint8_t>(data));
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(Load<uint16_t>(data));
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(Load<uint32_t>(data));
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(Load<uint64_t>(data));
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(Load<hugeint_t>(data));
	case LogicalTypeId::UHUGEINT:
		return Value::UHUGEINT(Load<uhugeint_t>(data));
	case LogicalTypeId::UUID:
		// The internal hugeint form, exactly as the vector view exposes it.
		return Value::UUID(Load<hugeint_t>(data));
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(Load<float>(data));
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(Load<double>(data));
	case LogicalTypeId::DATE:
		return Value::DATE(Load<date_t>(data));
	case LogicalTypeId::TIME:
		return Value::TIME(Load<dtime_t>(data));
	case LogicalTypeId::TIME_NS:
		return Value::TIME_NS(Load<dtime_ns_t>(data));
	case LogicalTypeId::TIME_TZ:
		// The packed 64-bit form.
		return Value::TIMETZ(Load<dtime_tz_t>(data));
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(Load<timestamp_sec_t>(data));
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(Load<timestamp_ms_t>(data));
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(Load<timestamp_t>(data));
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(Load<timestamp_ns_t>(data));
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(Load<timestamp_tz_t>(data));
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return Value::TIMESTAMPTZNS(Load<timestamp_tz_ns_t>(data));
	case LogicalTypeId::INTERVAL:
		return Value::INTERVAL(Load<interval_t>(data));
	case LogicalTypeId::DECIMAL: {
		// The scaled integer of the width tier.
		auto width = DecimalType::GetWidth(lt);
		auto scale = DecimalType::GetScale(lt);
		switch (lt.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(Load<int16_t>(data), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(Load<int32_t>(data), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(Load<int64_t>(data), width, scale);
		default:
			return Value::DECIMAL(Load<hugeint_t>(data), width, scale);
		}
	}
	case LogicalTypeId::ENUM: {
		// The dictionary index of the size tier, bounds-checked: an
		// out-of-range index is not addressable storage.
		uint64_t index = 0;
		switch (lt.InternalType()) {
		case PhysicalType::UINT8:
			index = Load<uint8_t>(data);
			break;
		case PhysicalType::UINT16:
			index = Load<uint16_t>(data);
			break;
		default:
			index = Load<uint32_t>(data);
			break;
		}
		if (index >= EnumType::GetSize(lt)) {
			throw InvalidInputException("duckdb_v2_value_create_from_data: enum index out of range");
		}
		return Value::ENUM(index, lt);
	}
	case LogicalTypeId::VARCHAR:
		// The engine rejects invalid UTF-8 at construction.
		return Value(data ? std::string(const_char_ptr_cast(data), len) : std::string());
	case LogicalTypeId::BLOB:
		return Value::BLOB(data, len);
	case LogicalTypeId::BIT:
		return Value::BIT(data, len);
	default:
		throw InternalException("LeafValueFromData called for a kind without a committed leaf layout");
	}
}

// Address + size of a non-NULL leaf value's payload, borrowed until the
// value is destroyed. Wire-bytes kinds borrow from the managed string;
// fixed-size kinds borrow the internal storage slot, dispatched by
// physical type.
std::pair<const void *, idx_t> LeafPayload(const Value &v) {
	if (IsWireBytesKind(v.type().id())) {
		auto &str = StringValue::Get(v);
		return {str.data(), str.size()};
	}
	auto physical = v.type().InternalType();
	if (!TypeIsConstantSize(physical)) {
		throw InternalException("LeafPayload called for a kind without a committed leaf layout");
	}
	return {v.GetPointerToData(), GetTypeIdSize(physical)};
}

} // anonymous namespace
} // namespace duckdb

// ---------------------------------------------------------------------------
// Lifecycle + NULL
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_value_destroy(duckdb_v2_value_handle *value) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!value) {
			return;
		}
		if (*value) {
			delete duckdb::ToValue(*value);
			*value = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_null(duckdb_v2_logical_type_handle type, duckdb_v2_value_handle *out_value,
                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!type || !out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_create_null");
		}
		*out_value = nullptr;
		// ANY is a signature wildcard; a value carries data, so reject it.
		if (duckdb::ToLogicalType(type)->id() == duckdb::LogicalTypeId::ANY) {
			throw duckdb::InvalidInputException("duckdb_v2_value_create_null: type cannot be ANY");
		}
		// Value(LogicalType) constructs a typed NULL — exactly what we want.
		auto *v = new duckdb::Value(*duckdb::ToLogicalType(type));
		*out_value = reinterpret_cast<_duckdb_v2_value *>(v);
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_is_null(duckdb_v2_value_handle value, bool *out_is_null,
                                        duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!value || !out_is_null) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_is_null");
		}
		*out_is_null = duckdb::ToValue(value)->IsNull();
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_logical_type(duckdb_v2_value_handle value, duckdb_v2_logical_type_handle *out_type,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!value || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_logical_type");
		}
		*out_type = nullptr;
		auto *lt = new duckdb::LogicalType(duckdb::ToValue(value)->type());
		*out_type = reinterpret_cast<_duckdb_v2_logical_type *>(lt);
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_to_string(duckdb_v2_value_handle value, char **out_string,
                                          duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!value || !out_string) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_to_string");
		}
		*out_string = nullptr;
		auto str = duckdb::ToValue(value)->ToString();
		auto *buf = static_cast<char *>(std::malloc(str.size() + 1));
		if (!buf) {
			throw duckdb::OutOfMemoryException("malloc failed in duckdb_v2_value_to_string");
		}
		std::memcpy(buf, str.data(), str.size());
		buf[str.size()] = '\0';
		*out_string = buf;
	});
}

// ---------------------------------------------------------------------------
// Leaf payload codec
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_value_create_from_data(duckdb_v2_logical_type_handle type, const void *data, idx_t len,
                                                 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!type || !out_value || (!data && len > 0)) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_create_from_data");
		}
		*out_value = nullptr;
		auto &lt = *duckdb::ToLogicalType(type);
		if (duckdb::IsFixedLeafKind(lt.id())) {
			auto expected = duckdb::GetTypeIdSize(lt.InternalType());
			if (len != expected) {
				throw duckdb::InvalidInputException("duckdb_v2_value_create_from_data: len " + std::to_string(len) +
				                                    " does not match the committed layout size " +
				                                    std::to_string(expected));
			}
		} else if (duckdb::IsWireBytesKind(lt.id())) {
			if (lt.id() == duckdb::LogicalTypeId::BIT && len == 0) {
				throw duckdb::InvalidInputException(
				    "duckdb_v2_value_create_from_data: the BIT wire form carries a mandatory padding header byte");
			}
		} else {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_value_create_from_data: type has no committed leaf layout; use value_create_type, "
			    "value_create_bignum, or value_create");
		}
		auto value = duckdb::LeafValueFromData(lt, duckdb::const_data_ptr_cast(data), len);
		// The base-typed leaf constructors drop the caller's alias / extension
		// info; re-stamp with the exact type (same physical layout, so this only
		// relabels) so extension types can be built straight from raw bytes.
		value.Reinterpret(lt);
		auto *v = new duckdb::Value(std::move(value));
		*out_value = reinterpret_cast<_duckdb_v2_value *>(v);
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_data(duckdb_v2_value_handle value, const void **out_data, idx_t *out_len,
                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!value || !out_data || !out_len) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_data");
		}
		*out_data = nullptr;
		*out_len = 0;
		auto &v = *duckdb::ToValue(value);
		auto id = v.type().id();
		if (!duckdb::IsFixedLeafKind(id) && !duckdb::IsWireBytesKind(id)) {
			throw duckdb::InvalidInputException("duckdb_v2_value_get_data: type has no committed leaf layout; use "
			                                    "value_get_type, value_get_bignum, or value_get_child");
		}
		if (v.IsNull()) {
			throw duckdb::InvalidInputException("duckdb_v2_value_get_data: value is NULL");
		}
		auto payload = duckdb::LeafPayload(v);
		*out_data = payload.first;
		*out_len = payload.second;
	});
}

// ---------------------------------------------------------------------------
// BIGNUM codec (wire encoding not committed; see the module spec)
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_value_create_bignum(const uint8_t *data, idx_t length, bool is_negative,
                                              duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_create_bignum");
		}
		*out_value = nullptr;
		// BIGNUM has no empty encoding: zero must be expressed as {0x00}.
		if (!data || length == 0) {
			throw duckdb::InvalidInputException("duckdb_v2_value_create_bignum requires data != NULL and length >= 1");
		}
		// Bignum::FromByteArray takes a non-const pointer but only reads.
		// Project rule bans const_cast, so copy into a writable buffer.
		// TODO(core): widen FromByteArray to const uint8_t * upstream and
		// drop this copy.
		duckdb::vector<uint8_t> tmp(data, data + length);
		auto blob = duckdb::Bignum::FromByteArray(tmp.data(), tmp.size(), is_negative);
		auto *v = new duckdb::Value(duckdb::Value::BIGNUM(blob));
		*out_value = reinterpret_cast<_duckdb_v2_value *>(v);
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_bignum(duckdb_v2_value_handle value, uint8_t **out_data, idx_t *out_length,
                                           bool *out_is_negative, duckdb_v2_error_info_handle *err) {
	if (!out_data || !out_length || !out_is_negative) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INPUT_INVALID, "null argument to duckdb_v2_value_get_bignum");
	}
	*out_data = nullptr;
	*out_length = 0;
	*out_is_negative = false;
	// Preflight is split out so we can use DecodeBignumStringT's existing
	// error-info plumbing for the malloc/decode failure paths.
	auto preflight = duckdb::WithErrorHandler(
	    err, [&]() { duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::BIGNUM, "duckdb_v2_value_get_bignum"); });
	if (preflight != DUCKDB_V2_ERROR_NONE) {
		return preflight;
	}
	auto &str = duckdb::StringValue::Get(*duckdb::ToValue(value));
	return duckdb::DecodeBignumStringT(duckdb::string_t(str), out_data, out_length, out_is_negative,
	                                   "duckdb_v2_value_get_bignum", err);
}

// ---------------------------------------------------------------------------
// VARIANT codec (wire encoding not committed; unwrap is the read path)
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_value_get_variant(duckdb_v2_value_handle value, duckdb_v2_value_handle *out_value,
                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_variant");
		}
		*out_value = nullptr;
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::VARIANT, "duckdb_v2_value_get_variant");
		// Engine-side decode of the uncommitted variant wire encoding.
		auto unwrapped = duckdb::VariantValue::GetValue(*duckdb::ToValue(value));
		*out_value = reinterpret_cast<_duckdb_v2_value *>(new duckdb::Value(std::move(unwrapped)));
	});
}

// ---------------------------------------------------------------------------
// TYPE values (a logical type carried as a value)
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_value_create_type(duckdb_v2_logical_type_handle type, duckdb_v2_value_handle *out_value,
                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!type || !out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_create_type");
		}
		*out_value = nullptr;
		// ANY is a signature wildcard; keep it out of value (and, via this path,
		// composite type) construction.
		if (duckdb::ToLogicalType(type)->id() == duckdb::LogicalTypeId::ANY) {
			throw duckdb::InvalidInputException("duckdb_v2_value_create_type: type cannot be ANY");
		}
		// Value::TYPE stores its own serialized copy of the borrowed type.
		auto *v = new duckdb::Value(duckdb::Value::TYPE(*duckdb::ToLogicalType(type)));
		*out_value = reinterpret_cast<_duckdb_v2_value *>(v);
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_type(duckdb_v2_value_handle value, duckdb_v2_logical_type_handle *out_type,
                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_type");
		}
		*out_type = nullptr;
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::TYPE, "duckdb_v2_value_get_type");
		// TypeValue::GetType deserializes the stored type into a fresh copy.
		auto *lt = new duckdb::LogicalType(duckdb::TypeValue::GetType(*duckdb::ToValue(value)));
		*out_type = reinterpret_cast<_duckdb_v2_logical_type *>(lt);
	});
}

// ---------------------------------------------------------------------------
// Composite construction + descent + cast
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_value_create(duckdb_v2_logical_type_handle type, const duckdb_v2_value_handle *children,
                                       idx_t child_count, duckdb_v2_value_handle *out_value,
                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!type || !out_value || (child_count > 0 && !children)) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_create");
		}
		*out_value = nullptr;
		duckdb::vector<duckdb::Value> vals;
		vals.reserve(child_count);
		for (idx_t i = 0; i < child_count; i++) {
			if (!children[i]) {
				throw duckdb::InvalidInputException("null child in duckdb_v2_value_create");
			}
			vals.push_back(*duckdb::ToValue(children[i]));
		}
		auto &lt = *duckdb::ToLogicalType(type);
		duckdb::Value v;
		// The engine constructors cast each child to the declared child/field
		// type (DefaultCastAs); cast failures propagate.
		switch (lt.id()) {
		case duckdb::LogicalTypeId::LIST:
			v = duckdb::Value::LIST(duckdb::ListType::GetChildType(lt), std::move(vals));
			break;
		case duckdb::LogicalTypeId::ARRAY:
			if (child_count != duckdb::ArrayType::GetSize(lt)) {
				throw duckdb::InvalidInputException(
				    "duckdb_v2_value_create: child count must equal the declared array size");
			}
			v = duckdb::Value::ARRAY(duckdb::ArrayType::GetChildType(lt), std::move(vals));
			break;
		case duckdb::LogicalTypeId::STRUCT:
		case duckdb::LogicalTypeId::TUPLE:
			if (child_count != duckdb::StructType::GetChildCount(lt)) {
				throw duckdb::InvalidInputException(
				    "duckdb_v2_value_create: child count must equal the declared field count");
			}
			v = duckdb::Value::STRUCT(lt, std::move(vals));
			break;
		case duckdb::LogicalTypeId::MAP: {
			if (child_count % 2 != 0) {
				throw duckdb::InvalidInputException(
				    "duckdb_v2_value_create: MAP children alternate key, value; count must be even");
			}
			duckdb::vector<duckdb::Value> keys;
			duckdb::vector<duckdb::Value> values;
			keys.reserve(child_count / 2);
			values.reserve(child_count / 2);
			for (idx_t i = 0; i < child_count; i += 2) {
				keys.push_back(std::move(vals[i]));
				values.push_back(std::move(vals[i + 1]));
			}
			v = duckdb::Value::MAP(duckdb::MapType::KeyType(lt), duckdb::MapType::ValueType(lt), std::move(keys),
			                       std::move(values));
			break;
		}
		default:
			throw duckdb::InvalidInputException("duckdb_v2_value_create builds LIST, ARRAY, STRUCT, and MAP values; "
			                                    "build UNION and ENUM values via duckdb_v2_value_cast_with_context "
			                                    "or duckdb_v2_value_cast_with_connection");
		}
		*out_value = reinterpret_cast<_duckdb_v2_value *>(new duckdb::Value(std::move(v)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_child_count(duckdb_v2_value_handle value, idx_t *out_count,
                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!value || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_child_count");
		}
		*out_count = duckdb::CompositeChildCount(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_child(duckdb_v2_value_handle value, idx_t index, duckdb_v2_value_handle *out_child,
                                          duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!value || !out_child) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_child");
		}
		*out_child = nullptr;
		auto &v = *duckdb::ToValue(value);
		if (index >= duckdb::CompositeChildCount(v)) {
			throw duckdb::InvalidInputException("child index out of range in duckdb_v2_value_get_child");
		}
		duckdb::Value child;
		switch (v.type().id()) {
		case duckdb::LogicalTypeId::LIST:
			child = duckdb::ListValue::GetChildren(v)[index];
			break;
		case duckdb::LogicalTypeId::ARRAY:
			child = duckdb::ArrayValue::GetChildren(v)[index];
			break;
		case duckdb::LogicalTypeId::STRUCT:
		case duckdb::LogicalTypeId::TUPLE:
			child = duckdb::StructValue::GetChildren(v)[index];
			break;
		case duckdb::LogicalTypeId::MAP: {
			// Entries are STRUCT(key, value) internally; surface them
			// alternating, symmetric with value_create.
			auto &entry = duckdb::MapValue::GetChildren(v)[index / 2];
			child = duckdb::StructValue::GetChildren(entry)[index % 2];
			break;
		}
		case duckdb::LogicalTypeId::UNION:
			child =
			    index == 0 ? duckdb::Value::UTINYINT(duckdb::UnionValue::GetTag(v)) : duckdb::UnionValue::GetValue(v);
			break;
		default:
			throw duckdb::InternalException("unreachable: bounds check rejects non-composites");
		}
		*out_child = reinterpret_cast<_duckdb_v2_value *>(new duckdb::Value(std::move(child)));
	});
}

static void CastValueV2(duckdb::ClientContext &ctx, duckdb_v2_value_handle value,
                        duckdb_v2_logical_type_handle target_type, duckdb_v2_value_handle *out_value) {
	if (!value || !target_type || !out_value) {
		throw duckdb::InvalidInputException("null argument to duckdb_v2_value_cast");
	}
	*out_value = nullptr;
	// Non-strict, through the context's cast function set (registered
	// custom casts included). Cast failures propagate.
	auto casted = duckdb::ToValue(value)->CastAs(ctx, *duckdb::ToLogicalType(target_type));
	*out_value = reinterpret_cast<_duckdb_v2_value *>(new duckdb::Value(std::move(casted)));
}

DUCKDB_V2_ERROR duckdb_v2_value_cast_with_connection(duckdb_v2_connection_handle conn, duckdb_v2_value_handle value,
                                                     duckdb_v2_logical_type_handle target_type,
                                                     duckdb_v2_value_handle *out_value,
                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn) {
			throw duckdb::InvalidInputException("Connection pointer cannot be null.");
		}
		auto &ctx = *duckdb::ToConn(conn)->context;
		ctx.RunFunctionInTransaction([&]() { CastValueV2(ctx, value, target_type, out_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_cast_with_context(duckdb_v2_context_handle ctx, duckdb_v2_value_handle value,
                                                  duckdb_v2_logical_type_handle target_type,
                                                  duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!ctx) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		CastValueV2(*duckdb::ToContext(ctx), value, target_type, out_value);
	});
}
