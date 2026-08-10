#include "capi_v2.hpp"

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

namespace duckdb::capiv2 {
namespace {

// Common preflight for typed-payload getters: handle must be non-null, value
// id must match, and the value must not be NULL. Throws InvalidInputException
// on any precondition failure so the outer WithErrorHandler routes it.
void RequireTypedValue(duckdb_v2_value_handle value, LogicalTypeId expected, const char *function_name) {
	if (!value) {
		throw InvalidInputException(std::string("null argument to ") + function_name);
	}
	auto *v = Convert(value);
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

// Shared body of the typed getters: gate the out-param and the value's type,
// then hand back the payload in the C layout.
template <class T, class FUNC>
void ReadTypedValue(duckdb_v2_value_handle value, T *out, LogicalTypeId expected, const char *function_name,
                    FUNC read) {
	if (!out) {
		throw InvalidInputException(std::string("null argument to ") + function_name);
	}
	RequireTypedValue(value, expected, function_name);
	*out = read(*Convert(value));
}

// Shared body of the typed constructors: gate the out-param, build the value,
// hand it over. The caller has already gated its context / connection handle.
template <class FUNC>
void EmitValue(duckdb_v2_value_handle *out_value, const char *function_name, FUNC make) {
	if (!out_value) {
		throw InvalidInputException(std::string("null argument to ") + function_name);
	}
	*out_value = nullptr;
	*out_value = Convert(new Value(make()));
}

// Scope gates for the typed constructors. A value's construction is scoped to
// a catalog, so the handle that supplies it is mandatory.
void RequireContext(duckdb_v2_context_handle ctx, const char *function_name) {
	if (!ctx) {
		throw InvalidInputException(std::string(function_name) + ": context pointer cannot be null");
	}
}

void RequireConnection(duckdb_v2_connection_handle conn, const char *function_name) {
	if (!conn) {
		throw InvalidInputException(std::string(function_name) + ": connection pointer cannot be null");
	}
}

// Nulls the slot up front so every failure path leaves it safe to read.
void RequireOutValue(duckdb_v2_value_handle *out_value, const char *function_name) {
	if (!out_value) {
		throw InvalidInputException(std::string("null argument to ") + function_name);
	}
	*out_value = nullptr;
}

// Gate for the constructors that take a type rather than a payload. ANY is a
// signature wildcard; a value carries data, so reject it.
void RequireValueType(duckdb_v2_logical_type_handle type, const char *function_name) {
	if (!type) {
		throw InvalidInputException(std::string("null argument to ") + function_name);
	}
	if (Convert(type)->id() == LogicalTypeId::ANY) {
		throw InvalidInputException(std::string(function_name) + ": type cannot be ANY");
	}
}

hugeint_t Convert(duckdb_v2_hugeint_t value) {
	return hugeint_t(value.upper, value.lower);
}

uhugeint_t Convert(duckdb_v2_uhugeint_t value) {
	return uhugeint_t(value.upper, value.lower);
}

duckdb_v2_hugeint_t Convert(hugeint_t value) {
	return duckdb_v2_hugeint_t {value.lower, value.upper};
}

duckdb_v2_uhugeint_t Convert(uhugeint_t value) {
	return duckdb_v2_uhugeint_t {value.lower, value.upper};
}

// A borrowed byte range as a std::string. A null pointer is only valid empty.
std::string ToString(duckdb_v2_str str, const char *function_name) {
	if (!str.ptr) {
		if (str.len > 0) {
			throw InvalidInputException(std::string("null argument to ") + function_name);
		}
		return std::string();
	}
	return std::string(str.ptr, str.len);
}

} // anonymous namespace
} // namespace duckdb::capiv2

// ---------------------------------------------------------------------------
// Lifecycle + NULL
// ---------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_value_destroy(duckdb_v2_value_handle *value) {
	return WithErrorHandler(nullptr, [&]() {
		if (!value) {
			return;
		}
		if (*value) {
			delete Convert(*value);
			*value = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_null(duckdb_v2_logical_type_handle type, duckdb_v2_value_handle *out_value,
                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!type || !out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_create_null");
		}
		*out_value = nullptr;
		// ANY is a signature wildcard; a value carries data, so reject it.
		if (Convert(type)->id() == duckdb::LogicalTypeId::ANY) {
			throw duckdb::InvalidInputException("duckdb_v2_value_create_null: type cannot be ANY");
		}
		// Value(LogicalType) constructs a typed NULL — exactly what we want.
		auto *v = new duckdb::Value(*Convert(type));
		*out_value = Convert(v);
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_is_null(duckdb_v2_value_handle value, bool *out_is_null,
                                        duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!value || !out_is_null) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_is_null");
		}
		*out_is_null = Convert(value)->IsNull();
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_logical_type(duckdb_v2_value_handle value, duckdb_v2_logical_type_handle *out_type,
                                                 duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!value || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_logical_type");
		}
		*out_type = nullptr;
		auto *lt = new duckdb::LogicalType(Convert(value)->type());
		*out_type = Convert(lt);
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_to_string(duckdb_v2_value_handle value, char *out_string, idx_t out_capacity,
                                          idx_t *out_length, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!value || !out_length) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_to_string");
		}
		*out_length = 0;
		FillCallerText(out_string, out_capacity, out_length, Convert(value)->ToString(), "duckdb_v2_value_to_string");
	});
}

// ---------------------------------------------------------------------------
// Typed accessors
//
// Each reads exactly its own type id: a getter is not a cast, so a mismatched
// value is refused rather than converted (value_cast is the converting path).
// NULL values have no payload and are refused too.
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_value_get_bool(duckdb_v2_value_handle value, bool *out, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::BOOLEAN, "duckdb_v2_value_get_bool",
		               [](const duckdb::Value &v) { return v.GetValueUnsafe<bool>(); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_tinyint(duckdb_v2_value_handle value, int8_t *out,
                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::TINYINT, "duckdb_v2_value_get_tinyint",
		               [](const duckdb::Value &v) { return v.GetValueUnsafe<int8_t>(); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_smallint(duckdb_v2_value_handle value, int16_t *out,
                                             duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::SMALLINT, "duckdb_v2_value_get_smallint",
		               [](const duckdb::Value &v) { return v.GetValueUnsafe<int16_t>(); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_int(duckdb_v2_value_handle value, int32_t *out, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::INTEGER, "duckdb_v2_value_get_int",
		               [](const duckdb::Value &v) { return v.GetValueUnsafe<int32_t>(); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_bigint(duckdb_v2_value_handle value, int64_t *out,
                                           duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::BIGINT, "duckdb_v2_value_get_bigint",
		               [](const duckdb::Value &v) { return v.GetValueUnsafe<int64_t>(); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_hugeint(duckdb_v2_value_handle value, duckdb_v2_hugeint_t *out,
                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::HUGEINT, "duckdb_v2_value_get_hugeint",
		               [](const duckdb::Value &v) { return Convert(v.GetValueUnsafe<duckdb::hugeint_t>()); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_utinyint(duckdb_v2_value_handle value, uint8_t *out,
                                             duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::UTINYINT, "duckdb_v2_value_get_utinyint",
		               [](const duckdb::Value &v) { return v.GetValueUnsafe<uint8_t>(); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_usmallint(duckdb_v2_value_handle value, uint16_t *out,
                                              duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::USMALLINT, "duckdb_v2_value_get_usmallint",
		               [](const duckdb::Value &v) { return v.GetValueUnsafe<uint16_t>(); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_uint(duckdb_v2_value_handle value, uint32_t *out,
                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::UINTEGER, "duckdb_v2_value_get_uint",
		               [](const duckdb::Value &v) { return v.GetValueUnsafe<uint32_t>(); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_ubigint(duckdb_v2_value_handle value, uint64_t *out,
                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::UBIGINT, "duckdb_v2_value_get_ubigint",
		               [](const duckdb::Value &v) { return v.GetValueUnsafe<uint64_t>(); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_uhugeint(duckdb_v2_value_handle value, duckdb_v2_uhugeint_t *out,
                                             duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::UHUGEINT, "duckdb_v2_value_get_uhugeint",
		               [](const duckdb::Value &v) { return Convert(v.GetValueUnsafe<duckdb::uhugeint_t>()); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_float(duckdb_v2_value_handle value, float *out, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::FLOAT, "duckdb_v2_value_get_float",
		               [](const duckdb::Value &v) { return v.GetValueUnsafe<float>(); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_double(duckdb_v2_value_handle value, double *out,
                                           duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::DOUBLE, "duckdb_v2_value_get_double",
		               [](const duckdb::Value &v) { return v.GetValueUnsafe<double>(); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_varchar(duckdb_v2_value_handle value, duckdb_v2_str *out,
                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		ReadTypedValue(value, out, duckdb::LogicalTypeId::VARCHAR, "duckdb_v2_value_get_varchar",
		               [](const duckdb::Value &v) { return Convert(duckdb::StringValue::Get(v)); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_blob(duckdb_v2_value_handle value, duckdb_v2_str *out,
                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!value || !out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_blob");
		}
		auto &v = *Convert(value);
		// The byte-string reader: BLOB plus the kinds whose payload is opaque
		// storage bytes (BIT's padding header + data, BIGNUM's bignum_decode
		// input). VARCHAR is text, and has its own getter.
		auto id = v.type().id();
		if (id != duckdb::LogicalTypeId::BLOB && id != duckdb::LogicalTypeId::BIT &&
		    id != duckdb::LogicalTypeId::BIGNUM) {
			throw duckdb::InvalidInputException("duckdb_v2_value_get_blob: value is not of the expected type");
		}
		if (v.IsNull()) {
			throw duckdb::InvalidInputException("duckdb_v2_value_get_blob: value is NULL");
		}
		*out = Convert(duckdb::StringValue::Get(v));
	});
}

// ---------------------------------------------------------------------------
// VARIANT codec (wire encoding not committed; unwrap is the read path)
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_value_get_variant(duckdb_v2_value_handle value, duckdb_v2_value_handle *out_value,
                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_variant");
		}
		*out_value = nullptr;
		RequireTypedValue(value, duckdb::LogicalTypeId::VARIANT, "duckdb_v2_value_get_variant");
		// Engine-side decode of the uncommitted variant wire encoding.
		auto unwrapped = duckdb::VariantValue::GetValue(*Convert(value));
		*out_value = Convert(new duckdb::Value(std::move(unwrapped)));
	});
}

// ---------------------------------------------------------------------------
// TYPE values (a logical type carried as a value)
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_value_get_type(duckdb_v2_value_handle value, duckdb_v2_logical_type_handle *out_type,
                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_type");
		}
		*out_type = nullptr;
		RequireTypedValue(value, duckdb::LogicalTypeId::TYPE, "duckdb_v2_value_get_type");
		// TypeValue::GetType deserializes the stored type into a fresh copy.
		auto *lt = new duckdb::LogicalType(duckdb::TypeValue::GetType(*Convert(value)));
		*out_type = Convert(lt);
	});
}

// ---------------------------------------------------------------------------
// Typed constructors
//
// Two forms per type: the context form for a live bind / execution context,
// the connection form for outside one. Both are gated on their handle, which
// is what makes a value's construction scoped to a catalog.
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_value_create_bool_from_context(duckdb_v2_context_handle ctx, bool in_value,
                                                         duckdb_v2_value_handle *out_value,
                                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_bool_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_bool_from_context",
		          [&]() { return duckdb::Value::BOOLEAN(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_bool_from_connection(duckdb_v2_connection_handle conn, bool in_value,
                                                            duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_bool_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_bool_from_connection",
		          [&]() { return duckdb::Value::BOOLEAN(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_tinyint_from_context(duckdb_v2_context_handle ctx, int8_t in_value,
                                                            duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_tinyint_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_tinyint_from_context",
		          [&]() { return duckdb::Value::TINYINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_tinyint_from_connection(duckdb_v2_connection_handle conn, int8_t in_value,
                                                               duckdb_v2_value_handle *out_value,
                                                               duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_tinyint_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_tinyint_from_connection",
		          [&]() { return duckdb::Value::TINYINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_smallint_from_context(duckdb_v2_context_handle ctx, int16_t in_value,
                                                             duckdb_v2_value_handle *out_value,
                                                             duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_smallint_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_smallint_from_context",
		          [&]() { return duckdb::Value::SMALLINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_smallint_from_connection(duckdb_v2_connection_handle conn, int16_t in_value,
                                                                duckdb_v2_value_handle *out_value,
                                                                duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_smallint_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_smallint_from_connection",
		          [&]() { return duckdb::Value::SMALLINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_int_from_context(duckdb_v2_context_handle ctx, int32_t in_value,
                                                        duckdb_v2_value_handle *out_value,
                                                        duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_int_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_int_from_context",
		          [&]() { return duckdb::Value::INTEGER(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_int_from_connection(duckdb_v2_connection_handle conn, int32_t in_value,
                                                           duckdb_v2_value_handle *out_value,
                                                           duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_int_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_int_from_connection",
		          [&]() { return duckdb::Value::INTEGER(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_bigint_from_context(duckdb_v2_context_handle ctx, int64_t in_value,
                                                           duckdb_v2_value_handle *out_value,
                                                           duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_bigint_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_bigint_from_context",
		          [&]() { return duckdb::Value::BIGINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_bigint_from_connection(duckdb_v2_connection_handle conn, int64_t in_value,
                                                              duckdb_v2_value_handle *out_value,
                                                              duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_bigint_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_bigint_from_connection",
		          [&]() { return duckdb::Value::BIGINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_hugeint_from_context(duckdb_v2_context_handle ctx, duckdb_v2_hugeint_t in_value,
                                                            duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_hugeint_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_hugeint_from_context",
		          [&]() { return duckdb::Value::HUGEINT(Convert(in_value)); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_hugeint_from_connection(duckdb_v2_connection_handle conn,
                                                               duckdb_v2_hugeint_t in_value,
                                                               duckdb_v2_value_handle *out_value,
                                                               duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_hugeint_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_hugeint_from_connection",
		          [&]() { return duckdb::Value::HUGEINT(Convert(in_value)); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_utinyint_from_context(duckdb_v2_context_handle ctx, uint8_t in_value,
                                                             duckdb_v2_value_handle *out_value,
                                                             duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_utinyint_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_utinyint_from_context",
		          [&]() { return duckdb::Value::UTINYINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_utinyint_from_connection(duckdb_v2_connection_handle conn, uint8_t in_value,
                                                                duckdb_v2_value_handle *out_value,
                                                                duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_utinyint_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_utinyint_from_connection",
		          [&]() { return duckdb::Value::UTINYINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_usmallint_from_context(duckdb_v2_context_handle ctx, uint16_t in_value,
                                                              duckdb_v2_value_handle *out_value,
                                                              duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_usmallint_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_usmallint_from_context",
		          [&]() { return duckdb::Value::USMALLINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_usmallint_from_connection(duckdb_v2_connection_handle conn, uint16_t in_value,
                                                                 duckdb_v2_value_handle *out_value,
                                                                 duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_usmallint_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_usmallint_from_connection",
		          [&]() { return duckdb::Value::USMALLINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_uint_from_context(duckdb_v2_context_handle ctx, uint32_t in_value,
                                                         duckdb_v2_value_handle *out_value,
                                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_uint_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_uint_from_context",
		          [&]() { return duckdb::Value::UINTEGER(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_uint_from_connection(duckdb_v2_connection_handle conn, uint32_t in_value,
                                                            duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_uint_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_uint_from_connection",
		          [&]() { return duckdb::Value::UINTEGER(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_ubigint_from_context(duckdb_v2_context_handle ctx, uint64_t in_value,
                                                            duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_ubigint_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_ubigint_from_context",
		          [&]() { return duckdb::Value::UBIGINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_ubigint_from_connection(duckdb_v2_connection_handle conn, uint64_t in_value,
                                                               duckdb_v2_value_handle *out_value,
                                                               duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_ubigint_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_ubigint_from_connection",
		          [&]() { return duckdb::Value::UBIGINT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_uhugeint_from_context(duckdb_v2_context_handle ctx,
                                                             duckdb_v2_uhugeint_t in_value,
                                                             duckdb_v2_value_handle *out_value,
                                                             duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_uhugeint_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_uhugeint_from_context",
		          [&]() { return duckdb::Value::UHUGEINT(Convert(in_value)); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_uhugeint_from_connection(duckdb_v2_connection_handle conn,
                                                                duckdb_v2_uhugeint_t in_value,
                                                                duckdb_v2_value_handle *out_value,
                                                                duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_uhugeint_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_uhugeint_from_connection",
		          [&]() { return duckdb::Value::UHUGEINT(Convert(in_value)); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_float_from_context(duckdb_v2_context_handle ctx, float in_value,
                                                          duckdb_v2_value_handle *out_value,
                                                          duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_float_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_float_from_context",
		          [&]() { return duckdb::Value::FLOAT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_float_from_connection(duckdb_v2_connection_handle conn, float in_value,
                                                             duckdb_v2_value_handle *out_value,
                                                             duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_float_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_float_from_connection",
		          [&]() { return duckdb::Value::FLOAT(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_double_from_context(duckdb_v2_context_handle ctx, double in_value,
                                                           duckdb_v2_value_handle *out_value,
                                                           duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_double_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_double_from_context",
		          [&]() { return duckdb::Value::DOUBLE(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_double_from_connection(duckdb_v2_connection_handle conn, double in_value,
                                                              duckdb_v2_value_handle *out_value,
                                                              duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_double_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_double_from_connection",
		          [&]() { return duckdb::Value::DOUBLE(in_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_varchar_from_context(duckdb_v2_context_handle ctx, duckdb_v2_str in_value,
                                                            duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_varchar_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_varchar_from_context",
		          [&]() { return duckdb::Value(ToString(in_value, "duckdb_v2_value_create_varchar_from_context")); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_varchar_from_connection(duckdb_v2_connection_handle conn, duckdb_v2_str in_value,
                                                               duckdb_v2_value_handle *out_value,
                                                               duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_varchar_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_varchar_from_connection", [&]() {
			return duckdb::Value(ToString(in_value, "duckdb_v2_value_create_varchar_from_connection"));
		});
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_blob_from_context(duckdb_v2_context_handle ctx, duckdb_v2_str in_value,
                                                         duckdb_v2_value_handle *out_value,
                                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_blob_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_blob_from_context", [&]() {
			return duckdb::Value::BLOB_RAW(ToString(in_value, "duckdb_v2_value_create_blob_from_context"));
		});
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_blob_from_connection(duckdb_v2_connection_handle conn, duckdb_v2_str in_value,
                                                            duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_blob_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_blob_from_connection", [&]() {
			return duckdb::Value::BLOB_RAW(ToString(in_value, "duckdb_v2_value_create_blob_from_connection"));
		});
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_null_from_context(duckdb_v2_context_handle ctx,
                                                         duckdb_v2_logical_type_handle type,
                                                         duckdb_v2_value_handle *out_value,
                                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_null_from_context");
		RequireValueType(type, "duckdb_v2_value_create_null_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_null_from_context",
		          [&]() { return duckdb::Value(*Convert(type)); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_null_from_connection(duckdb_v2_connection_handle conn,
                                                            duckdb_v2_logical_type_handle type,
                                                            duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_null_from_connection");
		RequireValueType(type, "duckdb_v2_value_create_null_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_null_from_connection",
		          [&]() { return duckdb::Value(*Convert(type)); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_type_from_context(duckdb_v2_context_handle ctx,
                                                         duckdb_v2_logical_type_handle in_type,
                                                         duckdb_v2_value_handle *out_value,
                                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_type_from_context");
		RequireValueType(in_type, "duckdb_v2_value_create_type_from_context");
		EmitValue(out_value, "duckdb_v2_value_create_type_from_context",
		          [&]() { return duckdb::Value::TYPE(*Convert(in_type)); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_type_from_connection(duckdb_v2_connection_handle conn,
                                                            duckdb_v2_logical_type_handle in_type,
                                                            duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_type_from_connection");
		RequireValueType(in_type, "duckdb_v2_value_create_type_from_connection");
		EmitValue(out_value, "duckdb_v2_value_create_type_from_connection",
		          [&]() { return duckdb::Value::TYPE(*Convert(in_type)); });
	});
}

// ---------------------------------------------------------------------------
// Composite construction + descent + cast
// ---------------------------------------------------------------------------

// The children cross as borrowed handles; every composite copies them in.
namespace {

duckdb::vector<duckdb::Value> CollectChildren(const duckdb_v2_value_handle *children, idx_t count,
                                              const char *function_name) {
	if (count > 0 && !children) {
		throw duckdb::InvalidInputException(std::string("null argument to ") + function_name);
	}
	duckdb::vector<duckdb::Value> values;
	values.reserve(count);
	for (idx_t i = 0; i < count; i++) {
		if (!children[i]) {
			throw duckdb::InvalidInputException(std::string("null child in ") + function_name);
		}
		values.push_back(*Convert(children[i]));
	}
	return values;
}

// The common type of a set of children, the same rule a list literal follows.
// An explicit type wins outright; without one an empty set has nothing to
// resolve, which is what makes the empty forms take their type.
duckdb::LogicalType ResolveChildType(duckdb::ClientContext &ctx, duckdb_v2_logical_type_handle declared,
                                     const duckdb::vector<duckdb::Value> &children, const char *what,
                                     const char *function_name) {
	if (declared) {
		return *Convert(declared);
	}
	if (children.empty()) {
		throw duckdb::InvalidInputException(std::string(function_name) + ": cannot resolve the " + what +
		                                    " of an empty set; pass it explicitly");
	}
	auto type = children[0].type();
	for (idx_t i = 1; i < children.size(); i++) {
		type = duckdb::LogicalType::MaxLogicalType(ctx, type, children[i].type());
	}
	return type;
}

void EmitComposite(duckdb_v2_value_handle *out_value, duckdb::Value value) {
	*out_value = Convert(new duckdb::Value(std::move(value)));
}

duckdb::Value BuildList(duckdb::ClientContext &ctx, duckdb_v2_logical_type_handle child_type,
                        const duckdb_v2_value_handle *children, idx_t child_count, const char *function_name) {
	auto values = CollectChildren(children, child_count, function_name);
	auto type = ResolveChildType(ctx, child_type, values, "element type", function_name);
	return duckdb::Value::LIST(type, std::move(values));
}

duckdb::Value BuildArray(duckdb::ClientContext &ctx, duckdb_v2_logical_type_handle child_type,
                         const duckdb_v2_value_handle *children, idx_t child_count, const char *function_name) {
	auto values = CollectChildren(children, child_count, function_name);
	// The engine's minimum array size is 1, so there is no empty ARRAY, with or
	// without a declared element type.
	if (values.empty()) {
		throw duckdb::InvalidInputException(std::string(function_name) + ": an ARRAY must have at least one element");
	}
	auto type = ResolveChildType(ctx, child_type, values, "element type", function_name);
	return duckdb::Value::ARRAY(type, std::move(values));
}

duckdb::Value BuildStruct(const duckdb_v2_identifier_t *names, const duckdb_v2_value_handle *children,
                          idx_t field_count, const char *function_name) {
	if (field_count > 0 && !names) {
		throw duckdb::InvalidInputException(std::string("null argument to ") + function_name);
	}
	auto values = CollectChildren(children, field_count, function_name);
	// Each field carries its own child's type, so the type is assembled here
	// rather than resolved across the fields.
	duckdb::child_list_t<duckdb::Value> fields;
	fields.reserve(field_count);
	for (idx_t i = 0; i < field_count; i++) {
		if (!names[i].ptr && names[i].len > 0) {
			throw duckdb::InvalidInputException(std::string("null field name in ") + function_name);
		}
		fields.emplace_back(std::string(names[i].ptr ? names[i].ptr : "", names[i].len), std::move(values[i]));
	}
	return duckdb::Value::STRUCT(std::move(fields));
}

duckdb::Value BuildTuple(const duckdb_v2_value_handle *children, idx_t field_count, const char *function_name) {
	auto values = CollectChildren(children, field_count, function_name);
	duckdb::vector<duckdb::LogicalType> types;
	types.reserve(field_count);
	for (auto &value : values) {
		types.push_back(value.type());
	}
	return duckdb::Value::STRUCT(duckdb::LogicalType::TUPLE(std::move(types)), std::move(values));
}

duckdb::Value BuildMap(duckdb::ClientContext &ctx, duckdb_v2_logical_type_handle key_type,
                       duckdb_v2_logical_type_handle value_type, const duckdb_v2_value_handle *keys,
                       const duckdb_v2_value_handle *values, idx_t entry_count, const char *function_name) {
	auto key_values = CollectChildren(keys, entry_count, function_name);
	auto value_values = CollectChildren(values, entry_count, function_name);
	auto resolved_key = ResolveChildType(ctx, key_type, key_values, "key type", function_name);
	auto resolved_value = ResolveChildType(ctx, value_type, value_values, "value type", function_name);
	return duckdb::Value::MAP(resolved_key, resolved_value, std::move(key_values), std::move(value_values));
}

} // namespace

DUCKDB_V2_ERROR duckdb_v2_value_create_list_from_context(duckdb_v2_context_handle ctx,
                                                         duckdb_v2_logical_type_handle child_type,
                                                         const duckdb_v2_value_handle *children, idx_t child_count,
                                                         duckdb_v2_value_handle *out_value,
                                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_list_from_context");
		RequireOutValue(out_value, "duckdb_v2_value_create_list_from_context");
		EmitComposite(out_value, BuildList(*Convert(ctx), child_type, children, child_count,
		                                   "duckdb_v2_value_create_list_from_context"));
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_list_from_connection(duckdb_v2_connection_handle conn,
                                                            duckdb_v2_logical_type_handle child_type,
                                                            const duckdb_v2_value_handle *children, idx_t child_count,
                                                            duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_list_from_connection");
		RequireOutValue(out_value, "duckdb_v2_value_create_list_from_connection");
		auto &ctx = *Convert(conn)->context;
		ctx.RunFunctionInTransaction([&]() {
			EmitComposite(out_value, BuildList(ctx, child_type, children, child_count,
			                                   "duckdb_v2_value_create_list_from_connection"));
		});
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_array_from_context(duckdb_v2_context_handle ctx,
                                                          duckdb_v2_logical_type_handle child_type,
                                                          const duckdb_v2_value_handle *children, idx_t child_count,
                                                          duckdb_v2_value_handle *out_value,
                                                          duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_array_from_context");
		RequireOutValue(out_value, "duckdb_v2_value_create_array_from_context");
		EmitComposite(out_value, BuildArray(*Convert(ctx), child_type, children, child_count,
		                                    "duckdb_v2_value_create_array_from_context"));
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_array_from_connection(duckdb_v2_connection_handle conn,
                                                             duckdb_v2_logical_type_handle child_type,
                                                             const duckdb_v2_value_handle *children, idx_t child_count,
                                                             duckdb_v2_value_handle *out_value,
                                                             duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_array_from_connection");
		RequireOutValue(out_value, "duckdb_v2_value_create_array_from_connection");
		auto &ctx = *Convert(conn)->context;
		ctx.RunFunctionInTransaction([&]() {
			EmitComposite(out_value, BuildArray(ctx, child_type, children, child_count,
			                                    "duckdb_v2_value_create_array_from_connection"));
		});
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_struct_from_context(duckdb_v2_context_handle ctx,
                                                           const duckdb_v2_identifier_t *names,
                                                           const duckdb_v2_value_handle *children, idx_t field_count,
                                                           duckdb_v2_value_handle *out_value,
                                                           duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_struct_from_context");
		RequireOutValue(out_value, "duckdb_v2_value_create_struct_from_context");
		EmitComposite(out_value,
		              BuildStruct(names, children, field_count, "duckdb_v2_value_create_struct_from_context"));
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_struct_from_connection(duckdb_v2_connection_handle conn,
                                                              const duckdb_v2_identifier_t *names,
                                                              const duckdb_v2_value_handle *children, idx_t field_count,
                                                              duckdb_v2_value_handle *out_value,
                                                              duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_struct_from_connection");
		RequireOutValue(out_value, "duckdb_v2_value_create_struct_from_connection");
		EmitComposite(out_value,
		              BuildStruct(names, children, field_count, "duckdb_v2_value_create_struct_from_connection"));
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_tuple_from_context(duckdb_v2_context_handle ctx,
                                                          const duckdb_v2_value_handle *children, idx_t field_count,
                                                          duckdb_v2_value_handle *out_value,
                                                          duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_tuple_from_context");
		RequireOutValue(out_value, "duckdb_v2_value_create_tuple_from_context");
		EmitComposite(out_value, BuildTuple(children, field_count, "duckdb_v2_value_create_tuple_from_context"));
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_create_tuple_from_connection(duckdb_v2_connection_handle conn,
                                                             const duckdb_v2_value_handle *children, idx_t field_count,
                                                             duckdb_v2_value_handle *out_value,
                                                             duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_tuple_from_connection");
		RequireOutValue(out_value, "duckdb_v2_value_create_tuple_from_connection");
		EmitComposite(out_value, BuildTuple(children, field_count, "duckdb_v2_value_create_tuple_from_connection"));
	});
}

DUCKDB_V2_ERROR
duckdb_v2_value_create_map_from_context(duckdb_v2_context_handle ctx, duckdb_v2_logical_type_handle key_type,
                                        duckdb_v2_logical_type_handle value_type, const duckdb_v2_value_handle *keys,
                                        const duckdb_v2_value_handle *values, idx_t entry_count,
                                        duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireContext(ctx, "duckdb_v2_value_create_map_from_context");
		RequireOutValue(out_value, "duckdb_v2_value_create_map_from_context");
		EmitComposite(out_value, BuildMap(*Convert(ctx), key_type, value_type, keys, values, entry_count,
		                                  "duckdb_v2_value_create_map_from_context"));
	});
}

DUCKDB_V2_ERROR
duckdb_v2_value_create_map_from_connection(duckdb_v2_connection_handle conn, duckdb_v2_logical_type_handle key_type,
                                           duckdb_v2_logical_type_handle value_type, const duckdb_v2_value_handle *keys,
                                           const duckdb_v2_value_handle *values, idx_t entry_count,
                                           duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		RequireConnection(conn, "duckdb_v2_value_create_map_from_connection");
		RequireOutValue(out_value, "duckdb_v2_value_create_map_from_connection");
		auto &ctx = *Convert(conn)->context;
		ctx.RunFunctionInTransaction([&]() {
			EmitComposite(out_value, BuildMap(ctx, key_type, value_type, keys, values, entry_count,
			                                  "duckdb_v2_value_create_map_from_connection"));
		});
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_child_count(duckdb_v2_value_handle value, idx_t *out_count,
                                                duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!value || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_child_count");
		}
		*out_count = CompositeChildCount(*Convert(value));
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_get_child(duckdb_v2_value_handle value, idx_t index, duckdb_v2_value_handle *out_child,
                                          duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!value || !out_child) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_child");
		}
		*out_child = nullptr;
		auto &v = *Convert(value);
		if (index >= CompositeChildCount(v)) {
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
		*out_child = Convert(new duckdb::Value(std::move(child)));
	});
}

// TODO: dont make static
static void CastValueV2(duckdb::ClientContext &ctx, duckdb_v2_value_handle value,
                        duckdb_v2_logical_type_handle target_type, duckdb_v2_value_handle *out_value) {
	if (!value || !target_type || !out_value) {
		throw duckdb::InvalidInputException("null argument to duckdb_v2_value_cast");
	}
	*out_value = nullptr;
	// Non-strict, through the context's cast function set (registered
	// custom casts included). Cast failures propagate.
	auto casted = Convert(value)->CastAs(ctx, *Convert(target_type));
	*out_value = Convert(new duckdb::Value(std::move(casted)));
}

DUCKDB_V2_ERROR duckdb_v2_value_cast_with_connection(duckdb_v2_connection_handle conn, duckdb_v2_value_handle value,
                                                     duckdb_v2_logical_type_handle target_type,
                                                     duckdb_v2_value_handle *out_value,
                                                     duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!conn) {
			throw duckdb::InvalidInputException("Connection pointer cannot be null.");
		}
		auto &ctx = *Convert(conn)->context;
		ctx.RunFunctionInTransaction([&]() { CastValueV2(ctx, value, target_type, out_value); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_value_cast_with_context(duckdb_v2_context_handle ctx, duckdb_v2_value_handle value,
                                                  duckdb_v2_logical_type_handle target_type,
                                                  duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!ctx) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		CastValueV2(*Convert(ctx), value, target_type, out_value);
	});
}
