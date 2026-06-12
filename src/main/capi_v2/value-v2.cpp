#include "capi_v2_internal.hpp"

#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/uuid.hpp"

#include <cstdlib>
#include <cstring>

// Out-param zeroing on failure:
//   - Pointer out-params (out_value, out_type, out_data) are set to nullptr
//     on every INVALID_INPUT path to keep dangling-pointer hazards out of
//     caller code.
//   - Scalar out-params (out_micros, out_lower, out_width, ...) are
//     unspecified on failure. Callers must check the return code before
//     reading scalars.

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

// Allocating constructor used by every primitive create_*: wraps `new Value(...)`
// in WithErrorHandler and handles the null-out-param check uniformly.
template <class Make>
DUCKDB_V2_API_CALL_t MakePrimitive(duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err,
                                   const char *function_name, Make make) {
	return WithErrorHandler(err, [&]() {
		if (!out_value) {
			throw InvalidInputException(std::string("null argument to ") + function_name);
		}
		*out_value = nullptr;
		auto *v = new Value(make());
		*out_value = reinterpret_cast<_duckdb_v2_value *>(v);
	});
}

// Borrowed-bytes getter for the string-backed types (VARCHAR/BLOB/BIT). The
// returned pointer is into the Value's StringValueInfo and stays valid until
// the value is destroyed.
template <class CharT>
DUCKDB_V2_API_CALL_t GetStringBytes(duckdb_v2_value_handle value, LogicalTypeId expected, const char *function_name,
                                    const CharT **out_data, idx_t *out_length, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!out_data || !out_length) {
			throw InvalidInputException(std::string("null argument to ") + function_name);
		}
		*out_data = nullptr;
		*out_length = 0;
		RequireTypedValue(value, expected, function_name);
		auto &str = StringValue::Get(*ToValue(value));
		*out_data = reinterpret_cast<const CharT *>(str.data());
		*out_length = str.size();
	});
}

} // anonymous namespace
} // namespace duckdb

// ---------------------------------------------------------------------------
// Lifecycle + NULL
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_destroy(duckdb_v2_value_handle *value) {
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

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_null(duckdb_v2_logical_type_handle type, duckdb_v2_value_handle *out_value,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!type || !out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_create_null");
		}
		*out_value = nullptr;
		// Value(LogicalType) constructs a typed NULL — exactly what we want.
		auto *v = new duckdb::Value(*duckdb::ToLogicalType(type));
		*out_value = reinterpret_cast<_duckdb_v2_value *>(v);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_is_null(duckdb_v2_value_handle value, bool *out_is_null,
                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!value || !out_is_null) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_is_null");
		}
		*out_is_null = duckdb::ToValue(value)->IsNull();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_logical_type(duckdb_v2_value_handle value,
                                                      duckdb_v2_logical_type_handle *out_type,
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

DUCKDB_V2_API_CALL_t duckdb_v2_value_to_string(duckdb_v2_value_handle value, char **out_string,
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
// Primitive numeric constructors (mechanical: dispatch through Value::TYPE())
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_bool(bool input, duckdb_v2_value_handle *out_value,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_bool",
	                             [input]() { return duckdb::Value::BOOLEAN(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_int8(int8_t input, duckdb_v2_value_handle *out_value,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_int8",
	                             [input]() { return duckdb::Value::TINYINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_int16(int16_t input, duckdb_v2_value_handle *out_value,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_int16",
	                             [input]() { return duckdb::Value::SMALLINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_int32(int32_t input, duckdb_v2_value_handle *out_value,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_int32",
	                             [input]() { return duckdb::Value::INTEGER(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_int64(int64_t input, duckdb_v2_value_handle *out_value,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_int64",
	                             [input]() { return duckdb::Value::BIGINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uint8(uint8_t input, duckdb_v2_value_handle *out_value,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uint8",
	                             [input]() { return duckdb::Value::UTINYINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uint16(uint16_t input, duckdb_v2_value_handle *out_value,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uint16",
	                             [input]() { return duckdb::Value::USMALLINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uint32(uint32_t input, duckdb_v2_value_handle *out_value,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uint32",
	                             [input]() { return duckdb::Value::UINTEGER(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uint64(uint64_t input, duckdb_v2_value_handle *out_value,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uint64",
	                             [input]() { return duckdb::Value::UBIGINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_hugeint(uint64_t lower, int64_t upper, duckdb_v2_value_handle *out_value,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_hugeint",
	                             [lower, upper]() { return duckdb::Value::HUGEINT(duckdb::hugeint_t(upper, lower)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uhugeint(uint64_t lower, uint64_t upper, duckdb_v2_value_handle *out_value,
                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uhugeint", [lower, upper]() {
		return duckdb::Value::UHUGEINT(duckdb::uhugeint_t(upper, lower));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_float(float input, duckdb_v2_value_handle *out_value,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_float",
	                             [input]() { return duckdb::Value::FLOAT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_double(double input, duckdb_v2_value_handle *out_value,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_double",
	                             [input]() { return duckdb::Value::DOUBLE(input); });
}

// ---------------------------------------------------------------------------
// VARCHAR / BLOB / BIT / BIGNUM constructors
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_varchar(duckdb_v2_str data, duckdb_v2_value_handle *out_value,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_create_varchar");
		}
		*out_value = nullptr;
		if (!data.ptr && data.len > 0) {
			throw duckdb::InvalidInputException("null data with positive length in duckdb_v2_value_create_varchar");
		}
		// Only run UTF-8 validation when there is something to validate.
		if (data.len > 0 && !duckdb::Value::StringIsValid(data.ptr, data.len)) {
			throw duckdb::InvalidInputException("invalid UTF-8 in duckdb_v2_value_create_varchar");
		}
		// len=0 + ptr=NULL is the documented empty-value shape (see spec).
		auto *v = new duckdb::Value(duckdb::ToString(data));
		*out_value = reinterpret_cast<_duckdb_v2_value *>(v);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_blob(const uint8_t *data, idx_t length, duckdb_v2_value_handle *out_value,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_create_blob");
		}
		*out_value = nullptr;
		if (!data && length > 0) {
			throw duckdb::InvalidInputException("null data with positive length in duckdb_v2_value_create_blob");
		}
		auto *v = new duckdb::Value(duckdb::Value::BLOB(data, length));
		*out_value = reinterpret_cast<_duckdb_v2_value *>(v);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_bit(const uint8_t *data, idx_t length, duckdb_v2_value_handle *out_value,
                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_create_bit");
		}
		*out_value = nullptr;
		// BIT has no empty encoding: the padding header byte is mandatory.
		if (!data || length == 0) {
			throw duckdb::InvalidInputException("duckdb_v2_value_create_bit requires data != NULL and length >= 1");
		}
		auto *v = new duckdb::Value(duckdb::Value::BIT(data, length));
		*out_value = reinterpret_cast<_duckdb_v2_value *>(v);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_bignum(const uint8_t *data, idx_t length, bool is_negative,
                                                   duckdb_v2_value_handle *out_value,
                                                   duckdb_v2_error_info_handle *err) {
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

// ---------------------------------------------------------------------------
// Date / time / timestamp / interval constructors
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_date(int32_t days, duckdb_v2_value_handle *out_value,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_date",
	                             [days]() { return duckdb::Value::DATE(duckdb::date_t(days)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_time(int64_t micros, duckdb_v2_value_handle *out_value,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_time",
	                             [micros]() { return duckdb::Value::TIME(duckdb::dtime_t(micros)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_time_ns(int64_t nanos, duckdb_v2_value_handle *out_value,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_time_ns",
	                             [nanos]() { return duckdb::Value::TIME_NS(duckdb::dtime_ns_t(nanos)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_time_tz(int64_t micros, int32_t offset_seconds,
                                                    duckdb_v2_value_handle *out_value,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_time_tz", [micros, offset_seconds]() {
		return duckdb::Value::TIMETZ(duckdb::dtime_tz_t(duckdb::dtime_t(micros), offset_seconds));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp(int64_t micros, duckdb_v2_value_handle *out_value,
                                                      duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp",
	                             [micros]() { return duckdb::Value::TIMESTAMP(duckdb::timestamp_t(micros)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp_sec(int64_t seconds, duckdb_v2_value_handle *out_value,
                                                          duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp_sec",
	                             [seconds]() { return duckdb::Value::TIMESTAMPSEC(duckdb::timestamp_sec_t(seconds)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp_ms(int64_t millis, duckdb_v2_value_handle *out_value,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp_ms",
	                             [millis]() { return duckdb::Value::TIMESTAMPMS(duckdb::timestamp_ms_t(millis)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp_ns(int64_t nanos, duckdb_v2_value_handle *out_value,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp_ns",
	                             [nanos]() { return duckdb::Value::TIMESTAMPNS(duckdb::timestamp_ns_t(nanos)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp_tz(int64_t micros, duckdb_v2_value_handle *out_value,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp_tz",
	                             [micros]() { return duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t(micros)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp_tz_ns(int64_t nanos, duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp_tz_ns",
	                             [nanos]() { return duckdb::Value::TIMESTAMPTZNS(duckdb::timestamp_tz_ns_t(nanos)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_interval(int32_t months, int32_t days, int64_t micros,
                                                     duckdb_v2_value_handle *out_value,
                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_interval",
	                             [months, days, micros]() { return duckdb::Value::INTERVAL(months, days, micros); });
}

// ---------------------------------------------------------------------------
// DECIMAL / UUID constructors
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_decimal(uint64_t lower, int64_t upper, uint8_t width, uint8_t scale,
                                                    duckdb_v2_value_handle *out_value,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_create_decimal");
		}
		*out_value = nullptr;
		// Dispatch on width, not on whether the payload fits in int64. Width
		// determines physical storage in core (SMALLINT for width<=4, INTEGER
		// <=9, BIGINT <=18, HUGEINT >=19), and a (width=38, payload=5) caller
		// expects HUGEINT physical even though 5 fits int64. The int64 ctor
		// goes up to BIGINT physical only; the hugeint ctor is the only one
		// that produces HUGEINT physical regardless of payload magnitude.
		constexpr uint8_t MAX_WIDTH_INT64 = 18;
		duckdb::hugeint_t hi(upper, lower);
		duckdb::Value v;
		if (width <= MAX_WIDTH_INT64) {
			int64_t fit = 0;
			if (!duckdb::Hugeint::TryCast<int64_t>(hi, fit)) {
				throw duckdb::InvalidInputException("decimal payload does not fit the chosen width");
			}
			v = duckdb::Value::DECIMAL(fit, width, scale);
		} else {
			v = duckdb::Value::DECIMAL(hi, width, scale);
		}
		*out_value = reinterpret_cast<_duckdb_v2_value *>(new duckdb::Value(std::move(v)));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uuid(uint64_t lower, uint64_t upper, duckdb_v2_value_handle *out_value,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uuid", [lower, upper]() {
		return duckdb::Value::UUID(duckdb::UUID::FromUHugeint(duckdb::uhugeint_t(upper, lower)));
	});
}

// ---------------------------------------------------------------------------
// Primitive getters
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_bool(duckdb_v2_value_handle value, bool *out,
                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_bool");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::BOOLEAN, "duckdb_v2_value_get_bool");
		*out = duckdb::BooleanValue::Get(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_int8(duckdb_v2_value_handle value, int8_t *out,
                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_int8");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::TINYINT, "duckdb_v2_value_get_int8");
		*out = duckdb::TinyIntValue::Get(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_int16(duckdb_v2_value_handle value, int16_t *out,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_int16");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::SMALLINT, "duckdb_v2_value_get_int16");
		*out = duckdb::SmallIntValue::Get(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_int32(duckdb_v2_value_handle value, int32_t *out,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_int32");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::INTEGER, "duckdb_v2_value_get_int32");
		*out = duckdb::IntegerValue::Get(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_int64(duckdb_v2_value_handle value, int64_t *out,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_int64");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::BIGINT, "duckdb_v2_value_get_int64");
		*out = duckdb::BigIntValue::Get(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uint8(duckdb_v2_value_handle value, uint8_t *out,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_uint8");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::UTINYINT, "duckdb_v2_value_get_uint8");
		*out = duckdb::UTinyIntValue::Get(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uint16(duckdb_v2_value_handle value, uint16_t *out,
                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_uint16");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::USMALLINT, "duckdb_v2_value_get_uint16");
		*out = duckdb::USmallIntValue::Get(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uint32(duckdb_v2_value_handle value, uint32_t *out,
                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_uint32");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::UINTEGER, "duckdb_v2_value_get_uint32");
		*out = duckdb::UIntegerValue::Get(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uint64(duckdb_v2_value_handle value, uint64_t *out,
                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_uint64");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::UBIGINT, "duckdb_v2_value_get_uint64");
		*out = duckdb::UBigIntValue::Get(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_float(duckdb_v2_value_handle value, float *out,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_float");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::FLOAT, "duckdb_v2_value_get_float");
		*out = duckdb::FloatValue::Get(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_double(duckdb_v2_value_handle value, double *out,
                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_double");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::DOUBLE, "duckdb_v2_value_get_double");
		*out = duckdb::DoubleValue::Get(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_hugeint(duckdb_v2_value_handle value, uint64_t *out_lower, int64_t *out_upper,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_lower || !out_upper) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_hugeint");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::HUGEINT, "duckdb_v2_value_get_hugeint");
		auto hi = duckdb::HugeIntValue::Get(*duckdb::ToValue(value));
		*out_lower = hi.lower;
		*out_upper = hi.upper;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uhugeint(duckdb_v2_value_handle value, uint64_t *out_lower,
                                                  uint64_t *out_upper, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_lower || !out_upper) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_uhugeint");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::UHUGEINT, "duckdb_v2_value_get_uhugeint");
		auto uhi = duckdb::UhugeIntValue::Get(*duckdb::ToValue(value));
		*out_lower = uhi.lower;
		*out_upper = uhi.upper;
	});
}

// ---------------------------------------------------------------------------
// VARCHAR / BLOB / BIT getters (borrowed) and BIGNUM getter (owned)
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_varchar(duckdb_v2_value_handle value, duckdb_v2_str *out_data,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_data) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_varchar");
		}
		*out_data = duckdb_v2_str {nullptr, 0};
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::VARCHAR, "duckdb_v2_value_get_varchar");
		*out_data = duckdb::ToStr(duckdb::StringValue::Get(*duckdb::ToValue(value)));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_blob(duckdb_v2_value_handle value, const uint8_t **out_data, idx_t *out_length,
                                              duckdb_v2_error_info_handle *err) {
	return duckdb::GetStringBytes(value, duckdb::LogicalTypeId::BLOB, "duckdb_v2_value_get_blob", out_data, out_length,
	                              err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_bit(duckdb_v2_value_handle value, const uint8_t **out_data, idx_t *out_length,
                                             duckdb_v2_error_info_handle *err) {
	return duckdb::GetStringBytes(value, duckdb::LogicalTypeId::BIT, "duckdb_v2_value_get_bit", out_data, out_length,
	                              err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_bignum(duckdb_v2_value_handle value, uint8_t **out_data, idx_t *out_length,
                                                bool *out_is_negative, duckdb_v2_error_info_handle *err) {
	if (!out_data || !out_length || !out_is_negative) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_bignum");
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
// Date / time / timestamp / interval getters
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_date(duckdb_v2_value_handle value, int32_t *out_days,
                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_days) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_date");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::DATE, "duckdb_v2_value_get_date");
		*out_days = duckdb::DateValue::Get(*duckdb::ToValue(value)).days;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_time(duckdb_v2_value_handle value, int64_t *out_micros,
                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_micros) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_time");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::TIME, "duckdb_v2_value_get_time");
		*out_micros = duckdb::TimeValue::Get(*duckdb::ToValue(value)).micros;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_time_ns(duckdb_v2_value_handle value, int64_t *out_nanos,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_nanos) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_time_ns");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::TIME_NS, "duckdb_v2_value_get_time_ns");
		// dtime_ns_t inherits dtime_t and reuses its `micros` field name, but
		// for TIME_NS the stored int64 actually carries nanoseconds (core
		// naming inconsistency — see duckdb/common/types/datetime.hpp).
		*out_nanos = duckdb::ToValue(value)->GetValueUnsafe<duckdb::dtime_ns_t>().micros;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_time_tz(duckdb_v2_value_handle value, int64_t *out_micros,
                                                 int32_t *out_offset_seconds, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_micros || !out_offset_seconds) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_time_tz");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::TIME_TZ, "duckdb_v2_value_get_time_tz");
		auto packed = duckdb::ToValue(value)->GetValueUnsafe<duckdb::dtime_tz_t>();
		*out_micros = packed.time().micros;
		*out_offset_seconds = packed.offset();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp(duckdb_v2_value_handle value, int64_t *out_micros,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_micros) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_timestamp");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::TIMESTAMP, "duckdb_v2_value_get_timestamp");
		*out_micros = duckdb::TimestampValue::Get(*duckdb::ToValue(value)).value;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp_sec(duckdb_v2_value_handle value, int64_t *out_seconds,
                                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_seconds) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_timestamp_sec");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::TIMESTAMP_SEC, "duckdb_v2_value_get_timestamp_sec");
		*out_seconds = duckdb::TimestampSValue::Get(*duckdb::ToValue(value)).value;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp_ms(duckdb_v2_value_handle value, int64_t *out_millis,
                                                      duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_millis) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_timestamp_ms");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::TIMESTAMP_MS, "duckdb_v2_value_get_timestamp_ms");
		*out_millis = duckdb::TimestampMSValue::Get(*duckdb::ToValue(value)).value;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp_ns(duckdb_v2_value_handle value, int64_t *out_nanos,
                                                      duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_nanos) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_timestamp_ns");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::TIMESTAMP_NS, "duckdb_v2_value_get_timestamp_ns");
		*out_nanos = duckdb::TimestampNSValue::Get(*duckdb::ToValue(value)).value;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp_tz(duckdb_v2_value_handle value, int64_t *out_micros,
                                                      duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_micros) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_timestamp_tz");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::TIMESTAMP_TZ, "duckdb_v2_value_get_timestamp_tz");
		*out_micros = duckdb::TimestampTZValue::Get(*duckdb::ToValue(value)).value;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp_tz_ns(duckdb_v2_value_handle value, int64_t *out_nanos,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_nanos) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_timestamp_tz_ns");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::TIMESTAMP_TZ_NS, "duckdb_v2_value_get_timestamp_tz_ns");
		*out_nanos = duckdb::TimestampTZNSValue::Get(*duckdb::ToValue(value)).value;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_interval(duckdb_v2_value_handle value, int32_t *out_months, int32_t *out_days,
                                                  int64_t *out_micros, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_months || !out_days || !out_micros) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_interval");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::INTERVAL, "duckdb_v2_value_get_interval");
		auto iv = duckdb::IntervalValue::Get(*duckdb::ToValue(value));
		*out_months = iv.months;
		*out_days = iv.days;
		*out_micros = iv.micros;
	});
}

// ---------------------------------------------------------------------------
// DECIMAL / UUID getters
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_decimal(duckdb_v2_value_handle value, uint64_t *out_lower, int64_t *out_upper,
                                                 uint8_t *out_width, uint8_t *out_scale,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_lower || !out_upper || !out_width || !out_scale) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_decimal");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::DECIMAL, "duckdb_v2_value_get_decimal");
		auto &v = *duckdb::ToValue(value);
		auto packed = duckdb::IntegralValue::Get(v);
		*out_lower = packed.lower;
		*out_upper = packed.upper;
		*out_width = duckdb::DecimalType::GetWidth(v.type());
		*out_scale = duckdb::DecimalType::GetScale(v.type());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uuid(duckdb_v2_value_handle value, uint64_t *out_lower, uint64_t *out_upper,
                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_lower || !out_upper) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_value_get_uuid");
		}
		duckdb::RequireTypedValue(value, duckdb::LogicalTypeId::UUID, "duckdb_v2_value_get_uuid");
		auto hi = duckdb::HugeIntValue::Get(*duckdb::ToValue(value));
		auto uhi = duckdb::UUID::ToUHugeint(hi);
		*out_lower = uhi.lower;
		*out_upper = uhi.upper;
	});
}
