#include "capi_v2_internal.hpp"

#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/uuid.hpp"

#include <cstdlib>
#include <cstring>

// Exception policy:
//   - All allocating sites wrap in try/catch: `new duckdb::Value(...)`, the
//     static Value::CreateValue<T> / Value::HUGEINT / Value::BIGNUM / etc.
//     factories (which may allocate a shared_ptr or copy a std::string), and
//     the malloc paths in value_to_string / value_get_bignum. Both
//     std::exception and ... catch arms are required.
//   - Non-allocating accessors (numeric / date / time / interval / decimal /
//     uuid getters) read pre-validated state and are unwrapped. Every such
//     getter first checks `lt.id()` matches and that the value is non-null,
//     which makes the unchecked Value::GetValueUnsafe<T> / IntegralValue::Get
//     calls safe.
//   - The borrowed-string getters (varchar / blob / bit) reach the value's
//     stored std::string through StringValue::Get(*v), which calls D_ASSERT
//     to verify the value is non-null with VARCHAR physical type. Both
//     invariants are enforced above the call.
//
// Out-param zeroing on failure:
//   - Pointer out-params (out_value, out_type, out_data) are set to nullptr
//     on every INVALID_INPUT path to keep dangling-pointer hazards out of
//     caller code.
//   - Scalar out-params (out_micros, out_lower, out_width, ...) are
//     unspecified on failure. Callers must check the return code before
//     reading scalars. This convention is also stated in C_API_V2.md
//     ("V2 conventions").

namespace duckdb {
namespace {

// Common preflight for typed-payload getters: handle must be non-null, all
// out-params must be non-null, value's id must match, and the value must not
// be NULL. Returns DUCKDB_V2_ERROR_NONE if the caller may proceed, otherwise
// the relevant INVALID_INPUT code (with err populated).
DUCKDB_V2_API_CALL_t CheckTypedGetter(duckdb_v2_value_ptr value, LogicalTypeId expected, const char *function_name,
                                      duckdb_v2_error_info_ptr *err) {
	if (!value) {
		std::string msg = std::string("null argument to ") + function_name;
		return SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, msg.c_str());
	}
	auto *v = ToValue(value);
	if (v->type().id() != expected) {
		std::string msg = std::string(function_name) + ": value is not of the expected type";
		return SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, msg.c_str());
	}
	if (v->IsNull()) {
		std::string msg = std::string(function_name) + ": value is NULL";
		return SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, msg.c_str());
	}
	return DUCKDB_V2_ERROR_NONE;
}

// Helper for the primitive numeric constructors: wraps `new Value(...)` in
// the standard try/catch.
template <class Make>
DUCKDB_V2_API_CALL_t MakePrimitive(duckdb_v2_value_ptr *out_value, duckdb_v2_error_info_ptr *err,
                                   const char *function_name, Make make) {
	if (!out_value) {
		std::string msg = std::string("null argument to ") + function_name;
		return SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, msg.c_str());
	}
	*out_value = nullptr;
	try {
		auto *v = new Value(make());
		*out_value = static_cast<duckdb_v2_value_ptr>(v);
		return ClearErrorInfo(err);
	} catch (std::exception &e) {
		return SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error allocating value");
	}
}

// Borrowed-bytes getter for the string-backed types (VARCHAR/BLOB/BIT). The
// returned pointer is into the Value's StringValueInfo and stays valid until
// the value is destroyed.
template <class CharT>
DUCKDB_V2_API_CALL_t GetStringBytes(duckdb_v2_value_ptr value, LogicalTypeId expected, const char *function_name,
                                    const CharT **out_data, idx_t *out_length, duckdb_v2_error_info_ptr *err) {
	if (!out_data || !out_length) {
		std::string msg = std::string("null argument to ") + function_name;
		return SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, msg.c_str());
	}
	*out_data = nullptr;
	*out_length = 0;
	auto pre = CheckTypedGetter(value, expected, function_name, err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	auto &str = StringValue::Get(*ToValue(value));
	*out_data = reinterpret_cast<const CharT *>(str.data());
	*out_length = str.size();
	return ClearErrorInfo(err);
}

} // anonymous namespace
} // namespace duckdb

// ---------------------------------------------------------------------------
// Lifecycle + NULL
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_destroy(duckdb_v2_value_ptr *value, duckdb_v2_error_info_ptr *err) {
	if (!value) {
		return duckdb::ClearErrorInfo(err);
	}
	if (*value) {
		delete duckdb::ToValue(*value);
		*value = nullptr;
	}
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_null(duckdb_v2_logical_type_ptr type, duckdb_v2_value_ptr *out_value,
                                                 duckdb_v2_error_info_ptr *err) {
	if (!type || !out_value) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_create_null");
	}
	*out_value = nullptr;
	try {
		// Value(LogicalType) constructs a typed NULL — exactly what we want.
		auto *v = new duckdb::Value(*duckdb::ToLogicalType(type));
		*out_value = static_cast<duckdb_v2_value_ptr>(v);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_value_create_null");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_is_null(duckdb_v2_value_ptr value, bool *out_is_null,
                                             duckdb_v2_error_info_ptr *err) {
	if (!value || !out_is_null) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_is_null");
	}
	*out_is_null = duckdb::ToValue(value)->IsNull();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_logical_type(duckdb_v2_value_ptr value, duckdb_v2_logical_type_ptr *out_type,
                                                      duckdb_v2_error_info_ptr *err) {
	if (!value || !out_type) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_get_logical_type");
	}
	*out_type = nullptr;
	try {
		auto *lt = new duckdb::LogicalType(duckdb::ToValue(value)->type());
		*out_type = static_cast<duckdb_v2_logical_type_ptr>(lt);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_value_get_logical_type");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_to_string(duckdb_v2_value_ptr value, char **out_string,
                                               duckdb_v2_error_info_ptr *err) {
	if (!value || !out_string) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_to_string");
	}
	*out_string = nullptr;
	try {
		auto str = duckdb::ToValue(value)->ToString();
		auto *buf = static_cast<char *>(std::malloc(str.size() + 1));
		if (!buf) {
			return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "malloc failed in duckdb_v2_value_to_string");
		}
		std::memcpy(buf, str.data(), str.size());
		buf[str.size()] = '\0';
		*out_string = buf;
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_value_to_string");
	}
}

// ---------------------------------------------------------------------------
// Primitive numeric constructors (mechanical: dispatch through Value::TYPE())
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_bool(bool input, duckdb_v2_value_ptr *out_value,
                                                 duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_bool",
	                             [input]() { return duckdb::Value::BOOLEAN(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_int8(int8_t input, duckdb_v2_value_ptr *out_value,
                                                 duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_int8",
	                             [input]() { return duckdb::Value::TINYINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_int16(int16_t input, duckdb_v2_value_ptr *out_value,
                                                  duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_int16",
	                             [input]() { return duckdb::Value::SMALLINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_int32(int32_t input, duckdb_v2_value_ptr *out_value,
                                                  duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_int32",
	                             [input]() { return duckdb::Value::INTEGER(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_int64(int64_t input, duckdb_v2_value_ptr *out_value,
                                                  duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_int64",
	                             [input]() { return duckdb::Value::BIGINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uint8(uint8_t input, duckdb_v2_value_ptr *out_value,
                                                  duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uint8",
	                             [input]() { return duckdb::Value::UTINYINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uint16(uint16_t input, duckdb_v2_value_ptr *out_value,
                                                   duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uint16",
	                             [input]() { return duckdb::Value::USMALLINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uint32(uint32_t input, duckdb_v2_value_ptr *out_value,
                                                   duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uint32",
	                             [input]() { return duckdb::Value::UINTEGER(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uint64(uint64_t input, duckdb_v2_value_ptr *out_value,
                                                   duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uint64",
	                             [input]() { return duckdb::Value::UBIGINT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_hugeint(uint64_t lower, int64_t upper, duckdb_v2_value_ptr *out_value,
                                                    duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_hugeint",
	                             [lower, upper]() { return duckdb::Value::HUGEINT(duckdb::hugeint_t(upper, lower)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uhugeint(uint64_t lower, uint64_t upper, duckdb_v2_value_ptr *out_value,
                                                     duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uhugeint", [lower, upper]() {
		return duckdb::Value::UHUGEINT(duckdb::uhugeint_t(upper, lower));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_float(float input, duckdb_v2_value_ptr *out_value,
                                                  duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_float",
	                             [input]() { return duckdb::Value::FLOAT(input); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_double(double input, duckdb_v2_value_ptr *out_value,
                                                   duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_double",
	                             [input]() { return duckdb::Value::DOUBLE(input); });
}

// ---------------------------------------------------------------------------
// VARCHAR / BLOB / BIT / BIGNUM constructors
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_varchar(const char *data, idx_t length, duckdb_v2_value_ptr *out_value,
                                                    duckdb_v2_error_info_ptr *err) {
	if (!out_value) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_create_varchar");
	}
	*out_value = nullptr;
	if (!data && length > 0) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null data with positive length in duckdb_v2_value_create_varchar");
	}
	// Only run UTF-8 validation when there is something to validate.
	if (length > 0 && !duckdb::Value::StringIsValid(data, length)) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "invalid UTF-8 in duckdb_v2_value_create_varchar");
	}
	try {
		// length=0 + data=NULL is the documented empty-value shape (see spec).
		// The ternary keeps the std::string(ptr, len) ctor happy without
		// touching the byte at `data` when length=0 and data is non-null.
		auto *v = new duckdb::Value(std::string(data ? data : "", length));
		*out_value = static_cast<duckdb_v2_value_ptr>(v);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_value_create_varchar");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_blob(const uint8_t *data, idx_t length, duckdb_v2_value_ptr *out_value,
                                                 duckdb_v2_error_info_ptr *err) {
	if (!out_value) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_create_blob");
	}
	*out_value = nullptr;
	if (!data && length > 0) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null data with positive length in duckdb_v2_value_create_blob");
	}
	try {
		// Value::BLOB takes a (const_data_ptr_t, idx_t) pair and copies.
		auto *v = new duckdb::Value(duckdb::Value::BLOB(data, length));
		*out_value = static_cast<duckdb_v2_value_ptr>(v);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_value_create_blob");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_bit(const uint8_t *data, idx_t length, duckdb_v2_value_ptr *out_value,
                                                duckdb_v2_error_info_ptr *err) {
	if (!out_value) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_create_bit");
	}
	*out_value = nullptr;
	// BIT has no empty encoding: the padding header byte is mandatory.
	// Reject zero-length / null input outright, same shape as BIGNUM.
	if (!data || length == 0) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "duckdb_v2_value_create_bit requires data != NULL and length >= 1");
	}
	try {
		auto *v = new duckdb::Value(duckdb::Value::BIT(data, length));
		*out_value = static_cast<duckdb_v2_value_ptr>(v);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_value_create_bit");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_bignum(const uint8_t *data, idx_t length, bool is_negative,
                                                   duckdb_v2_value_ptr *out_value, duckdb_v2_error_info_ptr *err) {
	if (!out_value) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_create_bignum");
	}
	*out_value = nullptr;
	// BIGNUM has no empty encoding: zero must be expressed as {0x00}. Reject
	// missing magnitude bytes outright rather than translating to a canonical
	// form — the spec contract is "data != NULL && length >= 1".
	if (!data || length == 0) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "duckdb_v2_value_create_bignum requires data != NULL and length >= 1");
	}
	try {
		// Bignum::FromByteArray takes a non-const pointer but only reads.
		// Project rule bans const_cast, so copy into a writable buffer.
		// TODO(core): widen FromByteArray to const uint8_t * upstream and
		// drop this copy.
		duckdb::vector<uint8_t> tmp(data, data + length);
		auto blob = duckdb::Bignum::FromByteArray(tmp.data(), tmp.size(), is_negative);
		auto *v = new duckdb::Value(duckdb::Value::BIGNUM(blob));
		*out_value = static_cast<duckdb_v2_value_ptr>(v);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_value_create_bignum");
	}
}

// ---------------------------------------------------------------------------
// Date / time / timestamp / interval constructors
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_date(int32_t days, duckdb_v2_value_ptr *out_value,
                                                 duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_date",
	                             [days]() { return duckdb::Value::DATE(duckdb::date_t(days)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_time(int64_t micros, duckdb_v2_value_ptr *out_value,
                                                 duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_time",
	                             [micros]() { return duckdb::Value::TIME(duckdb::dtime_t(micros)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_time_ns(int64_t nanos, duckdb_v2_value_ptr *out_value,
                                                    duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_time_ns",
	                             [nanos]() { return duckdb::Value::TIME_NS(duckdb::dtime_ns_t(nanos)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_time_tz(int64_t micros, int32_t offset_seconds,
                                                    duckdb_v2_value_ptr *out_value, duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_time_tz", [micros, offset_seconds]() {
		// dtime_tz_t packs micros (high 40 bits) + offset (low 24 bits).
		return duckdb::Value::TIMETZ(duckdb::dtime_tz_t(duckdb::dtime_t(micros), offset_seconds));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp(int64_t micros, duckdb_v2_value_ptr *out_value,
                                                      duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp",
	                             [micros]() { return duckdb::Value::TIMESTAMP(duckdb::timestamp_t(micros)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp_sec(int64_t seconds, duckdb_v2_value_ptr *out_value,
                                                          duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp_sec",
	                             [seconds]() { return duckdb::Value::TIMESTAMPSEC(duckdb::timestamp_sec_t(seconds)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp_ms(int64_t millis, duckdb_v2_value_ptr *out_value,
                                                         duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp_ms",
	                             [millis]() { return duckdb::Value::TIMESTAMPMS(duckdb::timestamp_ms_t(millis)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp_ns(int64_t nanos, duckdb_v2_value_ptr *out_value,
                                                         duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp_ns",
	                             [nanos]() { return duckdb::Value::TIMESTAMPNS(duckdb::timestamp_ns_t(nanos)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp_tz(int64_t micros, duckdb_v2_value_ptr *out_value,
                                                         duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp_tz",
	                             [micros]() { return duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t(micros)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_timestamp_tz_ns(int64_t nanos, duckdb_v2_value_ptr *out_value,
                                                            duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_timestamp_tz_ns",
	                             [nanos]() { return duckdb::Value::TIMESTAMPTZNS(duckdb::timestamp_tz_ns_t(nanos)); });
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_interval(int32_t months, int32_t days, int64_t micros,
                                                     duckdb_v2_value_ptr *out_value, duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_interval",
	                             [months, days, micros]() { return duckdb::Value::INTERVAL(months, days, micros); });
}

// ---------------------------------------------------------------------------
// DECIMAL / UUID constructors
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_decimal(uint64_t lower, int64_t upper, uint8_t width, uint8_t scale,
                                                    duckdb_v2_value_ptr *out_value, duckdb_v2_error_info_ptr *err) {
	if (!out_value) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_create_decimal");
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
	try {
		duckdb::Value v;
		if (width <= MAX_WIDTH_INT64) {
			// Validate the payload actually fits the chosen physical width
			// rather than silently truncating the upper half.
			int64_t fit = 0;
			if (!duckdb::Hugeint::TryCast<int64_t>(hi, fit)) {
				return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
				                            "decimal payload does not fit the chosen width");
			}
			v = duckdb::Value::DECIMAL(fit, width, scale);
		} else {
			v = duckdb::Value::DECIMAL(hi, width, scale);
		}
		*out_value = static_cast<duckdb_v2_value_ptr>(new duckdb::Value(std::move(v)));
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_value_create_decimal");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_create_uuid(uint64_t lower, uint64_t upper, duckdb_v2_value_ptr *out_value,
                                                 duckdb_v2_error_info_ptr *err) {
	return duckdb::MakePrimitive(out_value, err, "duckdb_v2_value_create_uuid", [lower, upper]() {
		// UUID storage is hugeint_t; user-facing input is uhugeint pieces.
		return duckdb::Value::UUID(duckdb::UUID::FromUHugeint(duckdb::uhugeint_t(upper, lower)));
	});
}

// ---------------------------------------------------------------------------
// Primitive getters
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_bool(duckdb_v2_value_ptr value, bool *out, duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_bool");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::BOOLEAN, "duckdb_v2_value_get_bool", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out = duckdb::BooleanValue::Get(*duckdb::ToValue(value));
	return duckdb::ClearErrorInfo(err);
}

// Per-getter functions are unrolled (rather than macro-generated) so the
// bridge adapter's regex (looking for `duckdb_v2_\w+\s*\(`) detects every
// implementation by name.

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_int8(duckdb_v2_value_ptr value, int8_t *out, duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_int8");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::TINYINT, "duckdb_v2_value_get_int8", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out = duckdb::TinyIntValue::Get(*duckdb::ToValue(value));
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_int16(duckdb_v2_value_ptr value, int16_t *out, duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_int16");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::SMALLINT, "duckdb_v2_value_get_int16", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out = duckdb::SmallIntValue::Get(*duckdb::ToValue(value));
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_int32(duckdb_v2_value_ptr value, int32_t *out, duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_int32");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::INTEGER, "duckdb_v2_value_get_int32", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out = duckdb::IntegerValue::Get(*duckdb::ToValue(value));
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_int64(duckdb_v2_value_ptr value, int64_t *out, duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_int64");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::BIGINT, "duckdb_v2_value_get_int64", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out = duckdb::BigIntValue::Get(*duckdb::ToValue(value));
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uint8(duckdb_v2_value_ptr value, uint8_t *out, duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_uint8");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::UTINYINT, "duckdb_v2_value_get_uint8", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out = duckdb::UTinyIntValue::Get(*duckdb::ToValue(value));
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uint16(duckdb_v2_value_ptr value, uint16_t *out,
                                                duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_uint16");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::USMALLINT, "duckdb_v2_value_get_uint16", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out = duckdb::USmallIntValue::Get(*duckdb::ToValue(value));
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uint32(duckdb_v2_value_ptr value, uint32_t *out,
                                                duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_uint32");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::UINTEGER, "duckdb_v2_value_get_uint32", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out = duckdb::UIntegerValue::Get(*duckdb::ToValue(value));
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uint64(duckdb_v2_value_ptr value, uint64_t *out,
                                                duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_uint64");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::UBIGINT, "duckdb_v2_value_get_uint64", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out = duckdb::UBigIntValue::Get(*duckdb::ToValue(value));
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_float(duckdb_v2_value_ptr value, float *out, duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_float");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::FLOAT, "duckdb_v2_value_get_float", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out = duckdb::FloatValue::Get(*duckdb::ToValue(value));
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_double(duckdb_v2_value_ptr value, double *out, duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_double");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::DOUBLE, "duckdb_v2_value_get_double", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out = duckdb::DoubleValue::Get(*duckdb::ToValue(value));
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_hugeint(duckdb_v2_value_ptr value, uint64_t *out_lower, int64_t *out_upper,
                                                 duckdb_v2_error_info_ptr *err) {
	if (!out_lower || !out_upper) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_hugeint");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::HUGEINT, "duckdb_v2_value_get_hugeint", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	auto hi = duckdb::HugeIntValue::Get(*duckdb::ToValue(value));
	*out_lower = hi.lower;
	*out_upper = hi.upper;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uhugeint(duckdb_v2_value_ptr value, uint64_t *out_lower, uint64_t *out_upper,
                                                  duckdb_v2_error_info_ptr *err) {
	if (!out_lower || !out_upper) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_get_uhugeint");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::UHUGEINT, "duckdb_v2_value_get_uhugeint", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	auto uhi = duckdb::UhugeIntValue::Get(*duckdb::ToValue(value));
	*out_lower = uhi.lower;
	*out_upper = uhi.upper;
	return duckdb::ClearErrorInfo(err);
}

// ---------------------------------------------------------------------------
// VARCHAR / BLOB / BIT getters (borrowed) and BIGNUM getter (owned)
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_varchar(duckdb_v2_value_ptr value, const char **out_data, idx_t *out_length,
                                                 duckdb_v2_error_info_ptr *err) {
	return duckdb::GetStringBytes(value, duckdb::LogicalTypeId::VARCHAR, "duckdb_v2_value_get_varchar", out_data,
	                              out_length, err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_blob(duckdb_v2_value_ptr value, const uint8_t **out_data, idx_t *out_length,
                                              duckdb_v2_error_info_ptr *err) {
	return duckdb::GetStringBytes(value, duckdb::LogicalTypeId::BLOB, "duckdb_v2_value_get_blob", out_data, out_length,
	                              err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_bit(duckdb_v2_value_ptr value, const uint8_t **out_data, idx_t *out_length,
                                             duckdb_v2_error_info_ptr *err) {
	return duckdb::GetStringBytes(value, duckdb::LogicalTypeId::BIT, "duckdb_v2_value_get_bit", out_data, out_length,
	                              err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_bignum(duckdb_v2_value_ptr value, uint8_t **out_data, idx_t *out_length,
                                                bool *out_is_negative, duckdb_v2_error_info_ptr *err) {
	if (!out_data || !out_length || !out_is_negative) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_bignum");
	}
	*out_data = nullptr;
	*out_length = 0;
	*out_is_negative = false;
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::BIGNUM, "duckdb_v2_value_get_bignum", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	try {
		auto &str = duckdb::StringValue::Get(*duckdb::ToValue(value));
		duckdb::vector<uint8_t> magnitude;
		bool is_negative = false;
		duckdb::Bignum::GetByteArray(magnitude, is_negative, duckdb::string_t(str));
		auto *buf = static_cast<uint8_t *>(std::malloc(magnitude.empty() ? 1 : magnitude.size()));
		if (!buf) {
			return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "malloc failed in duckdb_v2_value_get_bignum");
		}
		if (!magnitude.empty()) {
			std::memcpy(buf, magnitude.data(), magnitude.size());
		}
		*out_data = buf;
		*out_length = magnitude.size();
		*out_is_negative = is_negative;
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_value_get_bignum");
	}
}

// ---------------------------------------------------------------------------
// Date / time / timestamp / interval getters
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_date(duckdb_v2_value_ptr value, int32_t *out_days,
                                              duckdb_v2_error_info_ptr *err) {
	if (!out_days) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_date");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::DATE, "duckdb_v2_value_get_date", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out_days = duckdb::DateValue::Get(*duckdb::ToValue(value)).days;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_time(duckdb_v2_value_ptr value, int64_t *out_micros,
                                              duckdb_v2_error_info_ptr *err) {
	if (!out_micros) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_time");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::TIME, "duckdb_v2_value_get_time", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out_micros = duckdb::TimeValue::Get(*duckdb::ToValue(value)).micros;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_time_ns(duckdb_v2_value_ptr value, int64_t *out_nanos,
                                                 duckdb_v2_error_info_ptr *err) {
	if (!out_nanos) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_time_ns");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::TIME_NS, "duckdb_v2_value_get_time_ns", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	// dtime_ns_t inherits dtime_t and reuses its `micros` field name, but for
	// TIME_NS the stored int64 actually carries nanoseconds (core naming
	// inconsistency — see duckdb/common/types/datetime.hpp).
	*out_nanos = duckdb::ToValue(value)->GetValueUnsafe<duckdb::dtime_ns_t>().micros;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_time_tz(duckdb_v2_value_ptr value, int64_t *out_micros,
                                                 int32_t *out_offset_seconds, duckdb_v2_error_info_ptr *err) {
	if (!out_micros || !out_offset_seconds) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_time_tz");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::TIME_TZ, "duckdb_v2_value_get_time_tz", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	auto packed = duckdb::ToValue(value)->GetValueUnsafe<duckdb::dtime_tz_t>();
	*out_micros = packed.time().micros;
	*out_offset_seconds = packed.offset();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp(duckdb_v2_value_ptr value, int64_t *out_micros,
                                                   duckdb_v2_error_info_ptr *err) {
	if (!out_micros) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_get_timestamp");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::TIMESTAMP, "duckdb_v2_value_get_timestamp", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out_micros = duckdb::TimestampValue::Get(*duckdb::ToValue(value)).value;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp_sec(duckdb_v2_value_ptr value, int64_t *out_seconds,
                                                       duckdb_v2_error_info_ptr *err) {
	if (!out_seconds) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_get_timestamp_sec");
	}
	auto pre =
	    duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::TIMESTAMP_SEC, "duckdb_v2_value_get_timestamp_sec", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out_seconds = duckdb::TimestampSValue::Get(*duckdb::ToValue(value)).value;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp_ms(duckdb_v2_value_ptr value, int64_t *out_millis,
                                                      duckdb_v2_error_info_ptr *err) {
	if (!out_millis) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_get_timestamp_ms");
	}
	auto pre =
	    duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::TIMESTAMP_MS, "duckdb_v2_value_get_timestamp_ms", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out_millis = duckdb::TimestampMSValue::Get(*duckdb::ToValue(value)).value;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp_ns(duckdb_v2_value_ptr value, int64_t *out_nanos,
                                                      duckdb_v2_error_info_ptr *err) {
	if (!out_nanos) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_get_timestamp_ns");
	}
	auto pre =
	    duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::TIMESTAMP_NS, "duckdb_v2_value_get_timestamp_ns", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out_nanos = duckdb::TimestampNSValue::Get(*duckdb::ToValue(value)).value;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp_tz(duckdb_v2_value_ptr value, int64_t *out_micros,
                                                      duckdb_v2_error_info_ptr *err) {
	if (!out_micros) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_get_timestamp_tz");
	}
	auto pre =
	    duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::TIMESTAMP_TZ, "duckdb_v2_value_get_timestamp_tz", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out_micros = duckdb::TimestampTZValue::Get(*duckdb::ToValue(value)).value;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_timestamp_tz_ns(duckdb_v2_value_ptr value, int64_t *out_nanos,
                                                         duckdb_v2_error_info_ptr *err) {
	if (!out_nanos) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_get_timestamp_tz_ns");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::TIMESTAMP_TZ_NS,
	                                    "duckdb_v2_value_get_timestamp_tz_ns", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	*out_nanos = duckdb::TimestampTZNSValue::Get(*duckdb::ToValue(value)).value;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_interval(duckdb_v2_value_ptr value, int32_t *out_months, int32_t *out_days,
                                                  int64_t *out_micros, duckdb_v2_error_info_ptr *err) {
	if (!out_months || !out_days || !out_micros) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_value_get_interval");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::INTERVAL, "duckdb_v2_value_get_interval", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	auto iv = duckdb::IntervalValue::Get(*duckdb::ToValue(value));
	*out_months = iv.months;
	*out_days = iv.days;
	*out_micros = iv.micros;
	return duckdb::ClearErrorInfo(err);
}

// ---------------------------------------------------------------------------
// DECIMAL / UUID getters
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_decimal(duckdb_v2_value_ptr value, uint64_t *out_lower, int64_t *out_upper,
                                                 uint8_t *out_width, uint8_t *out_scale,
                                                 duckdb_v2_error_info_ptr *err) {
	if (!out_lower || !out_upper || !out_width || !out_scale) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_decimal");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::DECIMAL, "duckdb_v2_value_get_decimal", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	auto &v = *duckdb::ToValue(value);
	// IntegralValue::Get widens the stored scaled int (which may be
	// SMALLINT/INTEGER/BIGINT/HUGEINT physically) to a 128-bit signed value.
	auto packed = duckdb::IntegralValue::Get(v);
	*out_lower = packed.lower;
	*out_upper = packed.upper;
	*out_width = duckdb::DecimalType::GetWidth(v.type());
	*out_scale = duckdb::DecimalType::GetScale(v.type());
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_value_get_uuid(duckdb_v2_value_ptr value, uint64_t *out_lower, uint64_t *out_upper,
                                              duckdb_v2_error_info_ptr *err) {
	if (!out_lower || !out_upper) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_value_get_uuid");
	}
	auto pre = duckdb::CheckTypedGetter(value, duckdb::LogicalTypeId::UUID, "duckdb_v2_value_get_uuid", err);
	if (pre != DUCKDB_V2_ERROR_NONE) {
		return pre;
	}
	auto hi = duckdb::HugeIntValue::Get(*duckdb::ToValue(value));
	auto uhi = duckdb::UUID::ToUHugeint(hi);
	*out_lower = uhi.lower;
	*out_upper = uhi.upper;
	return duckdb::ClearErrorInfo(err);
}
