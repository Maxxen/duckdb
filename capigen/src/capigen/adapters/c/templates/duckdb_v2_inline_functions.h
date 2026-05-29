#ifndef DUCKDB_V2_INLINE_FUNCTIONS_H
#define DUCKDB_V2_INLINE_FUNCTIONS_H

#include <stdbool.h>
#include <stdint.h>

/* Internal mirror of duckdb::string_t's C ABI layout. Fixed structure;
   access only via the duckdb_v2_string_* helpers — do not read fields
   directly. Only sizeof == 16 and alignof == 8 are committed as ABI. */
struct duckdb_v2_impl_string {
	union {
		struct {
			uint32_t length;
			char prefix[4];
			char *ptr;
		} pointer;
		struct {
			uint32_t length;
			char inlined[12];
		} inlined;
	} value;
};

#define DUCKDB_V2_IMPL_STRING_INLINE_MAX 12u

_Static_assert(sizeof(duckdb_v2_string) == sizeof(struct duckdb_v2_impl_string),
               "duckdb_v2_string and duckdb_v2_impl_string must have the same size");

//===--------------------------------------------------------------------===//
// duckdb_v2_string helpers
//===--------------------------------------------------------------------===//

static inline DUCKDB_V2_API_CALL_t duckdb_v2_string_is_inlined(const duckdb_v2_string *string, bool *out_inlined,
                                                                duckdb_v2_error_info_ptr *err) {
	const struct duckdb_v2_impl_string *p = (const struct duckdb_v2_impl_string *)(const void *)string;
	*out_inlined = p->value.inlined.length <= DUCKDB_V2_IMPL_STRING_INLINE_MAX;
	return DUCKDB_V2_ERROR_NONE;
}

static inline DUCKDB_V2_API_CALL_t duckdb_v2_string_get_length(const duckdb_v2_string *string, uint32_t *out_length,
                                                                duckdb_v2_error_info_ptr *err) {
	const struct duckdb_v2_impl_string *p = (const struct duckdb_v2_impl_string *)(const void *)string;
	*out_length = p->value.inlined.length;
	return DUCKDB_V2_ERROR_NONE;
}

static inline DUCKDB_V2_API_CALL_t duckdb_v2_string_get_data(const duckdb_v2_string *string, const char **out_data,
                                                              duckdb_v2_error_info_ptr *err) {
	const struct duckdb_v2_impl_string *p = (const struct duckdb_v2_impl_string *)(const void *)string;
	*out_data = p->value.inlined.length <= DUCKDB_V2_IMPL_STRING_INLINE_MAX ? p->value.inlined.inlined
	                                                                         : p->value.pointer.ptr;
	return DUCKDB_V2_ERROR_NONE;
}

//===--------------------------------------------------------------------===//
// duckdb_v2_varchar_t helpers (same layout as string)
//===--------------------------------------------------------------------===//

static inline DUCKDB_V2_API_CALL_t duckdb_v2_varchar_is_inlined(const duckdb_v2_varchar_t *s, bool *out_inlined,
                                                                 duckdb_v2_error_info_ptr *err) {
	return duckdb_v2_string_is_inlined(s, out_inlined, err);
}

static inline DUCKDB_V2_API_CALL_t duckdb_v2_varchar_get_length(const duckdb_v2_varchar_t *s, uint32_t *out_length,
                                                                 duckdb_v2_error_info_ptr *err) {
	return duckdb_v2_string_get_length(s, out_length, err);
}

static inline DUCKDB_V2_API_CALL_t duckdb_v2_varchar_get_data(const duckdb_v2_varchar_t *s, const char **out_data,
                                                               duckdb_v2_error_info_ptr *err) {
	return duckdb_v2_string_get_data(s, out_data, err);
}

//===--------------------------------------------------------------------===//
// duckdb_v2_blob_t helpers (same layout as string; byte-typed accessors)
//===--------------------------------------------------------------------===//

static inline DUCKDB_V2_API_CALL_t duckdb_v2_blob_is_inlined(const duckdb_v2_blob_t *b, bool *out_inlined,
                                                              duckdb_v2_error_info_ptr *err) {
	return duckdb_v2_string_is_inlined(b, out_inlined, err);
}

static inline DUCKDB_V2_API_CALL_t duckdb_v2_blob_get_length(const duckdb_v2_blob_t *b, uint32_t *out_length,
                                                              duckdb_v2_error_info_ptr *err) {
	return duckdb_v2_string_get_length(b, out_length, err);
}

static inline DUCKDB_V2_API_CALL_t duckdb_v2_blob_get_data(const duckdb_v2_blob_t *b, const char **out_data,
                                                            duckdb_v2_error_info_ptr *err) {
	return duckdb_v2_string_get_data(b, out_data, err);
}

//===--------------------------------------------------------------------===//
// duckdb_v2_bit_t helpers
// Encoding: data[0] = padding bit count (0-7); data[1..] = bit payload.
//===--------------------------------------------------------------------===//

static inline DUCKDB_V2_API_CALL_t duckdb_v2_bit_padding(const duckdb_v2_bit_t *b, uint8_t *out_padding,
                                                          duckdb_v2_error_info_ptr *err) {
	const struct duckdb_v2_impl_string *p = (const struct duckdb_v2_impl_string *)(const void *)b;
	uint32_t len = p->value.inlined.length;
	if (len == 0) {
		*out_padding = 0;
		return DUCKDB_V2_ERROR_NONE;
	}
	const char *data = len <= DUCKDB_V2_IMPL_STRING_INLINE_MAX ? p->value.inlined.inlined : p->value.pointer.ptr;
	*out_padding = (uint8_t)data[0];
	return DUCKDB_V2_ERROR_NONE;
}

static inline DUCKDB_V2_API_CALL_t duckdb_v2_bit_count(const duckdb_v2_bit_t *b, uint64_t *out_count,
                                                        duckdb_v2_error_info_ptr *err) {
	const struct duckdb_v2_impl_string *p = (const struct duckdb_v2_impl_string *)(const void *)b;
	uint32_t len = p->value.inlined.length;
	if (len == 0) {
		*out_count = 0;
		return DUCKDB_V2_ERROR_NONE;
	}
	const char *data = len <= DUCKDB_V2_IMPL_STRING_INLINE_MAX ? p->value.inlined.inlined : p->value.pointer.ptr;
	*out_count = (uint64_t)(len - 1) * 8 - (uint8_t)data[0];
	return DUCKDB_V2_ERROR_NONE;
}

static inline DUCKDB_V2_API_CALL_t duckdb_v2_bit_get_data(const duckdb_v2_bit_t *b, const uint8_t **out_data,
                                                           duckdb_v2_error_info_ptr *err) {
	const char *raw = NULL;
	DUCKDB_V2_API_CALL_t rc = duckdb_v2_string_get_data(b, &raw, err);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return rc;
	}
	*out_data = (const uint8_t *)raw + 1;
	return DUCKDB_V2_ERROR_NONE;
}

//===--------------------------------------------------------------------===//
// duckdb_v2_bignum_t helpers
// Sign encoding: MSB of data[0] clear = negative, set = positive.
//===--------------------------------------------------------------------===//

static inline DUCKDB_V2_API_CALL_t duckdb_v2_bignum_is_negative(const duckdb_v2_bignum_t *b, bool *out_negative,
                                                                  duckdb_v2_error_info_ptr *err) {
	const struct duckdb_v2_impl_string *p = (const struct duckdb_v2_impl_string *)(const void *)b;
	uint32_t len = p->value.inlined.length;
	if (len == 0) {
		*out_negative = false;
		return DUCKDB_V2_ERROR_NONE;
	}
	const char *data = len <= DUCKDB_V2_IMPL_STRING_INLINE_MAX ? p->value.inlined.inlined : p->value.pointer.ptr;
	*out_negative = ((uint8_t)data[0] & 0x80) == 0;
	return DUCKDB_V2_ERROR_NONE;
}

#endif /* DUCKDB_V2_INLINE_FUNCTIONS_H */
