#include "capi_v2_internal.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/union_vector.hpp"

// Exception policy:
//   - vector_get_logical_type wraps `new LogicalType(...)` — the copy
//     constructor itself just bumps a shared_ptr on ExtraTypeInfo, but
//     the `new` is the throw source.
//   - vector_flatten wraps Vector::Flatten — touches buffers and can
//     allocate.
//   - vector_get_view's DICTIONARY path flattens the dictionary child
//     in-place if it isn't FLAT yet (matching DictionaryBuffer::ToUnifiedFormat
//     semantics); that flatten can throw.
//   - vector_get_child / vector_get_child_count / vector_list_get_size
//     call ListVector::GetChildMutable / GetListSize, ArrayVector::
//     GetChildMutable, StructVector::GetEntries, MapVector::GetKeys|
//     GetValues, UnionVector::GetTags|GetMember — all internally cast
//     through VectorBuffer::Cast<T>, which may throw InternalException
//     if the vector shape mismatches. Wrap with the standard try/catch.
//   - vector_get_vector_type and validity_row_is_valid read scalar
//     state and are unwrapped.
//   - bignum_decode funnels through DecodeBignumStringT, which carries
//     its own try/catch.
//
// Out-param zeroing on failure:
//   - Pointer-bearing out-params (out_view, out_child, out_data) are
//     set to nullptr on every INVALID_INPUT path.
//   - Scalar out-params (out_count, out_size, out_is_valid) are left
//     unspecified on failure; callers must consult the return code
//     first. vector_get_view zero-inits all three fields of out_view
//     via std::memset.

namespace duckdb {
namespace {

bool IsSupportedVectorType(VectorType vt) {
	return vt == VectorType::FLAT_VECTOR || vt == VectorType::CONSTANT_VECTOR || vt == VectorType::DICTIONARY_VECTOR;
}

} // anonymous namespace
} // namespace duckdb

// ---------------------------------------------------------------------------
// Introspection
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_vector_get_logical_type(duckdb_v2_vector_ptr vector,
                                                       duckdb_v2_logical_type_ptr *out_type,
                                                       duckdb_v2_error_info_ptr *err) {
	if (!vector || !out_type) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_vector_get_logical_type");
	}
	*out_type = nullptr;
	auto *vec = duckdb::ToVector(vector);
	try {
		auto *lt = new duckdb::LogicalType(vec->GetType());
		*out_type = static_cast<duckdb_v2_logical_type_ptr>(lt);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error allocating vector logical_type");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_vector_get_vector_type(duckdb_v2_vector_ptr vector, DUCKDB_V2_VECTOR_TYPE *out_type,
                                                      duckdb_v2_error_info_ptr *err) {
	if (!vector || !out_type) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_vector_get_vector_type");
	}
	*out_type = duckdb::MapVectorType(duckdb::ToVector(vector)->GetVectorType());
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_vector_flatten(duckdb_v2_vector_ptr vector, duckdb_v2_error_info_ptr *err) {
	if (!vector) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_vector_flatten");
	}
	try {
		duckdb::ToVector(vector)->Flatten();
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_vector_flatten");
	}
}

// ---------------------------------------------------------------------------
// The view-getter
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_vector_get_view(duckdb_v2_vector_ptr vector, duckdb_v2_vector_view *out_view,
                                               duckdb_v2_error_info_ptr *err) {
	if (out_view) {
		std::memset(out_view, 0, sizeof(*out_view));
	}
	if (!vector || !out_view) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_vector_get_view");
	}
	auto *vec = duckdb::ToVector(vector);
	auto vt = vec->GetVectorType();
	if (!duckdb::IsSupportedVectorType(vt)) {
		return duckdb::SetErrorInfo(
		    err, DUCKDB_V2_ERROR_INVALID_INPUT,
		    "duckdb_v2_vector_get_view: vector is FSST / SEQUENCE / SHREDDED — call duckdb_v2_vector_flatten first");
	}
	try {
		switch (vt) {
		case duckdb::VectorType::FLAT_VECTOR: {
			out_view->data = duckdb::FlatVector::GetData(*vec);
			out_view->validity = duckdb::FlatVector::Validity(*vec).GetData();
			out_view->sel = nullptr; // identity (UVF semantics)
			break;
		}
		case duckdb::VectorType::CONSTANT_VECTOR: {
			out_view->data = duckdb::ConstantVector::GetData(*vec);
			out_view->validity = duckdb::ConstantVector::Validity(*vec).GetData();
			out_view->sel =
			    reinterpret_cast<const duckdb_v2_sel_t *>(duckdb::ConstantVector::ZeroSelectionVector()->data());
			break;
		}
		case duckdb::VectorType::DICTIONARY_VECTOR: {
			// Flatten the dictionary child in-place if it isn't FLAT yet,
			// matching DictionaryBuffer::ToUnifiedFormat. The parent
			// vector stays DICTIONARY; only the underlying child is
			// flattened so the dictionary's sel pointer remains valid.
			auto &child = duckdb::DictionaryVector::Child(*vec);
			if (child.GetVectorType() != duckdb::VectorType::FLAT_VECTOR) {
				child.Flatten();
			}
			out_view->data = duckdb::FlatVector::GetData(child);
			out_view->validity = duckdb::FlatVector::Validity(child).GetData();
			out_view->sel = reinterpret_cast<const duckdb_v2_sel_t *>(duckdb::DictionaryVector::SelVector(*vec).data());
			break;
		}
		default:
			// Unreachable thanks to IsSupportedVectorType above.
			break;
		}
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_vector_get_view");
	}
}

// ---------------------------------------------------------------------------
// Generic structural accessors for nested kinds
//
// Per-kind child counts:
//   LIST    → 1 child  ([0] = elements)
//   MAP     → 2 children ([0] = keys, [1] = values; V2 hides MAP's
//                         internal LIST<STRUCT(K,V)>)
//   ARRAY   → 1 child  ([0] = elements)
//   STRUCT  → N children ([i] = field i)
//   UNION   → N+1 children ([0] = tag, [1..N] = members)
//   others  → 0
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_vector_get_child_count(duckdb_v2_vector_ptr vector, idx_t *out_count,
                                                      duckdb_v2_error_info_ptr *err) {
	if (!vector || !out_count) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_vector_get_child_count");
	}
	auto *vec = duckdb::ToVector(vector);
	try {
		switch (vec->GetType().id()) {
		case duckdb::LogicalTypeId::LIST:
		case duckdb::LogicalTypeId::ARRAY:
			*out_count = 1;
			return duckdb::ClearErrorInfo(err);
		case duckdb::LogicalTypeId::MAP:
			*out_count = 2;
			return duckdb::ClearErrorInfo(err);
		case duckdb::LogicalTypeId::STRUCT:
			*out_count = duckdb::StructType::GetChildCount(vec->GetType());
			return duckdb::ClearErrorInfo(err);
		case duckdb::LogicalTypeId::UNION:
			*out_count = duckdb::UnionType::GetMemberCount(vec->GetType()) + 1;
			return duckdb::ClearErrorInfo(err);
		default:
			*out_count = 0;
			return duckdb::ClearErrorInfo(err);
		}
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_vector_get_child_count");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_vector_get_child(duckdb_v2_vector_ptr vector, idx_t index,
                                                duckdb_v2_vector_ptr *out_child, duckdb_v2_error_info_ptr *err) {
	if (!vector || !out_child) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_vector_get_child");
	}
	*out_child = nullptr;
	auto *vec = duckdb::ToVector(vector);
	try {
		switch (vec->GetType().id()) {
		case duckdb::LogicalTypeId::LIST: {
			if (index != 0) {
				return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
				                            "duckdb_v2_vector_get_child: LIST has only child [0] (elements)");
			}
			auto &child = duckdb::ListVector::GetChildMutable(*vec);
			*out_child = static_cast<duckdb_v2_vector_ptr>(&child);
			return duckdb::ClearErrorInfo(err);
		}
		case duckdb::LogicalTypeId::ARRAY: {
			if (index != 0) {
				return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
				                            "duckdb_v2_vector_get_child: ARRAY has only child [0] (elements)");
			}
			auto &child = duckdb::ArrayVector::GetChildMutable(*vec);
			*out_child = static_cast<duckdb_v2_vector_ptr>(&child);
			return duckdb::ClearErrorInfo(err);
		}
		case duckdb::LogicalTypeId::MAP: {
			// V2 hides MAP's internal LIST<STRUCT(K,V)>: child [0] is the
			// key vector, child [1] is the value vector.
			if (index == 0) {
				auto &keys = duckdb::MapVector::GetKeys(*vec);
				*out_child = static_cast<duckdb_v2_vector_ptr>(&keys);
				return duckdb::ClearErrorInfo(err);
			}
			if (index == 1) {
				auto &values = duckdb::MapVector::GetValues(*vec);
				*out_child = static_cast<duckdb_v2_vector_ptr>(&values);
				return duckdb::ClearErrorInfo(err);
			}
			return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
			                            "duckdb_v2_vector_get_child: MAP children are [0]=keys, [1]=values");
		}
		case duckdb::LogicalTypeId::STRUCT: {
			auto &entries = duckdb::StructVector::GetEntries(*vec);
			if (index >= entries.size()) {
				return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
				                            "duckdb_v2_vector_get_child: STRUCT field index out of range");
			}
			*out_child = static_cast<duckdb_v2_vector_ptr>(&entries[index]);
			return duckdb::ClearErrorInfo(err);
		}
		case duckdb::LogicalTypeId::UNION: {
			// Child [0] is the tag vector; children [1..N] are the
			// member vectors.
			if (index == 0) {
				auto &tags = duckdb::UnionVector::GetTags(*vec);
				*out_child = static_cast<duckdb_v2_vector_ptr>(&tags);
				return duckdb::ClearErrorInfo(err);
			}
			// Compute the member-space index first; bounds-check that
			// directly against GetMemberCount. Mixing the child-space
			// index with the member-space count is too easy to get
			// off-by-one wrong on later edits.
			idx_t member_idx = index - 1;
			if (member_idx >= duckdb::UnionType::GetMemberCount(vec->GetType())) {
				return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
				                            "duckdb_v2_vector_get_child: UNION member index out of range");
			}
			auto &member = duckdb::UnionVector::GetMember(*vec, member_idx);
			*out_child = static_cast<duckdb_v2_vector_ptr>(&member);
			return duckdb::ClearErrorInfo(err);
		}
		default:
			return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
			                            "duckdb_v2_vector_get_child: vector has no children");
		}
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_vector_get_child");
	}
}

// ---------------------------------------------------------------------------
// LIST/MAP child-row-count
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_vector_list_get_size(duckdb_v2_vector_ptr vector, idx_t *out_size,
                                                    duckdb_v2_error_info_ptr *err) {
	if (!vector || !out_size) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_vector_list_get_size");
	}
	auto *vec = duckdb::ToVector(vector);
	auto id = vec->GetType().id();
	if (id != duckdb::LogicalTypeId::LIST && id != duckdb::LogicalTypeId::MAP) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "duckdb_v2_vector_list_get_size: vector is not a LIST or MAP");
	}
	try {
		*out_size = duckdb::ListVector::GetListSize(*vec);
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_vector_list_get_size");
	}
}

// ---------------------------------------------------------------------------
// String-backed kind decoders
//
// duckdb_v2_string_t (and its varchar/blob/bit/bignum aliases) is the
// opaque 16-byte public storage type. The reinterpret_cast to
// duckdb::string_t here is the one place in V2 source that depends on
// the layout equivalence between the two — if duckdb::string_t ever
// changes shape, these decoders are the only thing that needs to
// follow. The opaque struct's size (16 bytes) is the ABI commitment
// callers rely on.
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_varchar_decode(const duckdb_v2_varchar_t *varchar, const char **out_data,
                                              idx_t *out_length, duckdb_v2_error_info_ptr *err) {
	if (!varchar || !out_data || !out_length) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_varchar_decode");
	}
	const auto *storage = reinterpret_cast<const duckdb::string_t *>(varchar);
	*out_data = storage->GetData();
	*out_length = storage->GetSize();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_blob_decode(const duckdb_v2_blob_t *blob, const uint8_t **out_data, idx_t *out_length,
                                           duckdb_v2_error_info_ptr *err) {
	if (!blob || !out_data || !out_length) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_blob_decode");
	}
	const auto *storage = reinterpret_cast<const duckdb::string_t *>(blob);
	*out_data = reinterpret_cast<const uint8_t *>(storage->GetData());
	*out_length = storage->GetSize();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_bit_decode(const duckdb_v2_bit_t *bit, const uint8_t **out_data, idx_t *out_length,
                                          uint8_t *out_padding_bits, duckdb_v2_error_info_ptr *err) {
	if (!bit || !out_data || !out_length || !out_padding_bits) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_bit_decode");
	}
	const auto *storage = reinterpret_cast<const duckdb::string_t *>(bit);
	auto raw = reinterpret_cast<const uint8_t *>(storage->GetData());
	auto raw_len = storage->GetSize();
	// On-disk: byte 0 is the padding count; bytes 1.. are the bit data.
	*out_padding_bits = (raw_len > 0) ? raw[0] : static_cast<uint8_t>(0);
	*out_data = (raw_len > 0) ? raw + 1 : raw;
	*out_length = (raw_len > 0) ? raw_len - 1 : 0;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_bignum_decode(const duckdb_v2_bignum_t *bignum, uint8_t **out_data, idx_t *out_length,
                                             bool *out_is_negative, duckdb_v2_error_info_ptr *err) {
	if (!bignum || !out_data || !out_length || !out_is_negative) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_bignum_decode");
	}
	const auto *storage = reinterpret_cast<const duckdb::string_t *>(bignum);
	return duckdb::DecodeBignumStringT(*storage, out_data, out_length, out_is_negative, "duckdb_v2_bignum_decode", err);
}

// ---------------------------------------------------------------------------
// Per-row helpers
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_sel_at(const duckdb_v2_sel_t *sel, idx_t i, idx_t *out, duckdb_v2_error_info_ptr *err) {
	if (!out) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_sel_at");
	}
	*out = sel ? static_cast<idx_t>(sel[i]) : i;
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_validity_row_is_valid(const uint64_t *validity, idx_t row, bool *out_is_valid,
                                                     duckdb_v2_error_info_ptr *err) {
	if (!out_is_valid) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_validity_row_is_valid");
	}
	if (!validity) {
		*out_is_valid = true;
		return duckdb::ClearErrorInfo(err);
	}
	*out_is_valid = (validity[row >> 6] & (UINT64_C(1) << (row & 63))) != 0;
	return duckdb::ClearErrorInfo(err);
}
