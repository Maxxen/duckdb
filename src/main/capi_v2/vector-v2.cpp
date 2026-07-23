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

// Out-param zeroing on failure:
//   - Pointer-bearing out-params (out_view, out_child, out_data) are
//     set to nullptr on every INVALID_INPUT path.
//   - Scalar out-params (out_count, out_size) are left unspecified on
//     failure; callers must consult the return code first.
//     vector_get_view zero-inits all three fields of out_view via
//     std::memset.

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

DUCKDB_V2_ERROR duckdb_v2_vector_get_logical_type(duckdb_v2_vector_handle vector,
                                                  duckdb_v2_logical_type_handle *out_type,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_logical_type");
		}
		*out_type = nullptr;
		auto *vec = duckdb::ToVector(vector);
		auto *lt = new duckdb::LogicalType(vec->GetType());
		*out_type = reinterpret_cast<_duckdb_v2_logical_type *>(lt);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vector_get_vector_type(duckdb_v2_vector_handle vector, DUCKDB_V2_VECTOR_TYPE *out_type,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_vector_type");
		}
		*out_type = duckdb::MapVectorType(duckdb::ToVector(vector)->GetVectorType());
	});
}

DUCKDB_V2_ERROR duckdb_v2_vector_flatten(duckdb_v2_vector_handle vector, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_flatten");
		}
		duckdb::ToVector(vector)->Flatten();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vector_reference(duckdb_v2_vector_handle vector, duckdb_v2_vector_handle source,
                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector || !source) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_reference");
		}
		duckdb::ToVector(vector)->Reference(*duckdb::ToVector(source));
	});
}

// ---------------------------------------------------------------------------
// The view-getter
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_vector_get_view(duckdb_v2_vector_handle vector, duckdb_v2_vector_view *out_view,
                                          duckdb_v2_error_info_handle *err) {
	if (out_view) {
		std::memset(out_view, 0, sizeof(*out_view));
	}
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector || !out_view) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_view");
		}
		auto *vec = duckdb::ToVector(vector);
		auto vt = vec->GetVectorType();
		if (!duckdb::IsSupportedVectorType(vt)) {
			throw duckdb::InvalidInputException("duckdb_v2_vector_get_view: vector is FSST / SEQUENCE / SHREDDED — "
			                                    "call duckdb_v2_vector_flatten first");
		}
		switch (vt) {
		case duckdb::VectorType::FLAT_VECTOR: {
			out_view->data = duckdb::FlatVector::GetData(*vec);
			out_view->validity = duckdb::FlatVector::Validity(*vec).GetData();
			out_view->sel = nullptr; // identity (UVF semantics)
			out_view->count = vec->size();
			break;
		}
		case duckdb::VectorType::CONSTANT_VECTOR: {
			out_view->data = duckdb::ConstantVector::GetData(*vec);
			out_view->validity = duckdb::ConstantVector::Validity(*vec).GetData();
			out_view->sel =
			    reinterpret_cast<const duckdb_v2_sel_t *>(duckdb::ConstantVector::ZeroSelectionVector()->data());
			out_view->count = vec->size();
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
			out_view->count = vec->size();
			break;
		}
		default:
			// Unreachable thanks to IsSupportedVectorType above.
			break;
		}
	});
}

// ---------------------------------------------------------------------------
// Mutable Accessors
// ---------------------------------------------------------------------------
DUCKDB_V2_ERROR duckdb_v2_vector_get_data_mutable(duckdb_v2_vector_handle vector, void **out_data,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector) {
			throw duckdb::InvalidInputException("Vector pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto *vec = duckdb::ToVector(vector);

		const auto vec_type = vec->GetVectorType();
		if (vec_type != duckdb::VectorType::FLAT_VECTOR && vec_type != duckdb::VectorType::CONSTANT_VECTOR) {
			throw duckdb::InvalidInputException("Mutable data access is only supported for FLAT_VECTOR and "
			                                    "CONSTANT_VECTOR types. Please flatten the vector first.");
		}
		// We can get unsafe here cause we've already verified the vector type
		*out_data = duckdb::FlatVector::GetDataMutableUnsafe(*vec);
	});
}

// ---------------------------------------------------------------------------
// Vector type setup
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_vector_make_constant(duckdb_v2_vector_handle vector, duckdb_v2_value_handle value,
                                               idx_t count, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector) {
			throw duckdb::InvalidInputException("Vector cannot be null.");
		}
		if (!value) {
			throw duckdb::InvalidInputException("Value cannot be null.");
		}
		auto *vec = duckdb::ToVector(vector);
		auto *val = duckdb::ToValue(value);
		// The engine only debug-asserts this in ConstantVector::Reference; a
		// mismatch in release would corrupt the vector silently.
		if (vec->GetType() != val->type()) {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_vector_make_constant: value type does not match the vector's logical type");
		}
		duckdb::ConstantVector::Reference(*vec, *val, duckdb::count_t(count));
	});
}

DUCKDB_V2_ERROR duckdb_v2_vector_make_sequence(duckdb_v2_vector_handle vector, int64_t start, int64_t increment,
                                               idx_t count, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_make_sequence");
		}
		duckdb::ToVector(vector)->Sequence(start, increment, count);
	});
}

// ---------------------------------------------------------------------------
// Validity (mutable)
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_vector_set_null(duckdb_v2_vector_handle vector, idx_t row, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_set_null");
		}
		auto *vec = duckdb::ToVector(vector);
		if (vec->GetVectorType() != duckdb::VectorType::FLAT_VECTOR) {
			throw duckdb::InvalidInputException("duckdb_v2_vector_set_null: only supported for FLAT vectors");
		}
		if (row >= vec->size()) {
			throw duckdb::InvalidInputException("row out of range in duckdb_v2_vector_set_null");
		}
		duckdb::FlatVector::SetNull(*vec, row, true);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vector_flat_get_validity_mutable(duckdb_v2_vector_handle vector, uint64_t **out_validity,
                                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector || !out_validity) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_flat_get_validity_mutable");
		}
		auto *vec = duckdb::ToVector(vector);
		if (vec->GetVectorType() != duckdb::VectorType::FLAT_VECTOR) {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_vector_flat_get_validity_mutable: only supported for FLAT vectors");
		}
		auto &validity = duckdb::FlatVector::ValidityMutable(*vec);
		validity.EnsureWritable();
		*out_validity = validity.GetData();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vector_constant_set_valid(duckdb_v2_vector_handle vector, bool validity,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_constant_set_valid");
		}
		auto *vec = duckdb::ToVector(vector);
		if (vec->GetVectorType() != duckdb::VectorType::CONSTANT_VECTOR) {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_vector_constant_set_valid: only supported for CONSTANT vectors");
		}
		duckdb::ConstantVector::SetNull(*vec, !validity);
	});
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

DUCKDB_V2_ERROR duckdb_v2_vector_get_child_count(duckdb_v2_vector_handle vector, idx_t *out_count,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_child_count");
		}
		auto *vec = duckdb::ToVector(vector);
		switch (vec->GetType().id()) {
		case duckdb::LogicalTypeId::LIST:
		case duckdb::LogicalTypeId::ARRAY:
			*out_count = 1;
			return;
		case duckdb::LogicalTypeId::MAP:
			*out_count = 2;
			return;
		case duckdb::LogicalTypeId::STRUCT:
		case duckdb::LogicalTypeId::TUPLE:
			*out_count = duckdb::StructType::GetChildCount(vec->GetType());
			return;
		case duckdb::LogicalTypeId::UNION:
			*out_count = duckdb::UnionType::GetMemberCount(vec->GetType()) + 1;
			return;
		default:
			*out_count = 0;
			return;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vector_get_child(duckdb_v2_vector_handle vector, idx_t index,
                                           duckdb_v2_vector_handle *out_child, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector || !out_child) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_child");
		}
		*out_child = nullptr;
		auto *vec = duckdb::ToVector(vector);
		switch (vec->GetType().id()) {
		case duckdb::LogicalTypeId::LIST: {
			if (index != 0) {
				throw duckdb::InvalidInputException("duckdb_v2_vector_get_child: LIST has only child [0] (elements)");
			}
			auto &child = duckdb::ListVector::GetChildMutable(*vec);
			*out_child = reinterpret_cast<_duckdb_v2_vector *>(&child);
			return;
		}
		case duckdb::LogicalTypeId::ARRAY: {
			if (index != 0) {
				throw duckdb::InvalidInputException("duckdb_v2_vector_get_child: ARRAY has only child [0] (elements)");
			}
			auto &child = duckdb::ArrayVector::GetChildMutable(*vec);
			*out_child = reinterpret_cast<_duckdb_v2_vector *>(&child);
			return;
		}
		case duckdb::LogicalTypeId::MAP: {
			// V2 hides MAP's internal LIST<STRUCT(K,V)>: child [0] is the
			// key vector, child [1] is the value vector.
			if (index == 0) {
				auto &keys = duckdb::MapVector::GetKeys(*vec);
				*out_child = reinterpret_cast<_duckdb_v2_vector *>(&keys);
				return;
			}
			if (index == 1) {
				auto &values = duckdb::MapVector::GetValues(*vec);
				*out_child = reinterpret_cast<_duckdb_v2_vector *>(&values);
				return;
			}
			throw duckdb::InvalidInputException("duckdb_v2_vector_get_child: MAP children are [0]=keys, [1]=values");
		}
		case duckdb::LogicalTypeId::STRUCT:
		case duckdb::LogicalTypeId::TUPLE: {
			auto &entries = duckdb::StructVector::GetEntries(*vec);
			if (index >= entries.size()) {
				throw duckdb::InvalidInputException("duckdb_v2_vector_get_child: field index out of range");
			}
			*out_child = reinterpret_cast<_duckdb_v2_vector *>(&entries[index]);
			return;
		}
		case duckdb::LogicalTypeId::UNION: {
			// Child [0] is the tag vector; children [1..N] are the
			// member vectors.
			if (index == 0) {
				auto &tags = duckdb::UnionVector::GetTags(*vec);
				*out_child = reinterpret_cast<_duckdb_v2_vector *>(&tags);
				return;
			}
			// Compute the member-space index first; bounds-check that
			// directly against GetMemberCount. Mixing the child-space
			// index with the member-space count is too easy to get
			// off-by-one wrong on later edits.
			idx_t member_idx = index - 1;
			if (member_idx >= duckdb::UnionType::GetMemberCount(vec->GetType())) {
				throw duckdb::InvalidInputException("duckdb_v2_vector_get_child: UNION member index out of range");
			}
			auto &member = duckdb::UnionVector::GetMember(*vec, member_idx);
			*out_child = reinterpret_cast<_duckdb_v2_vector *>(&member);
			return;
		}
		default:
			throw duckdb::InvalidInputException("duckdb_v2_vector_get_child: vector has no children");
		}
	});
}

// ---------------------------------------------------------------------------
// Generic row-count
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_vector_get_size(duckdb_v2_vector_handle vector, idx_t *out_size,
                                          duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector || !out_size) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_size");
		}
		auto *vec = duckdb::ToVector(vector);
		*out_size = vec->size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vector_set_size(duckdb_v2_vector_handle vector, idx_t size,
                                          duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_set_size");
		}
		auto *vec = duckdb::ToVector(vector);
		// Grow the underlying buffer first so the new logical size fits; constant vectors
		// carry a single physical element and must not be reserved against.
		if (vec->GetVectorType() == duckdb::VectorType::FLAT_VECTOR) {
			vec->Reserve(size);
		}
		duckdb::FlatVector::SetSize(*vec, size);
	});
}

// ---------------------------------------------------------------------------
// Single-cell value bridge
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_vector_get_value(duckdb_v2_vector_handle vector, idx_t row, duckdb_v2_value_handle *out_value,
                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector || !out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_value");
		}
		*out_value = nullptr;
		auto *vec = duckdb::ToVector(vector);
		if (row >= vec->size()) {
			throw duckdb::InvalidInputException("row out of range in duckdb_v2_vector_get_value");
		}
		// Buffer-dispatched read: total over vector representations (constant
		// clamps, dictionary resolves through its selection vector).
		*out_value = reinterpret_cast<_duckdb_v2_value *>(new duckdb::Value(vec->GetValue(row)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_vector_set_value(duckdb_v2_vector_handle vector, idx_t row, duckdb_v2_value_handle value,
                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!vector || !value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_set_value");
		}
		auto *vec = duckdb::ToVector(vector);
		if (vec->GetVectorType() != duckdb::VectorType::FLAT_VECTOR) {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_vector_set_value requires a FLAT vector; call duckdb_v2_vector_flatten first");
		}
		if (row >= vec->size()) {
			throw duckdb::InvalidInputException("row out of range in duckdb_v2_vector_set_value");
		}
		// The buffer casts the value to the vector's type; cast failures propagate.
		vec->SetValue(row, *duckdb::ToValue(value));
	});
}

// ---------------------------------------------------------------------------
// String-backed kind decoders
//
// duckdb_v2_string (and its bit/bignum aliases) is the transparent
// 16-byte public storage type; the static_asserts in
// capi_v2_internal.hpp pin its layout to duckdb::string_t, so the
// reinterpret_casts here are guarded. Only BIGNUM keeps a C decoder (it
// allocates an owned buffer); BIT is a trivial client-side split and
// VARCHAR / BLOB reads are direct field reads on the transparent type.
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_bignum_decode(const duckdb_v2_bignum_t *bignum, uint8_t **out_data, idx_t *out_length,
                                        bool *out_is_negative, duckdb_v2_error_info_handle *err) {
	if (!bignum || !out_data || !out_length || !out_is_negative) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INPUT_INVALID, "null argument to duckdb_v2_bignum_decode");
	}
	const auto *storage = reinterpret_cast<const duckdb::string_t *>(bignum);
	return duckdb::DecodeBignumStringT(*storage, out_data, out_length, out_is_negative, "duckdb_v2_bignum_decode", err);
}
