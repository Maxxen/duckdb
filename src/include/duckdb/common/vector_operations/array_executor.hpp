//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/array_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <functional>
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

class ArrayExecutor {

public:
	// execute a scalar unary function on a vector of arrays, element by element, assuming the child vector does not
	// contain NULL values
	template <class INPUT_TYPE, class RESULT_TYPE, class FUNC>
	static void ExecuteUnaryScalar(Vector &input, Vector &result, idx_t count, FUNC func) {
		D_ASSERT(input.GetType().id() == LogicalTypeId::ARRAY);
		D_ASSERT(result.GetType().id() == LogicalTypeId::ARRAY);

		auto is_constant = input.GetVectorType() == VectorType::CONSTANT_VECTOR;

		UnifiedVectorFormat format;
		input.ToUnifiedFormat(count, format);

		auto array_size = ArrayType::GetSize(input.GetType());
		D_ASSERT(array_size == ArrayType::GetSize(result.GetType()));

		auto &in_child = ArrayVector::GetEntry(input);
		auto &out_child = ArrayVector::GetEntry(result);

		auto in_data = FlatVector::GetData<INPUT_TYPE>(in_child);
		auto out_data = FlatVector::GetData<RESULT_TYPE>(out_child);

		for (idx_t out_idx = 0; out_idx < count; out_idx++) {
			auto in_idx = format.sel->get_index(out_idx);
			if (format.validity.RowIsValid(in_idx)) {
				auto in_offset = in_idx * array_size;
				auto out_offset = out_idx * array_size;
				for (idx_t elem_idx = 0; elem_idx < array_size; elem_idx++) {
					out_data[out_offset + elem_idx] = func(in_data[in_offset + elem_idx]);
				}
			} else {
				FlatVector::SetNull(result, out_idx, true);
			}
		}

		if (is_constant) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class FUNC>
	static void ExecuteBinaryScalar(Vector &left, Vector &right, Vector &result, idx_t count, FUNC func) {
		D_ASSERT(left.GetType().id() == LogicalTypeId::ARRAY);
		D_ASSERT(right.GetType().id() == LogicalTypeId::ARRAY);
		D_ASSERT(result.GetType().id() == LogicalTypeId::ARRAY);

		auto is_constant =
		    left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR;

		UnifiedVectorFormat left_format;
		left.ToUnifiedFormat(count, left_format);

		UnifiedVectorFormat right_format;
		right.ToUnifiedFormat(count, right_format);

		auto array_size = ArrayType::GetSize(left.GetType());
		D_ASSERT(array_size == ArrayType::GetSize(right.GetType()));
		D_ASSERT(array_size == ArrayType::GetSize(result.GetType()));

		auto &left_child = ArrayVector::GetEntry(left);
		auto &right_child = ArrayVector::GetEntry(right);
		auto &out_child = ArrayVector::GetEntry(result);

		auto left_data = FlatVector::GetData<LEFT_TYPE>(left_child);
		auto right_data = FlatVector::GetData<RIGHT_TYPE>(right_child);
		auto out_data = FlatVector::GetData<RESULT_TYPE>(out_child);

		for (idx_t out_idx = 0; out_idx < count; out_idx++) {
			auto left_idx = left_format.sel->get_index(out_idx);
			auto right_idx = right_format.sel->get_index(out_idx);

			if (left_format.validity.RowIsValid(left_idx) && right_format.validity.RowIsValid(right_idx)) {
				auto left_offset = left_idx * array_size;
				auto right_offset = right_idx * array_size;
				auto out_offset = out_idx * array_size;
				for (idx_t elem_idx = 0; elem_idx < array_size; elem_idx++) {
					out_data[out_offset + elem_idx] =
					    func(left_data[left_offset + elem_idx], right_data[right_offset + elem_idx]);
				}
			} else {
				FlatVector::SetNull(result, out_idx, true);
			}
		}

		if (is_constant) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteUnaryAggregate(Vector &input, Vector &result, idx_t count, OP op) {
		D_ASSERT(input.GetType().id() == LogicalTypeId::ARRAY);

		auto is_constant = input.GetVectorType() == VectorType::CONSTANT_VECTOR;

		UnifiedVectorFormat format;
		input.ToUnifiedFormat(count, format);

		auto array_size = ArrayType::GetSize(input.GetType());
		D_ASSERT(array_size > 1);

		auto &in_child = ArrayVector::GetEntry(input);

		auto in_data = FlatVector::GetData<INPUT_TYPE>(in_child);
		auto out_data = FlatVector::GetData<RESULT_TYPE>(result);

		for (idx_t out_idx = 0; out_idx < count; out_idx++) {
			auto in_idx = format.sel->get_index(out_idx);
			if (format.validity.RowIsValid(in_idx)) {
				auto in_offset = in_idx * array_size;
				RESULT_TYPE res = in_data[in_offset];
				for (idx_t elem_idx = 1; elem_idx < array_size; elem_idx++) {
					res = func(res, in_data[in_offset + elem_idx]);
				}
				out_data[out_idx] = res;
			} else {
				FlatVector::SetNull(result, out_idx, true);
			}
		}

		if (is_constant) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteBinaryAggregate(Vector &left, Vector &right, Vector &result, idx_t count, OP op) {
	}
};

} // namespace duckdb
