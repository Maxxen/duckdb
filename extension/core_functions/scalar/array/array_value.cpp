#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "core_functions/scalar/array_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/storage/statistics/array_stats.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

void ArrayValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto array_type = result.GetType();

	D_ASSERT(array_type.id() == LogicalTypeId::ARRAY);
	D_ASSERT(args.ColumnCount() == ArrayType::GetSize(array_type));

	auto &child_type = ArrayType::GetChildType(array_type);

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	auto num_rows = args.size();
	auto num_columns = args.ColumnCount();

	auto &child = ArrayVector::GetChildMutable(result);

	if (num_columns > 1) {
		// Ensure that the child has a validity mask of the correct size
		// The SetValue call below expects the validity mask to be initialized
		auto &child_validity = FlatVector::ValidityMutable(child);
		child_validity.Resize(num_rows * num_columns);
	}

	for (idx_t i = 0; i < num_rows; i++) {
		for (idx_t j = 0; j < num_columns; j++) {
			auto val = args.GetValue(j, i).CastAs(state.GetContext(), child_type);
			child.SetValue((i * num_columns) + j, val);
		}
	}

	result.Verify();
}

//! An array of the type the arguments unify into, with one element for each argument
LogicalType ArrayValueReturnType(const ResolveReturnTypeInput &input) {
	const auto argument_count = input.GetArgumentCount();
	if (argument_count == 0) {
		throw InvalidInputException("array_value requires at least one argument");
	}
	if (argument_count > ArrayType::MAX_ARRAY_SIZE) {
		throw OutOfRangeException("Array size exceeds maximum allowed size");
	}
	return LogicalType::ARRAY(input.GetArgumentType(0), argument_count);
}

unique_ptr<BaseStatistics> ArrayValueStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto list_stats = ArrayStats::CreateEmpty(expr.GetReturnType());
	auto &list_child_stats = ArrayStats::GetChildStats(list_stats);
	for (idx_t i = 0; i < child_stats.size(); i++) {
		list_child_stats.Merge(child_stats[i]);
	}
	list_stats.SetHasNoNullFast();
	return list_stats.ToUnique();
}

} // namespace

ScalarFunction ArrayValueFun::GetFunction() {
	auto element_type = TypeName::VARIABLE("T");
	ScalarFunction fun("array_value",
	                   FunctionSignature()
	                       .AddVariable("T")
	                       .AddArgs("args", element_type)
	                       .SetReturnType(ReturnType::ANY(ArrayValueReturnType)),
	                   ArrayValueFunction, VariableReturnBindData::Bind, ArrayValueStats);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	// casting the arguments to the common child type can fail, e.g. for arrays of different sizes
	fun.SetFallible();
	return fun;
}

} // namespace duckdb
