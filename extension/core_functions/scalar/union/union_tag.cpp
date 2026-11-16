#include "duckdb/common/vector/union_vector.hpp"
#include "core_functions/scalar/union_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

namespace {

//! The tag of a union is an enum of the names of its members
LogicalType UnionTagReturnType(const ResolveReturnTypeInput &input) {
	auto &union_type = input.GetArgumentType(0);
	if (union_type.id() != LogicalTypeId::UNION) {
		// the argument is NULL
		return union_type;
	}
	auto member_count = UnionType::GetMemberCount(union_type);
	if (member_count == 0) {
		// this should never happen, empty unions are not allowed
		throw InternalException("Can't get tags from an empty union");
	}
	auto varchar_vector = Vector(LogicalType::VARCHAR, member_count);
	auto result_data = FlatVector::Writer<string_t>(varchar_vector, member_count);
	for (idx_t i = 0; i < member_count; i++) {
		result_data.WriteValue(string_t(UnionType::GetMemberName(union_type, i).GetIdentifierName()));
	}
	return LogicalType::ENUM(varchar_vector, member_count);
}

void UnionTagFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::ENUM);
	result.Reinterpret(UnionVector::GetTags(args.data[0]));
}

} // namespace

ScalarFunction UnionTagFun::GetFunction() {
	// TODO: Statistics?
	ScalarFunction fun(FunctionSignature()
	                       .AddParameter("union", LogicalType::TEMPLATE("T", LogicalTypeId::UNION))
	                       .SetReturnType(ReturnType::ANY(UnionTagReturnType)),
	                   UnionTagFunction);
	return fun;
}

} // namespace duckdb
