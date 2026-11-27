#include "duckdb/common/vector/union_vector.hpp"
#include "core_functions/scalar/union_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

namespace {

struct UnionValueBindData : public FunctionData {
	UnionValueBindData() {
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<UnionValueBindData>();
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

void UnionValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// Assign the new entries to the result vector
	UnionVector::GetMember(result, 0).Reference(args.data[0]);

	// Set the result tag vector to a constant value
	auto &tag_vector = UnionVector::GetTags(result);
	tag_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<union_tag_t>(tag_vector)[0] = 0;
}

//! A union value has one member: the named argument
unique_ptr<FunctionData> UnionValueBind(BindScalarFunctionInput &input) {
	if (input.GetArguments().size() != 1) {
		throw BinderException("union_value takes exactly one argument");
	}
	return VariableReturnBindData::Bind(input);
}

unique_ptr<ParsedExpression> UnionValueUnbind(FunctionUnbindInput &input) {
	auto &expression = input.expression;
	D_ASSERT(input.children.size() == 1);
	D_ASSERT(UnionType::GetMemberCount(expression.GetReturnType()) == 1);
	vector<FunctionArgument> arguments;
	arguments.emplace_back(UnionType::GetMemberName(expression.GetReturnType(), 0), std::move(input.children[0]));
	return make_uniq<FunctionExpression>(expression.Function().GetDefinition()->GetQualifiedName(),
	                                     std::move(arguments));
}

} // namespace

ScalarFunction UnionValueFun::GetFunction() {
	ScalarFunction fun("union_value",
	                   FunctionSignature()
	                       .AddNamedPack("KV")
	                       .AddKwargs("kwargs", TypeName::NAMED_PACK("KV"))
	                       .SetReturnType(TypeName(Identifier("UNION"), {TypeName::NAMED_PACK("KV")})),
	                   UnionValueFunction, UnionValueBind);
	fun.GetProperties().SetRequiresExpressionNames(true);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetSerializeCallback(VariableReturnBindData::Serialize);
	fun.SetDeserializeCallback(VariableReturnBindData::Deserialize);
	fun.SetUnbindCallback(UnionValueUnbind);
	return fun;
}

} // namespace duckdb
