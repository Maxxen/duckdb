#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/storage/statistics/union_statistics.hpp"

namespace duckdb {

struct UnionValueBindData : public FunctionData {
	UnionValueBindData() {
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_unique<UnionValueBindData>();
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static void UnionValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	
	auto entries = UnionVector::GetData(result);
	auto &child = UnionVector::GetEntries(result)[0];

	// Assign the new entries to the result vector
	child->Reference(args.data[0]);
	for(idx_t i = 0; i < args.size(); i++) {
		entries[i].tag = 0;
	}

	if(args.AllConstant()){
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> UnionValueBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	
	if(arguments.size() != 1) {
		throw Exception("union_value takes exactly one argument");
	}
	auto &child = arguments[0];

	if(child->alias.empty()) {
		throw BinderException("Need named argument for union tag, e.g. UNION_VALUE(a := b)");
	}

	child_list_t<LogicalType> union_members;

	union_members.push_back(make_pair(child->alias, child->return_type));

	bound_function.return_type = LogicalType::UNION(move(union_members));
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction UnionValueFun::GetFunction() {
	auto fun = ScalarFunction("union_value", {}, LogicalTypeId::UNION, UnionValueFunction, UnionValueBind, nullptr, nullptr);
	fun.varargs = LogicalType::ANY;
	fun.serialize = VariableReturnBindData::Serialize;
	fun.deserialize = VariableReturnBindData::Deserialize;
	return fun;
}

void UnionValueFun::RegisterFunction(BuiltinFunctions &set) {
	auto fun = GetFunction();
	ScalarFunctionSet union_value("union_value");
	union_value.AddFunction(fun);
	set.AddFunction(union_value);
}

} // namespace duckdb
