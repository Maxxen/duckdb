#include "core_functions/scalar/generic_functions.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception/parser_exception.hpp"

namespace duckdb {

namespace {
struct CurrentSettingBindData : public FunctionData {
	explicit CurrentSettingBindData(Value value_p) : value(std::move(value_p)) {
	}

	Value value;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CurrentSettingBindData>(value);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CurrentSettingBindData>();
		return Value::NotDistinctFrom(value, other.value);
	}
};

void CurrentSettingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.BindInfo()->Cast<CurrentSettingBindData>();
	result.Reference(info.value, count_t(args.size()));
}

//! The value of the setting that is named - its extension is loaded if it has to be
Value GetSettingValue(ClientContext &context, const Value &key_val) {
	auto key = key_val.GetValue<Identifier>();
	if (key.empty()) {
		throw ParserException("Key name for current_setting must not be empty");
	}

	Value val;
	if (!context.TryGetCurrentSetting(key, val)) {
		auto extension_name = Catalog::AutoloadExtensionByConfigName(context, key);
		// If autoloader didn't throw, the config is now available
		context.TryGetCurrentSetting(key, val);
	}
	return val;
}

//! A setting is returned as the type of its value
LogicalType CurrentSettingReturnType(const ResolveReturnTypeInput &input) {
	return GetSettingValue(input.GetClientContext(), input.GetNonNullConstant(0)).type();
}

unique_ptr<FunctionData> CurrentSettingBind(BindScalarFunctionInput &input) {
	return make_uniq<CurrentSettingBindData>(GetSettingValue(input.GetClientContext(), input.GetNonNullConstant(0)));
}

} // namespace

ScalarFunction CurrentSettingFun::GetFunction() {
	auto fun =
	    ScalarFunction({}, ReturnType::ANY(CurrentSettingReturnType), CurrentSettingFunction, CurrentSettingBind);
	fun.GetSignature().AddParameter("setting_name", LogicalType::VARCHAR);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	// the value of the setting can change between queries
	fun.SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
	return fun;
}

} // namespace duckdb
