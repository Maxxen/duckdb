#include "duckdb/execution/operator/schema/physical_create_cast.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

PhysicalCreateCast::PhysicalCreateCast(PhysicalPlan &physical_plan, unique_ptr<CreateCastInfo> info_p,
                                       idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_CAST, {LogicalType::BIGINT}, estimated_cardinality),
      info(std::move(info_p)) {
}

//===--------------------------------------------------------------------===//
// Cast Function
//===--------------------------------------------------------------------===//
namespace {
struct UserCastData final : public BoundCastData {
	// Add any necessary data members here

	LogicalType source_type;
	unique_ptr<Expression> cast_expression;

	unique_ptr<BoundCastData> Copy() const override {
		auto copy = make_uniq<UserCastData>();
		copy->source_type = source_type;
		copy->cast_expression = cast_expression->Copy();
		return std::move(copy);
	}
};

struct UserCastState final : public FunctionLocalState {
	ExpressionExecutor executor;
	DataChunk input_chunk;

	explicit UserCastState(ClientContext &context) : executor(context) {
	}
};

unique_ptr<FunctionLocalState> UserCastInit(CastLocalStateParameters &parameters) {
	auto &data = parameters.cast_data->Cast<UserCastData>();
	auto state = make_uniq<UserCastState>(*parameters.context);

	// Initialize the executor or any other state here
	state->input_chunk.InitializeEmpty({data.source_type});
	state->executor.AddExpression(*data.cast_expression);

	return std::move(state);
}

bool UserCastFunction(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &data = parameters.cast_data;
	auto &state = parameters.local_state->Cast<UserCastState>();

	state.input_chunk.data[0].Reference(source);
	state.input_chunk.SetCardinality(count);
	state.executor.ExecuteExpression(state.input_chunk, result);

	UnifiedVectorFormat result_format;
	result.ToUnifiedFormat(count, result_format);
	auto &validity = result_format.validity;

	if (validity.AllValid()) {
		return true;
	}

	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(result_format.sel->get_index(i))) {
			if (parameters.strict) {
				HandleCastError::AssignError("Failed to cast", parameters);
			}
			return false;
		}
	}

	return true;
}

} // namespace

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCreateCast::GetData(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSourceInput &input) const {
	auto &target = info->target;
	auto &source = info->source;
	auto &dbconfig = DBConfig::GetConfig(context.client);

	// TODO: This doesnt really work.
	// We need to BIND at query-time when the cast is invoked, not at planning time when registering the cast.
	// (It also wont work because we need catalog entries for transactionality)

	// Lookup function name in the catalog
	auto &catalog = Catalog::GetCatalog(context.client, info->function_catalog);
	auto &function_entry = *catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    context.client, info->function_schema, info->function_name, OnEntryNotFound::THROW_EXCEPTION);

	auto func = function_entry.functions.GetFunctionByArguments(context.client, {source});

	// Now make an expression that represents the cast
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq_base<Expression, BoundReferenceExpression>(source, 0));

	// TODO: Move this to planning phase
	FunctionBinder binder(context.client);
	auto cast_expression = binder.BindScalarFunction(func, std::move(children));

	auto cast_data = make_uniq<UserCastData>();
	cast_data->source_type = source;
	cast_data->cast_expression = std::move(cast_expression);

	// Setup cast info
	BoundCastInfo cast_info(UserCastFunction, std::move(cast_data), UserCastInit);

	dbconfig.GetCastFunctions().RegisterCastFunction(source, target, std::move(cast_info), info->cast_cost);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
