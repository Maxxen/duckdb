#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_streaming_window.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/projection/physical_unnest.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/window/rows_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_nearest_join.hpp"

namespace duckdb {

static unique_ptr<Expression> CreateIsNotNull(unique_ptr<Expression> child) {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
	result->GetChildrenMutable().push_back(std::move(child));
	return std::move(result);
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalNearestJoin &op) {
	// Brute-force fallback: compute the top-k right rows of every left row with an aggregate over all pairs.
	// A row number primary key keeps duplicate left rows apart.
	//
	//		 ∏ left, match.right
	//		 |
	//		 UNNEST matches
	//		 |
	//		 Γ pk; first(left), arg_min/max_nulls_last(struct_pack(right), ranking, k) AS matches
	//		 |
	//		 σ ranking IS NOT NULL (INNER only)
	//		 |
	//		 ∏ pk, left, struct_pack(right), ranking, k
	//		 |
	//		 ⨝ cross product (INNER) or nested loop join ON ranking IS NOT NULL (LEFT)
	//		/  \
	//	   W    R
	//	   |
	//	   L
	D_ASSERT(op.children.size() == 2);
	D_ASSERT(op.join_type == JoinType::INNER || op.join_type == JoinType::LEFT);

	auto &left = CreatePlan(*op.children[0]);
	auto &right = CreatePlan(*op.children[1]);
	// use the types of the planned children, planning can change them
	const auto left_types = left.GetTypes();
	const auto right_types = right.GetTypes();
	const auto left_count = left_types.size();
	const auto right_count = right_types.size();
	const auto pk_type = LogicalType::BIGINT;
	const auto cardinality = op.estimated_cardinality;

	// add a primary key to the left side
	vector<unique_ptr<Expression>> window_select;
	auto pk = GetBuiltinWindowFunction(context, RowNumberFun::Name, {})->Bind(context);
	D_ASSERT(pk->GetReturnType() == pk_type);
	pk->WindowStartMutable() = WindowBoundary::UNBOUNDED_PRECEDING;
	pk->WindowEndMutable() = WindowBoundary::CURRENT_ROW_ROWS;
	pk->SetAlias("row_number");
	window_select.push_back(std::move(pk));
	auto window_types = left_types;
	window_types.push_back(pk_type);
	auto &window = Make<PhysicalStreamingWindow>(window_types, std::move(window_select), left.estimated_cardinality);
	window.children.push_back(left);

	// materialize the smaller side of the join
	const bool build_left = left.estimated_cardinality < right.estimated_cardinality;
	vector<LogicalType> join_types;
	idx_t left_offset, pk_index, right_offset;
	if (build_left) {
		join_types = right_types;
		join_types.insert(join_types.end(), window_types.begin(), window_types.end());
		right_offset = 0;
		left_offset = right_count;
		pk_index = right_count + left_count;
	} else {
		join_types = window_types;
		join_types.insert(join_types.end(), right_types.begin(), right_types.end());
		left_offset = 0;
		pk_index = left_count;
		right_offset = left_count + 1;
	}
	auto &probe = build_left ? right : window;
	auto &build = build_left ? window : right;

	// the ranking refers to [left, right], remap it to the join layout
	auto ranking = std::move(op.GetRankingMutable());
	ExpressionIterator::EnumerateExpression(ranking, [&](Expression &child) {
		if (child.GetExpressionClass() != ExpressionClass::BOUND_REF) {
			return;
		}
		auto &ref = child.Cast<BoundReferenceExpression>();
		ref.IndexMutable() =
		    ref.Index() < left_count ? left_offset + ref.Index() : right_offset + ref.Index() - left_count;
	});

	if (op.join_type == JoinType::LEFT) {
		// the ranking is also evaluated on the NULL row emitted for left rows without a match
		ExpressionIterator::EnumerateExpression(ranking,
		                                        [&](Expression &child) { child.SetVerificationStats(nullptr); });
	}

	// right rows with a NULL ranking only match if include_null_rankings is set
	const bool filter_null_rankings = !op.include_null_rankings;
	optional_ptr<PhysicalOperator> join;
	if (op.join_type == JoinType::INNER) {
		join = Make<PhysicalCrossProduct>(join_types, probe, build, cardinality);
	} else {
		// left rows without a match produce a single row with NULLs for the right side
		LogicalAnyJoin join_op(build_left ? JoinType::RIGHT : JoinType::LEFT);
		join_op.types = join_types;
		auto condition = filter_null_rankings ? CreateIsNotNull(ranking->Copy())
		                                      : make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		join =
		    Make<PhysicalBlockwiseNLJoin>(join_op, probe, build, std::move(condition), join_op.join_type, cardinality);
	}

	// bind the aggregate computing the matches of every left row
	FunctionBinder function_binder(context);
	unique_ptr<Expression> match;
	if (right_count == 0) {
		match = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	} else {
		vector<unique_ptr<Expression>> struct_children;
		for (idx_t i = 0; i < right_count; i++) {
			auto child = make_uniq<BoundReferenceExpression>(right_types[i], right_offset + i);
			child->SetAlias(Identifier(StringUtil::Format("c%llu", i)));
			struct_children.push_back(std::move(child));
		}
		match = BindBuiltinScalarFunction(context, Identifier("struct_pack"), std::move(struct_children));
	}
	const auto match_type = match->GetReturnType();
	vector<unique_ptr<Expression>> top_k_children;
	top_k_children.push_back(std::move(match));
	top_k_children.push_back(std::move(ranking));
	top_k_children.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(op.nearest_count))));
	auto top_k_name = op.order_type == OrderType::DESCENDING ? "arg_max_nulls_last" : "arg_min_nulls_last";
	vector<LogicalType> top_k_types;
	for (auto &child : top_k_children) {
		top_k_types.push_back(child->GetReturnType());
	}
	auto top_k_function = GetBuiltinAggregateFunction(context, Identifier(top_k_name), top_k_types);
	auto top_k = function_binder.BindAggregateFunction(top_k_function, std::move(top_k_children));
	const auto matches_type = top_k->GetReturnType();
	D_ASSERT(ListType::GetChildType(matches_type) == match_type);

	// project the pk, the left columns and the arguments of the aggregate
	vector<LogicalType> args_types;
	vector<unique_ptr<Expression>> args_list;
	args_types.push_back(pk_type);
	args_list.push_back(make_uniq<BoundReferenceExpression>(pk_type, pk_index));
	for (idx_t i = 0; i < left_count; i++) {
		args_types.push_back(left_types[i]);
		args_list.push_back(make_uniq<BoundReferenceExpression>(left_types[i], left_offset + i));
	}
	const auto ranking_index = args_list.size() + 1;
	for (auto &child : top_k->Cast<BoundAggregateExpression>().GetChildrenMutable()) {
		const auto child_type = child->GetReturnType();
		args_types.push_back(child_type);
		args_list.push_back(std::move(child));
		child = make_uniq<BoundReferenceExpression>(child_type, args_list.size() - 1);
	}
	auto &args = Make<PhysicalProjection>(args_types, std::move(args_list), cardinality);
	args.children.push_back(*join);
	reference<PhysicalOperator> aggregate_input = args;

	if (op.join_type == JoinType::INNER && filter_null_rankings) {
		vector<unique_ptr<Expression>> filters;
		filters.push_back(
		    CreateIsNotNull(make_uniq<BoundReferenceExpression>(args_types[ranking_index], ranking_index)));
		auto &filter = Make<PhysicalFilter>(args_types, std::move(filters), cardinality);
		filter.children.push_back(args);
		aggregate_input = filter;
	}

	// aggregate per left row
	vector<LogicalType> aggregate_types;
	vector<unique_ptr<Expression>> groups;
	vector<unique_ptr<Expression>> aggregates;
	aggregate_types.push_back(pk_type);
	groups.push_back(make_uniq<BoundReferenceExpression>(pk_type, 0));
	for (idx_t i = 0; i < left_count; i++) {
		vector<unique_ptr<Expression>> first_children;
		first_children.push_back(make_uniq<BoundReferenceExpression>(left_types[i], 1 + i));
		auto first = GetBuiltinAggregateFunction(context, FirstFun::Name, {left_types[i]})
		                 ->Bind(context, std::move(first_children));
		D_ASSERT(first->GetReturnType() == left_types[i]);
		aggregate_types.push_back(left_types[i]);
		aggregates.push_back(std::move(first));
	}
	aggregate_types.push_back(matches_type);
	aggregates.push_back(std::move(top_k));
	auto &aggregate = Make<PhysicalHashAggregate>(context, aggregate_types, std::move(aggregates), std::move(groups),
	                                              op.children[0]->estimated_cardinality);
	aggregate.children.push_back(aggregate_input);

	// unnest the matches
	const auto matches_index = aggregate_types.size() - 1;
	vector<unique_ptr<Expression>> unnest_list;
	auto unnest_expr = make_uniq<BoundUnnestExpression>(match_type);
	unnest_expr->ChildMutable() = make_uniq<BoundReferenceExpression>(matches_type, matches_index);
	unnest_list.push_back(std::move(unnest_expr));
	auto unnest_types = aggregate_types;
	unnest_types.push_back(match_type);
	auto &unnest = Make<PhysicalUnnest>(unnest_types, std::move(unnest_list), cardinality);
	unnest.children.push_back(aggregate);

	// project the left columns and the fields of the matches
	const auto match_index = unnest_types.size() - 1;
	vector<LogicalType> result_types;
	vector<unique_ptr<Expression>> result_list;
	for (idx_t i = 0; i < left_count; i++) {
		result_types.push_back(left_types[i]);
		result_list.push_back(make_uniq<BoundReferenceExpression>(left_types[i], 1 + i));
	}
	if (right_count > 0) {
		auto struct_extract =
		    GetBuiltinScalarFunction(context, Identifier("struct_extract"), {match_type, LogicalType::VARCHAR});
		auto &fields = StructType::GetChildTypes(match_type);
		D_ASSERT(fields.size() == right_count);
		for (idx_t i = 0; i < right_count; i++) {
			vector<unique_ptr<Expression>> extract_children;
			extract_children.push_back(make_uniq<BoundReferenceExpression>(match_type, match_index));
			extract_children.push_back(make_uniq<BoundConstantExpression>(Value(fields[i].first)));
			auto field = function_binder.BindScalarFunction(struct_extract, std::move(extract_children));
			D_ASSERT(field->GetReturnType() == right_types[i]);
			result_types.push_back(right_types[i]);
			result_list.push_back(std::move(field));
		}
	}
	auto &result = Make<PhysicalProjection>(std::move(result_types), std::move(result_list), cardinality);
	result.children.push_back(unnest);
	return result;
}

} // namespace duckdb
