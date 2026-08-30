#include "duckdb/optimizer/rule/distinct_aggregate_optimizer.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

DistinctAggregateOptimizer::DistinctAggregateOptimizer(ExpressionRewriter &rewriter) : Rule(rewriter) {
	root = make_uniq<ExpressionMatcher>();
	root->expr_class = ExpressionClass::BOUND_AGGREGATE;
}

unique_ptr<Expression> DistinctAggregateOptimizer::Apply(ClientContext &context, BoundAggregateExpression &aggr,
                                                         bool &changes_made) {
	if (!aggr.IsDistinct()) {
		// no DISTINCT defined
		return nullptr;
	}
	if (aggr.Function().GetDistinctDependent() == AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT) {
		// not a distinct-sensitive aggregate but we have an DISTINCT modifier - remove it
		aggr.GetAggregateTypeMutable() = AggregateType::NON_DISTINCT;
		changes_made = true;
		return nullptr;
	}
	return nullptr;
}

unique_ptr<Expression> DistinctAggregateOptimizer::Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
                                                         vector<reference<Expression>> &bindings, bool is_root) {
	auto &aggr = bindings[0].get().Cast<BoundAggregateExpression>();
	bool changes_made = false;
	auto result = Apply(rewriter.context, aggr, changes_made);
	if (result) {
		return result;
	}
	// the helper modifies the aggregate in place: hand back the modified expression as the replacement
	return changes_made ? std::move(expr_ptr) : nullptr;
}

DistinctWindowedOptimizer::DistinctWindowedOptimizer(ExpressionRewriter &rewriter) : Rule(rewriter) {
	root = make_uniq<ExpressionMatcher>();
	root->expr_class = ExpressionClass::BOUND_WINDOW;
}

unique_ptr<Expression> DistinctWindowedOptimizer::Apply(ClientContext &context, BoundWindowExpression &wexpr,
                                                        bool &changes_made) {
	if (!wexpr.Distinct()) {
		// no DISTINCT defined
		return nullptr;
	}
	if (!wexpr.AggregateFunction()) {
		// not an aggregate
		return nullptr;
	}
	if (wexpr.AggregateFunction()->GetDistinctDependent() == AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT) {
		// not a distinct-sensitive aggregate but we have an DISTINCT modifier - remove it
		wexpr.DistinctMutable() = false;
		changes_made = true;
		return nullptr;
	}
	return nullptr;
}

unique_ptr<Expression> DistinctWindowedOptimizer::Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
                                                        vector<reference<Expression>> &bindings, bool is_root) {
	auto &wexpr = bindings[0].get().Cast<BoundWindowExpression>();
	bool changes_made = false;
	auto result = Apply(rewriter.context, wexpr, changes_made);
	if (result) {
		return result;
	}
	// the helper modifies the window expression in place: hand back the modified expression as the replacement
	return changes_made ? std::move(expr_ptr) : nullptr;
}

} // namespace duckdb
