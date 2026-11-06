#include "duckdb/optimizer/nearest_join_ranking.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_nearest_join.hpp"

namespace duckdb {

NearestJoinRanking::NearestJoinRanking(Optimizer &optimizer) : optimizer(optimizer) {
}

//! Whether the expression is NULL whenever one of the columns it references is NULL
static bool PropagatesNull(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_CONSTANT:
		return true;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (function.Function().GetNullHandling() != FunctionNullHandling::DEFAULT_NULL_HANDLING) {
			return false;
		}
		for (auto &child : function.GetChildren()) {
			if (!PropagatesNull(*child)) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

//! The ranking output can replace the ranking above the join if both agree on rows with NULLs for the right side,
//! as produced by a LEFT join or by outer joins above the join
static bool CanReuseRanking(LogicalNearestJoin &join) {
	auto &ranking = join.GetRanking();
	if (ranking.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		// nothing to gain from reusing a column
		return false;
	}
	if (ranking.IsVolatile() || !PropagatesNull(ranking)) {
		return false;
	}
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*join.children[1], right_bindings);
	bool references_right = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(ranking, [&](const BoundColumnRefExpression &colref) {
		references_right |= right_bindings.count(colref.Binding().table_index) > 0;
	});
	return references_right;
}

void NearestJoinRanking::ReplaceRankings(unique_ptr<Expression> &expr,
                                         const vector<reference<LogicalNearestJoin>> &joins) {
	for (auto &join_ref : joins) {
		auto &join = join_ref.get();
		if (!expr->Equals(join.GetRanking())) {
			continue;
		}
		if (!join.OutputsRanking()) {
			join.ranking_index = optimizer.binder.GenerateTableIndex();
		}
		auto replacement =
		    make_uniq<BoundColumnRefExpression>(expr->GetAlias(), expr->GetReturnType(), join.GetRankingBinding());
		expr = std::move(replacement);
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) { ReplaceRankings(child, joins); });
}

vector<reference<LogicalNearestJoin>> NearestJoinRanking::VisitOperator(LogicalOperator &op) {
	vector<reference<LogicalNearestJoin>> joins;
	for (auto &child : op.children) {
		auto child_joins = VisitOperator(*child);
		joins.insert(joins.end(), child_joins.begin(), child_joins.end());
	}
	if (!joins.empty()) {
		LogicalOperatorVisitor::EnumerateExpressions(
		    op, [&](unique_ptr<Expression> *expr) { ReplaceRankings(*expr, joins); });
	}
	if (op.HasProjectionMap()) {
		// a new ranking output column would not be visible above this operator
		joins.clear();
	}
	if (op.type == LogicalOperatorType::LOGICAL_NEAREST_JOIN) {
		auto &join = op.Cast<LogicalNearestJoin>();
		if (CanReuseRanking(join)) {
			joins.push_back(join);
		}
	}
	return joins;
}

void NearestJoinRanking::Optimize(LogicalOperator &op) {
	VisitOperator(op);
}

} // namespace duckdb
