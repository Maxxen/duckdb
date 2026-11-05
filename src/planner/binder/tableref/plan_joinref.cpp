#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_binder/lateral_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_nearest_join.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_positional_join.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"

namespace duckdb {

//! Check if a filter can be safely pushed to the left child
//! This is used ONLY for join conditions in the ON clause, not for WHERE clause filters.
//! The logic determines whether a condition that references only the left side can be
//! pushed down as a filter on the left child operator.
static bool CanPushToLeftChild(JoinType type, JoinRefType ref_type) {
	// Unsupported arbitrary predicates for some ASOF types
	if (ref_type == JoinRefType::ASOF && type != JoinType::INNER && type != JoinType::LEFT) {
		return false;
	}

	switch (type) {
	case JoinType::INNER:
	case JoinType::SEMI:
	case JoinType::RIGHT:
		return true;
	case JoinType::ANTI:
	case JoinType::LEFT:
	case JoinType::OUTER:
		return false;
	default:
		return false;
	}
}

//! Check if a filter can be safely pushed to the right child
//! This is used ONLY for join conditions in the ON clause, not for WHERE clause filters.
//! The logic determines whether a condition that references only the right side can be
//! pushed down as a filter on the right child operator.
static bool CanPushToRightChild(JoinType type, JoinRefType ref_type) {
	// Unsupported arbitrary predicates for some ASOF types
	if (ref_type == JoinRefType::ASOF && type != JoinType::INNER && type != JoinType::LEFT) {
		return false;
	}

	switch (type) {
	case JoinType::INNER:
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::LEFT:
		return true;
	case JoinType::RIGHT:
	case JoinType::OUTER:
		return false;
	default:
		return false;
	}
}

//! Push a filter expression to a child operator
static void PushFilterToChild(unique_ptr<LogicalOperator> &child, unique_ptr<Expression> &expr) {
	if (child->type != LogicalOperatorType::LOGICAL_FILTER) {
		auto filter = make_uniq<LogicalFilter>();
		filter->AddChild(std::move(child));
		child = std::move(filter);
	}

	auto &filter = child->Cast<LogicalFilter>();
	filter.expressions.push_back(std::move(expr));
}

//! Check if a foldable expression evaluates to TRUE and can be eliminated
static bool CanEliminate(ClientContext &context, JoinType type, unique_ptr<Expression> &expr) {
	if (!expr->IsFoldable()) {
		return false;
	}

	Value result;
	if (!ExpressionExecutor::TryEvaluateScalar(context, *expr, result)) {
		return false;
	}

	if (result.IsNull()) {
		return false;
	}

	bool is_true = (result == Value(true));

	if (is_true) {
		switch (type) {
		case JoinType::INNER:
		case JoinType::LEFT:
		case JoinType::RIGHT:
		case JoinType::SEMI:
		case JoinType::ANTI:
		case JoinType::OUTER:
			return true;
		default:
			return false;
		}
	}

	return false;
}

//! Only use conditions that are valid for the join ref type
static bool IsJoinTypeCondition(const JoinRefType ref_type, const ExpressionType expr_type) {
	switch (ref_type) {
	case JoinRefType::ASOF:
		switch (expr_type) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_LESSTHAN:
			return true;
		default:
			return false;
		}
	default:
		return true;
	}
}

//! Check an expression is a usable comparison expression
static bool IsComparisonExpression(const Expression &expr) {
	switch (expr.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

//! Create a JoinCondition from a comparison
static bool CreateJoinCondition(Expression &expr, const unordered_set<TableIndex> &left_bindings,
                                const unordered_set<TableIndex> &right_bindings, vector<JoinCondition> &conditions) {
	// comparison
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &left_expr = BoundComparisonExpression::Left(comparison);
	auto &right_expr = BoundComparisonExpression::Right(comparison);
	auto left_side = JoinSide::GetJoinSide(left_expr, left_bindings, right_bindings);
	auto right_side = JoinSide::GetJoinSide(right_expr, left_bindings, right_bindings);
	const bool normal = (left_side == JoinSide::LEFT || left_side == JoinSide::NONE) &&
	                    (right_side == JoinSide::RIGHT || right_side == JoinSide::NONE);
	const bool reversed = (left_side == JoinSide::RIGHT || left_side == JoinSide::NONE) &&
	                      (right_side == JoinSide::LEFT || right_side == JoinSide::NONE);
	if (normal || reversed) {
		// join condition can be divided in a left/right side
		auto comp_type = expr.GetExpressionType();
		auto left = std::move(BoundComparisonExpression::LeftMutable(comparison));
		auto right = std::move(BoundComparisonExpression::RightMutable(comparison));
		if (!normal) {
			// left = right, right = left, flip the comparison symbol and reverse sides
			swap(left, right);
			comp_type = FlipComparisonExpression(comp_type);
		}
		conditions.push_back(JoinCondition(std::move(left), std::move(right), comp_type));
		return true;
	}
	return false;
}

//! Extract join conditions, pushing single-side filters to children when it's safe
void LogicalComparisonJoin::ExtractJoinConditions(ClientContext &context, JoinType type, JoinRefType ref_type,
                                                  unique_ptr<LogicalOperator> &left_child,
                                                  unique_ptr<LogicalOperator> &right_child,
                                                  const unordered_set<TableIndex> &left_bindings,
                                                  const unordered_set<TableIndex> &right_bindings,
                                                  vector<unique_ptr<Expression>> &expressions,
                                                  vector<JoinCondition> &conditions) {
	for (auto &expr : expressions) {
		auto side = JoinSide::GetJoinSide(*expr, left_bindings, right_bindings);

		if (side == JoinSide::NONE) {
			if (CanEliminate(context, type, expr)) {
				continue;
			}
		} else if (side == JoinSide::LEFT) {
			if (CanPushToLeftChild(type, ref_type)) {
				PushFilterToChild(left_child, expr);
				continue;
			}
		} else if (side == JoinSide::RIGHT) {
			if (CanPushToRightChild(type, ref_type)) {
				PushFilterToChild(right_child, expr);
				continue;
			}
		}
		if (side == JoinSide::BOTH || type == JoinType::MARK || type == JoinType::RIGHT_SEMI ||
		    type == JoinType::RIGHT_ANTI) {
			if (IsComparisonExpression(*expr) && IsJoinTypeCondition(ref_type, expr->GetExpressionType()) &&
			    CreateJoinCondition(*expr, left_bindings, right_bindings, conditions)) {
				continue;
			}
		}

		conditions.emplace_back(std::move(expr));
	}
}

void LogicalComparisonJoin::ExtractJoinConditions(ClientContext &context, JoinType type, JoinRefType ref_type,
                                                  unique_ptr<LogicalOperator> &left_child,
                                                  unique_ptr<LogicalOperator> &right_child,
                                                  vector<unique_ptr<Expression>> &expressions,
                                                  vector<JoinCondition> &conditions) {
	unordered_set<TableIndex> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*left_child, left_bindings);
	LogicalJoin::GetTableReferences(*right_child, right_bindings);
	return ExtractJoinConditions(context, type, ref_type, left_child, right_child, left_bindings, right_bindings,
	                             expressions, conditions);
}

void LogicalComparisonJoin::ExtractJoinConditions(ClientContext &context, JoinType type, JoinRefType ref_type,
                                                  unique_ptr<LogicalOperator> &left_child,
                                                  unique_ptr<LogicalOperator> &right_child,
                                                  unique_ptr<Expression> condition, vector<JoinCondition> &conditions) {
	// split the expressions by the AND clause
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(std::move(condition));
	LogicalFilter::SplitPredicates(expressions);
	return ExtractJoinConditions(context, type, ref_type, left_child, right_child, expressions, conditions);
}

//! Create the join operator based on conditions and join type
unique_ptr<LogicalOperator> LogicalComparisonJoin::CreateJoin(JoinType type, JoinRefType ref_type,
                                                              unique_ptr<LogicalOperator> left_child,
                                                              unique_ptr<LogicalOperator> right_child,
                                                              vector<JoinCondition> conditions) {
	// separate comparison and non-comparison conditions for validation
	vector<JoinCondition> comparison_conditions;
	vector<JoinCondition> non_comparison_conditions;
	for (auto &cond : conditions) {
		if (cond.IsComparison()) {
			comparison_conditions.push_back(std::move(cond));
		} else {
			non_comparison_conditions.push_back(std::move(cond));
		}
	}

	// validate ASOF join conditions
	auto is_asof = (ref_type == JoinRefType::ASOF);
	if (is_asof) {
		switch (type) {
		case JoinType::RIGHT:
		case JoinType::OUTER:
			//	We can't (yet) support arbitrary predicates with some ASOF joins
			if (!non_comparison_conditions.empty()) {
				throw NotImplementedException("Unsupported ASOF JOIN type (%s) with arbitrary predicate",
				                              EnumUtil::ToChars(type));
			}
			break;
		case JoinType::SEMI:
		case JoinType::ANTI:
			//	For these join types, we can use a regular join because the RHS match is not important
			//	But we will verify the requirements of an ASOF.
			is_asof = false;
			ref_type = JoinRefType::REGULAR;
			break;
		default:
			break;
		}
		idx_t asof_idx = comparison_conditions.size();
		for (size_t c = 0; c < comparison_conditions.size(); ++c) {
			auto &cond = comparison_conditions[c];
			switch (cond.GetComparisonType()) {
			case ExpressionType::COMPARE_EQUAL:
			case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			case ExpressionType::COMPARE_LESSTHAN:
				if (asof_idx < comparison_conditions.size()) {
					throw BinderException("Multiple ASOF JOIN inequalities");
				}
				asof_idx = c;
				break;
			default:
				throw BinderException("Invalid ASOF JOIN comparison");
			}
		}
		if (asof_idx >= comparison_conditions.size()) {
			throw BinderException("Missing ASOF JOIN inequality");
		}
	}

	// Reconstruct full conditions vector
	vector<JoinCondition> all_conditions;
	for (auto &cond : comparison_conditions) {
		all_conditions.push_back(std::move(cond));
	}
	for (auto &cond : non_comparison_conditions) {
		all_conditions.push_back(std::move(cond));
	}

	// what type of join to create now?
	// Case 1: ASOF join - use comparison join (all conditions already in vector)
	if (is_asof) {
		auto asof_join = make_uniq<LogicalComparisonJoin>(type, LogicalOperatorType::LOGICAL_ASOF_JOIN);
		asof_join->conditions = std::move(all_conditions);
		asof_join->children.push_back(std::move(left_child));
		asof_join->children.push_back(std::move(right_child));
		return std::move(asof_join);
	}

	// Case 2: No comparison conditions - use any join
	if (comparison_conditions.empty()) {
		if (all_conditions.empty()) {
			all_conditions.emplace_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
		}

		auto any_join = make_uniq<LogicalAnyJoin>(type);
		any_join->children.push_back(std::move(left_child));
		any_join->children.push_back(std::move(right_child));
		any_join->condition = JoinCondition::CreateExpression(std::move(all_conditions));
		return std::move(any_join);
	}

	// Case 3: Has comparison conditions - Use comparison join
	auto comp_join = make_uniq<LogicalComparisonJoin>(type, LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
	comp_join->conditions = std::move(all_conditions);
	comp_join->children.push_back(std::move(left_child));
	comp_join->children.push_back(std::move(right_child));

	return std::move(comp_join);
}

static bool HasCorrelatedColumns(const Expression &root_expr) {
	bool has_correlated_columns = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(root_expr,
	                                                              [&](const BoundColumnRefExpression &colref) {
		                                                              if (colref.Depth() > 0) {
			                                                              has_correlated_columns = true;
		                                                              }
	                                                              });
	return has_correlated_columns;
}

//! Whether the plan references columns that are not produced inside of it
static bool HasOuterReferences(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return true;
	}
	bool has_outer_references = false;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
		has_outer_references |= HasCorrelatedColumns(**expr) || (*expr)->HasSubquery();
	});
	for (auto &child : op.children) {
		has_outer_references |= HasOuterReferences(*child);
	}
	return has_outer_references;
}

static bool IsConstantTrue(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	auto &value = expr.Cast<BoundConstantExpression>().GetValue();
	return value.type() == LogicalType::BOOLEAN && !value.IsNull() && BooleanValue::Get(value);
}

//! The output binding of `prune` that passes through the column `binding` of the input of `projection`. If the
//! subquery does not return the column, it is added to both projections as an additional, unreferenced column.
static ColumnBinding GetPrunedColumn(LogicalProjection &prune, LogicalProjection &projection,
                                     const BoundColumnRefExpression &colref) {
	auto &binding = colref.Binding();
	for (idx_t proj_idx = 0; proj_idx < projection.expressions.size(); proj_idx++) {
		auto &proj_expr = *projection.expressions[proj_idx];
		if (proj_expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    proj_expr.Cast<BoundColumnRefExpression>().Binding() != binding) {
			continue;
		}
		for (idx_t prune_idx = 0; prune_idx < prune.expressions.size(); prune_idx++) {
			auto &prune_ref = prune.expressions[prune_idx]->Cast<BoundColumnRefExpression>();
			if (prune_ref.Binding() == ColumnBinding(projection.table_index, ProjectionIndex(proj_idx))) {
				return ColumnBinding(prune.table_index, ProjectionIndex(prune_idx));
			}
		}
	}
	const ColumnBinding projection_binding(projection.table_index, ProjectionIndex(projection.expressions.size()));
	projection.expressions.push_back(make_uniq<BoundColumnRefExpression>(colref.GetReturnType(), binding));
	const ColumnBinding prune_binding(prune.table_index, ProjectionIndex(prune.expressions.size()));
	prune.expressions.push_back(make_uniq<BoundColumnRefExpression>(colref.GetReturnType(), projection_binding));
	return prune_binding;
}

//! Plans a lateral join against the top-k rows of a target as a LogicalNearestJoin:
//!   <left> [LEFT] JOIN LATERAL (SELECT * FROM <target> [WHERE ...] ORDER BY <ranking> LIMIT <k>) ON TRUE
//! The ranking may reference the left side, the rest of the subquery may not.
static unique_ptr<LogicalOperator> TryPlanLateralNearestJoin(ClientContext &context, BoundJoinRef &ref,
                                                             unique_ptr<LogicalOperator> &left,
                                                             unique_ptr<LogicalOperator> &right) {
	if (ref.type != JoinType::INNER && ref.type != JoinType::LEFT) {
		return nullptr;
	}
	if (ref.condition && !IsConstantTrue(*ref.condition)) {
		return nullptr;
	}
	if (!ref.left_binder || !ref.left_binder->correlated_columns.empty()) {
		return nullptr;
	}
	for (auto &correlated : ref.correlated_columns) {
		if (correlated.depth != 1) {
			return nullptr;
		}
	}

	// match PROJECTION (prune) <- LIMIT <- ORDER BY <- PROJECTION [<- FILTER]
	if (right->type != LogicalOperatorType::LOGICAL_PROJECTION ||
	    right->children[0]->type != LogicalOperatorType::LOGICAL_LIMIT) {
		return nullptr;
	}
	auto &prune = right->Cast<LogicalProjection>();
	auto &limit = prune.children[0]->Cast<LogicalLimit>();
	if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE || limit.offset_val.Type() != LimitNodeType::UNSET ||
	    limit.limit_val.GetConstantValue() == 0 || limit.children[0]->type != LogicalOperatorType::LOGICAL_ORDER_BY) {
		return nullptr;
	}
	auto &order = limit.children[0]->Cast<LogicalOrder>();
	if (order.orders.size() != 1 || order.HasProjectionMap() ||
	    order.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return nullptr;
	}
	auto &order_node = order.orders[0];
	auto &projection = order.children[0]->Cast<LogicalProjection>();
	if (order_node.expression->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	auto &order_ref = order_node.expression->Cast<BoundColumnRefExpression>();
	// the ranking is only ordered on, the subquery does not return it
	const auto ranking_index = projection.expressions.size() - 1;
	if (order_ref.Depth() != 0 ||
	    order_ref.Binding() != ColumnBinding(projection.table_index, ProjectionIndex(ranking_index))) {
		return nullptr;
	}
	for (auto &prune_expr : prune.expressions) {
		if (prune_expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return nullptr;
		}
		auto &prune_ref = prune_expr->Cast<BoundColumnRefExpression>();
		if (prune_ref.Depth() != 0 || prune_ref.Binding().table_index != projection.table_index ||
		    prune_ref.Binding().column_index == ProjectionIndex(ranking_index)) {
			return nullptr;
		}
	}

	// the ranking can reference the left side and the columns of the target
	auto &ranking = projection.expressions[ranking_index];
	if (ranking->HasSubquery()) {
		return nullptr;
	}
	unordered_set<TableIndex> left_bindings;
	LogicalJoin::GetTableReferences(*left, left_bindings);
	bool valid_ranking = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(*ranking, [&](const BoundColumnRefExpression &col) {
		if (col.Depth() == 1) {
			valid_ranking &= left_bindings.count(col.Binding().table_index) > 0;
		} else if (col.Depth() > 1) {
			valid_ranking = false;
		}
	});
	if (!valid_ranking) {
		return nullptr;
	}

	// a "WHERE <ranking> IS NOT NULL" gives the subquery the NULL semantics of NEAREST BY
	optional_ptr<LogicalFilter> filter;
	optional_idx not_null_index;
	if (projection.children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
		filter = projection.children[0]->Cast<LogicalFilter>();
		for (idx_t i = 0; i < filter->expressions.size(); i++) {
			auto &expr = *filter->expressions[i];
			if (expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL &&
			    expr.Cast<BoundOperatorExpression>().GetChildren()[0]->Equals(*ranking)) {
				not_null_index = i;
				break;
			}
		}
		if (filter->HasProjectionMap()) {
			return nullptr;
		}
	}
	const bool include_null_rankings = !not_null_index.IsValid();
	auto null_order = DBConfig::GetConfig(context).ResolveNullOrder(context, order_node.type, order_node.null_order);
	if (include_null_rankings && null_order != OrderByNullType::NULLS_LAST) {
		return nullptr;
	}

	// apart from the ranking, the subquery must not be correlated
	for (idx_t i = 0; i < ranking_index; i++) {
		if (HasCorrelatedColumns(*projection.expressions[i]) || projection.expressions[i]->HasSubquery()) {
			return nullptr;
		}
	}
	if (filter) {
		for (idx_t i = 0; i < filter->expressions.size(); i++) {
			if (not_null_index.IsValid() && i == not_null_index.GetIndex()) {
				continue;
			}
			if (HasCorrelatedColumns(*filter->expressions[i]) || filter->expressions[i]->HasSubquery()) {
				return nullptr;
			}
		}
	}
	auto &target_source = filter ? *filter->children[0] : *projection.children[0];
	if (HasOuterReferences(target_source)) {
		return nullptr;
	}

	// rewrite the ranking to refer to the left side and to the columns returned by the target
	// (the ranking is removed from the projection first, so that added columns do not shift it)
	auto ranking_expr = std::move(projection.expressions[ranking_index]);
	projection.expressions.pop_back();
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    ranking_expr, [&](BoundColumnRefExpression &col, unique_ptr<Expression> &) {
		    if (col.Depth() == 1) {
			    col.DepthMutable() = 0;
		    } else {
			    col.BindingMutable() = GetPrunedColumn(prune, projection, col);
		    }
	    });
	if (not_null_index.IsValid()) {
		filter->expressions.erase_at(not_null_index.GetIndex());
		if (filter->expressions.empty()) {
			projection.children[0] = std::move(filter->children[0]);
		}
	}
	const auto order_type = order_node.type;
	const auto nearest_count = limit.limit_val.GetConstantValue();
	prune.children[0] = std::move(order.children[0]);

	auto join = make_uniq<LogicalNearestJoin>(ref.type, std::move(ranking_expr), nearest_count, order_type, false);
	join->include_null_rankings = include_null_rankings;
	join->children.push_back(std::move(left));
	join->children.push_back(std::move(right));
	return std::move(join);
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::CreateJoin(ClientContext &context, JoinType type,
                                                              JoinRefType reftype,
                                                              unique_ptr<LogicalOperator> left_child,
                                                              unique_ptr<LogicalOperator> right_child,
                                                              unique_ptr<Expression> condition) {
	vector<JoinCondition> conditions;
	LogicalComparisonJoin::ExtractJoinConditions(context, type, reftype, left_child, right_child, std::move(condition),
	                                             conditions);
	return LogicalComparisonJoin::CreateJoin(type, reftype, std::move(left_child), std::move(right_child),
	                                         std::move(conditions));
}

static void PlanMarkJoin(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.TryGetMarkJoinGroupTypes(join.mark_types)) {
			return;
		}
		bool all_equal = !join.conditions.empty();
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison()) {
				all_equal = false;
				break;
			}
			all_equal &= condition.GetComparisonType() == ExpressionType::COMPARE_EQUAL;
		}
		if (all_equal || join.HasNullSafeMarkJoinConditions() ||
		    (join.conditions.size() == 1 && join.conditions[0].IsComparison())) {
			return;
		}
	}
	throw NotImplementedException("Unsupported explicit MARK join conditions");
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundJoinRef &ref) {
	auto old_is_outside_flattened = is_outside_flattened;
	// Plan laterals from outermost to innermost
	if (ref.lateral) {
		// Set the flag to ensure that children do not flatten before the root
		is_outside_flattened = false;
	}
	auto left = std::move(ref.left.plan);
	auto right = std::move(ref.right.plan);
	is_outside_flattened = old_is_outside_flattened;

	// For joins, depth of the bindings will be one higher on the right because of the lateral binder
	// If the current join does not have correlations between left and right, then the right bindings
	// have depth 1 too high and can be reduced by 1 throughout
	if (!ref.lateral && !ref.correlated_columns.empty()) {
		LateralBinder::ReduceExpressionDepth(*right, ref.correlated_columns);
	}

	if (ref.lateral) {
		if (ref.type == JoinType::MARK) {
			throw NotImplementedException("Unsupported explicit MARK join conditions");
		}
		auto nearest_join = TryPlanLateralNearestJoin(context, ref, left, right);
		if (nearest_join) {
			return nearest_join;
		}
		auto new_plan = PlanLateralJoin(std::move(left), std::move(right), ref.correlated_columns, ref.type,
		                                std::move(ref.condition));
		return new_plan;
	}
	switch (ref.ref_type) {
	case JoinRefType::CROSS:
		return LogicalCrossProduct::Create(std::move(left), std::move(right));
	case JoinRefType::POSITIONAL:
		return LogicalPositionalJoin::Create(std::move(left), std::move(right));
	default:
		break;
	}
	auto has_dependent_condition = ref.ref_type == JoinRefType::REGULAR && ref.type != JoinType::INNER &&
	                               (ref.condition->HasSubquery() || HasCorrelatedColumns(*ref.condition)) &&
	                               ref.duplicate_eliminated_columns.empty();
	if (!has_dependent_condition && ref.type == JoinType::RIGHT && ref.ref_type != JoinRefType::ASOF &&
	    !Optimizer::OptimizerDisabled(context, OptimizerType::BUILD_SIDE_PROBE_SIDE)) {
		// Binding follows the syntactic order. Normalize finalized RIGHT joins only after both sides and ON are bound.
		ref.type = JoinType::LEFT;
		std::swap(left, right);
	}
	if (ref.type == JoinType::INNER && (ref.condition->HasSubquery() || HasCorrelatedColumns(*ref.condition)) &&
	    ref.ref_type == JoinRefType::REGULAR) {
		// inner join, generate a cross product + filter
		// this will be later turned into a proper join by the join order optimizer
		auto root = LogicalCrossProduct::Create(std::move(left), std::move(right));

		auto filter = make_uniq<LogicalFilter>(std::move(ref.condition));
		// visit the expressions in the filter
		for (auto &expression : filter->expressions) {
			PlanSubqueries(expression, root);
		}
		filter->AddChild(std::move(root));
		return std::move(filter);
	}
	if (has_dependent_condition) {
		if (ref.type == JoinType::MARK) {
			throw NotImplementedException("Unsupported explicit MARK join conditions");
		}
		auto join = make_uniq<LogicalAnyJoin>(ref.type);
		join->children.push_back(std::move(left));
		join->children.push_back(std::move(right));
		join->condition = std::move(ref.condition);
		join->mark_index = ref.mark_index;
		return std::move(join);
	}
	// now create the join operator from the join condition
	auto result = LogicalComparisonJoin::CreateJoin(context, ref.type, ref.ref_type, std::move(left), std::move(right),
	                                                std::move(ref.condition));
	optional_ptr<LogicalOperator> join;
	if (result->type == LogicalOperatorType::LOGICAL_FILTER) {
		join = result->children[0].get();
	} else {
		join = result.get();
	}

	if (ref.type == JoinType::MARK) {
		join->Cast<LogicalJoin>().mark_index = ref.mark_index;
		PlanMarkJoin(*join);
	}
	if (!ref.duplicate_eliminated_columns.empty()) {
		D_ASSERT(join->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
		auto &comp_join = join->Cast<LogicalComparisonJoin>();
		comp_join.type = LogicalOperatorType::LOGICAL_DELIM_JOIN;
		comp_join.delim_flipped = ref.delim_flipped;
		for (auto &col : ref.duplicate_eliminated_columns) {
			comp_join.duplicate_eliminated_columns.emplace_back(col->Copy());
		}
	}
	return result;
}

} // namespace duckdb
