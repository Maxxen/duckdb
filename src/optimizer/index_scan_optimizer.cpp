#include "duckdb/optimizer/index_scan_optimizer.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/function/table/index_scan.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/index/index_join.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include <functional>
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/table_index_list.hpp"

namespace duckdb {

IndexScanOptimizer::IndexScanOptimizer(Optimizer &optimizer) : optimizer(optimizer), context(optimizer.context) {
}

bool IndexScanOptimizer::TryBindPredicate(LogicalGet &get, const Expression &predicate,
                                          optional_ptr<BoundIndex> &out_index, unique_ptr<IndexMatch> &out_match) {
	auto table = get.GetTable();
	if (!table || !table->IsDuckTable()) {
		return false;
	}
	auto &table_info = *table->GetStorage().GetDataTableInfo();
	auto &index_types = DBConfig::GetConfig(context).GetIndexTypes();

	table_info.BindIndexes(context);
	for (auto &index : table_info.GetIndexes().Indexes()) {
		if (!index.IsBound()) {
			continue;
		}
		auto &bound = index.Cast<BoundIndex>();
		auto index_type = index_types.FindByName(bound.GetIndexType());
		if (!index_type || !index_type->optimizer_capabilities.Supports(IndexScanCapability::PREDICATE_SCAN)) {
			continue;
		}
		auto match = bound.TryBindPredicateScan(context, get, predicate);
		if (match) {
			out_index = &bound;
			out_match = std::move(match);
			return true;
		}
	}
	return false;
}

void IndexScanOptimizer::RewriteToIndexScan(unique_ptr<LogicalOperator> &get_ptr, optional_ptr<LogicalFilter> filter,
                                            BoundIndex &index, IndexMatch &match) {
	auto &get = get_ptr->Cast<LogicalGet>();
	auto &duck_table = get.GetTable()->Cast<DuckTableEntry>();

	auto bind_data =
	    make_uniq<IndexScanBindData>(duck_table, index, std::move(match.query_params), match.estimated_cardinality);
	get.function = IndexScanFunction::GetFunction();
	const auto cardinality = get.function.cardinality(context, bind_data.get());
	get.has_estimated_cardinality = cardinality->has_estimated_cardinality;
	get.estimated_cardinality = cardinality->estimated_cardinality;
	get.bind_data = std::move(bind_data);

	if (!get.table_filters.HasFilters()) {
		return;
	}

	// Before clearing projection ids, fix up the enclosing filter's projection map (if any).
	if (!get.projection_ids.empty() && filter) {
		for (auto &id : filter->projection_map) {
			id = get.projection_ids[id];
		}
	}
	get.projection_ids.clear();
	get.types.clear();

	// The index scan does not apply table filters; pull them up into a LogicalFilter above the get.
	// For a coarse (inexact) index this LogicalFilter is the required residual recheck.
	auto new_filter = make_uniq<LogicalFilter>();
	for (const auto &entry : get.table_filters) {
		auto index_ref = entry.GetIndex();
		auto &column_id = get.GetColumnIndex(index_ref);
		auto &type = get.returned_types[column_id.GetPrimaryIndex()];
		auto column = make_uniq<BoundColumnRefExpression>(type, ColumnBinding(get.table_index, index_ref));
		new_filter->expressions.push_back(entry.Filter().ToExpression(*column));
	}
	new_filter->children.push_back(std::move(get_ptr));
	new_filter->ResolveOperatorTypes();
	get_ptr = std::move(new_filter);
}

bool IndexScanOptimizer::TryOptimizeGet(unique_ptr<LogicalOperator> &get_ptr, optional_ptr<LogicalFilter> filter,
                                        const Expression &predicate) {
	auto &get = get_ptr->Cast<LogicalGet>();
	if (get.function.name != "seq_scan") {
		return false;
	}
	// Do not fire when dynamic (runtime) filters could further restrict the scan.
	if (get.dynamic_filters && get.dynamic_filters->HasFilters()) {
		return false;
	}
	optional_ptr<BoundIndex> index;
	unique_ptr<IndexMatch> match;
	if (!TryBindPredicate(get, predicate, index, match)) {
		return false;
	}
	RewriteToIndexScan(get_ptr, filter, *index, *match);
	return true;
}

bool IndexScanOptimizer::TryOptimizePredicateScan(unique_ptr<LogicalOperator> &plan) {
	auto &op = *plan;

	// Shape A: LOGICAL_FILTER with a single predicate over a seq_scan LogicalGet.
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		if (filter.expressions.size() != 1) {
			return false;
		}
		if (filter.children.front()->type != LogicalOperatorType::LOGICAL_GET) {
			return false;
		}
		return TryOptimizeGet(filter.children.front(), filter, *filter.expressions[0]);
	}

	// Shape B: a seq_scan LogicalGet carrying pushed-down expression filters.
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		for (auto &entry : get.table_filters) {
			auto proj_id = entry.GetIndex();
			auto &table_filter = entry.Filter();
			if (table_filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
				continue;
			}
			auto &column_id = get.GetColumnIndex(proj_id);
			// Skip filters on virtual columns (e.g. rowid): no predicate index applies, and they
			// have no entry in returned_types.
			if (column_id.GetPrimaryIndex() >= get.returned_types.size()) {
				continue;
			}
			auto &type = get.returned_types[column_id.GetPrimaryIndex()];
			auto column_ref = make_uniq<BoundColumnRefExpression>(type, ColumnBinding(get.table_index, proj_id));
			auto predicate = table_filter.ToExpression(*column_ref);
			if (TryOptimizeGet(plan, nullptr, *predicate)) {
				return true;
			}
		}
		return false;
	}
	return false;
}

void IndexScanOptimizer::OptimizePredicateScan(unique_ptr<LogicalOperator> &plan) {
	if (!TryOptimizePredicateScan(plan)) {
		for (auto &child : plan->children) {
			OptimizePredicateScan(child);
		}
	}
}

//===--------------------------------------------------------------------===//
// OrderLimit scan (e.g. HNSW KNN: ORDER BY distance(col, const) LIMIT k)
//===--------------------------------------------------------------------===//
bool IndexScanOptimizer::TryBindOrderLimit(LogicalGet &get, const Expression &order_expr, idx_t limit,
                                           optional_ptr<BoundIndex> &out_index, unique_ptr<IndexMatch> &out_match) {
	auto table = get.GetTable();
	if (!table || !table->IsDuckTable()) {
		return false;
	}
	auto &table_info = *table->GetStorage().GetDataTableInfo();
	auto &index_types = DBConfig::GetConfig(context).GetIndexTypes();

	table_info.BindIndexes(context);
	for (auto &index : table_info.GetIndexes().Indexes()) {
		if (!index.IsBound()) {
			continue;
		}
		auto &bound = index.Cast<BoundIndex>();
		auto index_type = index_types.FindByName(bound.GetIndexType());
		if (!index_type || !index_type->optimizer_capabilities.Supports(IndexScanCapability::ORDER_LIMIT_SCAN)) {
			continue;
		}
		auto match = bound.TryBindOrderScan(context, get, IndexOrderKind::DISTANCE_ASC, order_expr, limit);
		if (match) {
			out_index = &bound;
			out_match = std::move(match);
			return true;
		}
	}
	return false;
}

bool IndexScanOptimizer::TryOptimizeOrderLimitScan(unique_ptr<LogicalOperator> &plan) {
	auto &op = *plan;
	if (op.type != LogicalOperatorType::LOGICAL_TOP_N) {
		return false;
	}
	auto &top_n = op.Cast<LogicalTopN>();
	if (top_n.orders.size() != 1) {
		return false;
	}
	const auto &order = top_n.orders[0];
	if (order.type != OrderType::ASCENDING) {
		return false;
	}
	// The order expression must reference a column produced by the child projection.
	if (order.expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	const auto &bound_column_ref = order.expression->Cast<BoundColumnRefExpression>();
	if (top_n.children.size() != 1 || top_n.children.front()->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	auto &projection = top_n.children.front()->Cast<LogicalProjection>();
	if (bound_column_ref.Binding().column_index >= projection.expressions.size()) {
		return false;
	}
	auto &projection_expr = *projection.expressions[bound_column_ref.Binding().column_index];

	if (projection.children.size() != 1 || projection.children.front()->type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get_ptr = projection.children.front();
	auto &get = get_ptr->Cast<LogicalGet>();
	if (get.function.name != "seq_scan") {
		return false;
	}
	if (get.dynamic_filters && get.dynamic_filters->HasFilters()) {
		return false;
	}

	optional_ptr<BoundIndex> index;
	unique_ptr<IndexMatch> match;
	if (!TryBindOrderLimit(get, projection_expr, top_n.limit, index, match)) {
		return false;
	}

	// Swap the get to an ordered index scan (pulling up residual filters), then - since the index
	// returns exactly the k ordered rows - delete the TopN by splicing the projection up.
	RewriteToIndexScan(get_ptr, nullptr, *index, *match);
	plan = std::move(top_n.children[0]);
	return true;
}

void IndexScanOptimizer::MergeProjections(unique_ptr<LogicalOperator> &plan) {
	if (plan->type == LogicalOperatorType::LOGICAL_PROJECTION &&
	    plan->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &child = plan->children[0];
		if (child->children[0]->type == LogicalOperatorType::LOGICAL_GET &&
		    child->children[0]->Cast<LogicalGet>().function.name == "index_scan") {
			auto &parent_projection = plan->Cast<LogicalProjection>();
			auto &child_projection = child->Cast<LogicalProjection>();

			column_binding_set_t referenced_bindings;
			for (auto &expr : parent_projection.expressions) {
				ExpressionIterator::EnumerateExpression(expr, [&](Expression &expr_ref) {
					if (expr_ref.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
						referenced_bindings.insert(expr_ref.Cast<BoundColumnRefExpression>().Binding());
					}
				});
			}
			auto child_bindings = child_projection.GetColumnBindings();
			for (idx_t i = 0; i < child_projection.expressions.size(); i++) {
				if (referenced_bindings.find(child_bindings[i]) == referenced_bindings.end()) {
					// Not referenced: replace with a constant to preserve positionality.
					child_projection.expressions[i] =
					    make_uniq_base<Expression, BoundConstantExpression>(Value(LogicalType::TINYINT));
				}
			}
			return;
		}
	}
	for (auto &child : plan->children) {
		MergeProjections(child);
	}
}

void IndexScanOptimizer::OptimizeOrderLimitRecursive(unique_ptr<LogicalOperator> &plan) {
	if (!TryOptimizeOrderLimitScan(plan)) {
		for (auto &child : plan->children) {
			OptimizeOrderLimitRecursive(child);
		}
	}
}

void IndexScanOptimizer::OptimizeOrderLimitScan(unique_ptr<LogicalOperator> &plan) {
	OptimizeOrderLimitRecursive(plan);
	MergeProjections(plan);
}

//===--------------------------------------------------------------------===//
// Index join (e.g. HNSW knn-join: per outer row, k nearest inner rows)
//===--------------------------------------------------------------------===//
bool IndexScanOptimizer::TryBindIndexJoin(LogicalGet &inner_get, const Expression &distance_expr, idx_t k,
                                          optional_ptr<BoundIndex> &out_index, unique_ptr<IndexMatch> &out_match) {
	auto table = inner_get.GetTable();
	if (!table || !table->IsDuckTable()) {
		return false;
	}
	auto &table_info = *table->GetStorage().GetDataTableInfo();
	auto &index_types = DBConfig::GetConfig(context).GetIndexTypes();

	table_info.BindIndexes(context);
	for (auto &index : table_info.GetIndexes().Indexes()) {
		if (!index.IsBound()) {
			continue;
		}
		auto &bound = index.Cast<BoundIndex>();
		auto index_type = index_types.FindByName(bound.GetIndexType());
		if (!index_type || !index_type->optimizer_capabilities.Supports(IndexScanCapability::ORDER_LIMIT_JOIN)) {
			continue;
		}
		auto match = bound.TryBindOrderLimitJoin(context, inner_get, distance_expr, k);
		if (match) {
			out_index = &bound;
			out_match = std::move(match);
			return true;
		}
	}
	return false;
}

bool IndexScanOptimizer::TryOptimizeIndexJoin(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &plan) {
	// Match the subtree DuckDB plans for a lateral per-outer-row KNN:
	//   delim_join
	//     -> [1] seq_scan (outer / probe side)
	//     -> [0] (optional projection) -> filter -> window -> projection -> cross_product
	//                                                                          -> delim_get
	//                                                                          -> seq_scan (inner)
#define MATCH_OPERATOR(OP, TYPE, CHILD_COUNT)                                                                          \
	if ((OP)->type != LogicalOperatorType::TYPE || ((OP)->children.size() != (CHILD_COUNT))) {                          \
		return false;                                                                                                  \
	}

	MATCH_OPERATOR(plan, LOGICAL_DELIM_JOIN, 2);
	auto &delim_join = plan->Cast<LogicalJoin>();

	MATCH_OPERATOR(delim_join.children[1], LOGICAL_GET, 0);
	auto outer_get_ptr = &delim_join.children[1];
	auto &outer_get = (*outer_get_ptr)->Cast<LogicalGet>();

	const unique_ptr<LogicalOperator> *filter_ptr = nullptr;
	const unique_ptr<LogicalOperator> *outer_proj_ptr = nullptr;
	auto &delim_child = delim_join.children[0];
	if (delim_child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &filter_proj = delim_child->Cast<LogicalProjection>();
		if (filter_proj.children.back()->type != LogicalOperatorType::LOGICAL_FILTER) {
			return false;
		}
		outer_proj_ptr = &delim_child;
		filter_ptr = &filter_proj.children.back();
	} else if (delim_child->type == LogicalOperatorType::LOGICAL_FILTER) {
		filter_ptr = &delim_child;
	} else {
		return false;
	}
	auto &filter = (*filter_ptr)->Cast<LogicalFilter>();

	MATCH_OPERATOR(filter.children.back(), LOGICAL_WINDOW, 1);
	auto &window = filter.children.back()->Cast<LogicalWindow>();
	MATCH_OPERATOR(window.children[0], LOGICAL_PROJECTION, 1);
	auto &inner_proj = window.children[0]->Cast<LogicalProjection>();
	MATCH_OPERATOR(inner_proj.children[0], LOGICAL_CROSS_PRODUCT, 2);
	auto &cross_product = inner_proj.children[0]->Cast<LogicalCrossProduct>();
#undef MATCH_OPERATOR

	unique_ptr<LogicalOperator> *delim_get_ptr;
	unique_ptr<LogicalOperator> *inner_get_ptr;
	auto &cp_lhs = cross_product.children[0];
	auto &cp_rhs = cross_product.children[1];
	if (cp_lhs->type == LogicalOperatorType::LOGICAL_DELIM_GET && cp_rhs->type == LogicalOperatorType::LOGICAL_GET) {
		delim_get_ptr = &cp_lhs;
		inner_get_ptr = &cp_rhs;
	} else if (cp_rhs->type == LogicalOperatorType::LOGICAL_DELIM_GET &&
	           cp_lhs->type == LogicalOperatorType::LOGICAL_GET) {
		delim_get_ptr = &cp_rhs;
		inner_get_ptr = &cp_lhs;
	} else {
		return false;
	}
	auto &delim_get = (*delim_get_ptr)->Cast<LogicalDelimGet>();
	auto &inner_get = (*inner_get_ptr)->Cast<LogicalGet>();
	if (inner_get.function.name != "seq_scan") {
		return false;
	}

	// The filter must be `row_number <= k` referencing the window's row_number column.
	if (filter.expressions.size() != 1) {
		return false;
	}
	if (filter.expressions.back()->GetExpressionType() != ExpressionType::COMPARE_LESSTHANOREQUALTO) {
		return false;
	}
	const auto &compare_expr = filter.expressions.back()->Cast<BoundFunctionExpression>();
	if (BoundComparisonExpression::Right(compare_expr).GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		return false;
	}
	const auto &constant_expr = BoundComparisonExpression::Right(compare_expr).Cast<BoundConstantExpression>();
	if (constant_expr.GetReturnType() != LogicalType::BIGINT) {
		return false;
	}
	auto k_value = constant_expr.GetValue().GetValue<int64_t>();
	if (k_value < 0 || k_value >= STANDARD_VECTOR_SIZE) {
		return false;
	}
	if (BoundComparisonExpression::Left(compare_expr).GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &filter_ref_expr = BoundComparisonExpression::Left(compare_expr).Cast<BoundColumnRefExpression>();
	if (filter_ref_expr.Binding().table_index != window.window_index ||
	    filter_ref_expr.Binding().column_index != ProjectionIndex(0)) {
		return false;
	}

	// The window must be a single row_number() ordered ASC by a distance expression in inner_proj.
	if (window.expressions.size() != 1 ||
	    window.expressions.back()->GetExpressionType() != ExpressionType::WINDOW_ROW_NUMBER) {
		return false;
	}
	auto &window_expr = window.expressions.back()->Cast<BoundWindowExpression>();
	if (window_expr.OrderBy().size() != 1 || window_expr.OrderBy().back().type != OrderType::ASCENDING) {
		return false;
	}
	if (window_expr.OrderBy().back().expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	const auto &distance_ref_expr = window_expr.OrderBy().back().expression->Cast<BoundColumnRefExpression>();
	if (distance_ref_expr.Binding().table_index != inner_proj.table_index ||
	    distance_ref_expr.Binding().column_index >= inner_proj.expressions.size()) {
		return false;
	}
	auto &distance_expr = *inner_proj.expressions[distance_ref_expr.Binding().column_index];

	// Ask a capable index on the inner table whether it can serve this probe.
	optional_ptr<BoundIndex> index;
	unique_ptr<IndexMatch> match;
	if (!TryBindIndexJoin(inner_get, distance_expr, idx_t(k_value), index, match)) {
		return false;
	}
	auto &duck_table = inner_get.GetTable()->Cast<DuckTableEntry>();

	// Build the core index join.
	auto index_join = make_uniq<LogicalIndexJoin>(optimizer.binder.GenerateTableIndex(), duck_table, *index,
	                                              std::move(match->query_params), /* emit_rank */ true);
	for (auto &column_id : inner_get.GetColumnIds()) {
		index_join->inner_column_ids.emplace_back(column_id.GetPrimaryIndex());
	}
	index_join->inner_projection_ids = inner_get.projection_ids;
	index_join->inner_returned_types = inner_get.returned_types;

	// Rewire column bindings: replace the delim-join output with a top projection, inline the inner
	// projection's expressions, and point references at the outer get / the index join.
	ColumnBindingReplacer replacer;
	vector<unique_ptr<Expression>> projection_expressions;
	auto projection_table_index = optimizer.binder.GenerateTableIndex();
	auto delim_bindings = delim_join.GetColumnBindings();
	auto delim_types = delim_join.types;

	ProjectionIndex new_binding_idx(0);
	for (idx_t i = 0; i < delim_bindings.size(); i++) {
		auto &old_binding = delim_bindings[i];
		if (old_binding.table_index == window.window_index) {
			continue; // the window row_number is not used past the filter
		}
		auto &old_type = delim_types[i];
		projection_expressions.push_back(make_uniq<BoundColumnRefExpression>(old_type, old_binding));
		replacer.replacement_bindings.emplace_back(old_binding,
		                                           ColumnBinding(projection_table_index, new_binding_idx++));
	}
	// The window row_number becomes the index join's rank column (its last inner output column).
	ColumnBinding window_binding(window.window_index, ProjectionIndex(0));
	projection_expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, window_binding));
	replacer.replacement_bindings.emplace_back(window_binding,
	                                           ColumnBinding(projection_table_index, new_binding_idx++));

	auto new_projection = make_uniq<LogicalProjection>(projection_table_index, std::move(projection_expressions));

	replacer.VisitOperator(*root);
	replacer.replacement_bindings.clear();

	for (auto &expr : new_projection->expressions) {
		auto &ref = expr->Cast<BoundColumnRefExpression>();
		if (ref.Binding().table_index == inner_get.table_index) {
			ref.BindingMutable().table_index = index_join->table_index;
		}
		if (outer_proj_ptr) {
			auto &outer_proj = outer_proj_ptr->get()->Cast<LogicalProjection>();
			if (ref.Binding().table_index == outer_proj.table_index) {
				const auto &outer_expr = outer_proj.GetExpression(ref.Binding());
				const auto &outer_ref = outer_expr.Cast<BoundColumnRefExpression>();
				ref.BindingMutable() = outer_ref.Binding();
			}
		}
		if (ref.Binding().table_index == inner_proj.table_index) {
			const auto &inner_expr = inner_proj.GetExpression(ref.Binding());
			expr = inner_expr.Copy();
		} else if (ref.Binding().table_index == window.window_index) {
			ColumnBinding rank_binding(index_join->table_index, ProjectionIndex(index_join->inner_column_ids.size()));
			expr = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, rank_binding);
		}
	}

	for (const auto &old_binding : delim_get.GetColumnBindings()) {
		replacer.replacement_bindings.emplace_back(old_binding,
		                                           ColumnBinding(outer_get.table_index, old_binding.column_index));
	}
	for (const auto &old_binding : inner_get.GetColumnBindings()) {
		replacer.replacement_bindings.emplace_back(old_binding,
		                                           ColumnBinding(index_join->table_index, old_binding.column_index));
	}
	replacer.VisitOperator(*new_projection);

	index_join->children.emplace_back(std::move(*outer_get_ptr));
	new_projection->children.emplace_back(std::move(index_join));
	new_projection->EstimateCardinality(context);
	plan = std::move(new_projection);

	// Re-estimate cardinalities across the (possibly larger) rewritten plan.
	root->EstimateCardinality(context);
	return true;
}

// Origin of a column produced by the correlated subtree, traced back to the physical relation the
// index join will expose.
namespace {
struct ArgMinOrigin {
	enum class Kind { INNER, DELIM, OUTER, DISTANCE, UNKNOWN };
	Kind kind = Kind::UNKNOWN;
	ColumnBinding binding;
};

//! Match a single arg_min/min_by top-k aggregate over one group; extract k and the order (distance)
//! column reference. Returns the aggregate expression, or nullptr if not applicable.
optional_ptr<BoundAggregateExpression> MatchArgMinAggregate(LogicalAggregate &aggregate, int64_t &k_value_out,
                                                            optional_ptr<BoundColumnRefExpression> &order_ref_out) {
	if (aggregate.expressions.size() != 1 || aggregate.groups.size() != 1) {
		return nullptr;
	}
	if (aggregate.expressions[0]->GetExpressionType() != ExpressionType::BOUND_AGGREGATE) {
		return nullptr;
	}
	auto &agg_expr = aggregate.expressions[0]->Cast<BoundAggregateExpression>();
	const auto agg_name = agg_expr.Function().GetName();
	if (agg_name != "arg_min_nulls_last" && agg_name != "arg_min" && agg_name != "min_by") {
		return nullptr;
	}
	if (agg_expr.GetChildren().size() < 2 || agg_expr.GetChildren().size() > 3) {
		return nullptr;
	}
	int64_t k_value = 1;
	if (agg_expr.GetChildren().size() == 3) {
		if (agg_expr.GetChildren()[2]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
			return nullptr;
		}
		k_value = agg_expr.GetChildren()[2]->Cast<BoundConstantExpression>().GetValue().GetValue<int64_t>();
	}
	if (k_value <= 0 || k_value >= STANDARD_VECTOR_SIZE) {
		return nullptr;
	}
	if (agg_expr.GetChildren()[1]->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return nullptr;
	}
	k_value_out = k_value;
	order_ref_out = &agg_expr.GetChildren()[1]->Cast<BoundColumnRefExpression>();
	return &agg_expr;
}
} // namespace

bool IndexScanOptimizer::TryOptimizeIndexJoinArgMin(unique_ptr<LogicalOperator> &root,
                                                    unique_ptr<LogicalOperator> &plan) {
	// delim_join
	//   -> get (outer/lhs)                              [either child]
	//   -> projection(s) -> [unnest] -> aggregate arg_min(payload, distance, [k]) group by <corr>
	//        -> projection "inner" -> cross_product -> {delim_get, get (rhs, seq_scan, indexed)}
	if (plan->type != LogicalOperatorType::LOGICAL_DELIM_JOIN || plan->children.size() != 2) {
		return false;
	}
	auto &delim_join = plan->Cast<LogicalJoin>();

	unique_ptr<LogicalOperator> *outer_get_ptr = nullptr;
	unique_ptr<LogicalOperator> *corr_ptr = nullptr;
	for (idx_t i = 0; i < 2; i++) {
		if (delim_join.children[i]->type == LogicalOperatorType::LOGICAL_GET &&
		    delim_join.children[i]->children.empty()) {
			outer_get_ptr = &delim_join.children[i];
			corr_ptr = &delim_join.children[1 - i];
			break;
		}
	}
	if (!outer_get_ptr) {
		return false;
	}
	auto &outer_get = (*outer_get_ptr)->Cast<LogicalGet>();

	vector<reference<LogicalOperator>> corr_chain;
	LogicalOperator *cursor = corr_ptr->get();
	while (cursor && cursor->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		if ((cursor->type != LogicalOperatorType::LOGICAL_PROJECTION &&
		     cursor->type != LogicalOperatorType::LOGICAL_UNNEST) ||
		    cursor->children.size() != 1) {
			return false;
		}
		corr_chain.push_back(*cursor);
		cursor = cursor->children[0].get();
	}
	if (!cursor) {
		return false;
	}
	auto &aggregate = cursor->Cast<LogicalAggregate>();

	int64_t k_value = 1;
	optional_ptr<BoundColumnRefExpression> order_ref;
	auto agg_expr_ptr = MatchArgMinAggregate(aggregate, k_value, order_ref);
	if (!agg_expr_ptr) {
		return false;
	}
	auto &agg_expr = *agg_expr_ptr;

	if (aggregate.children.size() != 1 || aggregate.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	auto &inner_proj = aggregate.children[0]->Cast<LogicalProjection>();
	if (inner_proj.children.size() != 1 || inner_proj.children[0]->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return false;
	}
	auto &cross_product = inner_proj.children[0]->Cast<LogicalCrossProduct>();

	unique_ptr<LogicalOperator> *delim_get_ptr = nullptr;
	unique_ptr<LogicalOperator> *inner_get_ptr = nullptr;
	auto &cp_lhs = cross_product.children[0];
	auto &cp_rhs = cross_product.children[1];
	if (cp_lhs->type == LogicalOperatorType::LOGICAL_DELIM_GET && cp_rhs->type == LogicalOperatorType::LOGICAL_GET) {
		delim_get_ptr = &cp_lhs;
		inner_get_ptr = &cp_rhs;
	} else if (cp_rhs->type == LogicalOperatorType::LOGICAL_DELIM_GET &&
	           cp_lhs->type == LogicalOperatorType::LOGICAL_GET) {
		delim_get_ptr = &cp_rhs;
		inner_get_ptr = &cp_lhs;
	} else {
		return false;
	}
	auto &delim_get = (*delim_get_ptr)->Cast<LogicalDelimGet>();
	auto &inner_get = (*inner_get_ptr)->Cast<LogicalGet>();
	if (inner_get.function.name != "seq_scan") {
		return false;
	}

	const auto inner_proj_bindings = inner_proj.GetColumnBindings();
	bool order_in_inner_proj = false;
	for (auto &b : inner_proj_bindings) {
		if (b == order_ref->Binding()) {
			order_in_inner_proj = true;
			break;
		}
	}
	if (!order_in_inner_proj || order_ref->Binding().column_index >= inner_proj.expressions.size()) {
		return false;
	}
	auto &distance_expr_ptr = *inner_proj.expressions[order_ref->Binding().column_index];

	optional_ptr<BoundIndex> index;
	unique_ptr<IndexMatch> match;
	if (!TryBindIndexJoin(inner_get, distance_expr_ptr, idx_t(k_value), index, match)) {
		return false;
	}
	auto &duck_table = inner_get.GetTable()->Cast<DuckTableEntry>();

	// Resolver: trace a column produced by the correlated subtree to its physical origin.
	const auto inner_get_bindings = inner_get.GetColumnBindings();
	const auto delim_get_bindings = delim_get.GetColumnBindings();
	const auto outer_get_bindings = outer_get.GetColumnBindings();
	const auto agg_bindings = aggregate.GetColumnBindings();

	std::function<ArgMinOrigin(const ColumnBinding &)> resolve;
	std::function<ArgMinOrigin(Expression &)> resolve_expr;
	resolve_expr = [&](Expression &e) -> ArgMinOrigin {
		if (e.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			return resolve(e.Cast<BoundColumnRefExpression>().Binding());
		}
		if (e.GetExpressionType() == ExpressionType::BOUND_FUNCTION) {
			auto &fexpr = e.Cast<BoundFunctionExpression>();
			if (fexpr.Function().GetName() == "struct_extract" && fexpr.GetChildren().size() == 2 &&
			    fexpr.GetChildren()[1]->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
				const auto field = fexpr.GetChildren()[1]->Cast<BoundConstantExpression>().GetValue().ToString();
				if (agg_expr.GetChildren()[0]->GetExpressionType() == ExpressionType::BOUND_FUNCTION) {
					auto &pack = agg_expr.GetChildren()[0]->Cast<BoundFunctionExpression>();
					if (pack.Function().GetName() == "struct_pack") {
						for (auto &arg : pack.GetChildren()) {
							if (arg->ToString() == field) {
								return resolve_expr(*arg);
							}
						}
					}
				}
				return {ArgMinOrigin::Kind::UNKNOWN, {}};
			}
			return {ArgMinOrigin::Kind::DISTANCE, {}};
		}
		return {ArgMinOrigin::Kind::UNKNOWN, {}};
	};
	resolve = [&](const ColumnBinding &b) -> ArgMinOrigin {
		for (auto &ib : inner_get_bindings) {
			if (ib == b) {
				return {ArgMinOrigin::Kind::INNER, b};
			}
		}
		for (auto &db : delim_get_bindings) {
			if (db == b) {
				return {ArgMinOrigin::Kind::DELIM, b};
			}
		}
		for (auto &ob : outer_get_bindings) {
			if (ob == b) {
				return {ArgMinOrigin::Kind::OUTER, b};
			}
		}
		for (idx_t i = 0; i < inner_proj_bindings.size(); i++) {
			if (inner_proj_bindings[i] == b) {
				return resolve_expr(*inner_proj.expressions[i]);
			}
		}
		for (idx_t i = 0; i < aggregate.groups.size() && i < agg_bindings.size(); i++) {
			if (agg_bindings[i] == b) {
				return resolve_expr(*aggregate.groups[i]);
			}
		}
		for (idx_t i = aggregate.groups.size(); i < agg_bindings.size(); i++) {
			if (agg_bindings[i] == b) {
				return resolve_expr(*agg_expr.GetChildren()[0]);
			}
		}
		for (auto &op_ref : corr_chain) {
			auto &op = op_ref.get();
			if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
				auto &proj = op.Cast<LogicalProjection>();
				auto pbindings = proj.GetColumnBindings();
				for (idx_t i = 0; i < pbindings.size(); i++) {
					if (pbindings[i] == b) {
						return resolve_expr(*proj.expressions[i]);
					}
				}
			} else if (op.type == LogicalOperatorType::LOGICAL_UNNEST) {
				auto ubindings = op.GetColumnBindings();
				auto child_bindings = op.children[0]->GetColumnBindings();
				for (auto &ub : ubindings) {
					if (!(ub == b)) {
						continue;
					}
					bool passthrough = false;
					for (auto &cb : child_bindings) {
						if (cb == b) {
							passthrough = true;
							break;
						}
					}
					if (!passthrough) {
						return resolve_expr(*agg_expr.GetChildren()[0]);
					}
				}
			}
		}
		return {ArgMinOrigin::Kind::UNKNOWN, {}};
	};

	auto index_join = make_uniq<LogicalIndexJoin>(optimizer.binder.GenerateTableIndex(), duck_table, *index,
	                                              std::move(match->query_params), /* emit_rank */ true);
	for (auto &column_id : inner_get.GetColumnIds()) {
		index_join->inner_column_ids.emplace_back(column_id.GetPrimaryIndex());
	}
	index_join->inner_projection_ids = inner_get.projection_ids;
	index_join->inner_returned_types = inner_get.returned_types;

	ColumnBindingReplacer replacer;
	auto delim_bindings = delim_join.GetColumnBindings();
	auto delim_types = delim_join.types;

	vector<ArgMinOrigin> origins;
	origins.reserve(delim_bindings.size());
	for (auto &db : delim_bindings) {
		auto origin = resolve(db);
		if (origin.kind == ArgMinOrigin::Kind::UNKNOWN) {
			return false;
		}
		origins.push_back(origin);
	}

	vector<unique_ptr<Expression>> projection_expressions;
	auto projection_table_index = optimizer.binder.GenerateTableIndex();
	ProjectionIndex new_binding_idx(0);
	for (idx_t i = 0; i < delim_bindings.size(); i++) {
		if (origins[i].kind == ArgMinOrigin::Kind::DISTANCE) {
			projection_expressions.push_back(distance_expr_ptr.Copy());
		} else {
			projection_expressions.push_back(make_uniq<BoundColumnRefExpression>(delim_types[i], origins[i].binding));
		}
		replacer.replacement_bindings.emplace_back(delim_bindings[i],
		                                           ColumnBinding(projection_table_index, new_binding_idx++));
	}
	auto new_projection = make_uniq<LogicalProjection>(projection_table_index, std::move(projection_expressions));

	replacer.VisitOperator(*root);
	replacer.replacement_bindings.clear();

	for (const auto &old_binding : delim_get.GetColumnBindings()) {
		replacer.replacement_bindings.emplace_back(old_binding,
		                                           ColumnBinding(outer_get.table_index, old_binding.column_index));
	}
	for (const auto &old_binding : inner_get.GetColumnBindings()) {
		replacer.replacement_bindings.emplace_back(old_binding,
		                                           ColumnBinding(index_join->table_index, old_binding.column_index));
	}
	replacer.VisitOperator(*new_projection);

	index_join->children.emplace_back(std::move(*outer_get_ptr));
	new_projection->children.emplace_back(std::move(index_join));
	new_projection->EstimateCardinality(context);
	plan = std::move(new_projection);
	root->EstimateCardinality(context);
	return true;
}

bool IndexScanOptimizer::TryOptimizeIndexJoinArgMinCte(unique_ptr<LogicalOperator> &root,
                                                       unique_ptr<LogicalOperator> &plan) {
	// projection(s) / [unnest]  (anchor `plan`)
	//   -> aggregate arg_min(payload, distance, [k]) group by <corr>
	//     -> projection "inner" -> cross_product -> {get (rhs, seq_scan, indexed), corr_source(... cte_ref)}
	vector<reference<LogicalOperator>> corr_chain;
	LogicalOperator *cursor = plan.get();
	while (cursor && cursor->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		if ((cursor->type != LogicalOperatorType::LOGICAL_PROJECTION &&
		     cursor->type != LogicalOperatorType::LOGICAL_UNNEST) ||
		    cursor->children.size() != 1) {
			return false;
		}
		corr_chain.push_back(*cursor);
		cursor = cursor->children[0].get();
	}
	if (!cursor) {
		return false;
	}
	auto &aggregate = cursor->Cast<LogicalAggregate>();

	int64_t k_value = 1;
	optional_ptr<BoundColumnRefExpression> order_ref;
	auto agg_expr_ptr = MatchArgMinAggregate(aggregate, k_value, order_ref);
	if (!agg_expr_ptr) {
		return false;
	}
	auto &agg_expr = *agg_expr_ptr;

	if (aggregate.children.size() != 1 || aggregate.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	auto &inner_proj = aggregate.children[0]->Cast<LogicalProjection>();
	if (inner_proj.children.size() != 1 || inner_proj.children[0]->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return false;
	}
	auto &cross_product = inner_proj.children[0]->Cast<LogicalCrossProduct>();

	auto &cp_lhs = cross_product.children[0];
	auto &cp_rhs = cross_product.children[1];
	unique_ptr<LogicalOperator> *corr_source_ptr = nullptr;
	unique_ptr<LogicalOperator> *inner_get_ptr = nullptr;
	if (cp_lhs->type == LogicalOperatorType::LOGICAL_GET && cp_lhs->Cast<LogicalGet>().function.name == "seq_scan") {
		inner_get_ptr = &cp_lhs;
		corr_source_ptr = &cp_rhs;
	} else if (cp_rhs->type == LogicalOperatorType::LOGICAL_GET &&
	           cp_rhs->Cast<LogicalGet>().function.name == "seq_scan") {
		inner_get_ptr = &cp_rhs;
		corr_source_ptr = &cp_lhs;
	} else {
		return false;
	}
	auto &inner_get = (*inner_get_ptr)->Cast<LogicalGet>();
	auto &corr_source = **corr_source_ptr;
	if (corr_source.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return false; // the DELIM_GET shape is handled by TryOptimizeIndexJoinArgMin
	}
	std::function<bool(LogicalOperator &)> has_cte_ref = [&](LogicalOperator &op) -> bool {
		if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
			return true;
		}
		for (auto &c : op.children) {
			if (has_cte_ref(*c)) {
				return true;
			}
		}
		return false;
	};
	if (!has_cte_ref(corr_source)) {
		return false;
	}

	const auto inner_proj_bindings = inner_proj.GetColumnBindings();
	bool order_in_inner_proj = false;
	for (auto &b : inner_proj_bindings) {
		if (b == order_ref->Binding()) {
			order_in_inner_proj = true;
			break;
		}
	}
	if (!order_in_inner_proj || order_ref->Binding().column_index >= inner_proj.expressions.size()) {
		return false;
	}
	auto &distance_expr_ptr = *inner_proj.expressions[order_ref->Binding().column_index];

	optional_ptr<BoundIndex> index;
	unique_ptr<IndexMatch> match;
	if (!TryBindIndexJoin(inner_get, distance_expr_ptr, idx_t(k_value), index, match)) {
		return false;
	}
	auto &duck_table = inner_get.GetTable()->Cast<DuckTableEntry>();

	// The query vector must be a column of corr_source (the index join's child).
	const auto corr_bindings = corr_source.GetColumnBindings();
	bool outer_in_corr = false;
	for (auto &b : corr_bindings) {
		if (b == match->probe_key_binding) {
			outer_in_corr = true;
			break;
		}
	}
	if (!outer_in_corr) {
		return false;
	}

	// Resolver: corr_source plays both the OUTER and DELIM roles.
	const auto inner_get_bindings = inner_get.GetColumnBindings();
	const auto &delim_get_bindings = corr_bindings;
	const auto &outer_get_bindings = corr_bindings;
	const auto agg_bindings = aggregate.GetColumnBindings();

	std::function<ArgMinOrigin(const ColumnBinding &)> resolve;
	std::function<ArgMinOrigin(Expression &)> resolve_expr;
	resolve_expr = [&](Expression &e) -> ArgMinOrigin {
		if (e.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			return resolve(e.Cast<BoundColumnRefExpression>().Binding());
		}
		if (e.GetExpressionType() == ExpressionType::BOUND_FUNCTION) {
			auto &fexpr = e.Cast<BoundFunctionExpression>();
			if (fexpr.Function().GetName() == "struct_extract" && fexpr.GetChildren().size() == 2 &&
			    fexpr.GetChildren()[1]->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
				const auto field = fexpr.GetChildren()[1]->Cast<BoundConstantExpression>().GetValue().ToString();
				if (agg_expr.GetChildren()[0]->GetExpressionType() == ExpressionType::BOUND_FUNCTION) {
					auto &pack = agg_expr.GetChildren()[0]->Cast<BoundFunctionExpression>();
					if (pack.Function().GetName() == "struct_pack") {
						for (auto &arg : pack.GetChildren()) {
							if (arg->ToString() == field) {
								return resolve_expr(*arg);
							}
						}
					}
				}
				return {ArgMinOrigin::Kind::UNKNOWN, {}};
			}
			return {ArgMinOrigin::Kind::DISTANCE, {}};
		}
		return {ArgMinOrigin::Kind::UNKNOWN, {}};
	};
	resolve = [&](const ColumnBinding &b) -> ArgMinOrigin {
		for (auto &ib : inner_get_bindings) {
			if (ib == b) {
				return {ArgMinOrigin::Kind::INNER, b};
			}
		}
		for (auto &db : delim_get_bindings) {
			if (db == b) {
				return {ArgMinOrigin::Kind::DELIM, b};
			}
		}
		for (auto &ob : outer_get_bindings) {
			if (ob == b) {
				return {ArgMinOrigin::Kind::OUTER, b};
			}
		}
		for (idx_t i = 0; i < inner_proj_bindings.size(); i++) {
			if (inner_proj_bindings[i] == b) {
				return resolve_expr(*inner_proj.expressions[i]);
			}
		}
		for (idx_t i = 0; i < aggregate.groups.size() && i < agg_bindings.size(); i++) {
			if (agg_bindings[i] == b) {
				return resolve_expr(*aggregate.groups[i]);
			}
		}
		for (idx_t i = aggregate.groups.size(); i < agg_bindings.size(); i++) {
			if (agg_bindings[i] == b) {
				return resolve_expr(*agg_expr.GetChildren()[0]);
			}
		}
		for (auto &op_ref : corr_chain) {
			auto &op = op_ref.get();
			if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
				auto &proj = op.Cast<LogicalProjection>();
				auto pbindings = proj.GetColumnBindings();
				for (idx_t i = 0; i < pbindings.size(); i++) {
					if (pbindings[i] == b) {
						return resolve_expr(*proj.expressions[i]);
					}
				}
			} else if (op.type == LogicalOperatorType::LOGICAL_UNNEST) {
				auto ubindings = op.GetColumnBindings();
				auto child_bindings = op.children[0]->GetColumnBindings();
				for (auto &ub : ubindings) {
					if (!(ub == b)) {
						continue;
					}
					bool passthrough = false;
					for (auto &cb : child_bindings) {
						if (cb == b) {
							passthrough = true;
							break;
						}
					}
					if (!passthrough) {
						return resolve_expr(*agg_expr.GetChildren()[0]);
					}
				}
			}
		}
		return {ArgMinOrigin::Kind::UNKNOWN, {}};
	};

	auto index_join = make_uniq<LogicalIndexJoin>(optimizer.binder.GenerateTableIndex(), duck_table, *index,
	                                              std::move(match->query_params), /* emit_rank */ true);
	for (auto &column_id : inner_get.GetColumnIds()) {
		index_join->inner_column_ids.emplace_back(column_id.GetPrimaryIndex());
	}
	index_join->inner_projection_ids = inner_get.projection_ids;
	index_join->inner_returned_types = inner_get.returned_types;

	ColumnBindingReplacer replacer;
	auto output_bindings = plan->GetColumnBindings();
	auto output_types = plan->types;
	vector<ArgMinOrigin> origins;
	origins.reserve(output_bindings.size());
	for (auto &ob : output_bindings) {
		auto origin = resolve(ob);
		if (origin.kind == ArgMinOrigin::Kind::UNKNOWN) {
			return false;
		}
		origins.push_back(origin);
	}

	vector<unique_ptr<Expression>> projection_expressions;
	auto projection_table_index = optimizer.binder.GenerateTableIndex();
	ProjectionIndex new_binding_idx(0);
	for (idx_t i = 0; i < output_bindings.size(); i++) {
		if (origins[i].kind == ArgMinOrigin::Kind::DISTANCE) {
			projection_expressions.push_back(distance_expr_ptr.Copy());
		} else {
			projection_expressions.push_back(make_uniq<BoundColumnRefExpression>(output_types[i], origins[i].binding));
		}
		replacer.replacement_bindings.emplace_back(output_bindings[i],
		                                           ColumnBinding(projection_table_index, new_binding_idx++));
	}
	auto new_projection = make_uniq<LogicalProjection>(projection_table_index, std::move(projection_expressions));

	replacer.VisitOperator(*root);
	replacer.replacement_bindings.clear();

	for (const auto &old_binding : inner_get.GetColumnBindings()) {
		replacer.replacement_bindings.emplace_back(old_binding,
		                                           ColumnBinding(index_join->table_index, old_binding.column_index));
	}
	replacer.VisitOperator(*new_projection);

	index_join->children.emplace_back(std::move(*corr_source_ptr));
	new_projection->children.emplace_back(std::move(index_join));
	new_projection->EstimateCardinality(context);
	plan = std::move(new_projection);
	root->EstimateCardinality(context);
	return true;
}

//===--------------------------------------------------------------------===//
// Equi index nested-loop join (e.g. ART: a.k = b.k, b.k indexed)
//===--------------------------------------------------------------------===//
bool IndexScanOptimizer::TryBindEquiJoin(LogicalGet &inner_get, const Expression &inner_key, const Expression &outer_key,
                                         optional_ptr<BoundIndex> &out_index, unique_ptr<IndexMatch> &out_match) {
	auto table = inner_get.GetTable();
	if (!table || !table->IsDuckTable()) {
		return false;
	}
	auto &table_info = *table->GetStorage().GetDataTableInfo();
	auto &index_types = DBConfig::GetConfig(context).GetIndexTypes();

	table_info.BindIndexes(context);
	for (auto &index : table_info.GetIndexes().Indexes()) {
		if (!index.IsBound()) {
			continue;
		}
		auto &bound = index.Cast<BoundIndex>();
		auto index_type = index_types.FindByName(bound.GetIndexType());
		if (!index_type || !index_type->optimizer_capabilities.Supports(IndexScanCapability::PREDICATE_JOIN)) {
			continue;
		}
		auto match = bound.TryBindPredicateJoin(context, inner_get, inner_key, outer_key);
		if (match) {
			out_index = &bound;
			out_match = std::move(match);
			return true;
		}
	}
	return false;
}

bool IndexScanOptimizer::TryOptimizeIndexEquiJoin(unique_ptr<LogicalOperator> &root,
                                                  unique_ptr<LogicalOperator> &plan) {
	if (plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = plan->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::INNER || join.children.size() != 2 || join.conditions.size() != 1) {
		return false;
	}
	if (!join.duplicate_eliminated_columns.empty()) {
		return false; // not a plain inner join (delim/mark machinery)
	}
	auto &cond = join.conditions[0];
	if (!cond.IsComparison() || cond.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}

	// One child must be a seq_scan on an indexed duck table; the other is the probe (outer) side.
	// By LogicalComparisonJoin convention cond's LHS references the left child, RHS the right child.
	for (idx_t inner_side = 0; inner_side < 2; inner_side++) {
		auto &inner_child = join.children[inner_side];
		auto &outer_child = join.children[1 - inner_side];
		if (inner_child->type != LogicalOperatorType::LOGICAL_GET) {
			continue;
		}
		auto &inner_get = inner_child->Cast<LogicalGet>();
		if (inner_get.function.name != "seq_scan") {
			continue;
		}
		// Dynamic (runtime join) filters must not be present yet; this pass runs before join-filter
		// pushdown. Static table filters are fine - they are re-applied above the index join below.
		if (inner_get.dynamic_filters && inner_get.dynamic_filters->HasFilters()) {
			continue;
		}
		auto &inner_key = inner_side == 0 ? cond.GetLHS() : cond.GetRHS();
		auto &outer_key = inner_side == 0 ? cond.GetRHS() : cond.GetLHS();
		if (outer_key.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}

		// Restrict to fixed-width integer / UUID keys - the kind used as primary keys. Variable-length
		// keys (e.g. strings) build deep ARTs whose per-probe traversal is far slower than a hash
		// join, especially on persisted databases (buffer-managed node resolution per node).
		const auto &key_type = outer_key.GetReturnType();
		if (!key_type.IsIntegral() && key_type.id() != LogicalTypeId::UUID) {
			continue;
		}

		// The probe reads the outer key from a fixed input column position; require the key's column
		// index to be its flat position in the outer output (holds for direct column references).
		auto &outer_ref = outer_key.Cast<BoundColumnRefExpression>();
		auto outer_bindings = outer_child->GetColumnBindings();
		if (outer_ref.Binding().column_index >= outer_bindings.size() ||
		    !(outer_bindings[outer_ref.Binding().column_index] == outer_ref.Binding())) {
			continue;
		}

		// Cost gate: a per-outer-row index probe only beats DuckDB's hash join (with min/max + bloom
		// filter pushdown) when the outer is far smaller than the indexed inner. Benchmarks put the
		// crossover near inner/400 for a 1M-row inner, so gate conservatively at inner/100.
		const auto inner_card = inner_get.EstimateCardinality(context);
		const auto outer_card = outer_child->EstimateCardinality(context);
		if (inner_card < 2048 || outer_card * 100 > inner_card) {
			continue;
		}

		optional_ptr<BoundIndex> index;
		unique_ptr<IndexMatch> match;
		if (!TryBindEquiJoin(inner_get, inner_key, outer_key, index, match)) {
			continue;
		}
		RewriteInnerJoinToIndexJoin(root, plan, inner_get, outer_child, *index, *match);
		return true;
	}
	return false;
}

void IndexScanOptimizer::RewriteInnerJoinToIndexJoin(unique_ptr<LogicalOperator> &root,
                                                     unique_ptr<LogicalOperator> &plan, LogicalGet &inner_get,
                                                     unique_ptr<LogicalOperator> &outer_child, BoundIndex &index,
                                                     IndexMatch &match) {
	auto &duck_table = inner_get.GetTable()->Cast<DuckTableEntry>();
	auto index_join = make_uniq<LogicalIndexJoin>(optimizer.binder.GenerateTableIndex(), duck_table, index,
	                                              std::move(match.query_params), /* emit_rank */ false);
	for (auto &column_id : inner_get.GetColumnIds()) {
		index_join->inner_column_ids.emplace_back(column_id.GetPrimaryIndex());
	}
	index_join->inner_projection_ids = inner_get.projection_ids;
	index_join->inner_returned_types = inner_get.returned_types;
	const auto index_join_table_index = index_join->table_index;

	// References to the inner get become references to the index join's (left) output columns.
	// Outer references pass through the index join's right columns unchanged.
	ColumnBindingReplacer replacer;
	for (const auto &old_binding : inner_get.GetColumnBindings()) {
		replacer.replacement_bindings.emplace_back(old_binding,
		                                           ColumnBinding(index_join_table_index, old_binding.column_index));
	}
	replacer.VisitOperator(*root);

	index_join->children.emplace_back(std::move(outer_child));
	index_join->EstimateCardinality(context);

	// Re-apply any static table filters from the probed scan as a residual filter above the index join
	// (the index scan itself applies no filters). Correct whether user or join-derived: the probe
	// result is a subset either way.
	if (inner_get.table_filters.HasFilters()) {
		auto residual = make_uniq<LogicalFilter>();
		for (const auto &entry : inner_get.table_filters) {
			auto proj_id = entry.GetIndex();
			auto &column_id = inner_get.GetColumnIndex(proj_id);
			auto &type = inner_get.returned_types[column_id.GetPrimaryIndex()];
			auto column = make_uniq<BoundColumnRefExpression>(type, ColumnBinding(index_join_table_index, proj_id));
			residual->expressions.push_back(entry.Filter().ToExpression(*column));
		}
		residual->children.push_back(std::move(index_join));
		residual->ResolveOperatorTypes();
		plan = std::move(residual);
	} else {
		plan = std::move(index_join);
	}
}

bool IndexScanOptimizer::TryBindRangeJoin(LogicalGet &inner_get, const Expression &inner_key,
                                          optional_ptr<const Expression> lo_key, bool lo_inclusive,
                                          optional_ptr<const Expression> hi_key, bool hi_inclusive,
                                          optional_ptr<BoundIndex> &out_index, unique_ptr<IndexMatch> &out_match) {
	auto table = inner_get.GetTable();
	if (!table || !table->IsDuckTable()) {
		return false;
	}
	auto &table_info = *table->GetStorage().GetDataTableInfo();
	auto &index_types = DBConfig::GetConfig(context).GetIndexTypes();
	table_info.BindIndexes(context);
	for (auto &index : table_info.GetIndexes().Indexes()) {
		if (!index.IsBound()) {
			continue;
		}
		auto &bound = index.Cast<BoundIndex>();
		auto index_type = index_types.FindByName(bound.GetIndexType());
		if (!index_type || !index_type->optimizer_capabilities.Supports(IndexScanCapability::PREDICATE_JOIN)) {
			continue;
		}
		auto match = bound.TryBindRangeJoin(context, inner_get, inner_key, lo_key, lo_inclusive, hi_key, hi_inclusive);
		if (match) {
			out_index = &bound;
			out_match = std::move(match);
			return true;
		}
	}
	return false;
}

bool IndexScanOptimizer::TryOptimizeIndexRangeJoin(unique_ptr<LogicalOperator> &root,
                                                   unique_ptr<LogicalOperator> &plan) {
	if (plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = plan->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::INNER || join.children.size() != 2 || join.conditions.empty()) {
		return false;
	}
	if (!join.duplicate_eliminated_columns.empty()) {
		return false;
	}

	for (idx_t inner_side = 0; inner_side < 2; inner_side++) {
		auto &inner_child = join.children[inner_side];
		auto &outer_child = join.children[1 - inner_side];
		if (inner_child->type != LogicalOperatorType::LOGICAL_GET) {
			continue;
		}
		auto &inner_get = inner_child->Cast<LogicalGet>();
		if (inner_get.function.name != "seq_scan" ||
		    (inner_get.dynamic_filters && inner_get.dynamic_filters->HasFilters())) {
			continue;
		}

		// Classify each condition as a lower/upper bound on a single inner (indexed) column.
		optional_ptr<const Expression> inner_key;
		optional_ptr<const Expression> lo_key, hi_key;
		bool lo_inclusive = false, hi_inclusive = false;
		bool ok = true;
		for (auto &cond : join.conditions) {
			if (!cond.IsComparison()) {
				ok = false; // arbitrary predicate condition, not a left/right comparison
				break;
			}
			auto cmp = cond.GetComparisonType();
			auto &inner_expr = inner_side == 0 ? cond.GetLHS() : cond.GetRHS();
			auto &outer_expr = inner_side == 0 ? cond.GetRHS() : cond.GetLHS();
			if (inner_side == 1) {
				cmp = FlipComparisonExpression(cmp); // normalize to `inner <cmp> outer`
			}
			if (!inner_key) {
				inner_key = &inner_expr;
			} else if (!inner_key->Equals(inner_expr)) {
				ok = false;
				break;
			}
			switch (cmp) {
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				if (lo_key) {
					ok = false;
				}
				lo_key = &outer_expr;
				lo_inclusive = cmp == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
				break;
			case ExpressionType::COMPARE_LESSTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
				if (hi_key) {
					ok = false;
				}
				hi_key = &outer_expr;
				hi_inclusive = cmp == ExpressionType::COMPARE_LESSTHANOREQUALTO;
				break;
			default:
				ok = false; // equality/other: handled by the equi matcher, not here
				break;
			}
			if (!ok) {
				break;
			}
		}
		if (!ok || !inner_key || (!lo_key && !hi_key)) {
			continue;
		}

		// The probe reads bound values from fixed outer input columns; require each key's column index
		// to be its flat position in the outer output (holds for direct column references).
		auto outer_bindings = outer_child->GetColumnBindings();
		auto flat_ok = [&](optional_ptr<const Expression> key) {
			if (!key) {
				return true;
			}
			if (key->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				return false;
			}
			auto &ref = key->Cast<BoundColumnRefExpression>();
			return ref.Binding().column_index < outer_bindings.size() &&
			       outer_bindings[ref.Binding().column_index] == ref.Binding();
		};
		if (!flat_ok(lo_key) || !flat_ok(hi_key)) {
			continue;
		}

		// Cost gate: a per-outer-row range probe crushes piecewise-merge / IEJoin for a small outer
		// with selective bands (up to ~60x in benchmarks), but its cost scales with total fan-out, so
		// a wide band on a larger outer regresses badly. Without a cheap band-selectivity estimate we
		// gate conservatively on a small outer (~inner/1000), the reliable-win zone.
		const auto inner_card = inner_get.EstimateCardinality(context);
		const auto outer_card = outer_child->EstimateCardinality(context);
		if (inner_card < 2048 || outer_card * 1000 > inner_card) {
			continue;
		}

		optional_ptr<BoundIndex> index;
		unique_ptr<IndexMatch> match;
		if (!TryBindRangeJoin(inner_get, *inner_key, lo_key, lo_inclusive, hi_key, hi_inclusive, index, match)) {
			continue;
		}
		RewriteInnerJoinToIndexJoin(root, plan, inner_get, outer_child, *index, *match);
		return true;
	}
	return false;
}

void IndexScanOptimizer::OptimizeIndexJoin(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &plan) {
	if (!TryOptimizeIndexJoin(root, plan) && !TryOptimizeIndexJoinArgMin(root, plan) &&
	    !TryOptimizeIndexJoinArgMinCte(root, plan)) {
		for (auto &child : plan->children) {
			OptimizeIndexJoin(root, child);
		}
	}
}

void IndexScanOptimizer::OptimizeEquiIndexJoin(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &plan) {
	if (!TryOptimizeIndexEquiJoin(root, plan) && !TryOptimizeIndexRangeJoin(root, plan)) {
		for (auto &child : plan->children) {
			OptimizeEquiIndexJoin(root, child);
		}
	}
}

} // namespace duckdb
