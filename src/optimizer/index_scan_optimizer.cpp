#include "duckdb/optimizer/index_scan_optimizer.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/function/table/index_scan.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
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

} // namespace duckdb
