//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/distinct_aggregate_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/parser/expression_map.hpp"

namespace duckdb {

class DistinctAggregateOptimizer : public Rule {
public:
	explicit DistinctAggregateOptimizer(ExpressionRewriter &rewriter);

	static unique_ptr<Expression> Apply(ClientContext &context, BoundAggregateExpression &aggr, bool &changes_made);
	unique_ptr<Expression> Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
	                             vector<reference<Expression>> &bindings, bool is_root) override;
};

class DistinctWindowedOptimizer : public Rule {
public:
	explicit DistinctWindowedOptimizer(ExpressionRewriter &rewriter);

	static unique_ptr<Expression> Apply(ClientContext &context, BoundWindowExpression &wexpr, bool &changes_made);
	unique_ptr<Expression> Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
	                             vector<reference<Expression>> &bindings, bool is_root) override;
};

} // namespace duckdb
