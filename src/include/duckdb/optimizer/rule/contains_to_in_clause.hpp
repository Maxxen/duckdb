#pragma once
#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

class ContainsToInClauseRule : public Rule {
public:
	explicit ContainsToInClauseRule(ExpressionRewriter &rewriter);
	unique_ptr<Expression> Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
	                             vector<reference<Expression>> &bindings, bool is_root) override;
};

} // namespace duckdb
