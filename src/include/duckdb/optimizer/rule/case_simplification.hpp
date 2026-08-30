//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/case_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// The Case Simplification rule rewrites cases with a constant check (i.e. [CASE WHEN 1=1 THEN x ELSE y END] => x)
class CaseSimplificationRule : public Rule {
public:
	explicit CaseSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
	                             vector<reference<Expression>> &bindings, bool is_root) override;
};

} // namespace duckdb
