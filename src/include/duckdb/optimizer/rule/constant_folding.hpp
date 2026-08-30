//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/constant_folding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// Fold any constant scalar expressions into a single constant (i.e. [2 + 2] => [4], [2 = 2] => [True], etc...)
class ConstantFoldingRule : public Rule {
public:
	explicit ConstantFoldingRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
	                             vector<reference<Expression>> &bindings, bool is_root) override;
};

} // namespace duckdb
