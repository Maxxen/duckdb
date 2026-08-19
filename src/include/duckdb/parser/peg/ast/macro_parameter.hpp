#pragma once

#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {
struct MacroParameter {
	unique_ptr<ParsedExpression> expression;
	Identifier name;
	unique_ptr<TypeExpression> type;
	bool is_default = false;
};

} // namespace duckdb
