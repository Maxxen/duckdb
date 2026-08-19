#pragma once

#include "duckdb/parser/expression/type_expression.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

struct CastArguments {
	unique_ptr<ParsedExpression> expression;
	unique_ptr<TypeExpression> type;
};

} // namespace duckdb
