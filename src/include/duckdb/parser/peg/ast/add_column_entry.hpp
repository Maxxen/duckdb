#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/constraint.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {

struct AddColumnEntry {
	unique_ptr<TypeExpression> type;
	vector<Identifier> column_path;
	unique_ptr<ParsedExpression> default_value;
};

} // namespace duckdb
