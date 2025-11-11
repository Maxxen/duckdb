#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_cast_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateCast(duckdb_libpgquery::PGCreateCastStmt &stmt) {
	D_ASSERT(stmt.type == duckdb_libpgquery::T_PGCreateCastStmt);

	auto info = make_uniq<CreateCastInfo>();
	info->internal = false;
	info->source = TransformTypeName(*stmt.source);
	info->target = TransformTypeName(*stmt.target);
	info->cast_cost = stmt.context.cost;

	auto qname = TransformQualifiedName(*stmt.funcname);
	;
	info->function_name = qname.name;
	info->function_schema = qname.schema;
	info->function_catalog = qname.catalog;

	auto result = make_uniq<CreateStatement>();
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
