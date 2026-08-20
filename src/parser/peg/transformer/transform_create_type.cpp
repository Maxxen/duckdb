#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateTypeStmt(PEGTransformer &transformer,
                                                                           const optional<bool> &if_not_exists,
                                                                           const QualifiedName &qualified_name,
                                                                           unique_ptr<CreateTypeInfo> create_type) {
	auto result = make_uniq<CreateStatement>();
	create_type->SetQualifiedName(qualified_name);
	create_type->on_conflict =
	    if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	result->info = std::move(create_type);
	return result;
}

unique_ptr<CreateTypeInfo> PEGTransformerFactory::TransformCreateTypeFromType(PEGTransformer &transformer,
                                                                              unique_ptr<TypeExpression> type) {
	auto result = make_uniq<CreateTypeInfo>();
	// the parser's form is an expression; the binder folds it into a descriptor
	result->type_expression = std::move(type);
	return result;
}

unique_ptr<CreateTypeInfo>
PEGTransformerFactory::TransformEnumSelectType(PEGTransformer &transformer,
                                               unique_ptr<SelectStatement> select_statement_internal) {
	auto result = make_uniq<CreateTypeInfo>();
	result->query = std::move(select_statement_internal);
	return result;
}

unique_ptr<CreateTypeInfo>
PEGTransformerFactory::TransformEnumStringLiteralList(PEGTransformer &transformer,
                                                      const optional<vector<string>> &string_literal) {
	auto result = make_uniq<CreateTypeInfo>();
	idx_t enum_count = string_literal ? string_literal->size() : 0;
	Vector enum_vector(LogicalType::VARCHAR, enum_count);
	auto string_data = FlatVector::Writer<string_t>(enum_vector, enum_count);
	if (string_literal) {
		for (auto &literal : *string_literal) {
			string_data.WriteValue(string_t(literal));
		}
	}
	vector<unique_ptr<ParsedExpression>> values;
	values.reserve(enum_count);
	if (string_literal) {
		for (auto &literal : *string_literal) {
			values.push_back(make_uniq_base<ParsedExpression, ConstantExpression>(Value(literal)));
		}
	}
	result->type_expression = make_uniq<TypeExpression>(Identifier("ENUM"), std::move(values));
	return result;
}

} // namespace duckdb
