
#include "duckdb/parser/expression/match_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<MatchPattern> Transformer::TransformPattern(duckdb_libpgquery::PGPattern *pattern) {
	switch (pattern->type) {
	case duckdb_libpgquery::T_PGPatternWildcard:
		return make_uniq<MatchPatternWildcard>();
	case duckdb_libpgquery::T_PGPatternRest:
		return make_uniq<MatchPatternRest>();
	case duckdb_libpgquery::T_PGPatternConst: {

		auto const_pattern = PGPointerCast<duckdb_libpgquery::PGPatternConst>(pattern);
		auto const_value = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(const_pattern->value));
		return make_uniq<MatchPatternLiteral>(std::move(const_value->Cast<ConstantExpression>().value));
	}
	case duckdb_libpgquery::T_PGPatternVar: {
		auto identifier_pattern = PGPointerCast<duckdb_libpgquery::PGPatternVar>(pattern);
		return make_uniq<MatchPatternIdentifier>(string(identifier_pattern->name));
	}
	case duckdb_libpgquery::T_PGPatternList: {
		auto list_pattern = PGPointerCast<duckdb_libpgquery::PGPatternList>(pattern);
		vector<unique_ptr<MatchPattern>> elements;
		for (auto cell = list_pattern->items->head; cell != nullptr; cell = cell->next) {
			auto element_pattern = PGPointerCast<duckdb_libpgquery::PGPattern>(cell->data.ptr_value);
			elements.push_back(TransformPattern(element_pattern.get()));
		}
		return make_uniq<MatchPatternList>(std::move(elements));
	}
	case duckdb_libpgquery::T_PGPatternStruct: {
		auto struct_pattern = PGPointerCast<duckdb_libpgquery::PGPatternStruct>(pattern);
		child_list_t<unique_ptr<MatchPattern>> fields;
		for (auto cell = struct_pattern->fields->head; cell != nullptr; cell = cell->next) {
			auto field = PGPointerCast<duckdb_libpgquery::PGNamedArgExpr>(cell->data.ptr_value);
			auto field_name = string(field->name);
			auto field_pattern = PGPointerCast<duckdb_libpgquery::PGPattern>(field->arg);

			fields.emplace_back(field_name, TransformPattern(field_pattern.get()));
		}
		return make_uniq<MatchPatternStruct>(std::move(fields));
	}
	default:
		throw NotImplementedException("Unsupported pattern type in MATCH expression");
	}
}

unique_ptr<ParsedExpression> Transformer::TransformMatch(duckdb_libpgquery::PGMatchExpr &root) {

	auto match_node = make_uniq<MatchExpression>();


	// Transform the argument expression
	match_node->arg = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(root.arg));

	// Transform the match arms
	for (auto cell = root.arms->head; cell != nullptr; cell = cell->next) {

		auto parsed_arm = PGPointerCast<duckdb_libpgquery::PGMatchArm>(cell->data.ptr_value);
		auto pattern = TransformPattern(parsed_arm->pattern);

		auto arm = make_uniq<MatchArm>();
		arm->pattern = std::move(pattern);
		arm->expression = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(parsed_arm->result));

		if (parsed_arm->guard) {
			arm->guard = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(parsed_arm->guard));
		}

		match_node->arms.push_back(std::move(arm));
	}

	// Transform the ELSE expression
	if (root.defresult) {
		match_node->else_expr = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(root.defresult));
	} else {
		match_node->else_expr = make_uniq<ConstantExpression>(Value(LogicalType::SQLNULL));
	}

	SetQueryLocation(*match_node, root.location);
	return std::move(match_node);
}

} // namespace duckdb
