#include "duckdb/parser/expression/match_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {


static void MatchPattern(const unique_ptr<ParsedExpression> &arg_expr, const unique_ptr<MatchPattern> &pattern,
	vector<unique_ptr<ParsedExpression>> &conditions,
	case_insensitive_tree_t<unique_ptr<ParsedExpression>> &bindings) {

	switch (pattern->GetType()) {
		case MatchPatternType::LITERAL: {
			auto &lit = pattern->Cast<MatchPatternLiteral>();

			// We just need to push a condition, that its
			conditions.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
				arg_expr->Copy(),
				make_uniq<ConstantExpression>(lit.value)));

		} break;
		case MatchPatternType::WILDCARD: {
			// Always matches
			conditions.push_back(make_uniq<ConstantExpression>(Value::BOOLEAN(true)));
		} break;
		case MatchPatternType::IDENTIFIER: {
			auto &id = pattern->Cast<MatchPatternIdentifier>();

			// We need to bind this identifier to the argument expression
			if (bindings.find(id.identifier) != bindings.end()) {
				throw BinderException("Duplicate identifier in match pattern: %s", id.identifier);
			}

			bindings[id.identifier] = arg_expr->Copy();

			// This always matches, so we can just push a true condition
			conditions.push_back(make_uniq<ConstantExpression>(Value::BOOLEAN(true)));

		} break;
		case MatchPatternType::LIST: {
			auto &list = pattern->Cast<MatchPatternList>();

			// Check if this pattern contains at most one rest pattern
			auto has_rest_pattern = false;
			idx_t rest_pattern_idx = 0;

			for (idx_t pattern_idx = 0; pattern_idx < list.elements.size(); pattern_idx++) {
				if (list.elements[pattern_idx]->GetType() == MatchPatternType::REST) {
					if (has_rest_pattern) {
						throw BinderException("Multiple '..' patterns in the same list pattern are not allowed");
					}

					has_rest_pattern = true;
					rest_pattern_idx = pattern_idx;
				}
			}

			if (!has_rest_pattern) {

				// If we dont have a rest pattern, there need to be exactly as many elements in the argument list as in
				// the pattern list. We compare the length of the argument list to the length of the pattern list.

				vector<unique_ptr<ParsedExpression>> length_args;
				length_args.push_back(arg_expr->Copy());

				conditions.push_back(
				make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
						make_uniq<FunctionExpression>("length", std::move(length_args)),
						make_uniq<ConstantExpression>(Value::BIGINT(list.elements.size())))
				);

				// Now match all list patterns
				for (idx_t pattern_idx = 0; pattern_idx < list.elements.size(); pattern_idx++) {

					vector<unique_ptr<ParsedExpression>> extract_args;
					extract_args.push_back(arg_expr->Copy());
					extract_args.push_back(make_uniq<ConstantExpression>(Value::BIGINT(pattern_idx + 1)));

					auto element_expr = make_uniq_base<ParsedExpression, FunctionExpression>("list_extract", std::move(extract_args));

					// Match the element against the pattern
					MatchPattern(element_expr, list.elements[pattern_idx], conditions, bindings);
				}

			} else {

				// If we have a rest pattern, there need to be at least as many elements in the argument list as there
				// are in the pattern list, minus one (the rest pattern can match zero or more elements).

				vector<unique_ptr<ParsedExpression>> length_args;
				length_args.push_back(arg_expr->Copy());

				conditions.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO,
					make_uniq<ConstantExpression>(Value::BIGINT(list.elements.size() - 1)),
					make_uniq<FunctionExpression>("length", std::move(length_args))));


				// Push all the conditions for the patterns before the rest pattern
				for (idx_t pattern_idx = 0; pattern_idx < rest_pattern_idx; pattern_idx++) {
					vector<unique_ptr<ParsedExpression>> extract_args;
					extract_args.push_back(arg_expr->Copy());
					extract_args.push_back(make_uniq<ConstantExpression>(Value::BIGINT(pattern_idx + 1)));

					auto element_expr = make_uniq_base<ParsedExpression, FunctionExpression>("list_extract", std::move(extract_args));

					// Match the element against the pattern
					MatchPattern(element_expr, list.elements[pattern_idx], conditions, bindings);
				}

				// Now push all the conditions for the patterns _after_ the rest pattern.
				// These are indexed from the end of the list, so we need to extract them using negative indexing
				for (idx_t pattern_idx = rest_pattern_idx + 1; pattern_idx < list.elements.size(); pattern_idx++) {

					vector<unique_ptr<ParsedExpression>> extract_args;
					extract_args.push_back(arg_expr->Copy());
					extract_args.push_back(make_uniq<ConstantExpression>(Value::BIGINT(-(list.elements.size() - pattern_idx))));

					auto element_expr = make_uniq_base<ParsedExpression, FunctionExpression>("list_extract", std::move(extract_args));

					// Match the element against the pattern
					MatchPattern(element_expr, list.elements[pattern_idx], conditions, bindings);
				}
			}
		} break;
		case MatchPatternType::REST: {
			throw BinderException("'..' patterns can only be used inside list patterns");
		} break;
	}
}

static unique_ptr<ParsedExpression> LiftLambda(
	const case_insensitive_tree_t<unique_ptr<ParsedExpression>> &bindings,
	unique_ptr<ParsedExpression> body) {

	if (bindings.empty()) {
		// We dont have any bindings, we can just return the body directly
		return body;
	}

	// Figure out which bindings we actually need in the body
	case_insensitive_set_t bindings_in_body;

	ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(*body, [&](const ColumnRefExpression &colref) {
		if (colref.IsQualified()) {
			// We dont care about qualified column references, those cannot refer to our bindings
			return;
		}
		auto binding_it = bindings.find(colref.GetColumnName());
		if (binding_it != bindings.end()) {
			bindings_in_body.insert(binding_it->first);
		}
	});

	if (bindings_in_body.empty()) {
		// No bindings are actually used in the body, we can just return the body directly
		return body;
	}

	// TODO: We can optimize futher, if we only have one binding thats not used in another condition
	// We can just inline the expression into the body instead of creating a lambda and invoking it.
	// Or maybe we just do this in a separate optimization pass. Lambda inlining seems useful anyway.

	// Now with these, we can create a lambda expression with the necessary bindings as parameters
	vector<string> lambda_parameters;
	vector<unique_ptr<ParsedExpression>> lambda_arguments;

	for (auto &binding_name : bindings_in_body) {
		lambda_parameters.push_back(binding_name);
		lambda_arguments.push_back(bindings.at(binding_name)->Copy());
	}

	auto lambda_expr = make_uniq<LambdaExpression>(std::move(lambda_parameters), std::move(body));

	// Push the lambda expression in front
	lambda_arguments.insert(lambda_arguments.begin(), std::move(lambda_expr));

	// Now make the invocation
	auto invoke = make_uniq<FunctionExpression>("invoke", std::move(lambda_arguments));

	return std::move(invoke);
}

BindResult ExpressionBinder::BindExpression(MatchExpression &expr, idx_t depth) {

	// We turn this into a case expression and bind that.
	auto case_expr = make_uniq<CaseExpression>();

	for (auto &arm : expr.arms) {

		auto &pattern = arm->GetPattern();

		// Extract all conditions and bindings from the pattern

		vector<unique_ptr<ParsedExpression>> conditions;
		case_insensitive_tree_t<unique_ptr<ParsedExpression>> bindings;

		MatchPattern(expr.arg, pattern, conditions, bindings);

		// Also push the guard as a condition if it exists
		if (arm->guard) {

			// Lift the bindings into a lambda so that they can be referenced in the "guard" expression
			conditions.push_back(LiftLambda(bindings, std::move(arm->GetGuard())));
		}

		CaseCheck check;

		// The WHEN expression is the conjunction of all conditions
		if (conditions.size() == 1) {
			check.when_expr = std::move(conditions[0]);
		} else {
			check.when_expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(conditions));
		}

		// Lift the bindings into a lambda so that they can be referenced in the "then" expression
		check.then_expr = LiftLambda(bindings, std::move(arm->GetExpression()));

		case_expr->case_checks.push_back(std::move(check));
 	}

	case_expr->else_expr = std::move(expr.else_expr);

	// Bind the case expression
	return BindExpression(*case_expr, depth);
}
} // namespace duckdb

