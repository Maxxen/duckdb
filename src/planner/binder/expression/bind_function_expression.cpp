#include "duckdb/catalog/catalog.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/column_qualifier.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

BindResult ExpressionBinder::TryBindLambdaOrJson(FunctionExpression &function, idx_t depth, CatalogEntry &func,
                                                 const LambdaSyntaxType syntax_type) {
	if (syntax_type == LambdaSyntaxType::LAMBDA_KEYWORD) {
		// lambda x: x + 1 syntax.
		return BindLambdaFunction(function, func.Cast<ScalarFunctionCatalogEntry>(), depth);
	}

	auto setting = Settings::Get<LambdaSyntaxSetting>(context);
	bool invalid_syntax = setting != LambdaSyntax::ENABLE_SINGLE_ARROW && syntax_type == LambdaSyntaxType::SINGLE_ARROW;
	bool warn_deprecated_syntax = setting == LambdaSyntax::DEFAULT && syntax_type == LambdaSyntaxType::SINGLE_ARROW;
	const string msg = "Deprecated lambda arrow (->) detected. Please transition to the new lambda syntax, "
	                   "i.e.., lambda x, i: x + i, before DuckDB's next release.\n"
	                   "Use SET lambda_syntax='ENABLE_SINGLE_ARROW' to revert to the deprecated behavior.\n"
	                   "For more information, see https://duckdb.org/docs/current/sql/functions/lambda.html.";

	BindResult lambda_bind_result;
	ErrorData error;
	try {
		lambda_bind_result = BindLambdaFunction(function, func.Cast<ScalarFunctionCatalogEntry>(), depth);
	} catch (const std::exception &ex) {
		error = ErrorData(ex);
	}

	if (error.HasError() && error.Type() == ExceptionType::PARAMETER_NOT_RESOLVED && invalid_syntax) {
		ErrorData deprecation_error(ExceptionType::BINDER, msg);
		deprecation_error.Throw();
	} else if (error.HasError()) {
		error.Throw();
	}

	if (!lambda_bind_result.HasError()) {
		if (!invalid_syntax) {
			if (warn_deprecated_syntax) {
				DUCKDB_LOG_WARNING(context, msg);
			}
			return lambda_bind_result;
		}
		throw BinderException(msg);
	}

	// The lambda reading failed - the JSON reading binds the same nodes from scratch. Any child the
	// lambda attempt already bound is therefore bound twice, which repeats that bind's side effects
	// (correlated-column registration, subquery planning). Only the deprecated arrow syntax is
	// ambiguous enough to reach here, so this goes away with it rather than being worked around.
	auto json_bind_result = BindFunction(function, func.Cast<ScalarFunctionCatalogEntry>(), depth);
	if (!json_bind_result.HasError()) {
		return json_bind_result;
	}

	return BindResult("failed to bind function, either: " + lambda_bind_result.error.RawMessage() +
	                  "\n"
	                  " or: " +
	                  json_bind_result.error.RawMessage());
}

CatalogEntry &ExpressionBinder::BindFunction(FunctionExpression &function) {
	QueryErrorContext error_context(function.GetQueryLocation());

	ColumnQualifier qualifier(binder);
	auto func = qualifier.QualifyFunction(function);
	if (!func) {
		// function was not found - check if we this is a table function (to throw a more helpful error message)
		EntryLookupInfo table_function_lookup(CatalogType::TABLE_FUNCTION_ENTRY, function.GetQualifiedName(),
		                                      error_context);
		auto table_func = GetCatalogEntry(table_function_lookup, OnEntryNotFound::RETURN_NULL);
		if (table_func) {
			throw BinderException(function,
			                      "Function %s is a table function but it was used as a scalar function. This "
			                      "function has to be called in a FROM clause (similar to a table).",
			                      function.FunctionName());
		}
		// not a table function - rebind to throw an error
		EntryLookupInfo function_lookup(CatalogType::SCALAR_FUNCTION_ENTRY, function.GetQualifiedName(), error_context);
		func = GetCatalogEntry(function_lookup, OnEntryNotFound::THROW_EXCEPTION);
	}
	return *func;
}

BindResult ExpressionBinder::BindExpression(FunctionExpression &function, idx_t depth,
                                            unique_ptr<ParsedExpression> &expr_ptr) {
	auto &func = BindFunction(function);

	switch (func.type) {
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
	case CatalogType::WINDOW_FUNCTION_ENTRY:
		break;
	default:
		if (function.Distinct() || function.Filter() || !function.OrderBy()->orders.empty()) {
			throw InvalidInputException("Function %s is a %s. \"DISTINCT\", \"FILTER\", and \"ORDER BY\" are only "
			                            "applicable to window and aggregate functions.",
			                            function.FunctionName(), CatalogTypeToString(func.type));
		}
		break;
	}

	switch (func.type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY: {
		auto child = function.IsLambdaFunction();
		if (child) {
			auto syntax_type = child->Cast<LambdaExpression>().GetLambdaSyntaxType();
			return TryBindLambdaOrJson(function, depth, func, syntax_type);
		}
		return BindFunction(function, func.Cast<ScalarFunctionCatalogEntry>(), depth);
	}
	case CatalogType::MACRO_ENTRY:
		// macro function - a prepared statement must be re-bound when the macro changes
		if (!func.ParentCatalog().IsSystemCatalog()) {
			binder.GetStatementProperties().RegisterDBRead(func.ParentCatalog(), context);
		}
		return BindMacro(function, func.Cast<ScalarMacroCatalogEntry>(), depth, expr_ptr);
	case CatalogType::AGGREGATE_FUNCTION_ENTRY: {
		// an aggregate belongs to the innermost query level in which one of its arguments resolves a
		// column of that level, so that the level collecting it is the one the arguments refer to
		if (!binder.GetEnclosingScopes().empty()) {
			auto owner = ResolveAggregateOwner(function, 0);
			if (owner.IsValid() && owner.GetIndex() != 0) {
				return BindAggregateInEnclosingScope(function, owner.GetIndex(), depth, expr_ptr);
			}
		}
		return BindAggregate(function, func.Cast<AggregateFunctionCatalogEntry>(), depth);
	}
	case CatalogType::WINDOW_FUNCTION_ENTRY:
		// window function
		return BindWindow(function, func.Cast<WindowFunctionCatalogEntry>(), depth);
	default:
		throw InvalidInputException("Unsupported catalog type when binding function");
	}
}

BindResult ExpressionBinder::BindAggregateInEnclosingScope(FunctionExpression &aggregate, idx_t owner, idx_t depth,
                                                           unique_ptr<ParsedExpression> &expr_ptr) {
	// Bind a copy at each candidate scope. Binding rewrites the nodes it manages to bind - a column
	// reference becomes the qualified form of the scope that owns it - so a failed attempt must not be
	// handed to the next scope out. Moving the original aside also keeps `aggregate` valid throughout,
	// since it aliases the node `expr_ptr` held on entry.
	auto original = std::move(expr_ptr);
	ErrorData bind_error;
	for (optional_idx scope = owner; scope.IsValid(); scope = ResolveAggregateOwner(aggregate, scope.GetIndex() + 1)) {
		expr_ptr = original->Copy();
		auto result = DispatchToScope(scope.GetIndex(), expr_ptr, depth);
		if (!result.HasError()) {
			return result;
		}
		if (!bind_error.HasError()) {
			// report the innermost owner's error: that is the level the arguments actually name
			bind_error = std::move(result.error);
		}
	}
	expr_ptr = std::move(original);
	return BindResult(std::move(bind_error));
}

BindResult ExpressionBinder::BindFunction(FunctionExpression &function, ScalarFunctionCatalogEntry &func, idx_t depth) {
	// bind the children of the function expression
	ErrorData error;

	// bind each child
	vector<unique_ptr<Expression>> bound_children;
	for (idx_t i = 0; i < function.GetArguments().size(); i++) {
		bound_children.push_back(BindChild(function.GetArgumentsMutable()[i].GetExpressionMutable(), depth, error));
	}

	if (error.HasError()) {
		return BindResult(std::move(error));
	}
	if (binder.GetBindingMode() == BindingMode::EXTRACT_NAMES ||
	    binder.GetBindingMode() == BindingMode::EXTRACT_QUALIFIED_NAMES) {
		return BindResult(make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}

	// all children bound successfully - collect them (with their explicit names, if any) into the full argument list.
	// The positional/named split (and, for capturing functions, the alias capture) is resolved later per candidate
	// overload.
	vector<pair<Identifier, unique_ptr<Expression>>> arguments;
	// for legacy calls with names: fallback arguments that keep the names, used if binding positionally fails
	vector<pair<Identifier, unique_ptr<Expression>>> named_arguments;
	arguments.reserve(function.GetArguments().size());
	for (idx_t i = 0; i < function.GetArgumentsMutable().size(); i++) {
		auto &arg = function.GetArgumentsMutable()[i];
		auto bound_arg = std::move(bound_children[i]);

		// legacy function calls cannot have named arguments, so we ignore the names of the arguments during binding
		// and pass them all positionally. We do alias them by their name though, so that alias-capturing functions
		// (e.g. struct_pack) still work and so that re-serializing to the old format can match arguments by name.
		// Only override the alias when the argument actually carries a name, otherwise we would clobber the
		// display alias the binding assigned (e.g. clearing a column reference's name to its raw binding).
		if (!arg.GetName().empty()) {
			bound_arg->SetAlias(arg.GetName());
		}
		if (function.IsLegacyFunctionCall()) {
			if (!named_arguments.empty() || !arg.GetName().empty()) {
				// first named argument: copy the unnamed arguments bound so far
				for (idx_t prev = named_arguments.size(); prev < arguments.size(); prev++) {
					named_arguments.emplace_back(Identifier(), arguments[prev].second->Copy());
				}
				named_arguments.emplace_back(arg.GetName(), bound_arg->Copy());
			}
			arguments.emplace_back(string(), std::move(bound_arg));
		} else {
			arguments.emplace_back(arg.GetName(), std::move(bound_arg));
		}
	}

	FunctionBinder function_binder(binder);
	auto result = function_binder.BindScalarFunction(func, std::move(arguments), error, function.IsOperator(), &binder);
	if (!result && !named_arguments.empty()) {
		// a legacy call that does not bind positionally may use names the function declares (e.g. **kwargs)
		ErrorData named_error;
		result = function_binder.BindScalarFunction(func, std::move(named_arguments), named_error,
		                                            function.IsOperator(), &binder);
	}
	if (!result) {
		error.AddQueryLocation(function);
		error.Throw();
	}
	if (result->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &bound_function = result->Cast<BoundFunctionExpression>();
		if (bound_function.Function().GetStability() == FunctionStability::CONSISTENT_WITHIN_QUERY) {
			binder.SetAlwaysRequireRebind();
		}
	}
	return BindResult(std::move(result));
}

//! Whether some function in the set takes a lambda with a declared signature
static bool TakesTypedLambda(const ScalarFunctionSet &functions) {
	for (auto &function : functions.functions) {
		for (auto &parameter : function->GetSignature().GetParameters()) {
			if (parameter.GetType().id() == LogicalTypeId::LAMBDA && parameter.GetType().HasParameters()) {
				return true;
			}
		}
	}
	return false;
}

BindResult ExpressionBinder::BindTypedLambda(ScalarFunctionCatalogEntry &func, LambdaExpression &lambda_expr,
                                             idx_t depth, idx_t lambda_idx, vector<LogicalType> argument_types) {
	for (idx_t i = 0; i < argument_types.size(); i++) {
		if (i != lambda_idx && argument_types[i].id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}
	// select the overload by the number of lambda parameters - their types are not known yet
	auto parameter_count = GetLambdaParameterCount(lambda_expr);
	argument_types[lambda_idx] =
	    LogicalType::LAMBDA(vector<LogicalType>(parameter_count, LogicalType::ANY), LogicalType::ANY);
	FunctionBinder function_binder(binder);
	ErrorData error;
	auto best_function = function_binder.BindFunction(func.name, func.functions, argument_types, error);
	if (!best_function.IsValid()) {
		return BindResult(std::move(error));
	}
	auto &function = *func.functions.GetFunctionByOffset(best_function.GetIndex());
	auto &lambda_type = function.GetSignature().GetParameter(lambda_idx).GetType();
	if (lambda_type.id() != LogicalTypeId::LAMBDA || !lambda_type.HasParameters()) {
		return BindResult("This scalar function does not support lambdas!");
	}

	// the parameter types follow from the other arguments
	auto parameter_types =
	    LambdaType::GetParameterTypes(function_binder.ResolveLambdaType(function, argument_types, lambda_idx));
	if (parameter_types.size() != parameter_count) {
		throw BinderException("The number of lambda parameters does not match the number of arguments passed to the "
		                      "'%s' function, expected %llu, got %llu.",
		                      func.name.GetIdentifierName(), parameter_count, parameter_types.size());
	}
	for (idx_t i = 0; i < parameter_types.size(); i++) {
		if (parameter_types[i].IsTemplated()) {
			throw BinderException("Could not determine the type of parameter %llu of the lambda passed to %s", i + 1,
			                      func.name);
		}
	}
	// a NULL argument does not determine a type yet - the body may refine it, e.g. an accumulator that starts as []
	bool has_null_parameter = false;
	for (auto &parameter_type : parameter_types) {
		has_null_parameter |= TypeVisitor::Contains(parameter_type, LogicalTypeId::SQLNULL);
	}
	unique_ptr<ParsedExpression> lambda_copy;
	if (has_null_parameter) {
		// binding consumes the parsed lambda, keep a copy in case we have to bind it again
		lambda_copy = lambda_expr.Copy();
	}
	auto result = BindLambda(lambda_expr, depth, parameter_types);
	if (result.HasError()) {
		return result;
	}
	if (has_null_parameter) {
		argument_types[lambda_idx] = result.expression->GetReturnType();
		auto refined_types =
		    LambdaType::GetParameterTypes(function_binder.ResolveLambdaType(function, argument_types, lambda_idx));
		if (refined_types != parameter_types) {
			parameter_types = std::move(refined_types);
			result = BindLambda(lambda_copy->Cast<LambdaExpression>(), depth, parameter_types);
			if (result.HasError()) {
				return result;
			}
		}
	}

	// the body is cast to the return type the parameter types imply - only templates that do not occur in the
	// parameters, e.g. the element type of a transformed list, are bound by the body
	auto &bound_lambda = result.expression->Cast<BoundLambdaExpression>();
	argument_types[lambda_idx] = LogicalType::LAMBDA(parameter_types, LogicalType::ANY);
	auto return_type =
	    LambdaType::GetReturnType(function_binder.ResolveLambdaType(function, argument_types, lambda_idx));
	auto &body_type = bound_lambda.LambdaExpr()->GetReturnType();
	// a lambda that may return anything keeps the type of its body
	if (!return_type.IsTemplated() && return_type.id() != LogicalTypeId::ANY && return_type != body_type) {
		// a return type that follows from the arguments has to be reachable implicitly, as for any other argument.
		// A return type the signature declares, e.g. the BOOLEAN of a filter, is a context the body is cast to
		auto &declared_return_type = LambdaType::GetReturnType(lambda_type);
		if (declared_return_type.IsTemplated() &&
		    CastFunctionSet::ImplicitCastCost(context, body_type, return_type) < 0) {
			throw BinderException("The lambda passed to %s returns %s, which cannot be implicitly cast to %s",
			                      func.name, body_type, return_type);
		}
		auto cast =
		    BoundCastExpression::AddCastToType(context, std::move(bound_lambda.LambdaExprMutable()), return_type);
		bound_lambda.SetLambdaExpr(std::move(cast));
	}
	return result;
}

BindResult ExpressionBinder::BindLambdaFunction(FunctionExpression &function, ScalarFunctionCatalogEntry &func,
                                                idx_t depth) {
	auto &args = function.GetArgumentsMutable();

	if (!TakesTypedLambda(func.functions)) {
		return BindResult("This scalar function does not support lambdas!");
	}

	optional_idx found_idx;
	for (idx_t i = 0; i < args.size(); i++) {
		if (args[i].GetExpression().GetExpressionClass() != ExpressionClass::LAMBDA) {
			continue;
		}
		if (found_idx.IsValid()) {
			return BindResult("No function matches the given name and argument types: '" + function.ToString() +
			                  "'. You might need to add explicit type casts.");
		}
		found_idx = i;
	}
	if (!found_idx.IsValid()) {
		return BindResult("This scalar function requires a lambda expression!");
	}
	const auto lambda_expr_idx = found_idx.GetIndex();

	vector<LogicalType> function_child_types;
	vector<unique_ptr<Expression>> bound_children(function.GetArguments().size());
	ErrorData error;

	for (idx_t i = 0; i < function.GetArguments().size(); i++) {
		if (i == lambda_expr_idx) {
			function_child_types.push_back(LogicalTypeId::LAMBDA);
			continue;
		}

		bound_children[i] = BindChild(function.GetArgumentsMutable()[i].GetExpressionMutable(), depth, error);
		if (error.HasError()) {
			return BindResult(std::move(error));
		}

		function_child_types.push_back(bound_children[i]->GetReturnType());
	}

	auto &lambda_expr = args[lambda_expr_idx].GetExpressionMutable()->Cast<LambdaExpression>();
	auto bind_lambda_result = BindTypedLambda(func, lambda_expr, depth, lambda_expr_idx, function_child_types);
	if (bind_lambda_result.HasError()) {
		return BindResult(bind_lambda_result.error);
	}

	// successfully bound: keep the bound lambda alongside the other bound children
	auto alias = args[lambda_expr_idx].GetExpression().GetAlias();
	bind_lambda_result.expression->SetAlias(alias);

	bound_children[lambda_expr_idx] = std::move(bind_lambda_result.expression);

	if (binder.GetBindingMode() == BindingMode::EXTRACT_NAMES) {
		return BindResult(make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}

	// all children bound successfully
	// extract the children and types
	auto children = std::move(bound_children);

	// capture the (lambda) columns
	auto &bound_lambda_expr = children[lambda_expr_idx]->Cast<BoundLambdaExpression>();
	CaptureLambdaColumns(bound_lambda_expr, bound_lambda_expr.LambdaExprMutable());

	FunctionBinder function_binder(binder);
	unique_ptr<Expression> result =
	    function_binder.BindScalarFunction(func, std::move(children), error, function.IsOperator(), &binder);
	if (!result) {
		error.AddQueryLocation(function);
		error.Throw();
	}

	auto &bound_function_expr = result->Cast<BoundFunctionExpression>();

	// the lambda expression stays in the children, so that the argument positions do not shift.
	// it has no runtime value - the expression executor fills its slot with a constant placeholder
	auto &bound_lambda = bound_function_expr.GetChildrenMutable()[lambda_expr_idx]->Cast<BoundLambdaExpression>();

	// push back (in reverse order) any nested lambda parameters so that we can later use them in the lambda
	// expression (rhs). This happens after we bound the lambda expression of this depth. So it is relevant for
	// correctly binding lambdas one level 'out'. Therefore, the current parameter count does not matter here.
	if (lambda_bindings) {
		idx_t offset = 0;

		for (idx_t i = lambda_bindings->size(); i > 0; i--) {
			auto &binding = (*lambda_bindings)[i - 1];
			auto &column_names = binding.GetColumnNames();
			auto &column_types = binding.GetColumnTypes();
			D_ASSERT(column_names.size() == column_types.size());

			for (idx_t column_idx = column_names.size(); column_idx > 0; column_idx--) {
				auto bound_lambda_param = make_uniq<BoundReferenceExpression>(column_names[column_idx - 1],
				                                                              column_types[column_idx - 1], offset);
				offset++;
				bound_function_expr.GetChildrenMutable().push_back(std::move(bound_lambda_param));
			}
		}
	}

	// push back the captures into the children vector
	for (auto &capture : bound_lambda.CapturesMutable()) {
		bound_function_expr.GetChildrenMutable().push_back(std::move(capture));
	}
	// the lambda stays in the children, so it must not hold on to the moved-from capture slots
	bound_lambda.CapturesMutable().clear();

	return BindResult(std::move(result));
}

BindResult ExpressionBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function,
                                           idx_t depth) {
	return BindUnsupportedExpression(expr, depth, UnsupportedAggregateMessage());
}

BindResult ExpressionBinder::BindWindow(FunctionExpression &expr, WindowFunctionCatalogEntry &function, idx_t depth) {
	return BindUnsupportedExpression(expr, depth, UnsupportedWindowMessage());
}

BindResult ExpressionBinder::BindUnnest(FunctionExpression &expr, idx_t depth, bool root_expression) {
	return BindUnsupportedExpression(expr, depth, UnsupportedUnnestMessage());
}

void ExpressionBinder::ThrowIfUnnestInLambda(const ColumnBinding &column_binding) {
}

string ExpressionBinder::UnsupportedAggregateMessage() {
	return "Aggregate functions are not supported here";
}

string ExpressionBinder::UnsupportedWindowMessage() {
	return "Window functions are not supported here";
}

string ExpressionBinder::UnsupportedUnnestMessage() {
	return "UNNEST not supported here";
}

optional_ptr<CatalogEntry> ExpressionBinder::GetCatalogEntry(const Identifier &catalog, const Identifier &schema,
                                                             const EntryLookupInfo &lookup_info,
                                                             OnEntryNotFound on_entry_not_found) {
	return binder.GetCatalogEntry(catalog, schema, lookup_info, on_entry_not_found);
}

optional_ptr<CatalogEntry> ExpressionBinder::GetCatalogEntry(const EntryLookupInfo &lookup_info,
                                                             OnEntryNotFound on_entry_not_found) {
	return binder.GetCatalogEntry(lookup_info, on_entry_not_found);
}

} // namespace duckdb
