#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/parsed_data/bound_pragma_info.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/function/type_constructor.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/default/default_types.hpp"

namespace duckdb {

FunctionBinder::FunctionBinder(ClientContext &context_p) : binder(nullptr), context(context_p), type_cache(&context_p) {
}
FunctionBinder::FunctionBinder(Binder &binder_p)
    : binder(&binder_p), context(binder_p.context), type_cache(&binder_p.context) {
}
FunctionBinder::FunctionBinder(ClientContext &context_p, optional_ptr<const DecimalWidthLimit> decimal_width_limit_p)
    : binder(nullptr), context(context_p), decimal_width_limit(decimal_width_limit_p), type_cache(&context_p) {
}

TypeCache &FunctionBinder::GetTypeCache() {
	return binder ? binder->GetTypeCache() : type_cache;
}

optional_ptr<const DecimalWidthLimit> FunctionBinder::FixedDecimalWidthLimit() const {
	return binder ? binder->GetFixedDecimalWidthLimit() : decimal_width_limit;
}

//! Split the full (maybe-named) argument list into the positional and the named arguments. Returns false, and leaves
//! the arguments as they are, if a positional argument follows a named one
static bool TrySplitArguments(vector<pair<Identifier, unique_ptr<Expression>>> &arguments,
                              vector<unique_ptr<Expression>> &positional,
                              vector<pair<Identifier, unique_ptr<Expression>>> &named) {
	idx_t positional_count = 0;
	while (positional_count < arguments.size() && arguments[positional_count].first.empty()) {
		positional_count++;
	}
	for (idx_t i = positional_count; i < arguments.size(); i++) {
		if (arguments[i].first.empty()) {
			return false;
		}
	}
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (i < positional_count) {
			positional.push_back(std::move(arguments[i].second));
		} else {
			named.push_back(std::move(arguments[i]));
		}
	}
	arguments.clear();
	return true;
}

void FunctionCallArguments::AddArgument(FunctionCallArgument argument) {
	if (argument.name.empty()) {
		if (positional_count != arguments.size()) {
			throw InternalException("FunctionCallArguments: a positional argument cannot follow a named one");
		}
		positional_count++;
	}
	arguments.push_back(std::move(argument));
}

FunctionCallArguments::FunctionCallArguments(std::initializer_list<FunctionCallArgument> arguments_p) {
	for (auto &argument : arguments_p) {
		AddArgument(argument);
	}
}

FunctionCallArguments::FunctionCallArguments(vector<LogicalType> positional,
                                             vector<pair<Identifier, LogicalType>> named) {
	for (auto &type : positional) {
		AddArgument(FunctionCallArgument(std::move(type)));
	}
	for (auto &argument : named) {
		AddArgument(FunctionCallArgument(std::move(argument.second), std::move(argument.first)));
	}
}

FunctionCallArguments::FunctionCallArguments(const vector<unique_ptr<Expression>> &positional,
                                             const vector<pair<Identifier, unique_ptr<Expression>>> &named) {
	for (auto &arg : positional) {
		FunctionCallArgument argument(ExpressionBinder::GetExpressionReturnType(*arg), Identifier(),
		                              arg->GetQueryLocation());
		argument.expression = arg.get();
		AddArgument(std::move(argument));
	}
	for (auto &arg : named) {
		FunctionCallArgument argument(ExpressionBinder::GetExpressionReturnType(*arg.second), arg.first,
		                              arg.second->GetQueryLocation());
		argument.expression = arg.second.get();
		AddArgument(std::move(argument));
	}
}

vector<LogicalType> FunctionCallArguments::GetPositionalTypes() const {
	vector<LogicalType> result;
	for (idx_t i = 0; i < positional_count; i++) {
		result.push_back(arguments[i].type);
	}
	return result;
}

vector<pair<Identifier, LogicalType>> FunctionCallArguments::GetNamedTypes() const {
	vector<pair<Identifier, LogicalType>> result;
	for (idx_t i = positional_count; i < arguments.size(); i++) {
		result.emplace_back(arguments[i].name, arguments[i].type);
	}
	return result;
}

//! The first named argument of a call that repeats the name of an earlier one, if there is one
static optional_idx FindDuplicateArgument(const FunctionCallArguments &call) {
	identifier_set_t seen_names;
	for (idx_t i = call.GetPositionalCount(); i < call.GetArgumentCount(); i++) {
		if (!seen_names.insert(call.GetArgument(i).name).second) {
			return i;
		}
	}
	return optional_idx();
}

FunctionBindResult FunctionBinder::SelectFunction(optional_ptr<ClientContext> context, const BaseFunctionSet &functions,
                                                  const FunctionCallArguments &call, bool explicit_casts,
                                                  optional_ptr<const DecimalWidthLimit> decimal_width_limit) {
	TypeCache cache(context);
	return SelectFunction(cache, functions, call, explicit_casts, decimal_width_limit);
}

FunctionBindResult FunctionBinder::SelectFunction(TypeCache &type_cache, const BaseFunctionSet &functions,
                                                  const FunctionCallArguments &call, bool explicit_casts,
                                                  optional_ptr<const DecimalWidthLimit> decimal_width_limit) {
	auto duplicate = FindDuplicateArgument(call);
	if (duplicate.IsValid()) {
		// no overload can take the same argument twice
		auto kind = functions.GetOverloadCount() > 0 ? functions.GetOverload(0).GetKind() : FunctionKind::SCALAR;
		return FunctionBindError::CreateDuplicateArgument(functions.GetName(), call, duplicate.GetIndex(), kind);
	}

	// Try to match each overload, keeping the ones of the best rank
	vector<FunctionBinding> cheapest;
	vector<FunctionMismatch> mismatches;
	for (idx_t overload = 0; overload < functions.GetOverloadCount(); overload++) {
		auto match = MatchOverload(type_cache, overload, functions.GetSharedOverload(overload), call, explicit_casts,
		                           decimal_width_limit);
		if (!match) {
			// Once an overload matches, unsuccessful candidates cannot contribute to the result or diagnostics.
			if (cheapest.empty()) {
				mismatches.push_back(std::move(match.GetError()));
			}
			continue;
		}
		auto &binding = *match;

		// We only keep the best matches
		if (!cheapest.empty() && cheapest[0] < binding) {
			continue;
		}
		if (!cheapest.empty() && binding < cheapest[0]) {
			cheapest.clear();
		}
		cheapest.push_back(std::move(binding));
	}

	if (cheapest.empty()) {
		// Sort mismatches by closest distance
		std::stable_sort(mismatches.begin(), mismatches.end());
		return FunctionBindError::CreateNoMatch(functions.GetName(), call, std::move(mismatches));
	}
	if (cheapest.size() == 1) {
		return std::move(cheapest[0]);
	}
	// several overloads match at the best rank
	for (idx_t i = 0; i < call.GetPositionalCount(); i++) {
		if (call.GetArgument(i).type.IsUnknown()) {
			// the types of the prepared statement parameters may decide between them
			throw ParameterNotResolvedException();
		}
	}
	return FunctionBindError::CreateAmbiguousMatch(functions.GetName(), call, std::move(cheapest));
}

FunctionBinding FunctionBinder::MatchFunction(optional_ptr<ClientContext> context,
                                              shared_ptr<const SimpleFunction> function,
                                              const FunctionCallArguments &call,
                                              optional_ptr<const DecimalWidthLimit> decimal_width_limit) {
	auto name = function->GetName();
	auto duplicate = FindDuplicateArgument(call);
	if (duplicate.IsValid()) {
		FunctionBindError::CreateDuplicateArgument(name, call, duplicate.GetIndex(), function->GetKind()).Throw();
	}
	TypeCache cache(context);
	auto match = MatchOverload(cache, 0, std::move(function), call, true, decimal_width_limit);
	if (!match) {
		vector<FunctionMismatch> mismatches;
		mismatches.push_back(std::move(match.GetError()));
		FunctionBindError::CreateNoMatch(std::move(name), call, std::move(mismatches)).Throw();
	}
	return std::move(*match);
}

template <class BOUND>
BOUND FunctionBinder::CreateBoundFunction(TypeCache &cache, const FunctionBinding &binding) {
	// the kind of function BOUND is a call of
	using DEFINITION = typename std::remove_const<
	    typename std::remove_reference<decltype(*std::declval<const BOUND &>().GetDefinition())>::type>::type;
	// a return type that is marked as ANY is left unset, for the resolver or the bind callback
	return BOUND(binding.GetFunction<DEFINITION>(), binding.layout.GetArgumentTypes(), BindReturnType(cache, binding));
}

BoundScalarFunction FunctionBinder::BindScalarFunction(optional_ptr<ClientContext> context,
                                                       const ScalarFunction &function,
                                                       const FunctionCallArguments &call) {
	auto binding = MatchFunction(context, make_shared_ptr<ScalarFunction>(function), call);
	TypeCache cache(context);
	return CreateBoundFunction<BoundScalarFunction>(cache, binding);
}

BoundAggregateFunction FunctionBinder::BindAggregateFunction(optional_ptr<ClientContext> context,
                                                             const AggregateFunction &function,
                                                             const FunctionCallArguments &call) {
	auto binding = MatchFunction(context, make_shared_ptr<AggregateFunction>(function), call);
	TypeCache cache(context);
	return CreateBoundFunction<BoundAggregateFunction>(cache, binding);
}

BoundAggregateFunction FunctionBinder::BindAggregateFunction(const AggregateFunction &function,
                                                             vector<LogicalType> arguments, LogicalType return_type) {
	return BoundAggregateFunction(make_shared_ptr<AggregateFunction>(function), std::move(arguments),
	                              std::move(return_type));
}

BoundWindowFunction FunctionBinder::BindWindowFunction(optional_ptr<ClientContext> context,
                                                       const WindowFunction &function,
                                                       const FunctionCallArguments &call) {
	auto binding = MatchFunction(context, make_shared_ptr<WindowFunction>(function), call);
	TypeCache cache(context);
	return CreateBoundFunction<BoundWindowFunction>(cache, binding);
}

unique_ptr<BoundFunctionExpression> FunctionBinder::BindScalarFunction(optional_ptr<ClientContext> context,
                                                                       const ScalarFunction &function,
                                                                       vector<unique_ptr<Expression>> children,
                                                                       unique_ptr<FunctionData> bind_data,
                                                                       bool is_operator) {
	auto bound_function = BindScalarFunction(context, function, FunctionCallArguments(children));
	return make_uniq<BoundFunctionExpression>(std::move(bound_function), std::move(children), std::move(bind_data),
	                                          is_operator);
}

BoundTableFunction FunctionBinder::BindTableFunction(optional_ptr<ClientContext> context, const TableFunction &function,
                                                     const FunctionCallArguments &call) {
	auto binding = MatchFunction(context, make_shared_ptr<const TableFunction>(function), call);
	return BoundTableFunction(binding.GetFunction<TableFunction>(), binding.layout);
}

static bool AnyOverloadSupportsImplicitArgumentNames(const BaseFunctionSet &functions) {
	for (idx_t i = 0; i < functions.GetOverloadCount(); i++) {
		auto properties = functions.GetOverload(i).GetFunctionProperties();
		if (properties && properties->GetCaptureArgumentAliases()) {
			return true;
		}
	}
	return false;
}

static ErrorData PositionalAfterNamedArgumentError(const vector<pair<Identifier, unique_ptr<Expression>>> &arguments) {
	bool seen_named = false;
	for (const auto &[name, expr] : arguments) {
		if (!name.empty()) {
			seen_named = true;
			continue;
		}
		if (seen_named) {
			return ErrorData(BinderException(expr->GetQueryLocation(),
			                                 "Positional argument '%s' cannot follow named arguments in function call.",
			                                 expr->ToString()));
		}
	}
	throw InternalException("ThrowPositionalAfterNamed called without a positional-after-named argument");
}

optional<FunctionBinding>
FunctionBinder::SelectFunctionFromArguments(const BaseFunctionSet &functions,
                                            vector<pair<Identifier, unique_ptr<Expression>>> arguments,
                                            vector<unique_ptr<Expression>> &positional,
                                            vector<pair<Identifier, unique_ptr<Expression>>> &named, ErrorData &error) {
	// Does this call have a valid positional -> named argument shape?
	const auto is_valid_call = TrySplitArguments(arguments, positional, named);
	const auto implicit_names = AnyOverloadSupportsImplicitArgumentNames(functions);
	if (is_valid_call) {
		// Yes. Let's try to bind
		auto result = SelectFunction(GetTypeCache(), functions, {positional, named}, false, FixedDecimalWidthLimit());
		if (result) {
			return result.GetBinding();
		}
		error = result.GetError();

		// We failed binding - does any other overload support implicit argument names?
		// If not, this is a hard failure.
		if (!implicit_names) {
			return nullopt;
		}
		// put the arguments back together, to name them for the retry
		for (auto &expr : positional) {
			arguments.emplace_back(Identifier(), std::move(expr));
		}
		for (auto &argument : named) {
			arguments.push_back(std::move(argument));
		}
		positional.clear();
		named.clear();
	}

	// Either a positional argument follows a named one, or no overload takes the positional arguments.
	if (implicit_names) {
		// Attempt to salvage the call by implicitly naming the positional arguments and retrying again
		for (auto &[name, expr] : arguments) {
			if (name.empty()) {
				name = expr->GetAlias();
			}
		}
		if (TrySplitArguments(arguments, positional, named)) {
			auto retry_result =
			    SelectFunction(GetTypeCache(), functions, {positional, named}, false, FixedDecimalWidthLimit());
			if (retry_result) {
				return retry_result.GetBinding();
			}
			if (!is_valid_call) {
				error = retry_result.GetError();
			}
			// otherwise the call only failed to match an overload, which is reported in the error
			return nullopt;
		}
	}
	if (!is_valid_call) {
		// No overload could rescue the positional-after-named call, give a clear error.
		error = PositionalAfterNamedArgumentError(arguments);
	}
	return nullopt;
}

//! Reject a named argument that no overload of the set accepts, naming the options that were available. Overload
//! selection would otherwise report only that nothing matched the call, which hides which name was wrong
static void VerifyNamedArgumentsAccepted(const BaseFunctionSet &functions,
                                         const vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	for (auto &named_argument : named_arguments) {
		bool accepted = false;
		for (idx_t i = 0; i < functions.GetOverloadCount() && !accepted; i++) {
			auto &signature = functions.GetOverload(i).GetSignature();
			// the options of a "**kwargs" are checked once the overload is chosen
			accepted =
			    signature.GetParameterIndexByName(named_argument.first).IsValid() || signature.GetKwargs() != nullptr;
		}
		if (accepted) {
			continue;
		}
		// list every option any overload accepts, in name order
		map<string, string> candidates;
		for (idx_t i = 0; i < functions.GetOverloadCount(); i++) {
			auto &signature = functions.GetOverload(i).GetSignature();
			for (auto &param : signature.GetParameters()) {
				if (!param.IsVariadic() && param.HasDefaultValue()) {
					candidates[param.GetName().GetIdentifierName()] = param.GetTypeName().ToString();
				}
			}
		}
		string candidate_list;
		for (auto &candidate : candidates) {
			candidate_list += "    " + candidate.first + " " + candidate.second + "\n";
		}
		throw BinderException("Invalid named parameter %s for function %s\n%s", named_argument.first,
		                      functions.GetName().GetIdentifierName(),
		                      candidate_list.empty() ? "Function does not accept any named parameters."
		                                             : "Candidates:\n" + candidate_list);
	}
}

enum class LogicalTypeComparisonResult : uint8_t { IDENTICAL_TYPE, TARGET_IS_ANY, DIFFERENT_TYPES };

static LogicalTypeComparisonResult RequiresCast(const LogicalType &source_type, const LogicalType &target_type) {
	if (target_type.id() == LogicalTypeId::ANY) {
		return LogicalTypeComparisonResult::TARGET_IS_ANY;
	}
	if (source_type == target_type) {
		return LogicalTypeComparisonResult::IDENTICAL_TYPE;
	}
	if (source_type.id() == LogicalTypeId::LIST && target_type.id() == LogicalTypeId::LIST) {
		return RequiresCast(ListType::GetChildType(source_type), ListType::GetChildType(target_type));
	}
	if (source_type.id() == LogicalTypeId::ARRAY && target_type.id() == LogicalTypeId::ARRAY) {
		auto target_size = ArrayType::GetSize(target_type);
		if (target_size != 0 && target_size != ArrayType::GetSize(source_type)) {
			return LogicalTypeComparisonResult::DIFFERENT_TYPES;
		}
		return RequiresCast(ArrayType::GetChildType(source_type), ArrayType::GetChildType(target_type));
	}
	return LogicalTypeComparisonResult::DIFFERENT_TYPES;
}

Value FunctionBinder::CastToParameterType(ClientContext &context, Value value, const LogicalType &parameter_type) {
	if (RequiresCast(value.type(), parameter_type) == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
		return value.CastAs(context, parameter_type);
	}
	return value;
}

//! Fold an argument to the constant it was bound to. A constant expression carries its value directly - evaluating
//! one is both wasteful and impossible for the types that have no vector representation, such as TABLE
static Value FoldArgument(ClientContext &context, Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return expr.Cast<BoundConstantExpression>().GetValue();
	}
	return ExpressionExecutor::EvaluateScalar(context, expr, true);
}

//! Fold an option to a constant and cast it to its type like an explicit cast - overload selection never checked it.
//! A failed cast names the option, as the cast alone cannot tell which one it was
static Value PlaceOption(ClientContext &context, Expression &expr, const TypedKwarg &option) {
	auto value = FoldArgument(context, expr);
	auto option_type = FunctionBinder::BindTypeName(&context, option.type);
	if (RequiresCast(value.type(), option_type) != LogicalTypeComparisonResult::DIFFERENT_TYPES) {
		return value;
	}
	string error_message;
	auto result = value.TryCastAs(context, option_type, &error_message);
	if (!result) {
		throw InvalidInputException(expr, "Could not cast value %s to named parameter %s of type %s: %s",
		                            value.ToSQLString(), option.name, option_type.ToString(), error_message);
	}
	return std::move(*result);
}

//! Drain the arguments of a call into the order the function is bound with: by the parameter that receives them,
//! with its default for every parameter the call leaves out
static vector<unique_ptr<Expression>>
ResolveArguments(const FunctionBinding &binding, vector<unique_ptr<Expression>> arguments,
                 vector<pair<Identifier, unique_ptr<Expression>>> named_arguments) {
	const auto &sig = binding.GetFunction().GetSignature();
	const auto received_positional_count = arguments.size();
	for (auto &argument : binding.layout.arguments) {
		if (argument.type.id() == LogicalTypeId::UNKNOWN) {
			// no argument determines the type of an unresolved parameter - the call is bound again at EXECUTE
			throw ParameterNotResolvedException();
		}
		D_ASSERT(!TypeVisitor::Contains(argument.type, LogicalTypeId::UNKNOWN));
	}

	vector<unique_ptr<Expression>> result;
	result.reserve(binding.layout.arguments.size());
	for (auto &argument : binding.layout.arguments) {
		if (!argument.argument_index.IsValid()) {
			// the call matches the function, so every parameter it leaves out has a default
			const auto &param = sig.GetParameter(argument.parameter_index);
			result.push_back(make_uniq<BoundConstantExpression>(*param.GetDefaultValue(), param.GetName()));
			continue;
		}
		auto i = argument.argument_index.GetIndex();
		result.push_back(std::move(
		    i < received_positional_count ? arguments[i] : named_arguments[i - received_positional_count].second));
	}
	return result;
}

//! Fold the arguments of a table or pragma function to the constants it is called with. The arguments passed by
//! position fill "parameters", so "range(1, col1 := 5)" reaches the callback exactly as "range(1, 5)" does. The
//! keyword-only and "**kwargs" arguments go into "named_parameters" - an option "**kwargs" declares by the name of the
//! option, cast to its type like an explicit cast, as it takes no part in selection - the layout is updated to pass
//! it by that name, as that type. "arguments" are the arguments the function is bound with, from "first_argument" on
static void FoldArguments(ClientContext &context, const SimpleFunction &function, ArgumentLayout &layout,
                          vector<unique_ptr<Expression>> &arguments, idx_t first_argument, vector<Value> &parameters,
                          named_argument_map_t &named_parameters) {
	auto &signature = function.GetSignature();
	auto option_schema = signature.GetTypedKwargs();
	for (idx_t i = first_argument; i < layout.arguments.size(); i++) {
		auto &argument = *arguments[i - first_argument];
		if (i < layout.positional_count) {
			// A TABLE parameter is filled by the subquery itself rather than by a value, so its slot holds a plain
			// NULL. A NULL of type TABLE would not do: TABLE has no physical type, so it cannot be deserialized
			parameters.push_back(
			    layout.arguments[i].type.id() == LogicalTypeId::TABLE ? Value() : FoldArgument(context, argument));
			continue;
		}
		auto &name = layout.named_arguments[i - layout.positional_count];
		if (!option_schema || signature.GetParameterIndexByName(name).IsValid()) {
			named_parameters.insert(make_pair(name, FoldArgument(context, argument)));
			continue;
		}
		auto option_ptr = option_schema->Find(name);
		if (!option_ptr) {
			// only the options that resemble the name - listing every option buries the one that was meant
			const double similarity_threshold = 0.8;
			auto lower_name = StringUtil::Lower(name.GetIdentifierName());
			vector<pair<string, double>> scores;
			for (auto &option_name : option_schema->GetNames()) {
				auto score = StringUtil::SimilarityRating(option_name.GetIdentifierName(), lower_name);
				if (score >= similarity_threshold) {
					scores.emplace_back(option_name.GetIdentifierName(), score);
				}
			}
			auto similar = StringUtil::TopNStrings(scores, 3, similarity_threshold);
			throw BinderException(argument.GetQueryLocation(), "Invalid named parameter %s for function %s%s", name,
			                      function.GetName().GetIdentifierName(),
			                      StringUtil::CandidatesMessage(similar, "Did you mean"));
		}
		auto &option = *option_ptr;
		if (named_parameters.find(option.name) != named_parameters.end()) {
			throw BinderException(argument.GetQueryLocation(),
			                      "Named parameter %s was passed more than once in function call to %s", option.name,
			                      function.GetName().GetIdentifierName());
		}
		auto value = PlaceOption(context, argument, option);
		layout.arguments[i].type = value.type();
		name = option.name;
		named_parameters.insert(make_pair(option.name, std::move(value)));
	}
}

unique_ptr<ResolvedTableFunction>
FunctionBinder::BindTableFunction(const TableFunctionSet &functions,
                                  vector<unique_ptr<Expression>> &positional_arguments,
                                  vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error) {
	VerifyNamedArgumentsAccepted(functions, named_arguments);
	auto selection = SelectFunction(GetTypeCache(), functions, {positional_arguments, named_arguments}, false,
	                                FixedDecimalWidthLimit());
	if (!selection) {
		error = selection.GetError();
		return nullptr;
	}
	auto &binding = selection.GetBinding();
	auto definition = binding.GetFunction<TableFunction>();
	auto arguments = ResolveArguments(binding, std::move(positional_arguments), std::move(named_arguments));
	CastToFunctionArguments(definition->GetName(), binding.layout.GetArgumentTypes(), arguments);
	vector<Value> parameters;
	named_argument_map_t named_parameters;
	auto layout = binding.layout;
	FoldArguments(context, *definition, layout, arguments, 0, parameters, named_parameters);
	// copied out of the set: binding the table function fills in the bound return types
	return make_uniq<ResolvedTableFunction>(BoundTableFunction(definition, layout), std::move(parameters),
	                                        std::move(named_parameters));
}

unique_ptr<ResolvedTableFunction>
FunctionBinder::BindTableInOutFunction(const TableFunctionSet &functions, const vector<LogicalType> &input_types,
                                       vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments,
                                       ErrorData &error) {
	VerifyNamedArgumentsAccepted(functions, named_arguments);
	vector<unique_ptr<Expression>> input_arguments;
	for (idx_t i = 0; i < input_types.size(); i++) {
		input_arguments.push_back(make_uniq<BoundReferenceExpression>(input_types[i], storage_t(i)));
	}
	auto selection =
	    SelectFunction(GetTypeCache(), functions, {input_arguments, named_arguments}, false, FixedDecimalWidthLimit());
	if (!selection) {
		error = selection.GetError();
		return nullptr;
	}
	auto &binding = selection.GetBinding();
	auto definition = binding.GetFunction<TableFunction>();
	for (const auto &argument : named_arguments) {
		auto parameter = definition->GetSignature().GetParameterIndexByName(argument.first);
		if (parameter.IsValid() && definition->GetSignature().GetParameter(parameter.GetIndex()).AcceptsPosition()) {
			throw BinderException("Table in-out function input columns must be passed positionally: %s",
			                      argument.first);
		}
	}
	auto arguments = ResolveArguments(binding, std::move(input_arguments), std::move(named_arguments));
	// Input columns remain in the subquery; only named options are cast and folded here.
	const auto positional_count = binding.layout.positional_count;
	arguments.erase(arguments.begin(),
	                arguments.begin() + NumericCast<vector<LogicalType>::difference_type>(positional_count));
	vector<LogicalType> option_types;
	for (idx_t i = positional_count; i < binding.layout.arguments.size(); i++) {
		option_types.push_back(binding.layout.arguments[i].type);
	}
	CastToFunctionArguments(definition->GetName(), option_types, arguments);
	vector<Value> parameters;
	named_argument_map_t named_parameters;
	auto layout = binding.layout;
	FoldArguments(context, *definition, layout, arguments, positional_count, parameters, named_parameters);
	return make_uniq<ResolvedTableFunction>(BoundTableFunction(definition, layout), std::move(parameters),
	                                        std::move(named_parameters));
}

unique_ptr<BoundPragmaInfo> FunctionBinder::BindPragmaFunction(
    const PragmaFunctionSet &functions, vector<unique_ptr<Expression>> &positional_arguments,
    vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error) {
	VerifyNamedArgumentsAccepted(functions, named_arguments);
	auto selection = SelectFunction(GetTypeCache(), functions, {positional_arguments, named_arguments}, false,
	                                FixedDecimalWidthLimit());
	if (!selection) {
		error = selection.GetError();
		return nullptr;
	}
	auto &binding = selection.GetBinding();
	auto definition = binding.GetFunction<PragmaFunction>();
	auto arguments = ResolveArguments(binding, std::move(positional_arguments), std::move(named_arguments));
	CastToFunctionArguments(definition->GetName(), binding.layout.GetArgumentTypes(), arguments);
	vector<Value> parameters;
	named_argument_map_t named_parameters;
	auto layout = binding.layout;
	FoldArguments(context, *definition, layout, arguments, 0, parameters, named_parameters);
	return make_uniq<BoundPragmaInfo>(*definition, std::move(parameters), std::move(named_parameters));
}

void FunctionBinder::CastToFunctionArguments(const Identifier &function_name, const vector<LogicalType> &argument_types,
                                             vector<unique_ptr<Expression>> &children) {
	// Varargs should be expanded by this point
	// If not, the function has somehow added more argument expressions during binding, which is not allowed.
	// There is one exception, count(*) which can be invoked internally with fewer arguments than the function
	// definition. That should be fixed separately though.
	D_ASSERT(children.size() <= argument_types.size());

	for (idx_t i = 0; i < children.size(); i++) {
		auto &target_type = argument_types[i];
		if (target_type.id() == LogicalTypeId::STRING_LITERAL || target_type.id() == LogicalTypeId::INTEGER_LITERAL) {
			throw InternalException(
			    "Function %s returned a STRING_LITERAL or INTEGER_LITERAL type - return an explicit type instead",
			    function_name);
		}
		target_type.Verify();
		// don't cast lambda children, their slot only ever holds a placeholder
		if (children[i]->GetReturnType().id() == LogicalTypeId::LAMBDA) {
			continue;
		}
		D_ASSERT(target_type.IsComplete());
		// check if the type of child matches the type of function argument
		// if not we need to add a cast
		auto cast_result = RequiresCast(children[i]->GetReturnType(), target_type);
		if (cast_result == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
			if (!target_type.IsComplete()) {
				throw InternalException("Function %s has incomplete argument type %s - an argument cannot be cast "
				                        "to it",
				                        function_name, target_type);
			}
			children[i] = BoundCastExpression::AddCastToType(context, std::move(children[i]), target_type);
		}
	}
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const Identifier &schema, const Identifier &name,
                                                          vector<unique_ptr<Expression>> children, ErrorData &error,
                                                          bool is_operator, optional_ptr<Binder> binder,
                                                          Identifier alias, QueryLocation query_location) {
	// bind the function
	auto &function = Catalog::GetSystemCatalog(context).GetEntry<ScalarFunctionCatalogEntry>(
	    context, QualifiedName(Catalog::GetSystemCatalog(context).GetName(), schema, name));
	D_ASSERT(function.type == CatalogType::SCALAR_FUNCTION_ENTRY);
	return BindScalarFunction(function, std::move(children), error, is_operator, binder, std::move(alias),
	                          query_location);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const Identifier &schema, const Identifier &name,
                                                          vector<unique_ptr<Expression>> children, bool is_operator,
                                                          optional_ptr<Binder> binder, Identifier alias,
                                                          QueryLocation query_location) {
	ErrorData error;
	auto result = BindScalarFunction(schema, name, std::move(children), error, is_operator, binder, std::move(alias),
	                                 query_location);
	if (!result) {
		error.Throw();
	}
	return result;
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunctionCatalogEntry &func,
                                                          vector<unique_ptr<Expression>> children, ErrorData &error,
                                                          bool is_operator, optional_ptr<Binder> binder,
                                                          Identifier alias, QueryLocation query_location) {
	vector<pair<Identifier, unique_ptr<Expression>>> arguments;
	arguments.reserve(children.size());
	for (auto &child : children) {
		arguments.emplace_back(string(), std::move(child));
	}
	return BindScalarFunction(func, std::move(arguments), error, is_operator, binder, std::move(alias), query_location);
}

//! Returns true if any argument is an untyped NULL, i.e. has type SQLNULL
static bool HasUntypedNullArgument(const vector<unique_ptr<Expression>> &regular_args,
                                   const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args) {
	for (auto &child : regular_args) {
		if (child->GetReturnType() == LogicalTypeId::SQLNULL) {
			return true;
		}
	}
	for (auto &child : keyword_args) {
		if (child.second->GetReturnType() == LogicalTypeId::SQLNULL) {
			return true;
		}
	}
	return false;
}

//! Evaluates a constant argument and returns true if it is NULL. A non-NULL result replaces the argument with a
//! constant: the argument has been evaluated anyway, and keeping the result means that it is not evaluated again by
//! every enclosing function call - which made binding nested constant expressions quadratic in their depth.
static bool FoldConstantArgument(ClientContext &context, unique_ptr<Expression> &child, optional_ptr<Binder> binder) {
	if (child->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return child->Cast<BoundConstantExpression>().GetValue().IsNull();
	}
	// a lambda has no value of its own, the function it is passed to executes its body
	if (child->GetExpressionClass() == ExpressionClass::BOUND_LAMBDA || !child->IsFoldable()) {
		return false;
	}
	if (binder && child->RequiresRebind()) {
		// the constant no longer shows that its value can change across queries
		binder->SetAlwaysRequireRebind();
	}
	Value result;
	if (!ExpressionExecutor::TryEvaluateScalar(context, *child, result)) {
		// the argument cannot be evaluated: leave it in place so that the error surfaces at execution
		return false;
	}
	if (result.IsNull()) {
		return true;
	}
	child = make_uniq<BoundConstantExpression>(std::move(result), child->GetAlias(), child->GetQueryLocation());
	return false;
}

//! Returns true if any argument is a constant expression that evaluates to NULL. Arguments that evaluate to a non-NULL
//! value are folded to constants along the way.
static bool HasNullConstantArgument(ClientContext &context, vector<unique_ptr<Expression>> &regular_args,
                                    vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
                                    optional_ptr<Binder> binder) {
	for (auto &child : regular_args) {
		if (FoldConstantArgument(context, child, binder)) {
			return true;
		}
	}
	for (auto &child : keyword_args) {
		if (FoldConstantArgument(context, child.second, binder)) {
			return true;
		}
	}
	return false;
}

static void PropagateReturnCollation(BoundSimpleFunction &bound_function, const FunctionProperties &props,
                                     const vector<unique_ptr<Expression>> &children, const string &pushed_collation);

//! The type of the NULL constant a function call is folded to - SQLNULL if the return type is left to the bind
//! callback, which is not invoked
static LogicalType NullReturnType(const LogicalType &return_type) {
	return return_type.IsComplete() ? return_type : LogicalType::SQLNULL;
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunctionCatalogEntry &func,
                                                          vector<pair<Identifier, unique_ptr<Expression>>> arguments,
                                                          ErrorData &error, bool is_operator,
                                                          optional_ptr<Binder> binder, Identifier alias,
                                                          QueryLocation query_location) {
	// select the best matching overload (this may name positional arguments by their alias for functions that opt
	// into implicit argument naming, e.g. struct_pack/row)
	vector<unique_ptr<Expression>> regular_args;
	vector<pair<Identifier, unique_ptr<Expression>>> keyword_args;
	auto best_function =
	    SelectFunctionFromArguments(func.functions, std::move(arguments), regular_args, keyword_args, error);
	if (!best_function) {
		return nullptr;
	}

	// found a matching function!
	auto &binding = *best_function;
	auto selected_function = binding.GetFunction<ScalarFunction>();
	auto &bound_function = *selected_function;

	// If any of the parameters are NULL, the function will just be replaced with a NULL constant.
	// Untyped NULLs never go through type resolution. For typed NULL constants, resolve the function's types only
	// when needed to determine the return type. Preserve result collation without invoking bind callbacks.
	if (bound_function.GetNullHandling() == FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		// whether the function declares a type that is the same whatever the arguments are
		const bool has_complete_return_type = bound_function.GetSignature().HasCompleteReturnType();
		if (HasUntypedNullArgument(regular_args, keyword_args)) {
			auto null_type =
			    has_complete_return_type ? BindReturnType(GetTypeCache(), binding) : LogicalType(LogicalType::SQLNULL);
			return make_uniq<BoundConstantExpression>(Value(std::move(null_type)), std::move(alias), query_location);
		}
		if (HasNullConstantArgument(context, regular_args, keyword_args, binder)) {
			if (has_complete_return_type) {
				auto null_function = CreateBoundFunction<BoundScalarFunction>(GetTypeCache(), binding);
				for (auto &child : keyword_args) {
					regular_args.push_back(std::move(child.second));
				}
				PropagateReturnCollation(null_function, null_function.GetProperties(), regular_args, string());
				return make_uniq<BoundConstantExpression>(Value(null_function.GetReturnType()), std::move(alias),
				                                          query_location);
			}
			// the return type follows from the arguments: resolve it without invoking the bind callback
			auto resolved = ResolveScalarFunction(binding, regular_args, keyword_args, false);
			return make_uniq<BoundConstantExpression>(Value(NullReturnType(resolved.first.GetReturnType())),
			                                          std::move(alias), query_location);
		}
	}
	return BindScalarFunction(binding, std::move(regular_args), std::move(keyword_args), is_operator, binder,
	                          std::move(alias), query_location);
}

static bool RequiresCollationPropagation(const LogicalType &type) {
	return type.id() == LogicalTypeId::VARCHAR && !type.HasAlias();
}

struct FunctionCollations {
	string collation;
	identifier_map_t<unique_ptr<FunctionCollations>> fields;

	FunctionCollations &GetField(const Identifier &name) {
		auto &field = fields[name];
		if (!field) {
			field = make_uniq<FunctionCollations>();
		}
		return *field;
	}
};

static Identifier CollationFieldName(const LogicalType &type, idx_t index) {
	return StructType::IsUnnamed(type) ? Identifier(to_string(index)) : StructType::GetChildName(type, index);
}

//! Recursively extracts the collation of a (possibly nested) type, e.g. the element collation of a LIST(VARCHAR).
//! target is the resolved argument type and may include fields from other arguments.
static void ExtractCollationFromType(const LogicalType &source_type, const LogicalType &target,
                                     FunctionCollations &collations) {
	// Leave collation of aliased types to their registered callbacks.
	if (source_type.HasAlias() || target.HasAlias()) {
		return;
	}
	switch (source_type.id()) {
	case LogicalTypeId::VARCHAR: {
		auto collation = StringType::GetCollation(source_type);
		if (collations.collation.empty()) {
			collations.collation = std::move(collation);
		} else if (!collation.empty() && collations.collation != collation) {
			throw BinderException("Cannot combine types with different collation");
		}
		break;
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		auto &source_child = source_type.id() == LogicalTypeId::LIST ? ListType::GetChildType(source_type)
		                                                             : ArrayType::GetChildType(source_type);
		auto &target_child = target.id() == LogicalTypeId::LIST    ? ListType::GetChildType(target)
		                     : target.id() == LogicalTypeId::ARRAY ? ArrayType::GetChildType(target)
		                                                           : source_child;
		ExtractCollationFromType(source_child, target_child, collations);
		break;
	}
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		auto &target_type = StructType::IsStruct(target) && target.HasParameters() ? target : source_type;
		auto &source_children = StructType::GetChildTypes(source_type);
		auto &target_children = StructType::GetChildTypes(target_type);

		// If BOTH source and target_type are named, we can use named indexing, else we use positional.
		if (!StructType::IsUnnamed(source_type) && !StructType::IsUnnamed(target_type)) {
			// Named indexing, so build a map to map source fields to their position
			identifier_map_t<idx_t> src_child_indices;
			for (idx_t i = 0; i < source_children.size(); i++) {
				src_child_indices.emplace(source_children[i].first, i);
			}

			for (idx_t i = 0; i < target_children.size(); i++) {
				// Find index in the source fields, or skip if non existent.
				auto entry = src_child_indices.find(target_children[i].first);
				if (entry == src_child_indices.end()) {
					continue;
				}
				const auto child_index = entry->second;
				auto &field = collations.GetField(CollationFieldName(target_type, i));
				ExtractCollationFromType(source_children[child_index].second, target_children[i].second, field);
			}
		} else {
			// Use positional indexing, as not both source_type and target_type are named.
			for (idx_t i = 0; i < MinValue(target_children.size(), source_children.size()); i++) {
				auto &field = collations.GetField(CollationFieldName(target_type, i));
				ExtractCollationFromType(source_children[i].second, target_children[i].second, field);
			}
		}
		break;
	}
	default:
		break;
	}
}

//! Returns a copy of the type with the collected collations applied to its VARCHAR fields.
static LogicalType ApplyCollationToType(const LogicalType &type, const FunctionCollations &collations) {
	// Rebuilding an aliased type would discard its alias and bypass its collation callbacks
	if (type.HasAlias()) {
		return type;
	}
	switch (type.id()) {
	case LogicalTypeId::VARCHAR:
		return collations.collation.empty() ? type : LogicalType::VARCHAR_COLLATION(collations.collation);
	case LogicalTypeId::LIST:
		return LogicalType::LIST(ApplyCollationToType(ListType::GetChildType(type), collations));
	case LogicalTypeId::ARRAY:
		return LogicalType::ARRAY(ApplyCollationToType(ArrayType::GetChildType(type), collations),
		                          ArrayType::GetSize(type));
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		auto children = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < children.size(); i++) {
			auto entry = collations.fields.find(CollationFieldName(type, i));
			if (entry != collations.fields.end()) {
				children[i].second = ApplyCollationToType(children[i].second, *entry->second);
			}
		}
		return StructType::IsUnnamed(type) ? LogicalType::TUPLE(std::move(children))
		                                   : LogicalType::STRUCT(std::move(children));
	}
	default:
		return type;
	}
}

static string ExtractCollation(const vector<unique_ptr<Expression>> &children) {
	string collation;
	for (auto &arg : children) {
		if (!RequiresCollationPropagation(arg->GetReturnType())) {
			// not a varchar column
			continue;
		}
		auto child_collation = StringType::GetCollation(arg->GetReturnType());
		if (collation.empty()) {
			collation = child_collation;
		} else if (!child_collation.empty() && collation != child_collation) {
			throw BinderException("Cannot combine types with different collation!");
		}
	}
	return collation;
}

//! Pushes the collations of the arguments into the children, returns the collation to propagate to the return type
static string PushCollations(ClientContext &context, BoundSimpleFunction &bound_function,
                             vector<unique_ptr<Expression>> &children, CollationType type, bool is_index_key) {
	FunctionCollations collations;
	auto &argument_types = bound_function.GetArguments();
	for (idx_t i = 0; i < children.size(); i++) {
		auto &source_type = children[i]->GetReturnType();
		D_ASSERT(argument_types[i].IsComplete());
		ExtractCollationFromType(source_type, argument_types[i], collations);
	}
	if (is_index_key && collations.collation.empty() && collations.fields.empty()) {
		// index keys only use explicit collations, comparison keys and the default collation would change them
		return string();
	}
	// push collations to the children
	for (idx_t i = 0; i < children.size(); i++) {
		auto &arg = children[i];
		// apply the collations to the (possibly nested) varchar leaves of the argument type
		auto collated_type = ApplyCollationToType(argument_types[i], collations);
		arg = BoundCastExpression::AddCastToType(context, std::move(arg), collated_type);
		if (RequiresCollationPropagation(arg->GetReturnType())) {
			// if this is a varchar type - propagate the collation
			arg->SetReturnType(collated_type);
		}
		// now push the actual collation handling
		if (ExpressionBinder::PushCollation(context, arg, collated_type, type)) {
			// Normalization can change field types, so do not cast back to the original argument type.
			argument_types[i] = arg->GetReturnType();
		}
	}
	return collations.collation;
}

//! Pushes collations into the children (if required), returns the collation to propagate to the return type
static string PushArgumentCollations(ClientContext &context, BoundSimpleFunction &bound_function,
                                     const FunctionProperties &props, vector<unique_ptr<Expression>> &children,
                                     bool is_index_key) {
	switch (props.GetCollationHandling()) {
	case FunctionCollationHandling::IGNORE_COLLATIONS:
	case FunctionCollationHandling::PROPAGATE_COLLATIONS:
		return string();
	case FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS:
		return PushCollations(context, bound_function, children, CollationType::COMBINABLE_COLLATIONS, is_index_key);
	case FunctionCollationHandling::PUSH_DISTINCT_COLLATIONS:
		throw InternalException("PUSH_DISTINCT_COLLATIONS is only supported for aggregate functions");
	default:
		throw InternalException("Unrecognized collation handling");
	}
}

//! Propagates the collation of the arguments to the return type of the function (if it returns a varchar)
static void PropagateReturnCollation(BoundSimpleFunction &bound_function, const FunctionProperties &props,
                                     const vector<unique_ptr<Expression>> &children, const string &pushed_collation) {
	if (!RequiresCollationPropagation(bound_function.GetReturnType())) {
		return;
	}
	auto collation = props.GetCollationHandling() == FunctionCollationHandling::PROPAGATE_COLLATIONS
	                     ? ExtractCollation(children)
	                     : pushed_collation;
	if (collation.empty()) {
		return;
	}
	bound_function.SetReturnType(LogicalType::VARCHAR_COLLATION(std::move(collation)));
}

// The names of the named arguments have to stay parallel to the last arguments
static void VerifyArgumentCount(const BoundSimpleFunction &bound_function, idx_t argument_count) {
	const auto resolved_count = bound_function.GetPositionalArgumentCount() + bound_function.GetNamedArguments().size();
	if (resolved_count != argument_count && !bound_function.GetNamedArguments().empty()) {
		throw InternalException("Function %s cannot add or remove arguments in its bind callback when it is "
		                        "called with keyword-only or '**kwargs' arguments",
		                        bound_function.GetName());
	}
}

//! The arguments are cast before the bind callback - verify that it did not change the argument types
static void VerifyBindArgumentTypes(const BoundScalarFunction &bound_function,
                                    const vector<unique_ptr<Expression>> &arguments) {
	auto &argument_types = bound_function.GetArguments();
	if (arguments.size() > argument_types.size()) {
		throw InternalException("Function %s added arguments in its bind callback", bound_function.GetName());
	}
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &source_type = arguments[i]->GetReturnType();
		if (source_type.id() == LogicalTypeId::LAMBDA) {
			continue;
		}
		auto target_type = argument_types[i];
		if (target_type.id() == LogicalTypeId::UNKNOWN || target_type.id() == LogicalTypeId::INVALID) {
			continue;
		}
		if (RequiresCast(source_type, target_type) == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
			throw InternalException("Function %s changed the type of argument %llu from %s to %s in its bind "
			                        "callback - an argument is cast when the return type is resolved",
			                        bound_function.GetName(), i + 1, source_type, target_type);
		}
	}
}

//! The types the function is called with, before the bind callback changes any of them
static vector<LogicalType> CaptureLogicalArguments(const BoundSimpleFunction &function,
                                                   const vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(function.GetArguments().size() == arguments.size());
	for (idx_t argument_index = 0; argument_index < arguments.size(); argument_index++) {
		D_ASSERT(function.GetArguments()[argument_index].IsComplete());
	}
	return function.GetArguments();
}

pair<BoundScalarFunction, unique_ptr<FunctionData>>
FunctionBinder::ResolveScalarFunction(const FunctionBinding &binding, vector<unique_ptr<Expression>> &arguments,
                                      vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	return ResolveScalarFunction(binding, arguments, named_arguments, true);
}

pair<BoundScalarFunction, unique_ptr<FunctionData>>
FunctionBinder::ResolveScalarFunction(const FunctionBinding &binding, vector<unique_ptr<Expression>> &arguments,
                                      vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments,
                                      bool invoke_bind) {
	auto &function = binding.GetFunction();
	// Make a BoundScalarFunction out of the ScalarFunction, so we can store bind info and other properties in it.
	auto bound_function = CreateBoundFunction<BoundScalarFunction>(GetTypeCache(), binding);

	// Reorder named args and expand the variadic arguments
	arguments = ResolveArguments(binding, std::move(arguments), std::move(named_arguments));
	bound_function.SetNamedArguments(binding.layout.positional_count, binding.layout.named_arguments);

	bound_function.SetLogicalArguments(CaptureLogicalArguments(bound_function, arguments));

	// The resolver and the bind callback are called with arguments that are cast to the argument types of the function
	const bool is_index_key = arguments_collated || (binder && binder->IsBindingIndexKey());
	auto pushed_collation =
	    PushArgumentCollations(context, bound_function, bound_function.GetProperties(), arguments, is_index_key);
	CastToFunctionArguments(bound_function.GetName(), bound_function.GetArguments(), arguments);
	// a return type that is marked as ANY is what the resolver of the signature makes of the arguments
	auto &signature = function.GetSignature();
	if (auto resolver = signature.GetReturnType().GetResolver()) {
		ResolveReturnTypeInput input(context, bound_function, signature, arguments, binder, decimal_width_limit);
		bound_function.SetReturnType(resolver(input));
	}

	unique_ptr<FunctionData> bind_info;
	if (invoke_bind) {
		if (binder && (bound_function.HasBindCallback() || bound_function.HasBindExpressionCallback())) {
			// the callbacks can evaluate constant arguments and drop them, which hides casts that read settings
			for (auto &argument : arguments) {
				if (argument->IsFoldable() && argument->RequiresRebind()) {
					binder->SetAlwaysRequireRebind();
					break;
				}
			}
		}
		if (bound_function.HasBindCallback()) {
			BindScalarFunctionInput input(context, bound_function, arguments, binder, decimal_width_limit);
			bind_info = bound_function.GetBindCallback()(input);
			VerifyArgumentCount(bound_function, arguments.size());
			VerifyBindArgumentTypes(bound_function, arguments);
		}

		if (bound_function.HasModifiedDatabasesCallback() && binder) {
			auto &properties = binder->GetStatementProperties();
			FunctionModifiedDatabasesInput input(bind_info.get(), properties);
			bound_function.GetModifiedDatabasesCallback()(context, input);
		}
		// a signature that declares no return type leaves it to the resolver or the bind callback
		D_ASSERT(bound_function.GetReturnType().id() != LogicalTypeId::INVALID);
	}

	PropagateReturnCollation(bound_function, bound_function.GetProperties(), arguments, pushed_collation);
	bound_function.SetLogicalReturnType(bound_function.GetReturnType());

	return {std::move(bound_function), std::move(bind_info)};
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const shared_ptr<const ScalarFunction> &function,
                                                          vector<unique_ptr<Expression>> children, bool is_operator,
                                                          optional_ptr<Binder> binder, Identifier alias,
                                                          QueryLocation query_location) {
	return BindScalarFunction(function, std::move(children), {}, is_operator, binder, std::move(alias), query_location);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunction &function,
                                                          vector<unique_ptr<Expression>> children, bool is_operator,
                                                          optional_ptr<Binder> binder, Identifier alias,
                                                          QueryLocation query_location) {
	return BindScalarFunction(make_shared_ptr<ScalarFunction>(function), std::move(children), {}, is_operator, binder,
	                          std::move(alias), query_location);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const ScalarFunction &function,
                                                          vector<unique_ptr<Expression>> children,
                                                          vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                                          bool is_operator, optional_ptr<Binder> binder,
                                                          Identifier alias, QueryLocation query_location) {
	return BindScalarFunction(make_shared_ptr<ScalarFunction>(function), std::move(children), std::move(keyword_args),
	                          is_operator, binder, std::move(alias), query_location);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const shared_ptr<const ScalarFunction> &function,
                                                          vector<unique_ptr<Expression>> children,
                                                          vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                                          bool is_operator, optional_ptr<Binder> binder,
                                                          Identifier alias, QueryLocation query_location) {
	auto binding = MatchFunction(&context, function, {children, keyword_args}, FixedDecimalWidthLimit());
	return BindScalarFunction(binding, std::move(children), std::move(keyword_args), is_operator, binder,
	                          std::move(alias), query_location);
}

unique_ptr<Expression> FunctionBinder::BindScalarFunction(const FunctionBinding &binding,
                                                          vector<unique_ptr<Expression>> children,
                                                          vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                                          bool is_operator, optional_ptr<Binder> binder,
                                                          Identifier alias, QueryLocation query_location) {
	auto [bound_function, bind_info] = ResolveScalarFunction(binding, children, keyword_args);
	auto result_func = make_uniq<BoundFunctionExpression>(std::move(bound_function), std::move(children),
	                                                      std::move(bind_info), is_operator, alias, query_location);

	if (binder && result_func->Function().GetStability() == FunctionStability::CONSISTENT_WITHIN_QUERY) {
		// the result depends on the query it is bound in, also when the bind_expression callback folds it
		binder->SetAlwaysRequireRebind();
	}
	if (result_func->Function().HasBindExpressionCallback()) {
		// if a bind_expression callback is registered - call it and emit the resulting expression
		FunctionBindExpressionInput input(context, result_func->FunctionMutable(), result_func->BindInfoMutable().get(),
		                                  result_func->GetChildrenMutable(), this->binder, decimal_width_limit);
		auto result = result_func->Function().GetBindExpressionCallback()(input);
		if (result) {
			// the callback constructs the expression itself
			if (!alias.empty()) {
				result->SetAlias(std::move(alias));
			}
			if (query_location.IsValid()) {
				result->SetQueryLocation(query_location);
			}
			return result;
		}
	}
	return std::move(result_func);
}

pair<BoundAggregateFunction, unique_ptr<FunctionData>>
FunctionBinder::ResolveAggregateFunction(const FunctionBinding &binding, vector<unique_ptr<Expression>> &children,
                                         vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	auto &function = binding.GetFunction();
	// Make a BoundFunction out of the func
	auto bound_function = CreateBoundFunction<BoundAggregateFunction>(GetTypeCache(), binding);

	// Reorder named args and expand the variadic arguments
	children = ResolveArguments(binding, std::move(children), std::move(named_arguments));
	bound_function.SetNamedArguments(binding.layout.positional_count, binding.layout.named_arguments);

	bound_function.SetLogicalArguments(CaptureLogicalArguments(bound_function, children));

	unique_ptr<FunctionData> bind_info;

	// a return type that is marked as ANY is what the resolver of the signature makes of the arguments
	auto &signature = function.GetSignature();
	if (auto resolver = signature.GetReturnType().GetResolver()) {
		ResolveReturnTypeInput input(context, bound_function, signature, children, binder, decimal_width_limit);
		bound_function.SetReturnType(resolver(input));
	}
	if (bound_function.GetCallbacks().HasBindCallback()) {
		BindAggregateFunctionInput input(context, bound_function, children, binder, decimal_width_limit);
		bind_info = bound_function.GetCallbacks().GetBindCallback()(input);

		// we may have lost some arguments in the bind
		children.resize(MinValue(bound_function.GetArguments().size(), children.size()));
		VerifyArgumentCount(bound_function, children.size());
	}
	// a signature that declares no return type leaves it to the resolver or the bind callback
	D_ASSERT(bound_function.GetReturnType().id() != LogicalTypeId::INVALID);

	bound_function.SetLogicalReturnType(bound_function.GetReturnType());

	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function.GetName(), bound_function.GetArguments(), children);

	return {std::move(bound_function), std::move(bind_info)};
}

unique_ptr<BoundAggregateExpression>
FunctionBinder::BindAggregateFunction(const shared_ptr<const AggregateFunction> &function,
                                      vector<unique_ptr<Expression>> children, unique_ptr<Expression> filter,
                                      AggregateType aggr_type) {
	return BindAggregateFunction(function, std::move(children), {}, std::move(filter), aggr_type);
}

unique_ptr<BoundAggregateExpression> FunctionBinder::BindAggregateFunction(const AggregateFunction &function,
                                                                           vector<unique_ptr<Expression>> children,
                                                                           unique_ptr<Expression> filter,
                                                                           AggregateType aggr_type) {
	return BindAggregateFunction(make_shared_ptr<AggregateFunction>(function), std::move(children), {},
	                             std::move(filter), aggr_type);
}

unique_ptr<BoundAggregateExpression>
FunctionBinder::BindAggregateFunction(const AggregateFunction &function, vector<unique_ptr<Expression>> children,
                                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                      unique_ptr<Expression> filter, AggregateType aggr_type) {
	return BindAggregateFunction(make_shared_ptr<AggregateFunction>(function), std::move(children),
	                             std::move(keyword_args), std::move(filter), aggr_type);
}

unique_ptr<BoundAggregateExpression>
FunctionBinder::BindAggregateFunction(const shared_ptr<const AggregateFunction> &function,
                                      vector<unique_ptr<Expression>> children,
                                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                      unique_ptr<Expression> filter, AggregateType aggr_type) {
	auto binding = MatchFunction(&context, function, {children, keyword_args}, FixedDecimalWidthLimit());
	return BindAggregateFunction(std::move(binding), std::move(children), std::move(keyword_args), std::move(filter),
	                             aggr_type);
}

unique_ptr<BoundAggregateExpression>
FunctionBinder::BindAggregateFunction(FunctionBinding binding, vector<unique_ptr<Expression>> children,
                                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                      unique_ptr<Expression> filter, AggregateType aggr_type) {
	auto function = binding.GetFunction<AggregateFunction>();
	if (aggr_type == AggregateType::DISTINCT &&
	    function->GetCollationHandling() == FunctionCollationHandling::PUSH_DISTINCT_COLLATIONS) {
		for (auto &child : children) {
			ExpressionBinder::PushCollation(context, child, child->GetReturnType());
		}
		for (auto &argument : keyword_args) {
			ExpressionBinder::PushCollation(context, argument.second, argument.second->GetReturnType());
		}
		// the arguments are replaced by their comparison keys, which may be of other types
		binding = MatchFunction(&context, function, {children, keyword_args}, FixedDecimalWidthLimit());
	}
	auto [bound_function, bind_info] = ResolveAggregateFunction(binding, children, keyword_args);

	return make_uniq<BoundAggregateExpression>(std::move(bound_function), std::move(children), std::move(filter),
	                                           std::move(bind_info), aggr_type);
}

unique_ptr<BoundAggregateExpression>
FunctionBinder::BindAggregateFunction(const AggregateFunctionCatalogEntry &func,
                                      vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
                                      unique_ptr<Expression> filter, AggregateType aggr_type) {
	// select the best matching overload (this may name positional arguments by their alias for functions that opt
	// into implicit argument naming, e.g. struct_pack/row)
	vector<unique_ptr<Expression>> regular_args;
	vector<pair<Identifier, unique_ptr<Expression>>> keyword_args;
	auto best_function =
	    SelectFunctionFromArguments(func.functions, std::move(arguments), regular_args, keyword_args, error);
	if (!best_function) {
		return nullptr;
	}

	return BindAggregateFunction(std::move(*best_function), std::move(regular_args), std::move(keyword_args),
	                             std::move(filter), aggr_type);
}

unique_ptr<BoundAggregateExpression> FunctionBinder::BindAggregateFunction(const AggregateFunctionCatalogEntry &func,
                                                                           vector<unique_ptr<Expression>> children,
                                                                           ErrorData &error,
                                                                           unique_ptr<Expression> filter,
                                                                           AggregateType aggr_type) {
	vector<pair<Identifier, unique_ptr<Expression>>> arguments;
	arguments.reserve(children.size());
	for (auto &child : children) {
		arguments.emplace_back(Identifier(), std::move(child));
	}
	return BindAggregateFunction(func, std::move(arguments), error, std::move(filter), aggr_type);
}

pair<BoundWindowFunction, unique_ptr<FunctionData>>
FunctionBinder::ResolveWindowFunction(const FunctionBinding &binding, vector<unique_ptr<Expression>> &children,
                                      vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments,
                                      optional_ptr<vector<LogicalType>> order_types,
                                      optional_ptr<vector<LogicalType>> arg_order_types) {
	auto &function = binding.GetFunction();
	auto bound_function = CreateBoundFunction<BoundWindowFunction>(GetTypeCache(), binding);

	// Reorder named args and expand the variadic arguments
	children = ResolveArguments(binding, std::move(children), std::move(named_arguments));
	bound_function.SetNamedArguments(binding.layout.positional_count, binding.layout.named_arguments);

	auto logical_arguments = CaptureLogicalArguments(bound_function, children);
	auto argument_types = bound_function.GetArguments();

	unique_ptr<FunctionData> bind_info;

	// a return type that is marked as ANY is what the resolver of the signature makes of the arguments
	auto &signature = function.GetSignature();
	if (auto resolver = signature.GetReturnType().GetResolver()) {
		ResolveReturnTypeInput input(context, bound_function, signature, children, binder, decimal_width_limit);
		bound_function.SetReturnType(resolver(input));
	}
	if (bound_function.HasBindCallback()) {
		BindWindowFunctionInput input(context, bound_function, children, order_types, arg_order_types, binder);
		bind_info = bound_function.GetBindCallback()(input);
		// we may have lost some arguments in the bind
		children.resize(MinValue(bound_function.GetArguments().size(), children.size()));
		VerifyArgumentCount(bound_function, children.size());
	}
	// a signature that declares no return type leaves it to the resolver or the bind callback
	D_ASSERT(bound_function.GetReturnType().id() != LogicalTypeId::INVALID);

	for (idx_t i = 0; i < argument_types.size() && i < bound_function.GetArguments().size(); i++) {
		// an argument the bind callback gave another type is called with that type
		if (argument_types[i] != bound_function.GetArguments()[i] && bound_function.GetArguments()[i].IsComplete()) {
			logical_arguments[i] = bound_function.GetArguments()[i];
		}
	}
	bound_function.SetLogicalArguments(std::move(logical_arguments));
	bound_function.SetLogicalReturnType(bound_function.GetReturnType());

	// check if we need to add casts to the children
	CastToFunctionArguments(bound_function.GetName(), bound_function.GetArguments(), children);

	return {std::move(bound_function), std::move(bind_info)};
}

unique_ptr<BoundWindowExpression>
FunctionBinder::BindWindowFunction(const shared_ptr<const WindowFunction> &function,
                                   vector<unique_ptr<Expression>> children,
                                   vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                   vector<LogicalType> &order_types, vector<LogicalType> &arg_order_types) {
	auto binding = MatchFunction(&context, function, {children, keyword_args}, FixedDecimalWidthLimit());
	return BindWindowFunction(binding, std::move(children), std::move(keyword_args), order_types, arg_order_types);
}

unique_ptr<BoundWindowExpression>
FunctionBinder::BindWindowFunction(const FunctionBinding &binding, vector<unique_ptr<Expression>> children,
                                   vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
                                   vector<LogicalType> &order_types, vector<LogicalType> &arg_order_types) {
	auto [bound_function, bind_info] =
	    ResolveWindowFunction(binding, children, keyword_args, order_types, arg_order_types);
	auto return_type = bound_function.GetReturnType();

	auto window = make_uniq<BoundWindowFunction>(std::move(bound_function));
	auto result = make_uniq<BoundWindowExpression>(return_type, nullptr, std::move(window), std::move(bind_info));
	result->GetChildrenMutable() = std::move(children);

	return result;
}

unique_ptr<BoundWindowExpression>
FunctionBinder::BindWindowFunction(const WindowFunctionCatalogEntry &func,
                                   vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
                                   vector<LogicalType> &order_types, vector<LogicalType> &arg_order_types) {
	// select the best matching overload
	vector<unique_ptr<Expression>> regular_args;
	vector<pair<Identifier, unique_ptr<Expression>>> keyword_args;
	auto best_function =
	    SelectFunctionFromArguments(func.functions, std::move(arguments), regular_args, keyword_args, error);
	if (!best_function) {
		return nullptr;
	}

	return BindWindowFunction(*best_function, std::move(regular_args), std::move(keyword_args), order_types,
	                          arg_order_types);
}

} // namespace duckdb
