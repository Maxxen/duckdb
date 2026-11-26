#include "duckdb/function/function_binder.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/function/type_constructor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

//! Whether a type is of the given kind. A struct with and without names are of one kind, and so are a list and an
//! array: which of the two a function takes is its to check, or up to the cast rules. A NULL is of every kind
static bool IsOfKind(const LogicalType &type, LogicalTypeId kind) {
	const bool takes_struct = kind == LogicalTypeId::STRUCT || kind == LogicalTypeId::TUPLE;
	const bool is_struct = type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::TUPLE;
	const bool takes_list = kind == LogicalTypeId::LIST || kind == LogicalTypeId::ARRAY;
	const bool is_list = type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::ARRAY;
	return type.id() == kind || type.id() == LogicalTypeId::SQLNULL || (takes_struct && is_struct) ||
	       (takes_list && is_list);
}

//! The type that a type is passed as where a type of the given kind is declared with its arguments: itself if it is
//! of the kind, else e.g. DECIMAL(10,0) for an INTEGER passed as a DECIMAL(W, S) - nullopt if it is not passed as
//! one. The cases are hard-coded for now: a generic coercion lookup of the cast rules is to take the place of this
//! function
static optional<LogicalType> CoerceToKind(const LogicalType &type, LogicalTypeId kind) {
	if (IsOfKind(type, kind)) {
		return type;
	}
	uint32_t width, scale;
	if (kind == LogicalTypeId::DECIMAL && type.GetDecimalProperties(width, scale)) {
		// a number is passed as the decimal that holds it
		return LogicalType::DECIMAL(width, scale);
	}
	return nullopt;
}

static int64_t ImplicitCastCost(optional_ptr<ClientContext> context, const LogicalType &source,
                                const LogicalType &target) {
	if (context) {
		return CastFunctionSet::ImplicitCastCost(*context, source, target);
	}
	// without a context we can only consider the built-in cast rules - no registered casts are reachable
	return CastRules::ImplicitCast(source, target);
}

//! Why a call does not match an overload, as far as the overload itself is not needed to say it. The distance is how
//! far the call is from matching (see FunctionMismatch::GetDistance): the number of arguments that cannot be passed -
//! 0 if it is only the variables that the arguments do not determine - and more than that for the wrong shape
struct MatchFailure {
	idx_t distance;
	FunctionMismatch::Reason reason;
};

//===--------------------------------------------------------------------===//
// Binding type names
//===--------------------------------------------------------------------===//
//! A NULL or an unresolved parameter: its type says nothing about the type it is passed as
static bool IsUntyped(const LogicalType &type) {
	return type.id() == LogicalTypeId::SQLNULL || type.id() == LogicalTypeId::UNKNOWN;
}

//! The variable of the signature a type name is, if it is one: no variable is named like a built-in type
static optional_ptr<const TypeVariable> FindVariable(const FunctionSignature &signature, const TypeName &name) {
	if (name.HasArguments() || name.GetBuiltinType() != LogicalTypeId::INVALID) {
		return nullptr;
	}
	return signature.FindVariable(name.GetName());
}

//! The value variable of the signature an argument of a type name is, if it is one
static optional_ptr<const TypeVariable> FindValueVariable(const FunctionSignature &signature,
                                                          const TypeNameArgument &argument) {
	auto variable = argument.IsType() ? FindVariable(signature, argument.GetType()) : nullptr;
	return variable && variable->IsValue() ? variable : nullptr;
}

//! The type of the values a value variable stands for
static LogicalType ValueVariableType(const TypeVariable &variable) {
	return variable.GetKind() == TypeVariable::Kind::INTEGER ? LogicalType::BIGINT : LogicalType::VARCHAR;
}

//! The type a type variable is, given the type the arguments say it is: normalized. A variable that only an
//! unresolved parameter says leaves the call unresolved. Nothing if there is no type, or it is not of the kind of a
//! constrained variable: no type is converted to be of the kind, and a type with an alias is a type of its own
static optional<LogicalType> GetVariableType(const LogicalType &type, LogicalTypeId constraint) {
	if (type.id() == LogicalTypeId::INVALID) {
		return nullopt;
	}
	// throws for an unresolved parameter
	auto result = LogicalType::NormalizeType(type);
	if (constraint != LogicalTypeId::INVALID && !(result.GetAlias().empty() && IsOfKind(result, constraint))) {
		return nullopt;
	}
	return result;
}

//! The parameter of a type that an argument of a type name is paired with: the one of its label if both have a name,
//! else the one at its position
static optional_idx FindTypeParameter(const LogicalType &type, const vector<TypeNameArgument> &arguments,
                                      idx_t argument_index) {
	const auto parameter_count = type.GetParameterCount();
	bool has_labels = false;
	for (auto &argument : arguments) {
		has_labels = has_labels || !argument.GetLabel().empty();
	}
	auto &label = arguments[argument_index].GetLabel();
	if (has_labels && !label.empty()) {
		for (idx_t i = 0; i < parameter_count; i++) {
			if (type.GetParameterName(i) == label) {
				return i;
			}
		}
	}
	if (argument_index < parameter_count && (!has_labels || type.GetParameterName(argument_index).empty())) {
		return argument_index;
	}
	return optional_idx();
}

//! Whether what an argument of a type name stands for is the parameter of the type it is paired with
static bool IsSameParameter(const Value &value, const Value &parameter) {
	const bool is_type = parameter.type().id() == LogicalTypeId::TYPE;
	if (is_type != (value.type().id() == LogicalTypeId::TYPE)) {
		return false;
	}
	if (is_type) {
		return TypeValue::GetType(value).EqualsIncludingCollation(TypeValue::GetType(parameter));
	}
	// the same value, as values of the type of the parameter
	auto cast_value = value.DefaultTryCastAs(parameter.type());
	return cast_value && Value::NotDistinctFrom(*cast_value, parameter);
}

//! The type whose parameters the arguments of a type name are paired with, for a type that is passed as the name:
//! itself if it is of the given kind, which the name constructs, else the type it is passed as where one of that kind
//! is expected - e.g. DECIMAL(10,0) for an INTEGER, a list for an array. Nothing if there is none: a type of another
//! kind, e.g. a map for a list, says nothing about the arguments of the name. Nor if there is no type, or if it is of
//! the kind and a value the name declares is not the one it has
static optional<LogicalType> PairType(const TypeName &name, LogicalTypeId kind, const LogicalType &type) {
	if (type.id() == LogicalTypeId::INVALID) {
		return nullopt;
	}
	// a name that is not a built-in kind is the alias of the types of its kind
	const bool is_kind = kind == LogicalTypeId::INVALID ? name.GetName() == type.GetAlias() : type.id() == kind;
	auto paired = is_kind ? optional<LogicalType>(type) : CoerceToKind(type, kind);
	auto &arguments = name.GetArguments();
	for (idx_t i = 0; is_kind && i < arguments.size(); i++) {
		auto parameter_idx = FindTypeParameter(type, arguments, i);
		if (arguments[i].IsType() || !parameter_idx.IsValid()) {
			continue;
		}
		if (!IsSameParameter(arguments[i].GetLiteral().ToValue(), type.GetParameter(parameter_idx.GetIndex()))) {
			return nullopt;
		}
	}
	return paired;
}

//! Binds a type name: the type it stands for with the variables of the signature as "variables" says they are. For
//! the type of a parameter, "instance" is the type of the argument that is passed to it - invalid for a part of the
//! name the argument has no type in the place of. A variable that "variables" does not say is what the one argument
//! says it is, see GetVariableType: ANY, which is a type variable of its own for every argument, and every variable
//! when it is the type an argument has by itself that is asked for. Without an argument ANY is a type. Returns
//! nothing if a variable is not determined
static optional<LogicalType> BindTypeNameInternal(TypeCache &cache, const TypeName &name,
                                                  const FunctionSignature &signature,
                                                  const FunctionVariables &variables,
                                                  optional_ptr<const LogicalType> instance) {
	auto &identifier = name.GetName();
	auto &arguments = name.GetArguments();
	auto variable = FindVariable(signature, name);
	if (variable && variable->IsValue()) {
		// a parameter that takes the value of the variable
		return ValueVariableType(*variable);
	}
	if (variable) {
		auto entry = variables.types.find(identifier.GetIdentifierName());
		if (entry != variables.types.end()) {
			return entry->second;
		}
	}
	if (instance && (variable || name.IsBuiltinType(LogicalTypeId::ANY))) {
		return GetVariableType(*instance, signature.GetConstraint(identifier));
	}
	if (variable) {
		return nullopt;
	}
	LogicalType builtin_type(name.GetBuiltinType());
	if (builtin_type.IsComplete() || builtin_type.id() == LogicalTypeId::ANY) {
		// the name of a built-in type that takes no arguments
		return builtin_type;
	}

	const auto kind = DefaultTypeGenerator::GetDefaultType(identifier);
	optional<LogicalType> paired;
	if (instance) {
		paired = PairType(name, kind, *instance);
	}
	// Whether the argument is what the name stands for already: of its kind, with every parameter what the name says
	// it is. It is then passed as it is - a type cannot always be constructed again from its parameters, e.g. a
	// geometry of a coordinate system that is not known here
	bool is_instance =
	    paired && paired->id() == kind && paired->GetAlias().empty() && paired->GetParameterCount() == arguments.size();
	vector<TypeArgument> bound_arguments;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument = arguments[i];
		// what the argument has in the place of this one
		optional<Value> parameter;
		auto parameter_idx = paired ? FindTypeParameter(*paired, arguments, i) : optional_idx();
		if (parameter_idx.IsValid()) {
			parameter = paired->GetParameter(parameter_idx.GetIndex());
		}
		const bool is_type = parameter && parameter->type().id() == LogicalTypeId::TYPE;
		auto value_variable = FindValueVariable(signature, argument);
		optional<Value> value;
		if (value_variable) {
			auto entry = variables.values.find(argument.GetType().GetName().GetIdentifierName());
			if (entry != variables.values.end()) {
				value = entry->second;
			} else if (parameter && !is_type && !parameter->IsNull()) {
				value = parameter->DefaultTryCastAs(ValueVariableType(*value_variable));
			}
		} else if (argument.IsType()) {
			// the type the argument has there, invalid if it has none - an unresolved parameter has no parts: it
			// says of every part what it says of the whole
			LogicalType part;
			if (is_type) {
				part = TypeValue::GetType(*parameter);
			} else if (instance && instance->id() == LogicalTypeId::UNKNOWN) {
				part = *instance;
			}
			auto &part_name = argument.GetType();
			auto type = BindTypeNameInternal(cache, part_name, signature, variables, instance ? &part : nullptr);
			if (type) {
				value = Value::TYPE(std::move(*type));
			}
		} else {
			value = argument.GetLiteral().ToValue();
		}
		if (!value) {
			return nullopt;
		}
		is_instance = is_instance && parameter_idx.IsValid() && parameter_idx.GetIndex() == i &&
		              IsSameParameter(*value, *parameter);
		bound_arguments.emplace_back(argument.GetLabel().GetIdentifierName(), std::move(*value));
	}
	if (is_instance) {
		return paired;
	}

	auto type = cache.Construct(identifier, std::move(bound_arguments));
	if (type.id() == LogicalTypeId::INVALID) {
		throw CatalogException("Type with name %s does not exist", identifier.GetIdentifierName());
	}
	return type;
}

LogicalType FunctionBinder::BindTypeName(optional_ptr<ClientContext> context, const TypeName &name) {
	TypeCache cache(context);
	return *BindTypeNameInternal(cache, name, FunctionSignature(), FunctionVariables(), nullptr);
}

//===--------------------------------------------------------------------===//
// Matching
//===--------------------------------------------------------------------===//
//! What CastFunctionSet::ImplicitCastCost returns for a cast to VARCHAR that only the old_implicit_casting setting
//! allows: more than any other cast costs
static constexpr int64_t OLD_IMPLICIT_CAST_COST = 10000000000;

//! What an argument says a variable of the signature is: the type a type variable stands for, or the value of a value
//! variable. For a NULL or an unresolved parameter the type is SQLNULL or UNKNOWN, which takes the type the other
//! arguments say, and the value is NULL, which says none
struct VariableBinding {
	reference<const TypeVariable> variable;
	Value value;
	//! The argument the function is bound with that says it
	idx_t argument;
};

//! What the arguments say the variables are, in the order of the arguments
using VariableBindings = vector<VariableBinding>;

//! One attempt to match a call against an overload. It holds what is the same for the whole attempt: every pass over
//! the arguments takes what the passes before it made, and returns what it makes
class OverloadMatcher {
public:
	OverloadMatcher(TypeCache &cache_p, const FunctionSignature &signature_p, const FunctionCallArguments &call_p,
	                bool explicit_casts_p, optional_ptr<const DecimalWidthLimit> decimal_width_limit_p)
	    : cache(cache_p), signature(signature_p), call(call_p), explicit_casts(explicit_casts_p),
	      decimal_width_limit(decimal_width_limit_p) {
	}

public:
	//! Match the call against the overload: how it binds - or why the call does not match
	Result<FunctionBinding, MatchFailure> Match() const;

private:
	//! The passes, in their order
	Result<ArgumentLayout, MatchFailure> Layout() const;
	Result<VariableBindings, MatchFailure> Collect(const ArgumentLayout &layout) const;
	Result<FunctionVariables, MatchFailure> Infer(const ArgumentLayout &layout, const VariableBindings &bindings) const;
	Result<vector<LogicalType>, MatchFailure> Substitute(const ArgumentLayout &layout,
	                                                     const FunctionVariables &variables) const;
	Result<FunctionRank, MatchFailure> Rank(const ArgumentLayout &layout, const vector<LogicalType> &types) const;

private:
	void AddUntypedBindings(const TypeName &name, const LogicalType &type, idx_t argument,
	                        VariableBindings &bindings) const;
	void CollectBindings(const TypeName &name, const LogicalType &type, idx_t argument,
	                     VariableBindings &bindings) const;
	optional<LogicalType> BindType(const TypeName &name, const FunctionVariables &variables,
	                               const LogicalType &instance) const;
	LogicalType GetKind(const TypeName &name) const;

private:
	TypeCache &cache;
	const FunctionSignature &signature;
	const FunctionCallArguments &call;
	const bool explicit_casts;
	optional_ptr<const DecimalWidthLimit> decimal_width_limit;
};

//===--------------------------------------------------------------------===//
// Layout
//===--------------------------------------------------------------------===//
//! Lay the arguments of a call out over the parameters of an overload. This is about the shape of the call only, not
//! about types: a positional argument is passed to the parameter at its position or to "*args", a named one to the
//! parameter of its name or to "**kwargs", and a parameter the call leaves out is filled by its default. Returns the
//! first reason the call does not fit: a wrong name, which often causes the counts to be off as well, then too many
//! arguments, then a parameter that is left out
Result<ArgumentLayout, MatchFailure> OverloadMatcher::Layout() const {
	const auto parameter_count = signature.GetParameterCount();
	const auto positional_parameter_count = signature.GetPositionalParameterCount();
	const auto args_idx = signature.GetArgsIndex();
	const auto kwargs_idx = signature.GetKwargsIndex();
	const auto argument_count = call.GetArgumentCount();
	const auto positional_count = call.GetPositionalCount();
	// the wrong shape is further away than arguments that cannot be cast, of which there are at most this many
	const auto distance = argument_count + 1;

	// the parameter every named argument is passed to
	vector<optional_idx> named_parameters;
	for (idx_t i = positional_count; i < argument_count; i++) {
		auto param_idx = signature.GetParameterIndexByName(call.GetArgument(i).name);
		if (!param_idx.IsValid()) {
			if (!kwargs_idx.IsValid()) {
				return Err(MatchFailure {distance, FunctionMismatch::UnknownNamedArgument {i}});
			}
			param_idx = kwargs_idx;
		} else if (param_idx.GetIndex() < positional_count &&
		           signature.GetParameter(param_idx.GetIndex()).AcceptsPosition()) {
			return Err(MatchFailure {distance, FunctionMismatch::AlreadyPassedByPosition {i}});
		}
		named_parameters.push_back(param_idx);
	}

	// every argument too many is one further away
	if (!args_idx.IsValid() && !kwargs_idx.IsValid() && argument_count > parameter_count) {
		// more arguments than parameters: as no two share a name, too many of them are passed by position
		return Err(
		    MatchFailure {argument_count + (argument_count - parameter_count),
		                  FunctionMismatch::TooManyArguments {parameter_count, positional_parameter_count, true}});
	}
	if (!args_idx.IsValid() && positional_count > positional_parameter_count) {
		// there is no "*args" to receive the extra positional arguments
		return Err(MatchFailure {
		    argument_count + (positional_count - positional_parameter_count),
		    FunctionMismatch::TooManyArguments {positional_parameter_count, positional_parameter_count, true}});
	}

	ArgumentLayout layout;
	for (idx_t param_idx = 0; param_idx < parameter_count; param_idx++) {
		auto &param = signature.GetParameter(param_idx);
		// the arguments the parameter can receive: the one at its position, the positional ones that follow the
		// positional parameters for "*args", else the named ones
		idx_t begin = positional_count;
		idx_t end = argument_count;
		if (param.GetKind() == FunctionParameterKind::VAR_POSITIONAL) {
			begin = positional_parameter_count;
			end = positional_count;
		} else if (param.AcceptsPosition() && param_idx < positional_count) {
			begin = param_idx;
			end = param_idx + 1;
		}
		const auto first = layout.arguments.size();
		for (idx_t i = begin; i < end; i++) {
			if (i >= positional_count && named_parameters[i - positional_count].GetIndex() != param_idx) {
				continue;
			}
			layout.arguments.push_back({param_idx, i, call.GetArgument(i).type});
			if (param.GetKind() == FunctionParameterKind::VAR_KEYWORD) {
				layout.named_arguments.push_back(call.GetArgument(i).name);
			}
		}
		if (param.GetKind() == FunctionParameterKind::KEYWORD_ONLY) {
			layout.named_arguments.push_back(param.GetName());
		}
		if (layout.arguments.size() > first || param.IsVariadic()) {
			continue;
		}
		// the call leaves the parameter out: it is filled by its default, if it has one
		if (!param.HasDefaultValue()) {
			return Err(MatchFailure {distance, FunctionMismatch::MissingParameter {
			                                       param_idx, param.IsNamed() ? param.GetName() : Identifier()}});
		}
		layout.arguments.push_back({param_idx, optional_idx(), param.GetDefaultValue()->type()});
	}
	layout.positional_count = layout.arguments.size() - layout.named_arguments.size();
	return std::move(layout);
}

//===--------------------------------------------------------------------===//
// Collect
//===--------------------------------------------------------------------===//
//! The value of an argument, if it is known when the call is bound
static optional<Value> GetArgumentConstant(optional_ptr<ClientContext> context, const FunctionCallArgument &argument) {
	if (argument.constant) {
		return argument.constant;
	}
	if (!argument.expression) {
		return nullopt;
	}
	auto &expression = *argument.expression;
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return expression.Cast<BoundConstantExpression>().GetValue();
	}
	Value result;
	if (!context || !expression.IsFoldable() || !ExpressionExecutor::TryEvaluateScalar(*context, expression, result)) {
		return nullopt;
	}
	return result;
}

//! The value a parameter that is declared as a value variable is passed: what it says the variable is. A NULL says
//! nothing about it. "argument" is the argument of the call that passes it, invalid if it is the default of the
//! parameter
static Result<Value, MatchFailure> GetVariableValue(const TypeVariable &variable, optional<Value> constant,
                                                    optional_idx argument, idx_t parameter) {
	if (!constant) {
		return Err(MatchFailure {
		    0, FunctionMismatch::ConstantRequired {argument.GetIndex(), variable.GetName().GetIdentifierName()}});
	}
	if (constant->IsNull()) {
		return Value();
	}
	auto value = constant->DefaultTryCastAs(ValueVariableType(variable));
	if (!value) {
		if (!argument.IsValid()) {
			throw InternalException("The default of a parameter cannot be the value of %s", variable.ToString());
		}
		// the one argument that cannot be passed - the parameter reads as the variable it is declared as
		return Err(MatchFailure {
		    1, FunctionMismatch::IncompatibleArgument {argument.GetIndex(), parameter, LogicalType(), true}});
	}
	return std::move(*value);
}

//! An argument that says nothing about the type its parameter declares says nothing about the variables it uses
void OverloadMatcher::AddUntypedBindings(const TypeName &name, const LogicalType &type, idx_t argument,
                                         VariableBindings &bindings) const {
	auto variable = FindVariable(signature, name);
	if (variable) {
		bindings.push_back({*variable, variable->IsValue() ? Value() : Value::TYPE(type), argument});
	}
	for (auto &name_argument : name.GetArguments()) {
		if (name_argument.IsType()) {
			AddUntypedBindings(name_argument.GetType(), type, argument, bindings);
		}
	}
}

//! Pair a type name with a type, part by part, and add what the type says the variables of the name are to "bindings"
//! - whatever is paired says so, whether the type can be passed as the name is up to the cast rules. A type of
//! another kind than the name has no parts to pair: it says nothing, see PairType
void OverloadMatcher::CollectBindings(const TypeName &name, const LogicalType &type, idx_t argument,
                                      VariableBindings &bindings) const {
	if (IsUntyped(type)) {
		// Nested functions propagate these types: the binder replaces a function that returns one by a NULL, or
		// binds it again once the parameter is resolved
		AddUntypedBindings(name, type, argument, bindings);
		return;
	}
	auto variable = FindVariable(signature, name);
	if (variable && !variable->IsValue() && GetVariableType(type, signature.GetConstraint(name.GetName()))) {
		// a type variable is paired with a type it stands for: one of its kind, for a constrained variable
		bindings.push_back({*variable, Value::TYPE(type), argument});
	}
	if (!name.HasArguments()) {
		return;
	}
	auto &arguments = name.GetArguments();
	auto paired = PairType(name, DefaultTypeGenerator::GetDefaultType(name.GetName()), type);
	for (idx_t i = 0; paired && i < arguments.size(); i++) {
		auto parameter_idx = FindTypeParameter(*paired, arguments, i);
		if (!arguments[i].IsType() || !parameter_idx.IsValid()) {
			continue;
		}
		auto parameter = paired->GetParameter(parameter_idx.GetIndex());
		const bool is_type = parameter.type().id() == LogicalTypeId::TYPE;
		auto value_variable = FindValueVariable(signature, arguments[i]);
		if (is_type && !value_variable) {
			CollectBindings(arguments[i].GetType(), TypeValue::GetType(parameter), argument, bindings);
		}
		if (is_type || !value_variable) {
			continue;
		}
		// a value variable is the value the type has in its place
		auto value = parameter.DefaultTryCastAs(ValueVariableType(*value_variable));
		if (value && !value->IsNull()) {
			bindings.push_back({*value_variable, std::move(*value), argument});
		}
	}
}

//! Collect what the arguments say the variables are: every argument is paired with the type of its parameter, and a
//! parameter that is declared as a value variable takes the value of its argument. Returns why not: such an argument
//! is not a constant, or not a value of the variable
Result<VariableBindings, MatchFailure> OverloadMatcher::Collect(const ArgumentLayout &layout) const {
	auto context = cache.GetContext();
	VariableBindings bindings;
	for (idx_t i = 0; i < layout.arguments.size(); i++) {
		auto &argument = layout.arguments[i];
		auto &param = signature.GetParameter(argument.parameter_index);
		CollectBindings(param.GetTypeName(), argument.type, i, bindings);
		auto variable = FindVariable(signature, param.GetTypeName());
		if (!variable || !variable->IsValue() || IsUntyped(argument.type)) {
			continue;
		}
		auto arg_idx = argument.argument_index;
		auto constant = arg_idx.IsValid() ? GetArgumentConstant(context, call.GetArgument(arg_idx.GetIndex()))
		                                  : optional<Value>(*param.GetDefaultValue());
		auto value = GetVariableValue(*variable, std::move(constant), arg_idx, argument.parameter_index);
		if (!value) {
			return Err(std::move(value.GetError()));
		}
		bindings.push_back({*variable, std::move(*value), i});
	}
	return std::move(bindings);
}

//===--------------------------------------------------------------------===//
// Infer
//===--------------------------------------------------------------------===//
//! The type two types are promoted to - without a client context, with the built-in casts only. A NULL and an
//! unresolved parameter take the other type as it is: a literal stays a literal until the type is final
static bool TryUnifyTypes(optional_ptr<ClientContext> context, const LogicalType &left, const LogicalType &right,
                          LogicalType &result, optional_ptr<const DecimalWidthLimit> decimal_width_limit) {
	return LogicalType::TryGetMaxLogicalType(context, left, right, result, decimal_width_limit,
	                                         PromotionPolicy::NormalizeLiterals(false));
}

//! Infer what the variables of a signature are from what the arguments say they are: the types the arguments say a
//! type variable is are promoted to one type, and the values they say a value variable is have to be the same. In
//! the promotion a NULL and an unresolved parameter take the other type, so a type variable is the type of a NULL if
//! only NULLs are passed as it. A value variable that no argument says is not determined. A variable that is left
//! to an unresolved parameter - no other argument says what it is - leaves the call unresolved, whatever the other
//! overloads are and whether or not the other arguments can be passed. Returns why not: the arguments say different
//! things about a variable, or what they say a constrained variable is is not of its kind. What the call needs to
//! be determined is up to Substitute
Result<FunctionVariables, MatchFailure> OverloadMatcher::Infer(const ArgumentLayout &layout,
                                                               const VariableBindings &bindings) const {
	auto context = cache.GetContext();
	auto &arguments = layout.arguments;
	FunctionVariables variables;
	// whether a value variable is left to an unresolved parameter
	bool is_unresolved = false;
	for (auto &variable : signature.GetVariables()) {
		auto &variable_name = variable.GetName().GetIdentifierName();
		FunctionMismatch::TemplateDeduction conflict;
		conflict.template_name = variable_name;

		if (variable.IsValue()) {
			Value value;
			bool has_parameter = false;
			for (auto &binding : bindings) {
				if (!RefersToSameObject(binding.variable.get(), variable)) {
					continue;
				}
				if (binding.value.IsNull()) {
					has_parameter = has_parameter || arguments[binding.argument].type.id() == LogicalTypeId::UNKNOWN;
					continue;
				}
				if (!value.IsNull() && !Value::NotDistinctFrom(value, binding.value)) {
					conflict.argument = arguments[binding.argument].argument_index;
					conflict.inferred_values = {value, binding.value};
					return Err(MatchFailure {0, std::move(conflict)});
				}
				value = binding.value;
			}
			is_unresolved = is_unresolved || (value.IsNull() && has_parameter);
			if (!value.IsNull()) {
				variables.values[variable_name] = std::move(value);
			}
			continue;
		}

		// The arguments say what the type is in their order: the first one sets it, and every other type is promoted
		// with it. A NULL or an unresolved parameter takes the other type
		LogicalType type;
		// what a conflict lists: the first type, then every type that changed it and what it was promoted to with it
		LogicalType first_type;
		vector<LogicalType> promotions;
		bool is_said = false;
		for (auto &binding : bindings) {
			if (!RefersToSameObject(binding.variable.get(), variable)) {
				continue;
			}
			auto said_type = TypeValue::GetType(binding.value);
			if (!is_said) {
				first_type = said_type;
				type = said_type;
				is_said = true;
				continue;
			}
			if (said_type.EqualsIncludingCollation(type)) {
				continue;
			}
			LogicalType promoted;
			if (!TryUnifyTypes(context, type, said_type, promoted, decimal_width_limit)) {
				conflict.argument = arguments[binding.argument].argument_index;
				conflict.inferred_types.push_back(std::move(first_type));
				conflict.inferred_types.insert(conflict.inferred_types.end(), promotions.begin(), promotions.end());
				conflict.inferred_types.push_back(std::move(said_type));
				return Err(MatchFailure {0, std::move(conflict)});
			}
			if (!promoted.EqualsIncludingCollation(type)) {
				promotions.push_back(said_type);
				promotions.push_back(promoted);
				type = std::move(promoted);
			}
		}
		if (is_said) {
			variables.types[variable_name] = std::move(type);
		}
	}

	// The types are final, once every variable is promoted without a conflict
	for (auto &variable : signature.GetVariables()) {
		auto &variable_name = variable.GetName().GetIdentifierName();
		auto entry = variables.types.find(variable_name);
		if (entry == variables.types.end()) {
			continue;
		}
		// arguments of the kind can still be promoted to a type that is not of it, e.g. two enums to VARCHAR
		auto type = GetVariableType(entry->second, signature.GetConstraint(variable.GetName()));
		if (!type) {
			FunctionMismatch::TemplateDeduction conflict;
			conflict.template_name = variable_name;
			conflict.inferred_types.push_back(LogicalType::NormalizeType(entry->second));
			return Err(MatchFailure {0, std::move(conflict)});
		}
		entry->second = std::move(*type);
	}
	if (is_unresolved) {
		throw ParameterNotResolvedException();
	}

	return std::move(variables);
}

//===--------------------------------------------------------------------===//
// Substitute
//===--------------------------------------------------------------------===//
//! The type a type name of the signature stands for when an argument of the given type is passed to it, see
//! BindTypeNameInternal. Nothing if a variable is not determined, or the constructor of a type does not take the
//! parts
optional<LogicalType> OverloadMatcher::BindType(const TypeName &name, const FunctionVariables &variables,
                                                const LogicalType &instance) const {
	try {
		return BindTypeNameInternal(cache, name, signature, variables, &instance);
	} catch (const BinderException &) {
		return nullopt;
	}
}

//! Substitute the variables: the type every argument is passed as is the type of its parameter with the variables as
//! the arguments determine them. Returns why not: nothing determines the type an argument is passed as, or a
//! variable of the return type
Result<vector<LogicalType>, MatchFailure> OverloadMatcher::Substitute(const ArgumentLayout &layout,
                                                                      const FunctionVariables &variables) const {
	vector<LogicalType> types;
	types.reserve(layout.arguments.size());
	for (auto &argument : layout.arguments) {
		auto &name = signature.GetParameter(argument.parameter_index).GetTypeName();
		auto type = BindType(name, variables, argument.type);
		if (!type && argument.type.id() == LogicalTypeId::SQLNULL) {
			// a NULL is passed as it is where nothing determines the type of the parameter
			type = argument.type;
		}
		if (!type) {
			return Err(MatchFailure {1, FunctionMismatch::IncompatibleArgument {argument.argument_index.GetIndex(),
			                                                                    argument.parameter_index, LogicalType(),
			                                                                    true}});
		}
		types.push_back(std::move(*type));
	}
	auto &return_type = signature.GetReturnType();
	for (auto &variable : signature.GetVariables()) {
		auto &variable_name = variable.GetName().GetIdentifierName();
		const bool is_determined = variables.types.count(variable_name) + variables.values.count(variable_name) > 0;
		if (!is_determined && return_type.IsType() && return_type.GetTypeName().Contains(variable.GetName())) {
			return Err(MatchFailure {0, FunctionMismatch::TemplateUnresolved {variable_name, variable.IsValue()}});
		}
	}
	return std::move(types);
}

//===--------------------------------------------------------------------===//
// Rank
//===--------------------------------------------------------------------===//
//! What an argument adds to the rank by the type of the parameter that receives it, see FunctionRank: whether it is
//! passed through an unconstrained type variable (rule 2) or a type variable at all (rule 4), and what the type
//! declares (rules 5 and 6)
static FunctionRank RankTypeName(const FunctionSignature &signature, const TypeName &name) {
	FunctionRank rank;
	auto variable = FindVariable(signature, name);
	if (variable && variable->IsValue()) {
		// counted by the type that takes it
		return rank;
	}
	if (variable || name.IsBuiltinType(LogicalTypeId::ANY)) {
		const bool is_constrained = signature.GetConstraint(name.GetName()) != LogicalTypeId::INVALID;
		rank.unconstrained_arguments = is_constrained ? 0 : 1;
		rank.variable_parameters = 1;
		rank.declared_structure = is_constrained ? 1 : 0;
		return rank;
	}
	rank.declared_structure = 2;
	for (auto &argument : name.GetArguments()) {
		if (!argument.IsType()) {
			rank.declared_values += 2;
			continue;
		}
		auto part = RankTypeName(signature, argument.GetType());
		rank.unconstrained_arguments = MaxValue(rank.unconstrained_arguments, part.unconstrained_arguments);
		rank.variable_parameters = MaxValue(rank.variable_parameters, part.variable_parameters);
		rank.declared_structure += part.declared_structure;
		rank.declared_values += part.declared_values + (FindValueVariable(signature, argument) ? 1 : 0);
	}
	return rank;
}

//! The kind of type a parameter declares, which is what a NULL is cast to: the type itself if it uses no variable,
//! and any type for an unconstrained type variable by itself
LogicalType OverloadMatcher::GetKind(const TypeName &name) const {
	auto type = BindTypeNameInternal(cache, name, signature, FunctionVariables(), nullptr);
	if (type) {
		return *type;
	}
	if (name.HasArguments()) {
		return LogicalType(DefaultTypeGenerator::GetDefaultType(name.GetName()));
	}
	auto constraint = signature.GetConstraint(name.GetName());
	return constraint == LogicalTypeId::INVALID ? LogicalType::ANY : LogicalType(constraint);
}

//! Rank how well the arguments are passed, see FunctionRank. An argument is passed as an instance of the type of its
//! parameter if it casts implicitly to that type with the variables as the argument itself says they are - else,
//! deferred, to a type with arguments if it casts implicitly to the type it is passed as, which "types" has. Returns
//! the first argument that cannot be passed either way. A default is not ranked, and nothing is when the arguments
//! are cast explicitly
Result<FunctionRank, MatchFailure> OverloadMatcher::Rank(const ArgumentLayout &layout,
                                                         const vector<LogicalType> &types) const {
	if (explicit_casts) {
		return FunctionRank();
	}
	auto context = cache.GetContext();
	auto &arguments = layout.arguments;
	// the variables of the type an argument has by itself: none but what it says they are
	const FunctionVariables own_variables;
	const bool has_variables = !signature.GetVariables().empty();
	FunctionRank rank;
	bool has_unknown_argument = false;
	optional_idx mismatch;
	idx_t mismatch_count = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument = arguments[i];
		if (argument.IsDefaultArgument()) {
			continue;
		}
		auto &name = signature.GetParameter(argument.parameter_index).GetTypeName();
		auto &arg_type = argument.type;
		const auto source = argument.argument_index.GetIndex();
		has_unknown_argument =
		    has_unknown_argument || (arg_type.id() == LogicalTypeId::UNKNOWN && source < call.GetPositionalCount());
		// a NULL or an unresolved parameter says nothing about the type: a NULL costs what it costs as the kind of
		// type the parameter declares
		int64_t cost = 0;
		if (arg_type.id() == LogicalTypeId::SQLNULL) {
			cost = ImplicitCastCost(context, arg_type, GetKind(name));
		} else if (arg_type.id() != LogicalTypeId::UNKNOWN) {
			// the type the argument has by itself: the one it is passed as, if there is no variable to determine
			auto own_type = has_variables ? BindType(name, own_variables, arg_type) : types[i];
			cost = own_type ? ImplicitCastCost(context, arg_type, *own_type) : -1;
		}
		if (cost < 0 && name.HasArguments()) {
			// deferred
			cost = ImplicitCastCost(context, arg_type, types[i]);
		}
		if (cost < 0) {
			// the first argument that cannot be passed is reported: the more there are, the further the call is
			if (!mismatch.IsValid()) {
				mismatch = i;
			}
			mismatch_count++;
			continue;
		}
		auto declared = RankTypeName(signature, name);
		rank.legacy_casts += cost >= OLD_IMPLICIT_CAST_COST ? 1 : 0;
		// a NULL is not passed through an unconstrained type variable: it says nothing about the type
		rank.unconstrained_arguments += arg_type.id() == LogicalTypeId::SQLNULL ? 0 : declared.unconstrained_arguments;
		rank.cast_cost += static_cast<idx_t>(cost);
		rank.variable_parameters += declared.variable_parameters;
		rank.declared_structure += declared.variable_parameters * declared.declared_structure;
		rank.declared_values += declared.declared_values;
	}
	if (mismatch.IsValid()) {
		auto &argument = arguments[mismatch.GetIndex()];
		return Err(MatchFailure {mismatch_count, FunctionMismatch::IncompatibleArgument {
		                                             argument.argument_index.GetIndex(), argument.parameter_index,
		                                             types[mismatch.GetIndex()], true}});
	}
	// with a parameter of an unknown type every overload ranks the same
	return has_unknown_argument ? FunctionRank() : rank;
}

//===--------------------------------------------------------------------===//
// Match
//===--------------------------------------------------------------------===//
//! Match the call against the overload, in passes over the arguments: lay them out over the parameters, collect what
//! they say the variables are, infer the variables, substitute them for the types the arguments are passed as, and
//! rank how well they are passed (see FunctionRank). An unresolved parameter is passed as a type the overload
//! declares or the other arguments determine - else the call is unresolved, see GetVariableType and Infer
Result<FunctionBinding, MatchFailure> OverloadMatcher::Match() const {
	auto layout = Layout();
	if (!layout) {
		return Err(std::move(layout.GetError()));
	}
	auto bindings = Collect(*layout);
	if (!bindings) {
		return Err(std::move(bindings.GetError()));
	}
	auto variables = Infer(*layout, *bindings);
	if (!variables) {
		return Err(std::move(variables.GetError()));
	}
	auto types = Substitute(*layout, *variables);
	if (!types) {
		return Err(std::move(types.GetError()));
	}
	auto rank = Rank(*layout, *types);
	if (!rank) {
		return Err(std::move(rank.GetError()));
	}
	FunctionBinding binding;
	binding.rank = *rank;
	binding.variables = std::move(*variables);
	binding.layout = std::move(*layout);
	for (idx_t i = 0; i < types->size(); i++) {
		D_ASSERT(!TypeVisitor::Contains((*types)[i], LogicalTypeId::UNKNOWN));
		binding.layout.arguments[i].type = std::move((*types)[i]);
	}
	return std::move(binding);
}

Result<FunctionBinding, FunctionMismatch>
FunctionBinder::MatchOverload(TypeCache &cache, idx_t overload, shared_ptr<const SimpleFunction> function,
                              const FunctionCallArguments &call, bool explicit_casts,
                              optional_ptr<const DecimalWidthLimit> decimal_width_limit) {
	// the overload is attached here to what the arguments are matched as, or to why they are not
	const auto &sig = function->GetSignature();
	OverloadMatcher matcher(cache, sig, call, explicit_casts, decimal_width_limit);
	auto binding = matcher.Match();
	if (!binding) {
		auto &failure = binding.GetError();
		return Err(FunctionMismatch(overload, std::move(function), failure.distance, std::move(failure.reason)));
	}
	binding->overload = overload;
	binding->function = std::move(function);
	return std::move(*binding);
}

LogicalType FunctionBinder::BindReturnType(TypeCache &cache, const FunctionBinding &binding) {
	auto &signature = binding.GetFunction().GetSignature();
	auto &return_type = signature.GetReturnType();
	if (!return_type.IsType()) {
		// the signature declares no type: the resolver or the bind callback of the function sets it
		return LogicalType::INVALID;
	}
	auto type = BindTypeNameInternal(cache, return_type.GetTypeName(), signature, binding.variables, nullptr);
	if (!type) {
		throw InternalException("The return type %s uses a variable the binding does not determine",
		                        return_type.GetTypeName().ToString());
	}
	return *type;
}

} // namespace duckdb
