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
//! array: which of the two a function takes is up to the cast rules. A NULL is of every kind. The parts of a struct
//! and of a tuple are paired by position where a type names them (TUPLE(K, V) passed a struct with two fields); a
//! pack is only made of the parameters of a type of the very kind of its name, or of one that has none, see
//! CollectBindings
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

//! What an implicit cast costs - nothing if there is none
static optional<int64_t> ImplicitCastCost(optional_ptr<ClientContext> context, const LogicalType &source,
                                          const LogicalType &target) {
	// without a context we can only consider the built-in cast rules - no registered casts are reachable
	auto cost =
	    context ? CastFunctionSet::ImplicitCastCost(*context, source, target) : CastRules::ImplicitCast(source, target);
	return cost < 0 ? optional<int64_t>() : optional<int64_t>(cost);
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

//! The variable of the signature a type name is, if it is one: no variable is named like a built-in type, and the
//! expansion of a pack is not the pack
static optional_ptr<const TypeVariable> FindVariable(const FunctionSignature &signature, const TypeName &name) {
	if (name.IsExpansion() || name.HasArguments() || name.GetBuiltinType() != LogicalTypeId::INVALID) {
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

//! The pack of the signature an expansion expands: the one pack its pattern uses, see FunctionSignature::Verify -
//! the pack itself for "*T" and "**KV", the T of "*(T[])"
static const TypeVariable &GetExpandedPack(const FunctionSignature &signature, const TypeName &expansion) {
	for (auto &variable : signature.GetVariables()) {
		if (variable.IsPack() && expansion.GetPattern().Contains(variable.GetName())) {
			return variable;
		}
	}
	throw InternalException("'%s' expands no pack of the signature", expansion.ToString());
}

//! The pack of the signature an argument of a type name expands, if it is an expansion: its elements take the place
//! of the argument. Only a pack by itself is expanded there: a pattern is expanded as the type of a variadic
//! parameter, see FunctionSignature::Verify
static optional_ptr<const TypeVariable> FindPack(const FunctionSignature &signature, const TypeNameArgument &argument) {
	if (!argument.IsType() || !argument.GetType().IsExpansion()) {
		return nullptr;
	}
	auto pack = FindVariable(signature, argument.GetType().GetPattern());
	if (!pack || !pack->IsPack()) {
		throw InternalException("'%s' is not the expansion of a pack by itself", argument.GetType().ToString());
	}
	return pack;
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
//! when it is the pattern of a parameter that is filled with the parts of an argument, see GetPassCost. The elements
//! of a pack take the place of the expansion that is an argument of the name, with their labels if it is a named
//! pack: TUPLE(*T) is the tuple of the types of T, STRUCT(**KV) the struct of the fields of KV. The name itself is
//! not an expansion: for a variadic parameter that is one it is the pattern, in which the pack is what the one
//! argument says it is - its element of the pack, see Collect. Without an argument ANY is a type. Returns nothing if
//! a variable is not determined
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
	bool is_instance = paired && paired->id() == kind && paired->GetAlias().empty();
	vector<TypeArgument> bound_arguments;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument = arguments[i];
		auto pack = FindPack(signature, argument);
		if (pack) {
			// the elements the arguments determine - else the parameters the one argument has, if it is of the kind of
			// the name or has none: a struct with fields has none for TUPLE(*T), nor a tuple with elements for
			// STRUCT(**KV), nor a NULL for either
			auto entry = variables.packs.find(pack->GetName().GetIdentifierName());
			const bool is_determined = entry != variables.packs.end();
			optional<TypePack> parts;
			if (paired && (paired->id() == kind || (!IsUntyped(*paired) && paired->GetParameterCount() == 0))) {
				parts = TypePack::ParametersOf(*paired);
			}
			if (!is_determined && !parts) {
				return nullopt;
			}
			auto &elements = is_determined ? entry->second : *parts;
			is_instance = is_instance && parts && (!is_determined || elements == *parts);
			if (is_instance) {
				// the pack is the only argument of the name, see FunctionSignature::Verify
				return paired;
			}
			for (idx_t element_idx = 0; element_idx < elements.Count(); element_idx++) {
				auto label = elements.GetLabel(element_idx);
				bound_arguments.emplace_back(label ? label->GetIdentifierName() : string(),
				                             Value::TYPE(elements.GetType(element_idx)));
			}
			continue;
		}
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
	if (is_instance && paired->GetParameterCount() == bound_arguments.size()) {
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

//! What an argument says a pack of the signature is: its elements
struct PackBinding {
	reference<const TypeVariable> pack;
	TypePack elements;
};

//! What the arguments say the variables and the packs are, in the order of the arguments
struct VariableBindings {
	vector<VariableBinding> variables;
	vector<PackBinding> packs;
};

//! How the arguments are passed: the type every argument the function is bound with is passed as, and how well the
//! arguments of the call are passed (see FunctionRank)
struct PassedArguments {
	vector<LogicalType> types;
	FunctionRank rank;
};

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
	Result<PassedArguments, MatchFailure> Substitute(const ArgumentLayout &layout,
	                                                 const FunctionVariables &variables) const;

private:
	void AddUntypedBindings(const TypeName &name, const LogicalType &type, idx_t argument,
	                        VariableBindings &bindings) const;
	void CollectBindings(const TypeName &name, const LogicalType &type, idx_t argument,
	                     VariableBindings &bindings) const;
	optional<LogicalType> BindType(const TypeName &name, const FunctionVariables &variables,
	                               const LogicalType &instance) const;
	optional<int64_t> GetPassCost(const TypeName &name, const LogicalType &arg_type,
	                              const optional<LogicalType> &type) const;

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

//! An argument that says nothing about the type its parameter declares says nothing about the variables it uses,
//! and has no elements for a pack
void OverloadMatcher::AddUntypedBindings(const TypeName &name, const LogicalType &type, idx_t argument,
                                         VariableBindings &bindings) const {
	if (name.IsExpansion()) {
		return;
	}
	auto variable = FindVariable(signature, name);
	if (variable) {
		bindings.variables.push_back({*variable, variable->IsValue() ? Value() : Value::TYPE(type), argument});
	}
	for (auto &name_argument : name.GetArguments()) {
		if (name_argument.IsType()) {
			AddUntypedBindings(name_argument.GetType(), type, argument, bindings);
		}
	}
}

//! Pair a type name with a type, part by part, and add what the type says the variables of the name are to "bindings"
//! - whatever is paired says so, whether the type can be passed as the name is up to the cast rules. A pack is
//! paired with the parameters of a type of the kind of the name: TUPLE(*T) passed a tuple says that T is the types
//! of the tuple, STRUCT(**KV) passed a struct that KV is its fields, and a struct or a tuple without parts says the
//! pack of no elements for either. A type of another kind than the name has no parts to pair: it says nothing, see
//! PairType
void OverloadMatcher::CollectBindings(const TypeName &name, const LogicalType &type, idx_t argument,
                                      VariableBindings &bindings) const {
	auto variable = FindVariable(signature, name);
	if (IsUntyped(type)) {
		// Nested functions propagate these types: the binder replaces a function that returns one by a NULL, or
		// binds it again once the parameter is resolved
		AddUntypedBindings(name, type, argument, bindings);
		return;
	}
	if (variable && !variable->IsValue() && GetVariableType(type, signature.GetConstraint(name.GetName()))) {
		// a type variable is paired with a type it stands for: one of its kind, for a constrained variable
		bindings.variables.push_back({*variable, Value::TYPE(type), argument});
	}
	if (!name.HasArguments()) {
		return;
	}
	auto &arguments = name.GetArguments();
	const auto kind = DefaultTypeGenerator::GetDefaultType(name.GetName());
	auto paired = PairType(name, kind, type);
	for (idx_t i = 0; paired && i < arguments.size(); i++) {
		auto pack = FindPack(signature, arguments[i]);
		if (pack) {
			// the pack is the only argument of the name: its elements are all the parameters of a type of that kind -
			// and a type without parameters says the pack without elements, whether it is a struct or a tuple
			if (paired->id() == kind || paired->GetParameterCount() == 0) {
				bindings.packs.push_back({*pack, TypePack::ParametersOf(*paired)});
			}
			continue;
		}
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
			bindings.variables.push_back({*value_variable, std::move(*value), argument});
		}
	}
}

//! Collect what the arguments say the variables are: every argument is paired with the type of its parameter - with
//! the pattern, for a variadic parameter that is an expansion - and a parameter that is declared as a value variable
//! takes the value of its argument. The pack such a variadic parameter expands takes one element from every argument:
//! what the argument says the pack is in the pattern. Returns why not: an argument that is passed as a value variable
//! is not a constant, or not a value of the variable
Result<VariableBindings, MatchFailure> OverloadMatcher::Collect(const ArgumentLayout &layout) const {
	auto context = cache.GetContext();
	VariableBindings bindings;
	for (idx_t i = 0; i < layout.arguments.size(); i++) {
		auto &argument = layout.arguments[i];
		auto &param = signature.GetParameter(argument.parameter_index);
		auto &param_name = param.GetTypeName();
		CollectBindings(param_name.IsExpansion() ? param_name.GetPattern() : param_name, argument.type, i, bindings);
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
		bindings.variables.push_back({*variable, std::move(*value), i});
	}

	// The pack a variadic parameter expands: every argument the parameter receives is one element, the type it says
	// the pack is in the pattern - itself for "*T", the type of its elements for "*(T[])" - with its name as the label
	// for "**kwargs". Without an argument it is the pack of no elements. It is not said if an argument does not say
	// its element: an unresolved parameter, or an argument that has no parts for the pattern
	for (auto &variadic_idx : {signature.GetArgsIndex(), signature.GetKwargsIndex()}) {
		if (!variadic_idx.IsValid()) {
			continue;
		}
		auto &variadic_name = signature.GetParameter(variadic_idx.GetIndex()).GetTypeName();
		if (!variadic_name.IsExpansion()) {
			continue;
		}
		auto &pack = GetExpandedPack(signature, variadic_name);
		const bool is_named = variadic_name.GetExpansionKind() == TypeMultiplicity::NAMED_PACK;
		idx_t argument_count = 0;
		for (auto &argument : layout.arguments) {
			argument_count += argument.parameter_index == variadic_idx.GetIndex() ? 1 : 0;
		}
		vector<LogicalType> types;
		child_list_t<LogicalType> labelled_types;
		for (auto &binding : bindings.variables) {
			if (!RefersToSameObject(binding.variable.get(), pack)) {
				continue;
			}
			auto said_type = TypeValue::GetType(binding.value);
			if (said_type.id() == LogicalTypeId::UNKNOWN) {
				continue;
			}
			auto type = LogicalType::NormalizeType(said_type);
			if (is_named) {
				auto argument_idx = layout.arguments[binding.argument].argument_index.GetIndex();
				labelled_types.emplace_back(call.GetArgument(argument_idx).name, std::move(type));
			} else {
				types.push_back(std::move(type));
			}
		}
		if (types.size() + labelled_types.size() == argument_count) {
			bindings.packs.push_back(
			    {pack, is_named ? TypePack::Named(std::move(labelled_types)) : TypePack::Positional(std::move(types))});
		}
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
//! only NULLs are passed as it. A pack is the elements the one parameter that uses it says, see Collect. A value
//! variable or a pack that no argument says is not determined. A variable that is left to an unresolved parameter -
//! no other argument says what it is - leaves the call unresolved, whatever the other overloads are and whether or
//! not the other arguments can be passed. Returns why not: the arguments say different things about a variable, or
//! what they say a constrained variable is is not of its kind. What the call needs to be determined is up to
//! Substitute
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

		if (variable.IsPack()) {
			// one parameter uses a pack, see FunctionSignature::Verify: what is passed to it says the pack
			for (auto &binding : bindings.packs) {
				if (RefersToSameObject(binding.pack.get(), variable)) {
					variables.packs.emplace(variable_name, binding.elements);
				}
			}
			// a NULL has no elements to say, an unresolved parameter is what could say them
			const bool is_said = variables.packs.count(variable_name) > 0;
			for (auto &argument : arguments) {
				auto &name = signature.GetParameter(argument.parameter_index).GetTypeName();
				const bool is_parameter = argument.type.id() == LogicalTypeId::UNKNOWN;
				is_unresolved = is_unresolved || (!is_said && is_parameter && name.Contains(variable.GetName()));
			}
			continue;
		}

		if (variable.IsValue()) {
			Value value;
			bool has_parameter = false;
			for (auto &binding : bindings.variables) {
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
		for (auto &binding : bindings.variables) {
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

//! What an argument adds to the rank by the type of the parameter that receives it, see FunctionRank: whether it is
//! passed through an unconstrained type variable (rule 2) or a type variable at all (rule 4), and what the type
//! declares (rules 5 and 6)
static FunctionRank RankTypeName(const FunctionSignature &signature, const TypeName &name) {
	if (name.IsExpansion()) {
		// what is expanded is what an argument is passed through: a pack counts like a type variable
		return RankTypeName(signature, name.GetPattern());
	}
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

//! What passing an argument to a parameter costs: the implicit cast to the type it has to fit - nothing if it cannot
//! be passed. "type" is the type it is passed as, if one could be made.
//! - It has to fit the pattern of the parameter, which is the type of the parameter with every variable left open:
//!   it casts implicitly to the pattern with its own parts in the place of the variables. INTEGER[3] fits T[] as
//!   INTEGER[], INTEGER fits DECIMAL(W, S) as DECIMAL(10, 0), and anything fits a type variable by itself. For a type
//!   that uses no variable the pattern is the type. The cast to the type it is passed as is then explicit: no
//!   implicit cast to that type is asked for.
//! - If it has no parts for the pattern, it has to cast implicitly to the type it is passed as: a VARIANT or a
//!   string literal for T[]. There is no such argument for a variable by itself.
//! - An unresolved parameter is passed as the type the parameter declares, at no cost.
//! - A NULL costs the cast of a NULL to the type it is passed as. Where no such type could be made it is passed as
//!   it is, and costs the cast of a NULL to the type the parameter names
optional<int64_t> OverloadMatcher::GetPassCost(const TypeName &name, const LogicalType &arg_type,
                                               const optional<LogicalType> &type) const {
	auto context = cache.GetContext();
	if (arg_type.id() == LogicalTypeId::SQLNULL && !type) {
		return CastRules::ImplicitCastNull(DefaultTypeGenerator::GetDefaultType(name.GetName()));
	}
	if (!type) {
		return nullopt;
	}
	if (arg_type.id() == LogicalTypeId::SQLNULL) {
		return ImplicitCastCost(context, arg_type, *type);
	}
	if (arg_type.id() == LogicalTypeId::UNKNOWN) {
		return 0;
	}
	// the pattern leaves every variable to the argument - without a variable every pattern is the type
	static const FunctionVariables OPEN_VARIABLES;
	auto fitted = signature.GetVariables().empty() ? type : BindType(name, OPEN_VARIABLES, arg_type);
	auto cost = fitted ? ImplicitCastCost(context, arg_type, *fitted) : nullopt;
	if (!cost && name.HasArguments()) {
		cost = ImplicitCastCost(context, arg_type, *type);
	}
	return cost;
}

//! Substitute the variables, and rank how the arguments are passed (see FunctionRank). The type every argument is
//! passed as is the type of its parameter with the variables as the arguments determine them - a NULL is passed as it
//! is where nothing determines that type. An argument of the call then adds what passing it costs, see GetPassCost,
//! and what the type of its parameter declares to the rank. A default is not ranked, and nothing is when the
//! arguments are cast explicitly: such an argument only needs a type. Returns why not: the first argument that
//! cannot be passed and how many there are, else a variable of the return type that nothing determines
Result<PassedArguments, MatchFailure> OverloadMatcher::Substitute(const ArgumentLayout &layout,
                                                                  const FunctionVariables &variables) const {
	PassedArguments result;
	result.types.reserve(layout.arguments.size());
	auto &rank = result.rank;
	bool has_unknown_argument = false;
	optional<FunctionMismatch::IncompatibleArgument> mismatch;
	idx_t mismatch_count = 0;
	for (auto &argument : layout.arguments) {
		// a variadic parameter that is an expansion passes every argument as the pattern, with its own element of
		// the pack: what it says the pack is there
		auto &parameter_name = signature.GetParameter(argument.parameter_index).GetTypeName();
		auto &name = parameter_name.IsExpansion() ? parameter_name.GetPattern() : parameter_name;
		auto &arg_type = argument.type;
		const bool is_null = arg_type.id() == LogicalTypeId::SQLNULL;
		auto type = BindType(name, variables, arg_type);
		const bool is_ranked = !explicit_casts && !argument.IsDefaultArgument();
		optional<int64_t> cost;
		if (is_ranked) {
			cost = GetPassCost(name, arg_type, type);
		} else if (type || is_null) {
			cost = 0;
		}
		if (!cost && !mismatch) {
			// the first argument that cannot be passed is reported
			mismatch = FunctionMismatch::IncompatibleArgument {
			    argument.argument_index.GetIndex(), argument.parameter_index, type ? *type : LogicalType(), true};
		}
		result.types.push_back(type ? std::move(*type) : arg_type);
		if (!cost) {
			// the more arguments cannot be passed, the further the call is
			mismatch_count++;
			continue;
		}
		if (!is_ranked) {
			continue;
		}
		has_unknown_argument = has_unknown_argument || (arg_type.id() == LogicalTypeId::UNKNOWN &&
		                                                argument.argument_index.GetIndex() < call.GetPositionalCount());
		auto declared = RankTypeName(signature, name);
		rank.legacy_casts += *cost >= OLD_IMPLICIT_CAST_COST ? 1 : 0;
		// a NULL is not passed through an unconstrained type variable: it says nothing about the type
		rank.unconstrained_arguments += is_null ? 0 : declared.unconstrained_arguments;
		rank.cast_cost += static_cast<idx_t>(*cost);
		rank.variable_parameters += declared.variable_parameters;
		rank.declared_structure += declared.variable_parameters * declared.declared_structure;
		rank.declared_values += declared.declared_values;
	}
	if (mismatch) {
		return Err(MatchFailure {mismatch_count, std::move(*mismatch)});
	}
	auto &return_type = signature.GetReturnType();
	for (auto &variable : signature.GetVariables()) {
		auto &variable_name = variable.GetName().GetIdentifierName();
		const auto determined_count = variables.types.count(variable_name) + variables.values.count(variable_name) +
		                              variables.packs.count(variable_name);
		if (determined_count == 0 && return_type.IsType() && return_type.GetTypeName().Contains(variable.GetName())) {
			return Err(MatchFailure {0, FunctionMismatch::TemplateUnresolved {variable_name, variable.IsValue()}});
		}
	}
	if (has_unknown_argument) {
		// with a parameter of an unknown type every overload ranks the same
		rank = FunctionRank();
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Match
//===--------------------------------------------------------------------===//
//! Match the call against the overload, in passes over the arguments: lay them out over the parameters, collect what
//! they say the variables are, infer the variables, and substitute them for the types the arguments are passed as,
//! which ranks how well they are passed (see FunctionRank). An unresolved parameter is passed as a type the overload
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
	auto passed = Substitute(*layout, *variables);
	if (!passed) {
		return Err(std::move(passed.GetError()));
	}
	FunctionBinding binding;
	binding.rank = passed->rank;
	binding.variables = std::move(*variables);
	binding.layout = std::move(*layout);
	for (idx_t i = 0; i < passed->types.size(); i++) {
		D_ASSERT(!TypeVisitor::Contains(passed->types[i], LogicalTypeId::UNKNOWN));
		binding.layout.arguments[i].type = std::move(passed->types[i]);
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
