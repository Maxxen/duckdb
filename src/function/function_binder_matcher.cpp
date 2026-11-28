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
//! CollectCandidates
static bool IsOfKind(const LogicalType &type, LogicalTypeId kind) {
	const bool takes_struct = kind == LogicalTypeId::STRUCT || kind == LogicalTypeId::TUPLE;
	const bool is_struct = type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::TUPLE;
	const bool takes_list = kind == LogicalTypeId::LIST || kind == LogicalTypeId::ARRAY;
	const bool is_list = type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::ARRAY;
	return type.id() == kind || type.id() == LogicalTypeId::SQLNULL || (takes_struct && is_struct) ||
	       (takes_list && is_list);
}

//! The type that a type is passed as where a type of the given kind is declared with its arguments: itself if it is
//! of the kind, else e.g. DECIMAL(10,0) for an INTEGER passed as a DECIMAL(W, S), or the list of its elements for an
//! array passed as a list - nullopt if it is not passed as one. The cases are hard-coded for now: a generic coercion
//! lookup of the cast rules is to take the place of this function
static optional<LogicalType> CoerceToKind(const LogicalType &type, LogicalTypeId kind) {
	if (kind == LogicalTypeId::LIST && type.id() == LogicalTypeId::ARRAY) {
		return LogicalType::LIST(ArrayType::GetChildType(type));
	}
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

//! What an argument says a variable of the signature is: the type a type variable stands for, or the value of a value
//! variable. For a NULL or an unresolved parameter the type is SQLNULL or UNKNOWN, and the value a NULL of the values
//! of the variable or of the type UNKNOWN: it takes what the other arguments say. For a pack that a variadic
//! parameter expands it is the type of one element
struct VariableCandidate {
	Value value;
	//! The argument the function is bound with that says it
	idx_t argument;
};

//! A variable of the signature while a call is matched against the overload: what the signature declares it as and
//! what the arguments say it is. What it is inferred to be from that is in the FunctionVariables of the call
struct VariableBinding {
	VariableBinding(const TypeVariable &declaration_p, optional<LogicalTypeId> constraint_p)
	    : declaration(declaration_p), constraint(constraint_p) {
	}

	reference<const TypeVariable> declaration;
	//! The kind of type a constrained type variable stands for
	optional<LogicalTypeId> constraint;
	//! What the arguments say it is, in their order
	vector<VariableCandidate> candidates;
	//! The elements the parameter that uses a pack says the pack has
	optional<TypePack> elements;
};

//! The variables of a signature by their names, see VariableBinding
using VariableTable = identifier_map_t<VariableBinding>;

//! The variables a signature declares, with nothing said about them yet
static VariableTable DeclareVariables(const FunctionSignature &signature) {
	VariableTable variables;
	for (auto &variable : signature.GetVariables()) {
		auto constraint = signature.GetConstraint(variable.GetName());
		const bool is_constrained = constraint != LogicalTypeId::INVALID;
		variables.emplace(variable.GetName(),
		                  VariableBinding(variable, is_constrained ? optional<LogicalTypeId>(constraint) : nullopt));
	}
	return variables;
}

//! An entry of the table, for a pass that reads it or one that adds to it
template <class TABLE>
using variable_binding_t = std::remove_reference_t<decltype((std::declval<TABLE &>().begin()->second))>;

//! The variable of the signature a type name is, if it is one: no variable is named like a built-in type, and the
//! expansion of a pack is not the pack
template <class TABLE>
static optional_ptr<variable_binding_t<TABLE>> FindVariable(TABLE &variables, const TypeName &name) {
	if (variables.empty() || name.IsExpansion() || name.HasArguments() ||
	    name.GetBuiltinType() != LogicalTypeId::INVALID) {
		return nullptr;
	}
	auto entry = variables.find(name.GetName());
	if (entry == variables.end()) {
		return nullptr;
	}
	return entry->second;
}

//! The value variable of the signature an argument of a type name is, if it is one
template <class TABLE>
static optional_ptr<variable_binding_t<TABLE>> FindValueVariable(TABLE &variables, const TypeNameArgument &argument) {
	if (!argument.IsType()) {
		return nullptr;
	}
	auto variable = FindVariable(variables, argument.GetType());
	if (!variable || !variable->declaration.get().IsValue()) {
		return nullptr;
	}
	return variable;
}

//! The pack of the signature an expansion expands: the one pack its pattern uses, see FunctionSignature::Verify -
//! the pack itself for "*T" and "**KV", the T of "*(T[])"
template <class TABLE>
static variable_binding_t<TABLE> &GetExpandedPack(TABLE &variables, const TypeName &expansion) {
	for (auto &entry : variables) {
		if (entry.second.declaration.get().IsPack() && expansion.GetPattern().Contains(entry.first)) {
			return entry.second;
		}
	}
	throw InternalException("'%s' expands no pack of the signature", expansion.ToString());
}

//! The pack of the signature an argument of a type name expands, if it is an expansion: its elements take the place
//! of the argument. Only a pack by itself is expanded there: a pattern is expanded as the type of a variadic
//! parameter, see FunctionSignature::Verify
template <class TABLE>
static optional_ptr<variable_binding_t<TABLE>> FindPack(TABLE &variables, const TypeNameArgument &argument) {
	if (!argument.IsType() || !argument.GetType().IsExpansion()) {
		return nullptr;
	}
	auto pack = FindVariable(variables, argument.GetType().GetPattern());
	if (!pack || !pack->declaration.get().IsPack()) {
		throw InternalException("'%s' is not the expansion of a pack by itself", argument.GetType().ToString());
	}
	return pack;
}

//! What a call resolves the variable to that a type name is, if it is one: no variable is named like a built-in type,
//! and the expansion of a pack is not the pack. A name that is none is the name of a type
static optional_ptr<const FunctionVariable> FindResolved(const FunctionVariables &resolved, const TypeName &name) {
	if (resolved.determined.empty() || name.IsExpansion() || name.HasArguments() ||
	    name.GetBuiltinType() != LogicalTypeId::INVALID) {
		return nullptr;
	}
	auto entry = resolved.determined.find(name.GetName());
	if (entry == resolved.determined.end()) {
		return nullptr;
	}
	return entry->second;
}

//! The type of the values a value variable stands for
static LogicalType ValueVariableType(const TypeVariable &variable) {
	return variable.GetKind() == TypeVariable::Kind::INTEGER ? LogicalType::BIGINT : LogicalType::VARCHAR;
}

//! The type a type variable is, given the type the arguments say it is: normalized. A variable that only an
//! unresolved parameter says leaves the call unresolved. Nothing if it is not of the kind of a constrained variable:
//! no type is converted to be of the kind, and a type with an alias is a type of its own
static optional<LogicalType> GetVariableType(const LogicalType &type, const optional<LogicalTypeId> &constraint) {
	// throws for an unresolved parameter
	auto result = LogicalType::NormalizeType(type);
	if (constraint && !(result.GetAlias().empty() && IsOfKind(result, *constraint))) {
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
//! kind, e.g. a map for a list, says nothing about the arguments of the name. Nor if it is of the kind and a value
//! the name declares is not the one it has
static optional<LogicalType> PairType(const TypeName &name, LogicalTypeId kind, const LogicalType &type) {
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

//! Binds a type name: the type it stands for with the variables of the signature as a call resolves them. A name
//! that is none of them is the name of a type, which is constructed of what its arguments stand for. A type variable
//! is the type it is resolved to, and a value variable the value: by itself, as the type of a parameter that takes
//! the value, it is the type of its values. The elements of a pack take the place of an expansion that is an argument
//! of the name, labelled if it is a named expansion: TUPLE(*T) is the tuple of the types of T, STRUCT(**KV) the struct
//! of the fields of KV, and TUPLE(*KV) the tuple of the types of those fields. The name itself is not an expansion:
//! for a variadic parameter that is one it is the pattern, see Substitute. ANY is a type here: a parameter that is
//! declared as ANY is not bound, see Substitute. Throws if the constructor of a type does not take what the name
//! stands for, e.g. a value variable that is a NULL
static LogicalType BindTypeNameInternal(TypeCache &cache, const TypeName &name, const FunctionVariables &resolved) {
	auto variable = FindResolved(resolved, name);
	if (variable && std::holds_alternative<LogicalType>(*variable)) {
		return std::get<LogicalType>(*variable);
	}
	if (variable && std::holds_alternative<Value>(*variable)) {
		// a parameter that takes the value of the variable
		return std::get<Value>(*variable).type();
	}
	if (variable) {
		throw InternalException("'%s' is a pack: it is a type where it is expanded", name.ToString());
	}
	LogicalType builtin_type(name.GetBuiltinType());
	if (builtin_type.IsComplete() || builtin_type.id() == LogicalTypeId::ANY) {
		// the name of a built-in type that takes no arguments
		return builtin_type;
	}

	vector<TypeArgument> bound_arguments;
	for (auto &argument : name.GetArguments()) {
		if (!argument.IsType()) {
			bound_arguments.emplace_back(argument.GetLabel().GetIdentifierName(), argument.GetLiteral().ToValue());
			continue;
		}
		auto &part = argument.GetType();
		if (part.IsExpansion()) {
			// the pack is the only argument of the name, see FunctionSignature::Verify
			auto pack = FindResolved(resolved, part.GetPattern());
			if (!pack || !std::holds_alternative<TypePack>(*pack)) {
				throw InternalException("'%s' is not the expansion of a pack by itself", part.ToString());
			}
			auto &elements = std::get<TypePack>(*pack);
			// a positional expansion gives the types without their labels, also those of a named pack
			const bool is_named = part.GetExpansionKind() == TypeMultiplicity::NAMED_PACK;
			for (idx_t element_idx = 0; element_idx < elements.Count(); element_idx++) {
				auto label = elements.GetLabel(element_idx);
				bound_arguments.emplace_back(is_named && label ? label->GetIdentifierName() : string(),
				                             Value::TYPE(elements.GetType(element_idx)));
			}
			continue;
		}
		auto part_variable = FindResolved(resolved, part);
		if (part_variable && std::holds_alternative<Value>(*part_variable)) {
			bound_arguments.emplace_back(argument.GetLabel().GetIdentifierName(), std::get<Value>(*part_variable));
			continue;
		}
		bound_arguments.emplace_back(argument.GetLabel().GetIdentifierName(),
		                             Value::TYPE(BindTypeNameInternal(cache, part, resolved)));
	}

	auto &identifier = name.GetName();
	auto type = cache.Construct(identifier, std::move(bound_arguments));
	if (type.id() == LogicalTypeId::INVALID) {
		throw CatalogException("Type with name %s does not exist", identifier.GetIdentifierName());
	}
	return type;
}

LogicalType FunctionBinder::BindTypeName(optional_ptr<ClientContext> context, const TypeName &name) {
	TypeCache cache(context);
	return BindTypeNameInternal(cache, name, FunctionVariables());
}

//===--------------------------------------------------------------------===//
// Matching
//===--------------------------------------------------------------------===//
//! What CastFunctionSet::ImplicitCastCost returns for a cast to VARCHAR that only the old_implicit_casting setting
//! allows: more than any other cast costs
static constexpr int64_t OLD_IMPLICIT_CAST_COST = 10000000000;

//! What the arguments say: the variables of the signature with what is said about them, and whether every argument
//! the function is bound with fits the type of its parameter, see CollectCandidates
struct CollectedArguments {
	VariableTable variables;
	vector<bool> fits;
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
	Result<CollectedArguments, MatchFailure> Collect(const ArgumentLayout &layout, VariableTable variables) const;
	Result<FunctionVariables, MatchFailure> Infer(const ArgumentLayout &layout,
	                                              const CollectedArguments &collected) const;
	Result<PassedArguments, MatchFailure> Substitute(const ArgumentLayout &layout, const CollectedArguments &collected,
	                                                 const FunctionVariables &resolved) const;

private:
	void AddUntypedCandidates(const TypeName &name, const LogicalType &type, idx_t argument,
	                          VariableTable &variables) const;
	bool CollectCandidates(const TypeName &name, const LogicalType &type, idx_t argument,
	                       VariableTable &variables) const;
	MatchFailure GetUnsaidVariable(const ArgumentLayout &layout, const CollectedArguments &collected,
	                               const FunctionVariables &resolved, const TypeVariable &variable) const;
	optional<LogicalType> BindType(const TypeName &name, const FunctionVariables &resolved) const;
	optional<int64_t> GetPassCost(const TypeName &name, const LogicalType &arg_type, const optional<LogicalType> &type,
	                              bool fits, bool is_variable) const;

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

//! The value a parameter that is declared as a value variable is passed: what it says the variable is - for a NULL
//! of any type, a NULL of the values of the variable. "argument" is the argument of the call that passes it, invalid
//! if it is the default of the parameter
static Result<Value, MatchFailure> GetVariableValue(const TypeVariable &variable, optional<Value> constant,
                                                    optional_idx argument, idx_t parameter) {
	if (!constant) {
		return Err(MatchFailure {
		    0, FunctionMismatch::ConstantRequired {argument.GetIndex(), variable.GetName().GetIdentifierName()}});
	}
	if (constant->IsNull()) {
		return Value(ValueVariableType(variable));
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

//! An argument that says nothing about the type its parameter declares - a NULL or an unresolved parameter - says
//! that a type variable the type uses is its own type and that a value variable is a NULL: of the values of the
//! variable for a NULL, of its own type for an unresolved parameter. It has no elements for a pack that a type
//! takes: a NULL says the pack without elements, and an unresolved parameter leaves it open
void OverloadMatcher::AddUntypedCandidates(const TypeName &name, const LogicalType &type, idx_t argument,
                                           VariableTable &variables) const {
	auto variable = FindVariable(variables, name);
	if (variable && variable->declaration.get().IsValue()) {
		const bool is_unresolved = type.id() == LogicalTypeId::UNKNOWN;
		variable->candidates.push_back(
		    {Value(is_unresolved ? type : ValueVariableType(variable->declaration)), argument});
	} else if (variable) {
		variable->candidates.push_back({Value::TYPE(type), argument});
	}
	for (auto &name_argument : name.GetArguments()) {
		auto pack = FindPack(variables, name_argument);
		if (pack && type.id() == LogicalTypeId::UNKNOWN) {
			pack->candidates.push_back({Value::TYPE(type), argument});
		} else if (pack && !pack->elements) {
			pack->elements = TypePack::Positional(vector<LogicalType>());
		} else if (!pack && name_argument.IsType()) {
			AddUntypedCandidates(name_argument.GetType(), type, argument, variables);
		}
	}
}

//! Pair a type name with a type, part by part, and add what the type says the variables of the name are to
//! "variables". A pack is paired with the parameters of a type of the kind of the name: TUPLE(*T) passed a tuple says
//! that T is the types of the tuple, STRUCT(**KV) passed a struct that KV is its fields, and a struct or a tuple
//! without parts says the pack of no elements for either. A type of another kind than the name has no parts to pair:
//! it says nothing, see PairType. Returns whether the type fits the name: every part of the name is paired with a
//! parameter of the type and every parameter of the type with a part of the name - a type variable with a type it
//! stands for, a value variable with a value, a pack with all of them, a value the name gives with that value.
//! INTEGER[3] fits T[] as a list, INTEGER fits DECIMAL(W, S) as DECIMAL(10, 0), and a VARIANT or a map does not fit
//! T[]. A name by itself that is not a type variable leaves nothing to pair
bool OverloadMatcher::CollectCandidates(const TypeName &name, const LogicalType &type, idx_t argument,
                                        VariableTable &variables) const {
	if (IsUntyped(type)) {
		// Nested functions propagate these types: the binder replaces a function that returns one by a NULL, or
		// binds it again once the parameter is resolved. It has no parts to pair
		AddUntypedCandidates(name, type, argument, variables);
		return !name.HasArguments();
	}
	auto variable = FindVariable(variables, name);
	if (variable && !variable->declaration.get().IsValue()) {
		// a type variable is paired with a type it stands for: one of its kind, for a constrained variable
		if (variable->constraint && !GetVariableType(type, variable->constraint)) {
			return false;
		}
		variable->candidates.push_back({Value::TYPE(type), argument});
		return true;
	}
	if (!name.HasArguments()) {
		return true;
	}
	auto &arguments = name.GetArguments();
	const auto kind = DefaultTypeGenerator::GetDefaultType(name.GetName());
	auto paired = PairType(name, kind, type);
	if (!paired) {
		return false;
	}
	const auto parameter_count = paired->GetParameterCount();
	bool fits = true;
	idx_t paired_count = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto pack = FindPack(variables, arguments[i]);
		if (pack) {
			// the pack is the only argument of the name: its elements are all the parameters of a type of that kind -
			// and a type without parameters says the pack without elements, whether it is a struct or a tuple
			const bool has_elements = paired->id() == kind || parameter_count == 0;
			if (has_elements && !pack->elements) {
				pack->elements = TypePack::ParametersOf(*paired);
			}
			fits = fits && has_elements;
			paired_count = parameter_count;
			continue;
		}
		auto parameter_idx = FindTypeParameter(*paired, arguments, i);
		if (!parameter_idx.IsValid()) {
			fits = false;
			continue;
		}
		paired_count++;
		auto parameter = paired->GetParameter(parameter_idx.GetIndex());
		const bool is_type = parameter.type().id() == LogicalTypeId::TYPE;
		if (!arguments[i].IsType()) {
			fits = fits && IsSameParameter(arguments[i].GetLiteral().ToValue(), parameter);
			continue;
		}
		auto value_variable = FindValueVariable(variables, arguments[i]);
		if (is_type && !value_variable) {
			const bool is_paired =
			    CollectCandidates(arguments[i].GetType(), TypeValue::GetType(parameter), argument, variables);
			fits = fits && is_paired;
			continue;
		}
		// a value variable is the value the type has in its place
		optional<Value> value;
		if (!is_type && value_variable) {
			value = parameter.DefaultTryCastAs(ValueVariableType(value_variable->declaration));
		}
		if (!value || value->IsNull()) {
			fits = false;
			continue;
		}
		value_variable->candidates.push_back({std::move(*value), argument});
	}
	return fits && paired_count == parameter_count;
}

//! Collect what the arguments say the variables are: every argument is paired with the type of its parameter - with
//! the pattern, for a variadic parameter that is an expansion - and a parameter that is declared as a value variable
//! takes the value of its argument, which the pairing says already for a NULL or an unresolved parameter. The pack such
//! a variadic parameter expands takes one element from every argument: what the argument says the pack is in the
//! pattern. Returns the variables with what is said about them, and whether every argument fits - or why not: an
//! argument that is passed as a value variable is not a constant, or not a value of the variable
Result<CollectedArguments, MatchFailure> OverloadMatcher::Collect(const ArgumentLayout &layout,
                                                                  VariableTable variables) const {
	auto context = cache.GetContext();
	vector<bool> fits;
	fits.reserve(layout.arguments.size());
	for (idx_t i = 0; i < layout.arguments.size(); i++) {
		auto &argument = layout.arguments[i];
		auto &param = signature.GetParameter(argument.parameter_index);
		auto &param_name = param.GetTypeName();
		auto &pattern = param_name.IsExpansion() ? param_name.GetPattern() : param_name;
		fits.push_back(CollectCandidates(pattern, argument.type, i, variables));
		auto variable = FindVariable(variables, param_name);
		if (!variable || !variable->declaration.get().IsValue() || IsUntyped(argument.type)) {
			continue;
		}
		auto arg_idx = argument.argument_index;
		auto constant = arg_idx.IsValid() ? GetArgumentConstant(context, call.GetArgument(arg_idx.GetIndex()))
		                                  : optional<Value>(*param.GetDefaultValue());
		auto value = GetVariableValue(variable->declaration, std::move(constant), arg_idx, argument.parameter_index);
		if (!value) {
			return Err(std::move(value.GetError()));
		}
		variable->candidates.push_back({std::move(*value), i});
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
		auto &pack = GetExpandedPack(variables, variadic_name);
		const bool is_named = variadic_name.GetExpansionKind() == TypeMultiplicity::NAMED_PACK;
		idx_t argument_count = 0;
		for (auto &argument : layout.arguments) {
			argument_count += argument.parameter_index == variadic_idx.GetIndex() ? 1 : 0;
		}
		vector<LogicalType> types;
		child_list_t<LogicalType> labelled_types;
		for (auto &candidate : pack.candidates) {
			auto said_type = TypeValue::GetType(candidate.value);
			if (said_type.id() == LogicalTypeId::UNKNOWN) {
				continue;
			}
			auto type = LogicalType::NormalizeType(said_type);
			if (is_named) {
				auto argument_idx = layout.arguments[candidate.argument].argument_index.GetIndex();
				labelled_types.emplace_back(call.GetArgument(argument_idx).name, std::move(type));
			} else {
				types.push_back(std::move(type));
			}
		}
		if (types.size() + labelled_types.size() == argument_count) {
			pack.elements =
			    is_named ? TypePack::Named(std::move(labelled_types)) : TypePack::Positional(std::move(types));
		}
	}
	return CollectedArguments {std::move(variables), std::move(fits)};
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
//! only NULLs are passed as it. A value variable takes the value the other arguments say in the same way, and is a
//! NULL of its values if only NULLs say it: no type is made of that, see Substitute. A pack is the elements the one
//! parameter that uses it says, see Collect - there without elements if a NULL is passed as it, so a pack is
//! always determined unless an argument does not fit. A variable that is left to an unresolved parameter - no other
//! argument says what it is - leaves the call unresolved, whatever the other overloads are and whether or not the
//! other arguments can be passed. Returns what the variables are resolved to, every one the signature declares - or
//! why not: the arguments say different things about a variable, what they say a constrained variable is is not of
//! its kind, or no argument says what a variable is, see GetUnsaidVariable
//! Why a variable is not resolved, when no argument says what it is. An argument that is passed as a type that uses
//! such a variable has no parts for that type, e.g. a map that is passed as T[]: it cannot be passed, and the failure
//! is the one of passing the arguments, see Substitute. Without such an argument nothing is passed as the variable:
//! it is not determined
MatchFailure OverloadMatcher::GetUnsaidVariable(const ArgumentLayout &layout, const CollectedArguments &collected,
                                                const FunctionVariables &resolved, const TypeVariable &variable) const {
	auto passed = Substitute(layout, collected, resolved);
	if (!passed) {
		return std::move(passed.GetError());
	}
	return MatchFailure {
	    0, FunctionMismatch::TemplateUnresolved {variable.GetName().GetIdentifierName(), variable.IsValue()}};
}

Result<FunctionVariables, MatchFailure> OverloadMatcher::Infer(const ArgumentLayout &layout,
                                                               const CollectedArguments &collected) const {
	auto &variables = collected.variables;
	auto context = cache.GetContext();
	auto &arguments = layout.arguments;
	FunctionVariables resolved;
	// whether a pack is left to an unresolved parameter
	bool is_unresolved = false;
	// in the order the signature declares them: the first variable that is not inferred is reported
	for (auto &declaration : signature.GetVariables()) {
		auto &variable = variables.find(declaration.GetName())->second;
		FunctionMismatch::TemplateDeduction conflict;
		conflict.template_name = declaration.GetName().GetIdentifierName();

		if (declaration.IsPack() && variable.elements) {
			// one parameter uses a pack, see FunctionSignature::Verify: what is passed to it says the pack
			resolved.determined.emplace(declaration.GetName(), *variable.elements);
			continue;
		}
		if (declaration.IsPack()) {
			// a pack that no argument says: an unresolved parameter is what could say it
			for (auto &candidate : variable.candidates) {
				is_unresolved = is_unresolved || TypeValue::GetType(candidate.value).id() == LogicalTypeId::UNKNOWN;
			}
			continue;
		}

		if (declaration.IsValue()) {
			// The values the arguments say have to be the same. A NULL takes the value another argument says, and so
			// does an unresolved parameter, which a NULL takes in turn
			optional_ptr<const Value> value;
			for (auto &candidate : variable.candidates) {
				auto &said_value = candidate.value;
				const bool is_said = !said_value.IsNull() || said_value.type().id() == LogicalTypeId::UNKNOWN;
				if (!value || (value->IsNull() && is_said)) {
					value = said_value;
				} else if (!said_value.IsNull() && !Value::NotDistinctFrom(*value, said_value)) {
					conflict.argument = arguments[candidate.argument].argument_index;
					conflict.inferred_values = {*value, said_value};
					return Err(MatchFailure {0, std::move(conflict)});
				}
			}
			if (value) {
				resolved.determined.emplace(declaration.GetName(), *value);
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
		for (auto &candidate : variable.candidates) {
			auto said_type = TypeValue::GetType(candidate.value);
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
				conflict.argument = arguments[candidate.argument].argument_index;
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
			resolved.determined.emplace(declaration.GetName(), std::move(type));
		}
	}

	// The types are final, once every variable is promoted without a conflict
	optional_ptr<const TypeVariable> unsaid;
	for (auto &declaration : signature.GetVariables()) {
		auto entry = resolved.determined.find(declaration.GetName());
		if (entry == resolved.determined.end()) {
			// no argument says it: the first such variable is reported, unless the call is unresolved
			unsaid = unsaid ? unsaid : optional_ptr<const TypeVariable>(declaration);
			continue;
		}
		if (declaration.IsValue() && std::get<Value>(entry->second).type().id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
		if (!std::holds_alternative<LogicalType>(entry->second)) {
			continue;
		}
		// arguments of the kind can still be promoted to a type that is not of it, e.g. two enums to VARCHAR
		auto &promoted = std::get<LogicalType>(entry->second);
		auto type = GetVariableType(promoted, variables.find(declaration.GetName())->second.constraint);
		if (!type) {
			FunctionMismatch::TemplateDeduction conflict;
			conflict.template_name = declaration.GetName().GetIdentifierName();
			conflict.inferred_types.push_back(LogicalType::NormalizeType(promoted));
			return Err(MatchFailure {0, std::move(conflict)});
		}
		promoted = std::move(*type);
	}
	if (is_unresolved) {
		throw ParameterNotResolvedException();
	}
	if (unsaid) {
		return Err(GetUnsaidVariable(layout, collected, resolved, *unsaid));
	}

	return std::move(resolved);
}

//===--------------------------------------------------------------------===//
// Substitute
//===--------------------------------------------------------------------===//
//! The type a type name of the signature stands for with the variables as the call resolves them, see
//! BindTypeNameInternal. Nothing if the constructor of a type does not take the parts, e.g. a value variable that is
//! a NULL: an error of the binder or of its input, as itself or as the error of a call that was not matched
optional<LogicalType> OverloadMatcher::BindType(const TypeName &name, const FunctionVariables &resolved) const {
	try {
		return BindTypeNameInternal(cache, name, resolved);
	} catch (const Exception &ex) {
		const auto error_type = ErrorData(ex).Type();
		if (error_type != ExceptionType::BINDER && error_type != ExceptionType::INVALID_INPUT) {
			throw;
		}
		return nullopt;
	}
}

//! What an argument adds to the rank by the type of the parameter that receives it, see FunctionRank: whether it is
//! passed through an unconstrained type variable (rule 2) or a type variable at all (rule 4), and what the type
//! declares (rules 5 and 6)
static FunctionRank RankTypeName(const VariableTable &variables, const TypeName &name) {
	if (name.IsExpansion()) {
		// what is expanded is what an argument is passed through: a pack counts like a type variable
		return RankTypeName(variables, name.GetPattern());
	}
	FunctionRank rank;
	auto variable = FindVariable(variables, name);
	if (variable && variable->declaration.get().IsValue()) {
		// counted by the type that takes it
		return rank;
	}
	if (variable || name.IsBuiltinType(LogicalTypeId::ANY)) {
		const bool is_constrained = variable && variable->constraint;
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
		auto part = RankTypeName(variables, argument.GetType());
		rank.unconstrained_arguments = MaxValue(rank.unconstrained_arguments, part.unconstrained_arguments);
		rank.variable_parameters = MaxValue(rank.variable_parameters, part.variable_parameters);
		rank.declared_structure += part.declared_structure;
		rank.declared_values += part.declared_values + (FindValueVariable(variables, argument) ? 1 : 0);
	}
	return rank;
}

//! What passing an argument to a parameter costs: the implicit cast to the type it is passed as, "type" - nothing if
//! it cannot be passed. Whether there has to be such a cast is up to the type of the parameter, and to whether the
//! argument fits it, see CollectCandidates:
//! - It fits a type that is passed through a type variable: it is cast explicitly, whatever the variables are
//!   inferred to be. No implicit cast is asked for: it costs nothing if there is none. INTEGER[3] fits T[], and
//!   anything fits a type variable by itself.
//! - It fits a type that is not passed through one, which is the type whatever the other arguments are: it has to
//!   cast implicitly to it.
//! - It has no parts for the type: it has to cast implicitly to the type it is passed as, e.g. a VARIANT or a string
//!   literal for T[].
//! - It is not a type a type variable by itself stands for: it cannot be passed.
//! - An unresolved parameter is passed as the type the parameter declares, at no cost.
//! - Where no type could be made a NULL is passed as it is, and costs the cast of a NULL to the type the parameter
//!   names. An argument that fits the type is passed as it is as well, at no cost: it is what the variables are, and
//!   a constructor does not make every type there is, e.g. a DECIMAL that is wider than the session allows
optional<int64_t> OverloadMatcher::GetPassCost(const TypeName &name, const LogicalType &arg_type,
                                               const optional<LogicalType> &type, bool fits, bool is_variable) const {
	if (arg_type.id() == LogicalTypeId::SQLNULL && !type) {
		return CastRules::ImplicitCastNull(DefaultTypeGenerator::GetDefaultType(name.GetName()));
	}
	if (!type && fits) {
		return 0;
	}
	if (!type || (!fits && !name.HasArguments())) {
		return nullopt;
	}
	if (arg_type.id() == LogicalTypeId::UNKNOWN) {
		return 0;
	}
	auto cost = ImplicitCastCost(cache.GetContext(), arg_type, *type);
	if (!cost && fits && is_variable) {
		return 0;
	}
	return cost;
}

//! Substitute the variables, and rank how the arguments are passed (see FunctionRank). The type every argument is
//! passed as is the type of its parameter with the variables as the call resolves them, built once - a NULL is passed
//! as it is where that type cannot be built. A parameter that is declared as ANY has no type to build: it takes the
//! type of its argument. A variadic parameter that is an expansion passes every argument as the pattern, in which
//! the pack is one element: the one of the argument, at its position among the arguments of the parameter. An
//! argument of the call then adds what passing it costs, see GetPassCost, and what the type of its parameter
//! declares to the rank. A default is not ranked, and nothing is when the arguments are cast explicitly: such an
//! argument only needs a type. Returns why not: the first argument that cannot be passed and how many there are.
//! Every variable is resolved, see Infer - which reports the arguments that cannot be passed with the variables that
//! are, where one is not: no type is made of that one
Result<PassedArguments, MatchFailure> OverloadMatcher::Substitute(const ArgumentLayout &layout,
                                                                  const CollectedArguments &collected,
                                                                  const FunctionVariables &resolved) const {
	auto &variables = collected.variables;
	PassedArguments result;
	result.types.reserve(layout.arguments.size());
	auto &rank = result.rank;
	bool has_unknown_argument = false;
	optional<FunctionMismatch::IncompatibleArgument> mismatch;
	idx_t mismatch_count = 0;
	// what a pattern is bound with: the variables with the pack as one of its elements, the pack the pattern expands,
	// and the first argument of the parameter
	optional<FunctionVariables> element_variables;
	optional_ptr<const Identifier> pack_name;
	idx_t first_argument = 0;
	// only where Infer reports why a variable is not resolved, see GetUnsaidVariable
	const bool has_unsaid = resolved.determined.size() < signature.GetVariables().size();
	for (idx_t i = 0; i < layout.arguments.size(); i++) {
		auto &argument = layout.arguments[i];
		auto &parameter_name = signature.GetParameter(argument.parameter_index).GetTypeName();
		auto &name = parameter_name.IsExpansion() ? parameter_name.GetPattern() : parameter_name;
		auto &arg_type = argument.type;
		const bool is_null = arg_type.id() == LogicalTypeId::SQLNULL;
		if (i == 0 || layout.arguments[i - 1].parameter_index != argument.parameter_index) {
			first_argument = i;
			pack_name = nullptr;
		}
		bool is_unsaid = false;
		for (idx_t variable_idx = 0; has_unsaid && variable_idx < signature.GetVariables().size(); variable_idx++) {
			auto &variable_name = signature.GetVariables()[variable_idx].GetName();
			is_unsaid = is_unsaid || (!resolved.IsDetermined(variable_name) && name.Contains(variable_name));
		}
		optional<LogicalType> type;
		if (is_unsaid) {
			// no type is made of a variable that is not resolved
		} else if (name.IsBuiltinType(LogicalTypeId::ANY)) {
			// throws for an unresolved parameter
			type = LogicalType::NormalizeType(arg_type);
		} else if (parameter_name.IsExpansion() && is_null && name.HasArguments()) {
			// a NULL has no parts for the pattern: it is passed as it is
		} else if (parameter_name.IsExpansion()) {
			if (!pack_name) {
				pack_name = GetExpandedPack(variables, parameter_name).declaration.get().GetName();
			}
			auto &elements = std::get<TypePack>(resolved.determined.find(*pack_name)->second);
			if (!element_variables) {
				element_variables = resolved;
			}
			element_variables->determined[*pack_name] = elements.GetType(i - first_argument);
			type = BindType(name, *element_variables);
		} else {
			type = BindType(name, resolved);
		}
		auto declared = RankTypeName(variables, name);
		const bool is_ranked = !explicit_casts && !argument.IsDefaultArgument();
		optional<int64_t> cost;
		if (is_ranked) {
			cost = GetPassCost(name, arg_type, type, collected.fits[i], declared.variable_parameters > 0);
		} else if (type || is_null || collected.fits[i]) {
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
	auto collected = Collect(*layout, DeclareVariables(signature));
	if (!collected) {
		return Err(std::move(collected.GetError()));
	}
	auto resolved = Infer(*layout, *collected);
	if (!resolved) {
		return Err(std::move(resolved.GetError()));
	}
	auto passed = Substitute(*layout, *collected, *resolved);
	if (!passed) {
		return Err(std::move(passed.GetError()));
	}
	FunctionBinding binding;
	binding.rank = passed->rank;
	binding.variables = std::move(*resolved);
	binding.layout = std::move(*layout);
	for (idx_t i = 0; i < passed->types.size(); i++) {
		D_ASSERT(!TypeVisitor::Contains(passed->types[i], LogicalTypeId::UNKNOWN));
		// ANY is the type of a whole parameter, see Substitute: within a type it would be passed on
		D_ASSERT(!TypeVisitor::Contains(passed->types[i], LogicalTypeId::ANY));
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
	auto &return_type = binding.GetFunction().GetSignature().GetReturnType();
	if (!return_type.IsType()) {
		// the signature declares no type: the resolver or the bind callback of the function sets it
		return LogicalType::INVALID;
	}
	// the overload is selected by its arguments: a type that cannot be made of what they say is an error of the call
	return BindTypeNameInternal(cache, return_type.GetTypeName(), binding.variables);
}

} // namespace duckdb
