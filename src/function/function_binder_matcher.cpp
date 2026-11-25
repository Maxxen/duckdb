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

//! The type that a type is passed as where a type of the given kind is expected: itself if it is of the kind, else
//! e.g. DECIMAL(10,0) for an INTEGER passed as a DECIMAL - nullopt if it is not passed as one. The cases are
//! hard-coded for now: a generic coercion lookup of the cast rules is to take the place of this function
static optional<LogicalType> CoerceToKind(const LogicalType &type, LogicalTypeId kind) {
	if (type.id() == kind) {
		return type;
	}
	const bool takes_struct = kind == LogicalTypeId::STRUCT || kind == LogicalTypeId::TUPLE;
	const bool is_struct = type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::TUPLE;
	if (takes_struct && is_struct) {
		// a struct with and without names are of one kind: which of the two a function takes is its to check
		return type;
	}
	uint32_t width, scale;
	if (kind == LogicalTypeId::DECIMAL && type.id() != LogicalTypeId::SQLNULL &&
	    type.GetDecimalProperties(width, scale)) {
		// a number is passed as the decimal that holds it - a NULL is not a number
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
//! What a type name is in the signature that declares it
enum class TypeNameClass : uint8_t {
	//! A type, by its name alone: a built-in type or a type of the catalog
	TYPE,
	//! A type variable of the signature
	TYPE_VARIABLE,
	//! A value variable of the signature
	VALUE_VARIABLE,
	//! ANY: a type variable of its own each time it is written, that stands for any type
	ANY,
	//! A type that is constructed from the arguments of the name
	CONSTRUCTED_TYPE
};

struct ClassifiedTypeName {
	TypeNameClass name_class;
	//! The variable, if the name is one
	optional_ptr<const TypeVariable> variable;
	//! The kind of type a type variable stands for, INVALID if it stands for any type
	LogicalTypeId constraint;
};

//! Classify a type name of a signature. A name without arguments is a variable of the signature if there is one of
//! that name, whatever else is called the same
static ClassifiedTypeName ClassifyTypeName(const FunctionSignature &signature, const TypeName &name) {
	if (name.HasArguments()) {
		return {TypeNameClass::CONSTRUCTED_TYPE, nullptr, LogicalTypeId::INVALID};
	}
	auto variable = signature.FindVariable(name.GetName());
	if (!variable) {
		const bool is_any = name.IsBuiltinType(LogicalTypeId::ANY);
		return {is_any ? TypeNameClass::ANY : TypeNameClass::TYPE, nullptr, LogicalTypeId::INVALID};
	}
	if (variable->IsValue()) {
		return {TypeNameClass::VALUE_VARIABLE, variable, LogicalTypeId::INVALID};
	}
	return {TypeNameClass::TYPE_VARIABLE, variable, signature.GetConstraint(name.GetName())};
}

//! The value variable of the signature an argument of a type name refers to, if it refers to one
static optional_ptr<const TypeVariable> FindValueVariable(const TypeNameArgument &argument,
                                                          const FunctionSignature &signature) {
	if (!argument.IsType()) {
		return nullptr;
	}
	auto classified = ClassifyTypeName(signature, argument.GetType());
	return classified.name_class == TypeNameClass::VALUE_VARIABLE ? classified.variable : nullptr;
}

//! The type of the values a value variable stands for
static LogicalType ValueVariableType(const TypeVariable &variable) {
	return variable.GetKind() == TypeVariable::Kind::INTEGER ? LogicalType::BIGINT : LogicalType::VARCHAR;
}

//! Binds a type name. "variables" is what the variables of the signature stand for - the value of a value variable
//! is NULL if it is not known - or nothing for a name that uses no variable, and any_type is what ANY stands for
static LogicalType BindTypeNameInternal(TypeCache &cache, const TypeName &name, const FunctionSignature &signature,
                                        optional_ptr<const FunctionVariables> variables,
                                        const LogicalType &any_type = LogicalType::ANY) {
	auto &identifier = name.GetName();
	auto &arguments = name.GetArguments();
	auto builtin_type = name.GetBuiltinType();
	if (builtin_type == LogicalTypeId::ANY) {
		return any_type;
	}
	if (builtin_type != LogicalTypeId::INVALID) {
		// the name of a built-in type that takes no arguments is that type, whatever else is called the same
		LogicalType type(builtin_type);
		if (type.IsComplete()) {
			return type;
		}
	}
	auto classified = ClassifyTypeName(signature, name);
	if (classified.name_class == TypeNameClass::VALUE_VARIABLE) {
		// a parameter that takes the value of the variable
		return ValueVariableType(*classified.variable);
	}
	if (classified.name_class == TypeNameClass::TYPE_VARIABLE) {
		if (!variables) {
			throw InternalException("Type variable %s in a type that uses none", identifier.GetIdentifierName());
		}
		auto entry = variables->types.find(identifier.GetIdentifierName());
		if (entry == variables->types.end()) {
			throw BinderException("Cannot deduce type variable '%s', it is not determined by the arguments",
			                      identifier.GetIdentifierName());
		}
		return entry->second;
	}

	vector<TypeArgument> bound_arguments;
	bool has_unknown_value = false;
	for (auto &argument : arguments) {
		Value value;
		if (FindValueVariable(argument, signature)) {
			auto &variable_name = argument.GetType().GetName().GetIdentifierName();
			if (!variables) {
				throw InternalException("Value variable %s in a type that uses none", variable_name);
			}
			auto entry = variables->values.find(variable_name);
			if (entry == variables->values.end()) {
				throw BinderException("Cannot deduce value '%s', it is not determined by the arguments", variable_name);
			}
			value = entry->second;
			has_unknown_value = has_unknown_value || value.IsNull();
		} else if (argument.IsType()) {
			value = Value::TYPE(BindTypeNameInternal(cache, argument.GetType(), signature, variables, any_type));
		} else {
			value = argument.GetLiteral().ToValue();
		}
		bound_arguments.emplace_back(argument.GetLabel().GetIdentifierName(), std::move(value));
	}
	if (has_unknown_value) {
		// until its values are known the type is of a kind: an array of any size, a decimal of any width and scale
		auto kind = DefaultTypeGenerator::GetDefaultType(identifier);
		if (kind == LogicalTypeId::ARRAY && arguments.size() == 2 && arguments[0].IsType()) {
			return LogicalType::ARRAY(TypeValue::GetType(bound_arguments[0].GetValue()), optional_idx());
		}
		if (kind == LogicalTypeId::INVALID) {
			throw NotImplementedException("Type %s cannot take a value variable", identifier.GetIdentifierName());
		}
		return LogicalType(kind);
	}

	auto type = cache.Construct(identifier, std::move(bound_arguments));
	if (type.id() != LogicalTypeId::INVALID) {
		return type;
	}
	throw CatalogException("Type with name %s does not exist", identifier.GetIdentifierName());
}

//! The type a type name that uses no variable of the signature stands for
static LogicalType BindDeclaredType(TypeCache &cache, const TypeName &name, const FunctionSignature &signature) {
	return BindTypeNameInternal(cache, name, signature, nullptr);
}

LogicalType FunctionBinder::BindTypeName(optional_ptr<ClientContext> context, const TypeName &name) {
	TypeCache cache(context);
	return BindDeclaredType(cache, name, FunctionSignature());
}

//! The type a type name of a signature stands for, given what a call determines its variables to be
static LogicalType BindSignatureType(TypeCache &cache, const TypeName &name, const FunctionSignature &signature,
                                     const FunctionVariables &variables,
                                     const LogicalType &any_type = LogicalType::ANY) {
	return BindTypeNameInternal(cache, name, signature, variables, any_type);
}

//===--------------------------------------------------------------------===//
// Type variables
//===--------------------------------------------------------------------===//
//! What CastFunctionSet::ImplicitCastCost returns for a cast to VARCHAR that only the old_implicit_casting setting
//! allows: more than any other cast costs
static constexpr int64_t OLD_IMPLICIT_CAST_COST = 10000000000;
//! Whether a NULL or an unresolved parameter that comes first is skipped when the types of a type variable are
//! unified, as it is in every other position. false is upstream's behaviour: the next type is promoted with it, which
//! turns a literal into its default type. Kept until a setting can select the new behaviour
static constexpr bool SKIP_LEADING_UNTYPED_ARGUMENT = false;

//! How a type name uses the type variables of its signature
struct VariableUse {
	//! Whether it uses a type variable
	bool uses_variable = false;
	//! Whether it uses ANY: a type variable of its own each time it occurs, that stands for any type
	bool uses_any = false;
	//! Whether it uses a type variable that stands for any type
	bool unconstrained = false;
	//! Whether it uses a value variable
	bool uses_value = false;
	//! What the name declares, as the rank counts it: 2 for every type it names and 1 for every constrained type
	//! variable (FunctionRank rule 5), 2 for every literal and 1 for every value variable (rule 6)
	idx_t structure = 0;
	idx_t values = 0;

	//! Whether it uses a variable the signature declares
	bool UsesVariable() const {
		return uses_variable || uses_value;
	}
	//! Whether the type it stands for depends on the call
	bool IsGeneric() const {
		return uses_variable || uses_any || uses_value;
	}
};

static VariableUse GetVariableUse(const FunctionSignature &signature, const TypeName &name) {
	VariableUse use;
	auto classified = ClassifyTypeName(signature, name);
	// ANY is used as ANY even if the signature has a variable of that name
	const bool is_any = name.IsBuiltinType(LogicalTypeId::ANY);
	switch (is_any ? TypeNameClass::ANY : classified.name_class) {
	case TypeNameClass::TYPE:
		use.structure = 2;
		return use;
	case TypeNameClass::VALUE_VARIABLE:
		use.uses_value = true;
		return use;
	case TypeNameClass::ANY:
		use.uses_any = true;
		use.unconstrained = true;
		return use;
	case TypeNameClass::TYPE_VARIABLE:
		use.uses_variable = true;
		use.unconstrained = classified.constraint == LogicalTypeId::INVALID;
		use.structure = use.unconstrained ? 0 : 1;
		return use;
	case TypeNameClass::CONSTRUCTED_TYPE:
		break;
	}
	use.structure = 2;
	for (auto &argument : name.GetArguments()) {
		if (!argument.IsType()) {
			use.values += 2;
			continue;
		}
		auto part = GetVariableUse(signature, argument.GetType());
		if (part.uses_value && !argument.GetType().HasArguments()) {
			// a value variable
			use.values += 1;
		}
		use.uses_variable = use.uses_variable || part.uses_variable;
		use.uses_any = use.uses_any || part.uses_any;
		use.uses_value = use.uses_value || part.uses_value;
		use.unconstrained = use.unconstrained || part.unconstrained;
		use.structure += part.structure;
		use.values += part.values;
	}
	return use;
}

//! What an argument says a variable of the signature is: the type a type variable stands for, or the value of a value
//! variable. A NULL or an unresolved parameter says nothing: the type is SQLNULL or UNKNOWN, the value NULL
struct VariableBinding {
	reference<const TypeVariable> variable;
	Value value;
	//! The argument the function is bound with that says it
	idx_t argument;
};

//! What the arguments say the variables are, in the order of the arguments
using VariableBindings = vector<VariableBinding>;

//! How an argument is matched against the type its parameter declares
enum class ArgumentClass : uint8_t {
	//! The parameter declares a type that uses no variable: the argument casts implicitly to it
	DECLARED,
	//! The type of the argument is an instance of the declared type, or casts implicitly to the one made of its parts
	INSTANCE,
	//! A NULL or an unresolved parameter, which says nothing about the variables
	UNTYPED,
	//! The type of the argument is not an instance of a type with arguments: it says nothing about the variables
	//! either, and is passed if it casts implicitly to the type the other arguments determine
	DEFERRED,
	//! The argument cannot be passed
	MISMATCH
};

//! How an argument is passed to the parameter that receives it, as far as the argument alone says
struct ArgumentMatch {
	ArgumentClass argument_class = ArgumentClass::MISMATCH;
	//! How the type of the parameter uses the variables
	VariableUse use;
	//! The type of the argument
	LogicalType argument_type;
	//! The type it is passed as by itself: the declared type, or the instance made of the parts of the argument.
	//! Invalid if that is up to the other arguments
	LogicalType type;
	//! What the implicit cast of the argument costs, negative if it cannot be passed. For a deferred argument it is
	//! known once the variables are substituted
	int64_t cost = 0;
	//! For a parameter that is declared as a value variable: the binding that takes the value of the argument
	optional_idx constant_binding;
};

//! One attempt to match a call against an overload: what is the same for every argument, and what the passes over the
//! arguments build up
class OverloadMatcher {
public:
	OverloadMatcher(TypeCache &cache_p, const FunctionSignature &signature_p, const FunctionCallArguments &call_p,
	                bool explicit_casts_p, optional_ptr<const DecimalWidthLimit> decimal_width_limit_p)
	    : cache(cache_p), signature(signature_p), call(call_p), explicit_casts(explicit_casts_p),
	      decimal_width_limit(decimal_width_limit_p) {
	}

public:
	//! Match the call against the overload: how it binds - or why the call does not match
	Result<FunctionBinding, MatchFailure> Match();

private:
	void AddUntypedBindings(const TypeName &name, const LogicalType &type, idx_t argument);
	Result<FunctionVariables, MatchFailure> InferVariables() const;
	optional<Value> InstantiateArgument(const TypeNameArgument &argument, const Value &parameter, idx_t call_argument);
	optional<LogicalType> InstantiateParameters(const TypeName &name, const LogicalType &type, idx_t call_argument);
	optional<LogicalType> InstantiateType(const TypeName &name, const VariableUse &use, const LogicalType &type,
	                                      idx_t argument);
	ArgumentMatch CollectArgument(const TypeName &name, const LogicalType &arg_type, idx_t argument, bool needs_type);
	optional<MatchFailure> CollectConstants();
	void SubstituteVariables(const FunctionVariables &variables);
	FunctionRank RankArguments() const;
	Result<FunctionBinding, MatchFailure> ResolveArgumentTypes();

private:
	TypeCache &cache;
	const FunctionSignature &signature;
	const FunctionCallArguments &call;
	const bool explicit_casts;
	optional_ptr<const DecimalWidthLimit> decimal_width_limit;
	//! How the arguments are laid out over the parameters
	ArgumentLayout layout;
	//! How every argument of the layout is passed, in the order of the layout
	vector<ArgumentMatch> matches;
	//! What the arguments say the variables are
	VariableBindings bindings;
};

//! An argument that says nothing about the type its parameter declares says nothing about the variables it uses
void OverloadMatcher::AddUntypedBindings(const TypeName &name, const LogicalType &type, idx_t argument) {
	auto classified = ClassifyTypeName(signature, name);
	switch (classified.name_class) {
	case TypeNameClass::TYPE_VARIABLE:
		bindings.push_back({*classified.variable, Value::TYPE(type), argument});
		return;
	case TypeNameClass::VALUE_VARIABLE:
		bindings.push_back({*classified.variable, Value(), argument});
		return;
	case TypeNameClass::TYPE:
	case TypeNameClass::ANY:
		return;
	case TypeNameClass::CONSTRUCTED_TYPE:
		break;
	}
	for (auto &name_argument : name.GetArguments()) {
		if (name_argument.IsType()) {
			AddUntypedBindings(name_argument.GetType(), type, argument);
		}
	}
}

//! The variable a type name uses that no argument determines, if there is one
static optional_ptr<const TypeVariable>
FindUndeterminedVariable(const FunctionSignature &signature, const TypeName &name, const FunctionVariables &variables) {
	auto classified = ClassifyTypeName(signature, name);
	auto &variable_name = name.GetName().GetIdentifierName();
	switch (classified.name_class) {
	case TypeNameClass::TYPE_VARIABLE:
		return variables.types.find(variable_name) == variables.types.end() ? classified.variable : nullptr;
	case TypeNameClass::VALUE_VARIABLE:
		return variables.values.find(variable_name) == variables.values.end() ? classified.variable : nullptr;
	case TypeNameClass::TYPE:
	case TypeNameClass::ANY:
		return nullptr;
	case TypeNameClass::CONSTRUCTED_TYPE:
		break;
	}
	for (auto &argument : name.GetArguments()) {
		if (!argument.IsType()) {
			continue;
		}
		auto variable = FindUndeterminedVariable(signature, argument.GetType(), variables);
		if (variable) {
			return variable;
		}
	}
	return nullptr;
}

//! The type two types are promoted to - without a client context, with the built-in casts only
static bool TryUnifyTypes(optional_ptr<ClientContext> context, const LogicalType &left, const LogicalType &right,
                          LogicalType &result, optional_ptr<const DecimalWidthLimit> decimal_width_limit) {
	if (!context) {
		return LogicalType::DefaultTryGetMaxLogicalTypeUnchecked(left, right, result);
	}
	if (decimal_width_limit) {
		return LogicalType::TryGetMaxLogicalType(*context, left, right, result, *decimal_width_limit);
	}
	return LogicalType::TryGetMaxLogicalType(*context, left, right, result);
}

//! Infer what the variables of a signature are from what the arguments say they are: the types the arguments say a
//! type variable is are promoted to one type, and the values they say a value variable is have to be the same. An
//! argument that says nothing leaves it to the others. Returns why not: the arguments say different things about a
//! variable, or do not determine every variable of the types the call uses
Result<FunctionVariables, MatchFailure> OverloadMatcher::InferVariables() const {
	auto context = cache.GetContext();
	auto &arguments = layout.arguments;
	FunctionVariables variables;
	for (auto &variable : signature.GetVariables()) {
		auto &variable_name = variable.GetName().GetIdentifierName();
		FunctionMismatch::TemplateDeduction conflict;
		conflict.template_name = variable_name;

		if (variable.IsValue()) {
			// the value stays unknown if every argument says nothing
			Value value;
			bool is_said = false;
			for (auto &binding : bindings) {
				if (!RefersToSameObject(binding.variable.get(), variable)) {
					continue;
				}
				is_said = true;
				if (binding.value.IsNull()) {
					continue;
				}
				if (!value.IsNull() && !Value::NotDistinctFrom(value, binding.value)) {
					conflict.argument = arguments[binding.argument].argument_index;
					conflict.inferred_values = {value, binding.value};
					return Err(MatchFailure {0, std::move(conflict)});
				}
				value = binding.value;
			}
			if (is_said) {
				variables.values[variable_name] = std::move(value);
			}
			continue;
		}

		// The arguments say what the type is in their order: the first one sets it, and every other type is promoted
		// with it. A NULL or an unresolved parameter takes the type that is there already
		auto constraint = signature.GetConstraint(variable.GetName());
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
			const bool says_nothing =
			    said_type.id() == LogicalTypeId::SQLNULL || said_type.id() == LogicalTypeId::UNKNOWN;
			// the first argument that says a type sets it, instead of being promoted with the ones that said nothing
			const bool replaces_untyped = SKIP_LEADING_UNTYPED_ARGUMENT && !says_nothing &&
			                              (type.id() == LogicalTypeId::SQLNULL || type.id() == LogicalTypeId::UNKNOWN);
			bool is_unified = true;
			if (!is_said || replaces_untyped) {
				first_type = said_type;
				type = said_type;
				is_said = true;
			} else if (says_nothing || said_type.EqualsIncludingCollation(type)) {
				continue;
			} else {
				LogicalType promoted;
				is_unified = TryUnifyTypes(context, type, said_type, promoted, decimal_width_limit);
				if (is_unified && !promoted.EqualsIncludingCollation(type)) {
					promotions.push_back(said_type);
					promotions.push_back(promoted);
					type = std::move(promoted);
				}
			}
			// arguments of the kind can still unify to a type that is not of it, e.g. two enums to VARCHAR. A NULL
			// or an unresolved parameter is of any kind
			const bool is_untyped = type.id() == LogicalTypeId::SQLNULL || type.id() == LogicalTypeId::UNKNOWN;
			if (is_unified && (constraint == LogicalTypeId::INVALID || is_untyped ||
			                   CoerceToKind(LogicalType::NormalizeType(type), constraint))) {
				continue;
			}
			conflict.argument = arguments[binding.argument].argument_index;
			conflict.inferred_types.push_back(std::move(first_type));
			conflict.inferred_types.insert(conflict.inferred_types.end(), promotions.begin(), promotions.end());
			if (!is_unified) {
				conflict.inferred_types.push_back(std::move(said_type));
			}
			return Err(MatchFailure {0, std::move(conflict)});
		}
		if (!is_said) {
			continue;
		}
		// the type is final: normalized - a variable only unresolved parameters are passed as stays unknown - and
		// coerced to the kind, e.g. the DECIMAL that integer arguments are passed as
		if (type.id() != LogicalTypeId::UNKNOWN) {
			type = LogicalType::NormalizeType(type);
		}
		optional<LogicalType> coerced_type;
		if (constraint != LogicalTypeId::INVALID) {
			coerced_type = CoerceToKind(type, constraint);
		}
		variables.types[variable_name] = coerced_type ? std::move(*coerced_type) : std::move(type);
	}

	// every variable of the types the call uses has to be determined by the arguments and defaults: the types of the
	// parameters that receive an argument - a variadic one may receive none - and the return type
	optional_ptr<const TypeVariable> undetermined;
	for (idx_t i = 0; i < arguments.size() && !undetermined; i++) {
		auto &name = signature.GetParameter(arguments[i].parameter_index).GetTypeName();
		undetermined = FindUndeterminedVariable(signature, name, variables);
	}
	if (!undetermined && signature.GetReturnType().IsType()) {
		undetermined = FindUndeterminedVariable(signature, signature.GetReturnType().GetTypeName(), variables);
	}
	if (undetermined) {
		return Err(MatchFailure {0, FunctionMismatch::TemplateUnresolved {undetermined->GetName().GetIdentifierName(),
		                                                                  undetermined->IsValue()}});
	}
	return std::move(variables);
}

//! The parameter of a type that an argument of a type name is paired with: the one of its label if both have a name,
//! else the one at its position
static optional_idx FindTypeParameter(const LogicalType &type, const vector<TypeNameArgument> &arguments,
                                      idx_t argument_index, bool has_labels) {
	const auto parameter_count = type.GetParameterCount();
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

//! What an argument of a type name stands for when it is the parameter of a type it is paired with: a type name the
//! type it stands for, a value variable the value, and a literal itself. Nothing if the parameter is not one the
//! argument stands for
optional<Value> OverloadMatcher::InstantiateArgument(const TypeNameArgument &argument, const Value &parameter,
                                                     idx_t call_argument) {
	const bool is_type = parameter.type().id() == LogicalTypeId::TYPE;
	auto value_variable = FindValueVariable(argument, signature);
	const bool names_type = argument.IsType() && !value_variable;
	if (is_type != names_type) {
		// a type where the name has a value, or a value where it has a type
		return nullopt;
	}
	if (!argument.IsType()) {
		// a value the name declares, e.g. the size of an array
		return argument.GetLiteral().ToValue();
	}
	auto &name = argument.GetType();
	if (is_type) {
		auto type =
		    InstantiateType(name, GetVariableUse(signature, name), TypeValue::GetType(parameter), call_argument);
		if (!type) {
			return nullopt;
		}
		return Value::TYPE(std::move(*type));
	}
	// the value of a value variable
	auto value = parameter.DefaultTryCastAs(ValueVariableType(*value_variable));
	if (value && !value->IsNull()) {
		bindings.push_back({*value_variable, *value, call_argument});
	}
	return value;
}

//! Whether what an argument of a type name stands for is the parameter of the type it is paired with
static bool IsSameParameter(const Value &value, const Value &parameter) {
	if (parameter.type().id() == LogicalTypeId::TYPE) {
		return TypeValue::GetType(value) == TypeValue::GetType(parameter);
	}
	// the same value, as values of the type of the parameter
	auto cast_value = value.DefaultTryCastAs(parameter.type());
	return cast_value && Value::NotDistinctFrom(*cast_value, parameter);
}

//! InstantiateType for a type name that has arguments, e.g. T[N]: every argument of the name is the parameter of
//! the type it is paired with
optional<LogicalType> OverloadMatcher::InstantiateParameters(const TypeName &name, const LogicalType &type,
                                                             idx_t call_argument) {
	auto &arguments = name.GetArguments();
	const auto kind = DefaultTypeGenerator::GetDefaultType(name.GetName());
	auto coerced_type = CoerceToKind(type, kind);
	if (!coerced_type && kind == LogicalTypeId::DECIMAL) {
		// only a number is passed as a decimal
		return nullopt;
	}
	// for any other kind a type that is not passed as it is paired as it is, e.g. a list passed as an array
	auto &instance = coerced_type ? *coerced_type : type;
	// whether the type is of the kind the name constructs, instead of being passed as a type of it
	const bool is_kind = kind == LogicalTypeId::INVALID ? name.GetName() == type.GetAlias() : type.id() == kind;
	bool has_labels = false;
	for (auto &argument : arguments) {
		has_labels = has_labels || !argument.GetLabel().empty();
	}
	// Whether the type is what the name stands for already: of its kind, with every parameter what the name says it
	// is. It is then taken as it is - a type cannot always be constructed again from its parameters, e.g. a geometry
	// of a coordinate system that is not known here
	bool is_instance =
	    instance.id() == kind && instance.GetAlias().empty() && instance.GetParameterCount() == arguments.size();
	vector<TypeArgument> bound_arguments;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument = arguments[i];
		auto parameter_idx = FindTypeParameter(instance, arguments, i, has_labels);
		optional<Value> value;
		bool is_same = false;
		if (parameter_idx.IsValid()) {
			auto parameter = instance.GetParameter(parameter_idx.GetIndex());
			value = InstantiateArgument(argument, parameter, call_argument);
			const bool is_parameter = value && IsSameParameter(*value, parameter);
			if (is_kind && !argument.IsType() && !is_parameter) {
				// a value the name declares has to be the one a type of its kind has
				return nullopt;
			}
			is_same = is_parameter && parameter_idx.GetIndex() == i;
		} else if (!argument.IsType()) {
			// what the type does not say has to be declared, e.g. the size of the array a list is passed as
			value = argument.GetLiteral().ToValue();
		}
		if (!value) {
			return nullopt;
		}
		is_instance = is_instance && is_same;
		bound_arguments.emplace_back(argument.GetLabel().GetIdentifierName(), std::move(*value));
	}
	if (is_instance) {
		return instance;
	}
	LogicalType result;
	try {
		result = cache.Construct(name.GetName(), std::move(bound_arguments));
	} catch (const BinderException &) {
		// the parts of the type are not what the constructor of the name takes
		return nullopt;
	}
	if (result.id() == LogicalTypeId::INVALID) {
		return nullopt;
	}
	return result;
}

//! The type a type name stands for when its variables are what a type has in their place - the type of the argument
//! that is passed as it. What the type says the variables are is added to "bindings". Nothing if the type is not one
//! the name stands for. Whether the type can be passed as the result is up to the cast rules
optional<LogicalType> OverloadMatcher::InstantiateType(const TypeName &name, const VariableUse &use,
                                                       const LogicalType &type, idx_t argument) {
	if (!use.IsGeneric()) {
		return BindDeclaredType(cache, name, signature);
	}
	if (type.id() == LogicalTypeId::UNKNOWN || type.id() == LogicalTypeId::SQLNULL) {
		// The argument says nothing about the type, so the variables it would determine are UNKNOWN or SQLNULL,
		// unless another argument determines them. Nested functions propagate these types: the binder replaces
		// a function that returns one by a NULL, or binds it again once the parameter is resolved
		AddUntypedBindings(name, type, argument);
		return type;
	}
	auto classified = ClassifyTypeName(signature, name);
	if (classified.name_class == TypeNameClass::CONSTRUCTED_TYPE) {
		return InstantiateParameters(name, type, argument);
	}
	if (classified.name_class == TypeNameClass::VALUE_VARIABLE) {
		// a parameter that takes the value of the variable
		return ValueVariableType(*classified.variable);
	}
	if (name.IsBuiltinType(LogicalTypeId::ANY)) {
		// a type variable of its own, that is the type of the argument
		return LogicalType::NormalizeType(type);
	}
	// a type variable: of the kind it stands for, if it stands for one - a type with an alias is a type of its own
	optional<LogicalType> result = LogicalType::NormalizeType(type);
	if (classified.constraint != LogicalTypeId::INVALID) {
		result = type.GetAlias().empty() ? CoerceToKind(type, classified.constraint) : nullopt;
	}
	if (result) {
		bindings.push_back({*classified.variable, Value::TYPE(type), argument});
	}
	return result;
}

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

//! Collect how an argument is passed to a parameter of the given type name, and what it says the variables are, which
//! is added to "bindings" as said by the argument at the given index. This is about the argument alone: what it costs
//! is the implicit cast to the type of the parameter with its own parts, as the cast to the type the arguments
//! determine a variable to be is explicit - only an argument that is deferred is left to them. "needs_type" is
//! whether the argument is passed even if it does not match
ArgumentMatch OverloadMatcher::CollectArgument(const TypeName &name, const LogicalType &arg_type, idx_t argument,
                                               bool needs_type) {
	auto context = cache.GetContext();
	ArgumentMatch match;
	match.argument_type = arg_type;
	match.use = GetVariableUse(signature, name);
	auto &use = match.use;
	if (!use.IsGeneric()) {
		match.type = BindDeclaredType(cache, name, signature);
		match.cost = ImplicitCastCost(context, arg_type, match.type);
		match.argument_class = match.cost < 0 ? ArgumentClass::MISMATCH : ArgumentClass::DECLARED;
		return match;
	}

	auto classified = ClassifyTypeName(signature, name);
	const bool is_untyped = arg_type.id() == LogicalTypeId::SQLNULL || arg_type.id() == LogicalTypeId::UNKNOWN;
	const auto first_binding = bindings.size();
	auto own_type = InstantiateType(name, use, arg_type, argument);
	if (!own_type) {
		match.cost = -1;
	} else if (arg_type.id() == LogicalTypeId::SQLNULL) {
		// a NULL says nothing about the type: it costs what it costs as the kind of type the parameter stands for,
		// which is any type for an unconstrained type variable
		LogicalType kind;
		if (classified.name_class == TypeNameClass::CONSTRUCTED_TYPE) {
			kind = LogicalType(DefaultTypeGenerator::GetDefaultType(name.GetName()));
		} else if (classified.name_class == TypeNameClass::VALUE_VARIABLE) {
			kind = ValueVariableType(*classified.variable);
		} else if (use.unconstrained) {
			kind = LogicalType::ANY;
		} else {
			kind = LogicalType(classified.constraint);
		}
		match.cost = ImplicitCastCost(context, arg_type, kind);
	} else if (*own_type != arg_type) {
		match.cost = ImplicitCastCost(context, arg_type, *own_type);
	}
	if (match.cost >= 0) {
		match.argument_class = is_untyped ? ArgumentClass::UNTYPED : ArgumentClass::INSTANCE;
		match.type = std::move(*own_type);
	} else {
		// Not an instance of the type of the parameter, so it says nothing about the variables. It is deferred to a
		// type with arguments, which the other arguments can determine - unless it uses ANY, which is the type of
		// the argument itself. Nothing is deferred to a variable by itself, which its own argument determines
		while (bindings.size() > first_binding) {
			bindings.pop_back();
		}
		const bool is_deferrable = name.HasArguments() && use.UsesVariable() && !use.uses_any;
		match.argument_class = is_deferrable ? ArgumentClass::DEFERRED : ArgumentClass::MISMATCH;
		match.cost = is_deferrable ? 0 : -1;
		if (!is_deferrable && needs_type && !use.UsesVariable()) {
			// cast explicitly, it is cast to the type with ANY as the argument itself
			const auto any_type = LogicalType::NormalizeType(arg_type);
			match.type = BindSignatureType(cache, name, signature, FunctionVariables(), any_type);
		}
	}
	if (classified.name_class == TypeNameClass::VALUE_VARIABLE && !is_untyped) {
		// a parameter that is declared as a value variable takes the value of its argument, see CollectConstants
		match.constant_binding = bindings.size();
		bindings.push_back({*classified.variable, Value(), argument});
	}
	return match;
}

//! Collect what the values of the arguments say the value variables are: a parameter that is declared as a value
//! variable takes the value of its argument. The binding it fills is where the argument is in the order of the
//! arguments. Returns why not: an argument is not a constant, or not a value of the variable
optional<MatchFailure> OverloadMatcher::CollectConstants() {
	auto context = cache.GetContext();
	auto &arguments = layout.arguments;
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (!matches[i].constant_binding.IsValid()) {
			continue;
		}
		auto &argument = arguments[i];
		auto &param = signature.GetParameter(argument.parameter_index);
		auto &binding = bindings[matches[i].constant_binding.GetIndex()];
		auto arg_idx = argument.argument_index;
		auto constant = arg_idx.IsValid() ? GetArgumentConstant(context, call.GetArgument(arg_idx.GetIndex()))
		                                  : optional<Value>(*param.GetDefaultValue());
		auto value = GetVariableValue(binding.variable.get(), std::move(constant), arg_idx, argument.parameter_index);
		if (!value) {
			return std::move(value.GetError());
		}
		binding.value = std::move(*value);
	}
	return nullopt;
}

//! Substitute the variables: every argument is passed as the type of its parameter with the variables as the
//! arguments determine them. An argument that is passed as ANY, or as a type of values only, stays its own
void OverloadMatcher::SubstituteVariables(const FunctionVariables &variables) {
	auto &arguments = layout.arguments;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument = arguments[i];
		auto &match = matches[i];
		auto &use = match.use;
		if (!use.UsesVariable()) {
			argument.type = match.type;
			continue;
		}
		const bool is_instance = match.argument_class == ArgumentClass::INSTANCE;
		if (is_instance && !use.uses_variable) {
			argument.type = match.type;
			continue;
		}
		// ANY is not used next to a type variable, so it stands for an argument that says nothing about the type
		D_ASSERT(!is_instance || !use.uses_any);
		auto &name = signature.GetParameter(argument.parameter_index).GetTypeName();
		auto &arg_type = match.argument_type;
		auto type = BindSignatureType(cache, name, signature, variables, arg_type);
		const bool is_untyped = arg_type.id() == LogicalTypeId::UNKNOWN || arg_type.id() == LogicalTypeId::SQLNULL;
		if (!argument.IsDefaultArgument() && is_untyped && (use.uses_any || !type.IsComplete())) {
			// a NULL or an unresolved parameter is passed as it is where nothing determines the type of the
			// parameter: as ANY, and e.g. as a DECIMAL of any width
			argument.type = arg_type;
			continue;
		}
		argument.type = std::move(type);
	}
}

//! Rank how well the arguments are passed, see FunctionRank. A default is not ranked
FunctionRank OverloadMatcher::RankArguments() const {
	auto &arguments = layout.arguments;
	FunctionRank rank;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &match = matches[i];
		if (arguments[i].IsDefaultArgument() || match.argument_class == ArgumentClass::MISMATCH) {
			continue;
		}
		auto &use = match.use;
		rank.legacy_casts += match.cost >= OLD_IMPLICIT_CAST_COST ? 1 : 0;
		// a NULL is not passed through an unconstrained type variable: it says nothing about the type
		rank.unconstrained_arguments += use.unconstrained && match.argument_type.id() != LogicalTypeId::SQLNULL ? 1 : 0;
		rank.cast_cost += static_cast<idx_t>(match.cost);
		if (use.uses_variable || use.uses_any) {
			rank.variable_parameters++;
			rank.declared_structure += use.structure;
		}
		rank.declared_values += use.values;
	}
	return rank;
}

//===--------------------------------------------------------------------===//
// Matching
//===--------------------------------------------------------------------===//
//! Lay the arguments of a call out over the parameters of an overload. This is about the shape of the call only, not
//! about types: a positional argument is passed to the parameter at its position or to "*args", a named one to the
//! parameter of its name or to "**kwargs", and a parameter the call leaves out is filled by its default. Returns the
//! first reason the call does not fit: a wrong name, which often causes the counts to be off as well, then too many
//! arguments, then a parameter that is left out
static Result<ArgumentLayout, MatchFailure> LayoutArguments(const FunctionSignature &sig,
                                                            const FunctionCallArguments &call) {
	const auto parameter_count = sig.GetParameterCount();
	const auto positional_parameter_count = sig.GetPositionalParameterCount();
	const auto args_idx = sig.GetArgsIndex();
	const auto kwargs_idx = sig.GetKwargsIndex();
	const auto argument_count = call.GetArgumentCount();
	const auto positional_count = call.GetPositionalCount();
	// the wrong shape is further away than arguments that cannot be cast, of which there are at most this many
	const auto distance = argument_count + 1;

	// the parameter every named argument is passed to
	vector<optional_idx> named_parameters;
	for (idx_t i = positional_count; i < argument_count; i++) {
		auto param_idx = sig.GetParameterIndexByName(call.GetArgument(i).name);
		if (!param_idx.IsValid()) {
			if (!kwargs_idx.IsValid()) {
				return Err(MatchFailure {distance, FunctionMismatch::UnknownNamedArgument {i}});
			}
			param_idx = kwargs_idx;
		} else if (param_idx.GetIndex() < positional_count &&
		           sig.GetParameter(param_idx.GetIndex()).AcceptsPosition()) {
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
		auto &param = sig.GetParameter(param_idx);
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

//! Determine the types the arguments of a function are passed as, and how well the call matches (see FunctionRank),
//! in passes over the arguments: collect what each one says, infer the variables, substitute them, and rank. The type
//! of every argument of the layout is replaced by the type it is passed as. Returns the binding - or why the
//! arguments cannot be passed
Result<FunctionBinding, MatchFailure> OverloadMatcher::ResolveArgumentTypes() {
	auto &arguments = layout.arguments;

	// collect: match every argument against the type its parameter declares
	matches.reserve(arguments.size());
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument = arguments[i];
		auto &name = signature.GetParameter(argument.parameter_index).GetTypeName();
		// an argument that does not match is still passed if it is cast explicitly, or is a default
		const bool needs_type = explicit_casts || argument.IsDefaultArgument();
		matches.push_back(CollectArgument(name, argument.type, i, needs_type));
	}
	// The first argument that cannot be passed is reported, and the more there are the further the call is from
	// matching. A default is not cast, and neither is an unresolved parameter. If the call does not match, an
	// argument that is deferred reads as one that cannot be passed - other than a string literal, which casts to
	// any type
	bool has_unknown_argument = false;
	bool has_mismatch = false;
	optional<FunctionMismatch::Reason> mismatch;
	idx_t mismatch_count = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument = arguments[i];
		auto &match = matches[i];
		if (argument.IsDefaultArgument()) {
			continue;
		}
		const auto source = argument.argument_index.GetIndex();
		if (match.argument_type.id() == LogicalTypeId::UNKNOWN && source < call.GetPositionalCount()) {
			has_unknown_argument = true;
			continue;
		}
		const bool is_mismatch = match.argument_class == ArgumentClass::MISMATCH;
		const bool is_string_literal = match.argument_type.id() == LogicalTypeId::STRING_LITERAL;
		const bool is_deferred = match.argument_class == ArgumentClass::DEFERRED && !is_string_literal;
		if (explicit_casts || (!is_mismatch && !is_deferred)) {
			continue;
		}
		if (!mismatch) {
			// a parameter that uses a variable reads as it is declared
			const bool is_generic = match.use.IsGeneric();
			mismatch = FunctionMismatch::IncompatibleArgument {source, argument.parameter_index,
			                                                   is_generic ? LogicalType() : match.type, is_generic};
		}
		mismatch_count++;
		has_mismatch = has_mismatch || is_mismatch;
	}
	if (has_mismatch) {
		return Err(MatchFailure {mismatch_count, std::move(*mismatch)});
	}

	auto context = cache.GetContext();
	FunctionVariables variables;
	if (!signature.GetVariables().empty()) {
		// the types of the arguments said what the variables are as they were collected, their values do now
		auto failure = CollectConstants();
		if (!failure) {
			// infer: what every variable is
			auto inferred = InferVariables();
			if (inferred) {
				variables = std::move(*inferred);
			} else {
				failure = std::move(inferred.GetError());
			}
		}
		if (failure && mismatch) {
			return Err(MatchFailure {mismatch_count, std::move(*mismatch)});
		}
		if (failure) {
			return Err(std::move(*failure));
		}
	}
	// substitute: the arguments are passed as the types of their parameters with the variables
	SubstituteVariables(variables);

	// a deferred argument has to cast implicitly to the type it is passed as, which is known now
	bool is_passed = true;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument = arguments[i];
		auto &match = matches[i];
		if (match.argument_class != ArgumentClass::DEFERRED || explicit_casts || argument.IsDefaultArgument()) {
			continue;
		}
		match.cost = ImplicitCastCost(context, match.argument_type, argument.type);
		if (match.cost >= 0) {
			continue;
		}
		const bool is_string_literal = match.argument_type.id() == LogicalTypeId::STRING_LITERAL;
		if (is_string_literal && !argument.type.IsComplete()) {
			// the type is not determined yet, e.g. by an unresolved parameter: a string literal casts to any type
			match.cost = 0;
			continue;
		}
		is_passed = false;
		if (is_string_literal) {
			if (!mismatch) {
				mismatch = FunctionMismatch::IncompatibleArgument {argument.argument_index.GetIndex(),
				                                                   argument.parameter_index, LogicalType(), true};
			}
			mismatch_count++;
		}
	}
	if (!is_passed) {
		return Err(MatchFailure {mismatch_count, std::move(*mismatch)});
	}

	FunctionBinding binding;
	// rank: nothing is ranked when the arguments are cast explicitly, and with a parameter of an unknown type every
	// overload ranks the same
	if (!explicit_casts && !has_unknown_argument) {
		binding.rank = RankArguments();
	}
	binding.variables = std::move(variables);
	binding.layout = std::move(layout);
	return std::move(binding);
}

Result<FunctionBinding, MatchFailure> OverloadMatcher::Match() {
	auto result = LayoutArguments(signature, call);
	if (!result) {
		return Err(std::move(result.GetError()));
	}
	layout = std::move(*result);
	return ResolveArgumentTypes();
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
	return BindTypeNameInternal(cache, return_type.GetTypeName(), signature, binding.variables);
}

} // namespace duckdb
