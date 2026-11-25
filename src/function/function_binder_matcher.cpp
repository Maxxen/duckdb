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

//! The type of the kind of the constraint that an argument of the given type is passed as, e.g. DECIMAL(10,0) for an
//! INTEGER passed as a DECIMAL - nullopt if the argument determines none
static optional<LogicalType> ConstrainType(LogicalTypeId constraint, const LogicalType &type) {
	if (type.id() == constraint) {
		return type;
	}
	const bool takes_struct = constraint == LogicalTypeId::STRUCT || constraint == LogicalTypeId::TUPLE;
	const bool is_struct = type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::TUPLE;
	if (takes_struct && is_struct) {
		// a struct with and without names are of one kind: which of the two a function takes is its to check
		return type;
	}
	uint32_t width, scale;
	if (constraint == LogicalTypeId::DECIMAL && type.id() != LogicalTypeId::SQLNULL &&
	    type.GetDecimalProperties(width, scale)) {
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

//===--------------------------------------------------------------------===//
// Binding type names
//===--------------------------------------------------------------------===//
//! The value variable of the signature a type name is, if it is one
static optional_ptr<const TypeVariable> FindValueVariable(const TypeName &name, const FunctionSignature &signature) {
	if (name.HasArguments()) {
		return nullptr;
	}
	auto variable = signature.FindVariable(name.GetName());
	return variable && variable->IsValue() ? variable : nullptr;
}

//! The value variable of the signature an argument of a type name refers to, if it refers to one
static optional_ptr<const TypeVariable> FindValueVariable(const TypeNameArgument &argument,
                                                          const FunctionSignature &signature) {
	return argument.IsType() ? FindValueVariable(argument.GetType(), signature) : nullptr;
}

//! The type of the values a value variable stands for
static LogicalType ValueVariableType(const TypeVariable &variable) {
	return variable.GetKind() == TypeVariable::Kind::INTEGER ? LogicalType::BIGINT : LogicalType::VARCHAR;
}

//! Binds a type name. What a variable of the signature stands for is up to the caller: bind_variable gives the
//! type of a type variable, bind_value the value of a value variable - NULL if it is not known - and any_type is
//! what ANY stands for
template <class BIND_VARIABLE, class BIND_VALUE>
static LogicalType BindTypeNameInternal(TypeCache &cache, const TypeName &name, const FunctionSignature &signature,
                                        const BIND_VARIABLE &bind_variable, const BIND_VALUE &bind_value,
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
	if (arguments.empty()) {
		auto variable = signature.FindVariable(identifier);
		if (variable) {
			if (variable->IsValue()) {
				// a parameter that takes the value of the variable
				return ValueVariableType(*variable);
			}
			return bind_variable(identifier, signature.GetConstraint(identifier));
		}
	}

	vector<TypeArgument> bound_arguments;
	bool has_unknown_value = false;
	for (auto &argument : arguments) {
		Value value;
		if (FindValueVariable(argument, signature)) {
			value = bind_value(argument.GetType().GetName());
			has_unknown_value = has_unknown_value || value.IsNull();
		} else if (argument.IsType()) {
			value = Value::TYPE(
			    BindTypeNameInternal(cache, argument.GetType(), signature, bind_variable, bind_value, any_type));
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
	return BindTypeNameInternal(
	    cache, name, signature,
	    [](const Identifier &variable, LogicalTypeId) -> LogicalType {
		    throw InternalException("Type variable %s in a type that uses none", variable.GetIdentifierName());
	    },
	    [](const Identifier &variable) -> Value {
		    throw InternalException("Value variable %s in a type that uses none", variable.GetIdentifierName());
	    });
}

//! The type a call determines a type variable to be
static LogicalType GetVariableType(const Identifier &variable, LogicalTypeId constraint,
                                   const FunctionVariables &variables) {
	auto entry = variables.types.find(variable.GetIdentifierName());
	if (entry == variables.types.end()) {
		throw BinderException("Cannot deduce type variable '%s', it is not determined by the arguments",
		                      variable.GetIdentifierName());
	}
	// a variable only unresolved parameters are passed as stays unknown
	auto &inferred_type = entry->second;
	auto type =
	    inferred_type.id() == LogicalTypeId::UNKNOWN ? inferred_type : LogicalType::NormalizeType(inferred_type);
	if (constraint == LogicalTypeId::INVALID) {
		return type;
	}
	// e.g. the DECIMAL that integer arguments are passed as
	auto constrained = ConstrainType(constraint, type);
	return constrained ? *constrained : type;
}

LogicalType FunctionBinder::BindTypeName(optional_ptr<ClientContext> context, const TypeName &name) {
	TypeCache cache(context);
	return BindDeclaredType(cache, name, FunctionSignature());
}

//! The type a type name of a signature stands for, given what a call determines its variables to be
static LogicalType BindSignatureType(TypeCache &cache, const TypeName &name, const FunctionSignature &signature,
                                     const FunctionVariables &variables,
                                     const LogicalType &any_type = LogicalType::ANY) {
	return BindTypeNameInternal(
	    cache, name, signature,
	    [&](const Identifier &variable, LogicalTypeId constraint) {
		    return GetVariableType(variable, constraint, variables);
	    },
	    [&](const Identifier &variable) {
		    auto entry = variables.values.find(variable.GetIdentifierName());
		    if (entry == variables.values.end()) {
			    throw BinderException("Cannot deduce value '%s', it is not determined by the arguments",
			                          variable.GetIdentifierName());
		    }
		    return entry->second;
	    },
	    any_type);
}

//===--------------------------------------------------------------------===//
// Type variables
//===--------------------------------------------------------------------===//
//! What it costs to pass an argument as a type variable that stands for any type, on top of the cast to the type the
//! variable is determined to be: more than any cast, so that an overload that declares the type is preferred
static constexpr idx_t TYPE_VARIABLE_COST = 1000000;
//! A NULL that is passed as a type variable that stands for any type: preferred over a NULL of a declared type
static constexpr idx_t NULL_VARIABLE_COST = 5;
//! A string literal that is passed as a type made of parts the other arguments determine: what the cast rules cost
//! a string literal to a type that is not VARCHAR
static constexpr idx_t STRING_LITERAL_COST = 20;

//! How a type name uses the type variables of its signature
struct VariableUse {
	//! Whether it uses a type variable
	bool uses_variable = false;
	//! Whether it uses ANY: a type variable of its own each time it occurs, that stands for any type
	bool uses_any = false;
	//! Whether it uses a type variable that stands for any type, and how deep the outermost such use is nested
	bool unconstrained = false;
	idx_t depth = 0;
	//! Whether it uses a value variable
	bool uses_value = false;

	//! Whether it uses a variable the signature declares
	bool UsesVariable() const {
		return uses_variable || uses_value;
	}
	//! Whether the type it stands for depends on the call
	bool IsGeneric() const {
		return uses_variable || uses_any || uses_value;
	}
};

static VariableUse GetVariableUse(const FunctionSignature &signature, const TypeName &name, idx_t depth = 0) {
	VariableUse use;
	if (!name.HasArguments()) {
		const bool is_any = name.IsBuiltinType(LogicalTypeId::ANY);
		auto variable = is_any ? nullptr : signature.FindVariable(name.GetName());
		if (!variable && !is_any) {
			return use;
		}
		if (variable && variable->IsValue()) {
			use.uses_value = true;
			return use;
		}
		use.uses_any = is_any;
		use.uses_variable = !is_any;
		if (is_any || signature.GetConstraint(name.GetName()) == LogicalTypeId::INVALID) {
			use.unconstrained = true;
			use.depth = depth;
		}
		return use;
	}
	for (auto &argument : name.GetArguments()) {
		if (!argument.IsType()) {
			continue;
		}
		auto part = GetVariableUse(signature, argument.GetType(), depth + 1);
		use.uses_variable = use.uses_variable || part.uses_variable;
		use.uses_any = use.uses_any || part.uses_any;
		use.uses_value = use.uses_value || part.uses_value;
		if (part.unconstrained && (!use.unconstrained || part.depth < use.depth)) {
			use.unconstrained = true;
			use.depth = part.depth;
		}
	}
	return use;
}

//! What an argument says a variable of the signature is: the type a type variable stands for, or the value of a value
//! variable. A NULL or an unresolved parameter says nothing: the type is SQLNULL or UNKNOWN, the value NULL
struct VariableBinding {
	Value value;
	//! The argument the function is bound with that says it
	idx_t argument;
};

//! What the arguments say every variable is, in the order of the arguments
using VariableBindings = case_insensitive_map_t<vector<VariableBinding>>;

//! An argument that says nothing about the type its parameter declares says nothing about the variables it uses
static void AddUntypedBindings(const FunctionSignature &signature, const TypeName &name, const LogicalType &type,
                               idx_t argument, VariableBindings &bindings) {
	if (!name.HasArguments()) {
		auto variable = signature.FindVariable(name.GetName());
		if (variable) {
			bindings[name.GetName().GetIdentifierName()].push_back(
			    {variable->IsValue() ? Value() : Value::TYPE(type), argument});
		}
		return;
	}
	for (auto &name_argument : name.GetArguments()) {
		if (name_argument.IsType()) {
			AddUntypedBindings(signature, name_argument.GetType(), type, argument, bindings);
		}
	}
}

//! The variable a type name uses that no argument determines, if there is one
static optional_ptr<const TypeVariable>
FindUndeterminedVariable(const FunctionSignature &signature, const TypeName &name, const FunctionVariables &variables) {
	if (!name.HasArguments()) {
		auto variable = signature.FindVariable(name.GetName());
		auto &variable_name = name.GetName().GetIdentifierName();
		if (!variable) {
			return nullptr;
		}
		const bool determined = variable->IsValue() ? variables.values.find(variable_name) != variables.values.end()
		                                            : variables.types.find(variable_name) != variables.types.end();
		return determined ? nullptr : variable;
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

//! Unify what the arguments say the variables of a signature are: the types the arguments say a type variable is are
//! promoted to one type, and the values they say a value variable is have to be the same. An argument that says
//! nothing leaves it to the others. Returns why not: the arguments say different things about a variable, or do not
//! determine every variable of the types the call uses
static Result<FunctionVariables, FunctionMismatch::Reason>
UnifyBindings(optional_ptr<ClientContext> context, const FunctionSignature &signature, const VariableBindings &bindings,
              const vector<BoundArgument> &arguments, optional_ptr<const DecimalWidthLimit> decimal_width_limit) {
	FunctionVariables variables;
	for (auto &variable : signature.GetVariables()) {
		auto &variable_name = variable.GetName().GetIdentifierName();
		auto entry = bindings.find(variable_name);
		if (entry == bindings.end() || entry->second.empty()) {
			continue;
		}
		auto &history = entry->second;
		FunctionMismatch::TemplateDeduction conflict;
		conflict.template_name = variable_name;

		if (variable.IsValue()) {
			// the value stays unknown if every argument says nothing
			Value value;
			for (auto &binding : history) {
				if (binding.value.IsNull()) {
					continue;
				}
				if (!value.IsNull() && !Value::NotDistinctFrom(value, binding.value)) {
					conflict.argument = arguments[binding.argument].argument_index;
					conflict.inferred_values = {value, binding.value};
					return Err(FunctionMismatch::Reason(std::move(conflict)));
				}
				value = binding.value;
			}
			variables.values[variable_name] = std::move(value);
			continue;
		}

		// The arguments say what the type is in their order: the first one sets it, and every other type is promoted
		// with it. A NULL or an unresolved parameter takes the type that is there already
		auto constraint = signature.GetConstraint(variable.GetName());
		LogicalType type;
		// what a conflict lists: the first type, then every type that changed it and what it was promoted to with it
		LogicalType first_type;
		vector<LogicalType> promotions;
		for (auto &binding : history) {
			auto said_type = TypeValue::GetType(binding.value);
			const bool says_nothing =
			    said_type.id() == LogicalTypeId::SQLNULL || said_type.id() == LogicalTypeId::UNKNOWN;
			bool is_unified = true;
			if (type.id() == LogicalTypeId::INVALID) {
				first_type = said_type;
				type = said_type;
			} else if (says_nothing || said_type == type) {
				continue;
			} else {
				LogicalType promoted;
				is_unified = TryUnifyTypes(context, type, said_type, promoted, decimal_width_limit);
				if (is_unified && promoted != type) {
					promotions.push_back(said_type);
					promotions.push_back(promoted);
					type = std::move(promoted);
				}
			}
			// arguments of the kind can still unify to a type that is not of it, e.g. two enums to VARCHAR. A NULL
			// or an unresolved parameter is of any kind
			const bool is_untyped = type.id() == LogicalTypeId::SQLNULL || type.id() == LogicalTypeId::UNKNOWN;
			if (is_unified && (constraint == LogicalTypeId::INVALID || is_untyped ||
			                   ConstrainType(constraint, LogicalType::NormalizeType(type)))) {
				continue;
			}
			conflict.argument = arguments[binding.argument].argument_index;
			conflict.inferred_types.push_back(std::move(first_type));
			conflict.inferred_types.insert(conflict.inferred_types.end(), promotions.begin(), promotions.end());
			if (!is_unified) {
				conflict.inferred_types.push_back(std::move(said_type));
			}
			return Err(FunctionMismatch::Reason(std::move(conflict)));
		}
		variables.types[variable_name] = std::move(type);
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
		return Err(FunctionMismatch::Reason(FunctionMismatch::TemplateUnresolved {
		    undetermined->GetName().GetIdentifierName(), undetermined->IsValue()}));
	}
	return std::move(variables);
}

static optional<LogicalType> InstantiateType(TypeCache &cache, const FunctionSignature &signature, const TypeName &name,
                                             const VariableUse &use, const LogicalType &type, idx_t argument,
                                             VariableBindings &bindings);

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
static optional<Value> InstantiateArgument(TypeCache &cache, const FunctionSignature &signature,
                                           const TypeNameArgument &argument, const Value &parameter,
                                           idx_t call_argument, VariableBindings &bindings) {
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
		auto type = InstantiateType(cache, signature, name, GetVariableUse(signature, name),
		                            TypeValue::GetType(parameter), call_argument, bindings);
		if (!type) {
			return nullopt;
		}
		return Value::TYPE(std::move(*type));
	}
	// the value of a value variable
	auto value = parameter.DefaultTryCastAs(ValueVariableType(*value_variable));
	if (value && !value->IsNull()) {
		bindings[name.GetName().GetIdentifierName()].push_back({*value, call_argument});
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
static optional<LogicalType> InstantiateParameters(TypeCache &cache, const FunctionSignature &signature,
                                                   const TypeName &name, const LogicalType &type, idx_t call_argument,
                                                   VariableBindings &bindings) {
	auto &arguments = name.GetArguments();
	const auto kind = DefaultTypeGenerator::GetDefaultType(name.GetName());
	LogicalType decimal_type;
	if (kind == LogicalTypeId::DECIMAL && type.id() != LogicalTypeId::DECIMAL) {
		// a number is passed as the decimal that holds it
		uint32_t width, scale;
		if (!type.GetDecimalProperties(width, scale)) {
			return nullopt;
		}
		decimal_type = LogicalType::DECIMAL(width, scale);
	}

	auto &instance = decimal_type.id() == LogicalTypeId::INVALID ? type : decimal_type;
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
			value = InstantiateArgument(cache, signature, argument, parameter, call_argument, bindings);
			is_same = value && parameter_idx.GetIndex() == i && IsSameParameter(*value, parameter);
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
	auto result = cache.Construct(name.GetName(), std::move(bound_arguments));
	if (result.id() == LogicalTypeId::INVALID) {
		return nullopt;
	}
	return result;
}

//! The type a type name stands for when its variables are what a type has in their place - the type of the argument
//! that is passed as it. What the type says the variables are is added to "bindings". Nothing if the type is not one
//! the name stands for. Whether the type can be passed as the result is up to the cast rules
static optional<LogicalType> InstantiateType(TypeCache &cache, const FunctionSignature &signature, const TypeName &name,
                                             const VariableUse &use, const LogicalType &type, idx_t argument,
                                             VariableBindings &bindings) {
	if (!use.IsGeneric()) {
		return BindDeclaredType(cache, name, signature);
	}
	if (type.id() == LogicalTypeId::UNKNOWN || type.id() == LogicalTypeId::SQLNULL) {
		// The argument says nothing about the type, so the variables it would determine are UNKNOWN or SQLNULL,
		// unless another argument determines them. Nested functions propagate these types: the binder replaces
		// a function that returns one by a NULL, or binds it again once the parameter is resolved
		AddUntypedBindings(signature, name, type, argument, bindings);
		return type;
	}
	if (name.HasArguments()) {
		return InstantiateParameters(cache, signature, name, type, argument, bindings);
	}
	if (auto value_variable = FindValueVariable(name, signature)) {
		// a parameter that takes the value of the variable
		return ValueVariableType(*value_variable);
	}
	if (name.IsBuiltinType(LogicalTypeId::ANY)) {
		// a type variable of its own, that is the type of the argument
		return LogicalType::NormalizeType(type);
	}
	// a type variable: of the kind it stands for, if it stands for one - a type with an alias is a type of its own
	auto constraint = signature.GetConstraint(name.GetName());
	optional<LogicalType> result = LogicalType::NormalizeType(type);
	if (constraint != LogicalTypeId::INVALID) {
		result = type.GetAlias().empty() ? ConstrainType(constraint, type) : nullopt;
	}
	if (result) {
		bindings[name.GetName().GetIdentifierName()].push_back({Value::TYPE(type), argument});
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
static Result<Value, FunctionMismatch::Reason> GetVariableValue(const TypeVariable &variable, optional<Value> constant,
                                                                optional_idx argument, idx_t parameter) {
	if (!constant) {
		return Err(FunctionMismatch::Reason(
		    FunctionMismatch::ConstantRequired {argument.GetIndex(), variable.GetName().GetIdentifierName()}));
	}
	if (constant->IsNull()) {
		return Value();
	}
	auto value = constant->DefaultTryCastAs(ValueVariableType(variable));
	if (!value) {
		if (!argument.IsValid()) {
			throw InternalException("The default of a parameter cannot be the value of %s", variable.ToString());
		}
		// the parameter reads as the variable it is declared as
		return Err(FunctionMismatch::Reason(
		    FunctionMismatch::IncompatibleArgument {argument.GetIndex(), parameter, LogicalType(), true}));
	}
	return std::move(*value);
}

//! How an argument is passed as a parameter
struct ArgumentMatch {
	//! The type the parameter declares, with the parts of the argument as the variables of a type that uses them:
	//! what the argument is passed as, unless the arguments determine a type variable to be something else. Invalid
	//! if that is up to the other arguments
	LogicalType type;
	//! What passing the argument costs, negative if it is not of a type the parameter stands for
	int64_t cost = 0;
};

//! Match an argument against the type its parameter declares. This is about the argument alone: what it costs is
//! the implicit cast to the type of the parameter with its own parts, as the cast to the type the arguments
//! determine a variable to be is explicit. What it says the variables are is added to "bindings", as said by the
//! argument at the given index. "needs_type" is whether the argument is passed even if it does not match
static ArgumentMatch MatchArgument(TypeCache &cache, const FunctionSignature &sig, const TypeName &name,
                                   const LogicalType &arg_type, idx_t argument, bool needs_type,
                                   VariableBindings &bindings) {
	auto context = cache.GetContext();
	ArgumentMatch match;
	const auto use = GetVariableUse(sig, name);
	if (!use.IsGeneric()) {
		match.type = BindDeclaredType(cache, name, sig);
		match.cost = ImplicitCastCost(context, arg_type, match.type);
		return match;
	}

	const bool is_null = arg_type.id() == LogicalTypeId::SQLNULL;
	// a string literal has nothing that says what the variables of a type with arguments are: it is cast to the type
	// the other arguments determine, which they do not for ANY
	const bool is_literal = arg_type.id() == LogicalTypeId::STRING_LITERAL && name.HasArguments();
	optional<LogicalType> own_type;
	if (!is_literal) {
		own_type = InstantiateType(cache, sig, name, use, arg_type, argument, bindings);
	}
	match.type = own_type ? *own_type : LogicalType();
	if (is_literal) {
		match.cost = use.uses_any ? -1 : NumericCast<int64_t>(STRING_LITERAL_COST);
	} else if (!own_type) {
		match.cost = -1;
	} else if (is_null && !name.HasArguments() && use.unconstrained) {
		match.cost = NULL_VARIABLE_COST;
	} else if (is_null) {
		// a NULL says nothing about the type: it costs what it costs as the kind of type the parameter stands for
		LogicalType kind;
		if (name.HasArguments()) {
			kind = LogicalType(DefaultTypeGenerator::GetDefaultType(name.GetName()));
		} else if (auto value_variable = FindValueVariable(name, sig)) {
			kind = ValueVariableType(*value_variable);
		} else {
			kind = LogicalType(sig.GetConstraint(name.GetName()));
		}
		match.cost = ImplicitCastCost(context, arg_type, kind);
	} else if (match.type != arg_type) {
		match.cost = ImplicitCastCost(context, arg_type, match.type);
	}
	if (match.cost < 0) {
		// not of a type the parameter stands for, so it says nothing about the variables. If it is cast explicitly, it
		// is cast to the type the other arguments determine - or to the type with ANY as the argument itself
		for (auto &entry : bindings) {
			auto &history = entry.second;
			while (!history.empty() && history.back().argument == argument) {
				history.pop_back();
			}
		}
		match.type = LogicalType();
		if (needs_type && !use.UsesVariable()) {
			const auto any_type = LogicalType::NormalizeType(arg_type);
			match.type = BindSignatureType(cache, name, sig, FunctionVariables(), any_type);
		}
		return match;
	}
	if (use.unconstrained && !is_null) {
		// an overload that takes any type is selected last, the more so the less the parameter says about the type
		match.cost += TYPE_VARIABLE_COST - use.depth;
	}
	return match;
}

//! Collect what the values of the arguments say the variables of a signature are: a parameter that is declared as a
//! value variable takes the value of its argument. What the types of the arguments say is collected as they are
//! matched. Returns why not: an argument is not a constant, or not a value of the variable
static optional<FunctionMismatch::Reason> CollectConstantBindings(optional_ptr<ClientContext> context,
                                                                  const FunctionSignature &sig,
                                                                  const FunctionCallArguments &call,
                                                                  const vector<BoundArgument> &arguments,
                                                                  VariableBindings &bindings) {
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument = arguments[i];
		auto &param = sig.GetParameter(argument.parameter_index);
		auto value_variable = FindValueVariable(param.GetTypeName(), sig);
		// a NULL or an unresolved parameter says nothing, which is collected as it is matched
		const bool is_untyped =
		    argument.type.id() == LogicalTypeId::UNKNOWN || argument.type.id() == LogicalTypeId::SQLNULL;
		if (!value_variable || is_untyped) {
			continue;
		}
		auto arg_idx = argument.argument_index;
		auto constant = arg_idx.IsValid() ? GetArgumentConstant(context, call.GetArgument(arg_idx.GetIndex()))
		                                  : optional<Value>(*param.GetDefaultValue());
		auto value = GetVariableValue(*value_variable, std::move(constant), arg_idx, argument.parameter_index);
		if (!value) {
			return std::move(value.GetError());
		}
		// what the arguments say a variable is, is in the order of the arguments
		auto &history = bindings[value_variable->GetName().GetIdentifierName()];
		auto position = std::find_if(history.begin(), history.end(),
		                             [&](const VariableBinding &binding) { return binding.argument > i; });
		history.insert(position, VariableBinding {std::move(*value), i});
	}
	return nullopt;
}

//! Pass every argument as the type of its parameter with the variables as the arguments determine them. A part of an
//! argument that is passed as ANY, or as a type of values only, stays its own
static void SubstituteVariables(TypeCache &cache, const FunctionSignature &sig, const FunctionCallArguments &call,
                                const FunctionVariables &variables, vector<BoundArgument> &arguments) {
	for (auto &argument : arguments) {
		auto &param = sig.GetParameter(argument.parameter_index);
		auto &name = param.GetTypeName();
		const auto use = GetVariableUse(sig, name);
		if (!use.UsesVariable()) {
			continue;
		}
		// the type the call passes: the argument is of its own type by now
		const bool is_default = argument.IsDefaultArgument();
		auto &arg_type =
		    is_default ? param.GetDefaultValue()->type() : call.GetArgument(argument.argument_index.GetIndex()).type;
		const bool is_untyped = arg_type.id() == LogicalTypeId::UNKNOWN || arg_type.id() == LogicalTypeId::SQLNULL;
		const bool has_own_type = !is_untyped && argument.type.id() != LogicalTypeId::INVALID;
		if (has_own_type && !use.uses_variable) {
			continue;
		}
		// ANY is not used next to a type variable, so it stands for an argument that says nothing about the type
		D_ASSERT(!has_own_type || !use.uses_any);
		auto type = BindSignatureType(cache, name, sig, variables, arg_type);
		if (!is_default && is_untyped && (use.uses_any || !type.IsComplete())) {
			// a NULL or an unresolved parameter is passed as it is where nothing determines the type of the
			// parameter: as ANY, and e.g. as a DECIMAL of any width
			continue;
		}
		argument.type = std::move(type);
	}
}

//===--------------------------------------------------------------------===//
// Specificity
//===--------------------------------------------------------------------===//
//! How much a type name, or an overload, says about what it takes, compared to another one
enum class Specificity : uint8_t { EQUAL, MORE, LESS, INCOMPARABLE };

//! More specific if no part is less specific and one part is more specific
static Specificity CombineSpecificity(Specificity left, Specificity right) {
	if (left == Specificity::EQUAL || left == right) {
		return right;
	}
	return right == Specificity::EQUAL ? left : Specificity::INCOMPARABLE;
}

//! The kind of type a type name stands for: the kind a type variable is constrained to, or the built-in type of
//! the name. INVALID if it is neither
static LogicalTypeId GetTypeKind(const FunctionSignature &signature, const TypeName &name, bool &is_variable) {
	auto variable = name.HasArguments() ? nullptr : signature.FindVariable(name.GetName());
	is_variable = variable && !variable->IsValue();
	if (is_variable) {
		return signature.GetConstraint(name.GetName());
	}
	return DefaultTypeGenerator::GetDefaultType(name.GetName());
}

//! Whether a type name says more than another one about the types it stands for: a type says more than a type
//! variable, a type variable of a kind more than one of any type, DECIMAL(W, S) more than DECIMAL, and
//! DECIMAL(W, 0) more than DECIMAL(W, S)
static Specificity CompareSpecificity(const FunctionSignature &left_signature, const TypeName &left,
                                      const FunctionSignature &right_signature, const TypeName &right) {
	bool left_is_variable, right_is_variable;
	auto left_kind = GetTypeKind(left_signature, left, left_is_variable);
	auto right_kind = GetTypeKind(right_signature, right, right_is_variable);
	const bool left_is_any =
	    left.IsBuiltinType(LogicalTypeId::ANY) || (left_is_variable && left_kind == LogicalTypeId::INVALID);
	const bool right_is_any =
	    right.IsBuiltinType(LogicalTypeId::ANY) || (right_is_variable && right_kind == LogicalTypeId::INVALID);
	if (left_is_any || right_is_any) {
		if (left_is_any && right_is_any) {
			return Specificity::EQUAL;
		}
		return left_is_any ? Specificity::LESS : Specificity::MORE;
	}
	if (left_is_variable || right_is_variable) {
		// a type variable of a kind, and a type of that kind
		if (left_kind != right_kind) {
			return Specificity::INCOMPARABLE;
		}
		if (left_is_variable && right_is_variable) {
			return Specificity::EQUAL;
		}
		return left_is_variable ? Specificity::LESS : Specificity::MORE;
	}
	if (left.GetName() != right.GetName()) {
		return Specificity::INCOMPARABLE;
	}
	auto &left_arguments = left.GetArguments();
	auto &right_arguments = right.GetArguments();
	if (left_arguments.empty() || right_arguments.empty()) {
		// the name alone is every type of the name
		if (left_arguments.empty() && right_arguments.empty()) {
			return Specificity::EQUAL;
		}
		return left_arguments.empty() ? Specificity::LESS : Specificity::MORE;
	}
	if (left_arguments.size() != right_arguments.size()) {
		return Specificity::INCOMPARABLE;
	}
	auto result = Specificity::EQUAL;
	for (idx_t i = 0; i < left_arguments.size(); i++) {
		auto &left_argument = left_arguments[i];
		auto &right_argument = right_arguments[i];
		const bool left_is_value = FindValueVariable(left_argument, left_signature);
		const bool right_is_value = FindValueVariable(right_argument, right_signature);
		Specificity argument;
		if (left_is_value || right_is_value) {
			// a value says more than a value variable
			if (left_is_value && right_is_value) {
				argument = Specificity::EQUAL;
			} else if (left_is_value) {
				argument = right_argument.IsType() ? Specificity::INCOMPARABLE : Specificity::LESS;
			} else {
				argument = left_argument.IsType() ? Specificity::INCOMPARABLE : Specificity::MORE;
			}
		} else if (left_argument.IsType() && right_argument.IsType()) {
			argument =
			    CompareSpecificity(left_signature, left_argument.GetType(), right_signature, right_argument.GetType());
		} else {
			argument = left_argument == right_argument ? Specificity::EQUAL : Specificity::INCOMPARABLE;
		}
		result = CombineSpecificity(result, argument);
	}
	return result;
}

//! Whether an overload says more than another one about the arguments of the call both match
static Specificity CompareSpecificity(const FunctionBinding &left, const FunctionBinding &right) {
	auto &left_signature = left.GetFunction().GetSignature();
	auto &right_signature = right.GetFunction().GetSignature();
	// the parameter of the right overload that receives each argument of the call
	vector<idx_t> right_parameters(right.layout.arguments.size());
	for (auto &argument : right.layout.arguments) {
		if (argument.argument_index.IsValid()) {
			right_parameters[argument.argument_index.GetIndex()] = argument.parameter_index;
		}
	}
	auto result = Specificity::EQUAL;
	for (auto &argument : left.layout.arguments) {
		if (!argument.argument_index.IsValid()) {
			continue;
		}
		auto &left_type = left_signature.GetParameter(argument.parameter_index).GetTypeName();
		auto &right_type =
		    right_signature.GetParameter(right_parameters[argument.argument_index.GetIndex()]).GetTypeName();
		result = CombineSpecificity(result, CompareSpecificity(left_signature, left_type, right_signature, right_type));
	}
	return result;
}

bool FunctionBinder::IsMoreSpecific(const FunctionBinding &left, const FunctionBinding &right) {
	return CompareSpecificity(left, right) == Specificity::MORE;
}

//! Lay the arguments of a call out over the parameters of an overload. This is about the shape of the call only, not
//! about types: a positional argument is passed to the parameter at its position or to "*args", a named one to the
//! parameter of its name or to "**kwargs", and a parameter the call leaves out is filled by its default. Returns the
//! first reason the call does not fit: a wrong name, which often causes the counts to be off as well, then too many
//! arguments, then a parameter that is left out
static Result<ArgumentLayout, FunctionMismatch>
LayoutArguments(idx_t overload, const shared_ptr<const SimpleFunction> &function, const FunctionCallArguments &call) {
	const auto &sig = function->GetSignature();
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
				return Err(FunctionMismatch(overload, function, distance, FunctionMismatch::UnknownNamedArgument {i}));
			}
			param_idx = kwargs_idx;
		} else if (param_idx.GetIndex() < positional_count &&
		           sig.GetParameter(param_idx.GetIndex()).AcceptsPosition()) {
			return Err(FunctionMismatch(overload, function, distance, FunctionMismatch::AlreadyPassedByPosition {i}));
		}
		named_parameters.push_back(param_idx);
	}

	// every argument too many is one further away
	if (!args_idx.IsValid() && !kwargs_idx.IsValid() && argument_count > parameter_count) {
		// more arguments than parameters: as no two share a name, too many of them are passed by position
		return Err(
		    FunctionMismatch(overload, function, argument_count + (argument_count - parameter_count),
		                     FunctionMismatch::TooManyArguments {parameter_count, positional_parameter_count, true}));
	}
	if (!args_idx.IsValid() && positional_count > positional_parameter_count) {
		// there is no "*args" to receive the extra positional arguments
		return Err(FunctionMismatch(
		    overload, function, argument_count + (positional_count - positional_parameter_count),
		    FunctionMismatch::TooManyArguments {positional_parameter_count, positional_parameter_count, true}));
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
			return Err(FunctionMismatch(
			    overload, function, distance,
			    FunctionMismatch::MissingParameter {param_idx, param.IsNamed() ? param.GetName() : Identifier()}));
		}
		layout.arguments.push_back({param_idx, optional_idx(), param.GetDefaultValue()->type()});
	}
	layout.positional_count = layout.arguments.size() - layout.named_arguments.size();
	return std::move(layout);
}

//! The mismatch of an overload every argument can be passed to, but whose variables the arguments do not determine:
//! the closest miss there is - unless it is the value of an argument that cannot be passed
static FunctionMismatch VariableMismatch(idx_t overload, const shared_ptr<const SimpleFunction> &function,
                                         FunctionMismatch::Reason reason) {
	const bool is_incompatible = std::holds_alternative<FunctionMismatch::IncompatibleArgument>(reason);
	return FunctionMismatch(overload, function, is_incompatible ? 1 : 0, std::move(reason));
}

//! Determine the types the arguments of a function are passed as, and what the casts to them cost: the type of every
//! argument of the layout is replaced by the type of the parameter that receives it. Returns the binding - or why
//! the arguments cannot be passed
static Result<FunctionBinding, FunctionMismatch>
ResolveArgumentTypes(TypeCache &cache, idx_t overload, const shared_ptr<const SimpleFunction> &function,
                     const FunctionCallArguments &call, ArgumentLayout layout, bool explicit_casts,
                     optional_ptr<const DecimalWidthLimit> decimal_width_limit) {
	const auto &sig = function->GetSignature();
	auto &arguments = layout.arguments;

	// Match every argument against the type its parameter declares. The first argument that cannot be cast is
	// reported, and the more there are the further the call is from matching
	VariableBindings bindings;
	idx_t cost = 0;
	bool has_unknown_argument = false;
	optional<FunctionMismatch::Reason> mismatch;
	idx_t mismatch_count = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument = arguments[i];
		auto &name = sig.GetParameter(argument.parameter_index).GetTypeName();
		const auto arg_type = argument.type;
		// an argument that does not match is still passed if it is cast explicitly, or is a default
		const bool needs_type = explicit_casts || argument.IsDefaultArgument();
		auto match = MatchArgument(cache, sig, name, arg_type, i, needs_type, bindings);
		argument.type = match.type;
		if (argument.IsDefaultArgument()) {
			// a default is not cast
			continue;
		}
		const auto source = argument.argument_index.GetIndex();
		if (arg_type.id() == LogicalTypeId::UNKNOWN && source < call.GetPositionalCount()) {
			has_unknown_argument = true;
			continue;
		}
		if (explicit_casts) {
			continue;
		}
		if (match.cost < 0) {
			if (!mismatch) {
				// a parameter that uses a variable reads as it is declared
				const bool is_generic = GetVariableUse(sig, name).IsGeneric();
				mismatch = FunctionMismatch::IncompatibleArgument {source, argument.parameter_index,
				                                                   is_generic ? LogicalType() : match.type, is_generic};
			}
			mismatch_count++;
			continue;
		}
		cost += static_cast<idx_t>(match.cost);
	}
	if (mismatch) {
		return Err(FunctionMismatch(overload, function, mismatch_count, std::move(*mismatch)));
	}

	FunctionBinding binding;
	if (!sig.GetVariables().empty()) {
		auto context = cache.GetContext();
		// collect what the arguments say the variables are: their types did as they were matched, their values do now
		if (auto failure = CollectConstantBindings(context, sig, call, arguments, bindings)) {
			return Err(VariableMismatch(overload, function, std::move(*failure)));
		}
		// unify it to what every variable is
		auto unified = UnifyBindings(context, sig, bindings, arguments, decimal_width_limit);
		if (!unified) {
			return Err(VariableMismatch(overload, function, std::move(unified.GetError())));
		}
		auto &variables = *unified;
		// substitute: the arguments are passed as the types of their parameters with the variables
		SubstituteVariables(cache, sig, call, variables, arguments);
		for (auto &variable : sig.GetVariables()) {
			auto &variable_name = variable.GetName().GetIdentifierName();
			if (!variable.IsValue() && variables.types.find(variable_name) != variables.types.end()) {
				binding.variable_types[variable_name] =
				    GetVariableType(variable.GetName(), sig.GetConstraint(variable.GetName()), variables);
			}
		}
		binding.variable_values = std::move(variables.values);
	}

	// with a parameter of an unknown type, the cost is 0
	binding.cost = has_unknown_argument ? 0 : cost;
	binding.layout = std::move(layout);
	return std::move(binding);
}

Result<FunctionBinding, FunctionMismatch>
FunctionBinder::MatchOverload(TypeCache &cache, idx_t overload, shared_ptr<const SimpleFunction> function,
                              const FunctionCallArguments &call, bool explicit_casts,
                              optional_ptr<const DecimalWidthLimit> decimal_width_limit) {
	auto layout = LayoutArguments(overload, function, call);
	if (!layout) {
		return Err(std::move(layout.GetError()));
	}
	auto binding =
	    ResolveArgumentTypes(cache, overload, function, call, std::move(*layout), explicit_casts, decimal_width_limit);
	if (binding) {
		binding->overload = overload;
		binding->function = std::move(function);
	}
	return binding;
}

LogicalType FunctionBinder::BindReturnType(TypeCache &cache, const FunctionBinding &binding) {
	auto &signature = binding.GetFunction().GetSignature();
	auto &return_type = signature.GetReturnType();
	if (!return_type.IsType()) {
		// the signature declares no type: the resolver or the bind callback of the function sets it
		return LogicalType::INVALID;
	}
	return BindTypeNameInternal(
	    cache, return_type.GetTypeName(), signature,
	    [&](const Identifier &variable, LogicalTypeId) {
		    auto entry = binding.variable_types.find(variable.GetIdentifierName());
		    if (entry == binding.variable_types.end()) {
			    throw BinderException("Cannot deduce type variable '%s', it is not determined by the arguments",
			                          variable.GetIdentifierName());
		    }
		    return entry->second;
	    },
	    [&](const Identifier &variable) {
		    auto entry = binding.variable_values.find(variable.GetIdentifierName());
		    if (entry == binding.variable_values.end()) {
			    throw BinderException("Cannot deduce value '%s', it is not determined by the arguments",
			                          variable.GetIdentifierName());
		    }
		    return entry->second;
	    });
}

} // namespace duckdb
