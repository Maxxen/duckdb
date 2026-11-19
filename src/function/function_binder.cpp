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

FunctionBinder::FunctionBinder(ClientContext &context_p) : binder(nullptr), context(context_p) {
}
FunctionBinder::FunctionBinder(Binder &binder_p) : binder(&binder_p), context(binder_p.context) {
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

//! The type of the kind of the constraint that an argument of the given type is passed as, e.g. DECIMAL(10,0) for an
//! INTEGER passed as a DECIMAL - nullopt if the argument determines none
static optional<LogicalType> ConstrainType(LogicalTypeId constraint, const LogicalType &type) {
	if (type.id() == constraint) {
		return type;
	}
	const auto is_struct = [](LogicalTypeId id) {
		return id == LogicalTypeId::STRUCT || id == LogicalTypeId::TUPLE;
	};
	if (is_struct(constraint) && is_struct(type.id())) {
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
//! type of a type variable, bind_value the value of a value variable - NULL if it is not known
template <class BIND_VARIABLE, class BIND_VALUE>
static LogicalType BindTypeNameInternal(optional_ptr<ClientContext> context, const TypeName &name,
                                        const FunctionSignature &signature, const BIND_VARIABLE &bind_variable,
                                        const BIND_VALUE &bind_value) {
	auto &identifier = name.GetName();
	auto &arguments = name.GetArguments();
	auto builtin_type = name.GetBuiltinType();
	if (builtin_type != LogicalTypeId::INVALID) {
		// the name of a built-in type that takes no arguments is that type, whatever else is called the same
		LogicalType type(builtin_type);
		if (type.IsComplete() || builtin_type == LogicalTypeId::ANY) {
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
			value =
			    Value::TYPE(BindTypeNameInternal(context, argument.GetType(), signature, bind_variable, bind_value));
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

	if (context) {
		auto entry =
		    Catalog::GetEntry<TypeCatalogEntry>(*context, QualifiedName(identifier), OnEntryNotFound::RETURN_NULL);
		if (entry) {
			return entry->constructors.Bind(context, entry->user_type, bound_arguments);
		}
	} else {
		// without a context only the built-in types are found
		vector<pair<string, Value>> parameters;
		for (auto &argument : bound_arguments) {
			parameters.emplace_back(argument.GetName(), argument.GetValue());
		}
		auto type = DefaultTypeGenerator::TryDefaultBind(identifier.GetIdentifierName(), parameters);
		if (type.id() != LogicalTypeId::INVALID) {
			return type;
		}
	}

	if (builtin_type == LogicalTypeId::LAMBDA || builtin_type == LogicalTypeId::TABLE) {
		return LogicalType(builtin_type);
	}
	throw CatalogException("Type with name %s does not exist", identifier.GetIdentifierName());
}

//! A type variable that stands for any type, or for any type of the kind it is constrained to
static LogicalType BindVariablePattern(const Identifier &, LogicalTypeId constraint) {
	return constraint == LogicalTypeId::INVALID ? LogicalType(LogicalType::ANY) : LogicalType(constraint);
}

//! A value variable whose value is not known
static Value BindValuePattern(const Identifier &) {
	return Value();
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
	auto &inferred_type = entry->second.back();
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
	return BindTypeNameInternal(context, name, FunctionSignature(), BindVariablePattern, BindValuePattern);
}

LogicalType FunctionBinder::BindTypePattern(optional_ptr<ClientContext> context, const TypeName &name,
                                            const FunctionSignature &signature) {
	return BindTypeNameInternal(context, name, signature, BindVariablePattern, BindValuePattern);
}

LogicalType FunctionBinder::BindTypeName(optional_ptr<ClientContext> context, const TypeName &name,
                                         const FunctionSignature &signature, const FunctionVariables &variables) {
	return BindTypeNameInternal(
	    context, name, signature,
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
	    });
}

//===--------------------------------------------------------------------===//
// Type variables
//===--------------------------------------------------------------------===//
//! What it costs to pass an argument as a type variable that stands for any type, on top of the cast to the type the
//! variable is determined to be: more than any cast, so that an overload that declares the type is preferred
static constexpr idx_t TYPE_VARIABLE_COST = 1000000;

//! How a type name uses the type variables of its signature
struct VariableUse {
	//! Whether it uses any
	bool uses_variable = false;
	//! Whether it uses one that stands for any type, and how deep the outermost such use is nested
	bool unconstrained = false;
	idx_t depth = 0;
	//! Whether it uses a value variable
	bool uses_value = false;

	bool UsesAny() const {
		return uses_variable || uses_value;
	}
};

static void FindVariableUse(const FunctionSignature &signature, const TypeName &name, idx_t depth, VariableUse &use) {
	if (!name.HasArguments()) {
		auto variable = signature.FindVariable(name.GetName());
		if (!variable) {
			return;
		}
		if (variable->IsValue()) {
			use.uses_value = true;
			return;
		}
		use.uses_variable = true;
		if (signature.GetConstraint(name.GetName()) == LogicalTypeId::INVALID &&
		    (!use.unconstrained || depth < use.depth)) {
			use.unconstrained = true;
			use.depth = depth;
		}
		return;
	}
	for (auto &argument : name.GetArguments()) {
		if (argument.IsType()) {
			FindVariableUse(signature, argument.GetType(), depth + 1, use);
		}
	}
}

static VariableUse GetVariableUse(const FunctionSignature &signature, const TypeName &name) {
	VariableUse use;
	if (!signature.GetVariables().empty()) {
		FindVariableUse(signature, name, 0, use);
	}
	return use;
}

//! Bind every variable a type name uses that is not bound already: a type variable to a type, a value variable to
//! an unknown value
static void BindUndeterminedVariables(const FunctionSignature &signature, const TypeName &name, const LogicalType &type,
                                      FunctionVariables &variables) {
	if (!name.HasArguments()) {
		auto variable = signature.FindVariable(name.GetName());
		auto &variable_name = name.GetName().GetIdentifierName();
		if (!variable) {
			return;
		}
		if (variable->IsValue()) {
			variables.values.emplace(variable_name, Value());
		} else if (variables.types.find(variable_name) == variables.types.end()) {
			variables.types[variable_name] = {type};
		}
		return;
	}
	for (auto &argument : name.GetArguments()) {
		if (argument.IsType()) {
			BindUndeterminedVariables(signature, argument.GetType(), type, variables);
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

//! Bind a value variable to a value, or check that it is bound to that value. An unknown value is replaced
static bool UnifyValue(const Identifier &variable, Value value, FunctionVariables &variables,
                       FunctionMismatch::TemplateDeduction &failure) {
	auto &variable_name = variable.GetIdentifierName();
	auto entry = variables.values.find(variable_name);
	if (entry == variables.values.end() || entry->second.IsNull()) {
		variables.values[variable_name] = std::move(value);
		return true;
	}
	if (Value::NotDistinctFrom(entry->second, value)) {
		return true;
	}
	failure.template_name = variable_name;
	failure.inferred_values = {entry->second, std::move(value)};
	return false;
}

//! Infer the variables a type name uses from the type of the argument that is passed as it
static bool InferVariables(optional_ptr<ClientContext> context, const FunctionSignature &signature,
                           const TypeName &name, const LogicalType &type, FunctionVariables &variables,
                           FunctionMismatch::TemplateDeduction &failure) {
	if (!GetVariableUse(signature, name).UsesAny()) {
		return true;
	}
	if (type.id() == LogicalTypeId::UNKNOWN || type.id() == LogicalTypeId::SQLNULL) {
		// The argument says nothing about the types, so the variables it would determine are UNKNOWN or SQLNULL,
		// unless another argument determines them. Nested functions propagate these types: the binder replaces
		// a function that returns one by a NULL, or binds it again once the parameter is resolved
		BindUndeterminedVariables(signature, name, LogicalType(type.id()), variables);
		return true;
	}
	if (FindValueVariable(name, signature)) {
		// the value of the argument determines the variable, not its type
		return true;
	}
	auto &variable_types = variables.types;

	if (!name.HasArguments()) {
		// a type variable: bind it, or unify what it is bound to with the type of the argument
		auto &variable_name = name.GetName().GetIdentifierName();
		auto constraint = signature.GetConstraint(name.GetName());
		if (constraint != LogicalTypeId::INVALID && !ConstrainType(constraint, type)) {
			// the argument is not of the kind of type the variable stands for
			failure.template_name = variable_name;
			failure.inferred_types = {type};
			failure.constraint = constraint;
			return false;
		}
		auto entry = variable_types.find(variable_name);
		if (entry == variable_types.end()) {
			variable_types[variable_name] = {type};
		} else if (entry->second.back() != type) {
			// promote the types - without a client context, with the built-in casts only
			LogicalType result;
			const bool unified =
			    context ? LogicalType::TryGetMaxLogicalType(*context, entry->second.back(), type, result)
			            : LogicalType::DefaultTryGetMaxLogicalTypeUnchecked(entry->second.back(), type, result);
			if (!unified) {
				failure.template_name = variable_name;
				failure.inferred_types = entry->second;
				failure.inferred_types.push_back(type);
				return false;
			}
			if (entry->second.back() != result) {
				entry->second.push_back(type);
				entry->second.push_back(std::move(result));
			}
		}
		// arguments of the kind can still unify to a type that is not of it, e.g. two enums to VARCHAR
		auto &inferred_types = variable_types[variable_name];
		if (constraint != LogicalTypeId::INVALID &&
		    !ConstrainType(constraint, LogicalType::NormalizeType(inferred_types.back()))) {
			failure.template_name = variable_name;
			failure.inferred_types = inferred_types;
			return false;
		}
		return true;
	}

	auto &arguments = name.GetArguments();
	auto infer_argument = [&](idx_t index, const LogicalType &argument_type) {
		return index >= arguments.size() || !arguments[index].IsType() ||
		       InferVariables(context, signature, arguments[index].GetType(), argument_type, variables, failure);
	};
	// the value an argument of the type name stands for, if that argument is a value variable
	auto infer_value = [&](idx_t index, Value value) {
		return index >= arguments.size() || !FindValueVariable(arguments[index], signature) ||
		       UnifyValue(arguments[index].GetType().GetName(), std::move(value), variables, failure);
	};
	auto infer_integer = [&](idx_t index, idx_t value) {
		return infer_value(index, Value::BIGINT(NumericCast<int64_t>(value)));
	};
	const auto kind = DefaultTypeGenerator::GetDefaultType(name.GetName());
	if (kind == LogicalTypeId::DECIMAL) {
		// the width and scale of a decimal, or of the decimal a number is passed as
		uint32_t width, scale;
		return !type.GetDecimalProperties(width, scale) || (infer_integer(0, width) && infer_integer(1, scale));
	}
	if (kind == LogicalTypeId::GEOMETRY) {
		// the coordinate reference system of a geometry that has one
		return type.id() != LogicalTypeId::GEOMETRY || !GeoType::HasCRS(type) ||
		       infer_value(0, Value(GeoType::GetCRS(type).GetDefinition()));
	}

	// a nested type: infer the variables of the types it is made of from those the argument is made of
	if (!type.IsNested() || !type.HasParameters()) {
		return true;
	}
	switch (kind) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
		if (type.id() == LogicalTypeId::LIST) {
			return infer_argument(0, ListType::GetChildType(type));
		}
		if (type.id() == LogicalTypeId::ARRAY) {
			return infer_argument(0, ArrayType::GetChildType(type)) &&
			       (ArrayType::IsAnySize(type) || infer_integer(1, ArrayType::GetSize(type)));
		}
		return true;
	case LogicalTypeId::MAP:
		// a map is only implicitly castable to a map
		if (type.id() == LogicalTypeId::MAP) {
			return infer_argument(0, MapType::KeyType(type)) && infer_argument(1, MapType::ValueType(type));
		}
		return true;
	case LogicalTypeId::UNION:
		// TODO: Support union types with template member types.
		throw NotImplementedException("Union types cannot infer templated member types yet!");
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		bool unnamed = true;
		for (auto &argument : arguments) {
			unnamed = unnamed && argument.GetLabel().empty();
		}
		if (!StructType::IsStruct(type) || !unnamed) {
			// TODO: Support named structs with template child types.
			throw NotImplementedException("Named structs cannot infer templated child types yet!");
		}
		// a struct is only implicitly castable to a struct
		auto &children = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < MinValue(arguments.size(), children.size()); i++) {
			if (!infer_argument(i, children[i].second)) {
				return false;
			}
		}
		return true;
	}
	default:
		return true;
	}
}

//! Whether a type is the type a type name stands for, given the values the type determines: DECIMAL(W, S) is every
//! decimal, GEOMETRY(C) is every geometry that has a coordinate reference system
static bool IsTypeOfValues(optional_ptr<ClientContext> context, const FunctionSignature &signature,
                           const TypeName &name, const LogicalType &type) {
	if (!name.HasArguments() || type.id() != DefaultTypeGenerator::GetDefaultType(name.GetName())) {
		return false;
	}
	FunctionVariables own_variables;
	FunctionMismatch::TemplateDeduction failure;
	if (!InferVariables(context, signature, name, type, own_variables, failure)) {
		return false;
	}
	for (auto &argument : name.GetArguments()) {
		if (!FindValueVariable(argument, signature)) {
			return false;
		}
		auto entry = own_variables.values.find(argument.GetType().GetName().GetIdentifierName());
		if (entry == own_variables.values.end() || entry->second.IsNull()) {
			return false;
		}
	}
	return true;
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

//! Determine the value variable a parameter is declared as by the value that is passed to it. "argument" is the
//! argument that passes it, invalid if it is the default of the parameter
static optional<FunctionMismatch::Reason> InferValue(const TypeVariable &variable, optional<Value> constant,
                                                     optional_idx argument, FunctionVariables &variables) {
	auto &variable_name = variable.GetName().GetIdentifierName();
	if (!constant) {
		return FunctionMismatch::Reason(FunctionMismatch::ConstantRequired {argument.GetIndex(), variable_name});
	}
	if (constant->IsNull()) {
		// says nothing about the value, unless another argument determines it
		variables.values.emplace(variable_name, Value());
		return nullopt;
	}
	auto value = constant->DefaultTryCastAs(ValueVariableType(variable));
	if (!value) {
		if (!argument.IsValid()) {
			throw InternalException("The default of a parameter cannot be the value of %s", variable.ToString());
		}
		return FunctionMismatch::Reason(FunctionMismatch::IncompatibleArgument {argument.GetIndex(), variable_name});
	}
	FunctionMismatch::TemplateDeduction deduction;
	if (!UnifyValue(variable.GetName(), std::move(*value), variables, deduction)) {
		deduction.argument = argument;
		return FunctionMismatch::Reason(std::move(deduction));
	}
	return nullopt;
}

//! Infer the variables of a signature, as the arguments and the defaults of the parameters they leave out
//! determine them. Returns why not, if they do not determine every variable of the types the call uses
static optional<FunctionMismatch::Reason>
DeduceVariables(optional_ptr<ClientContext> context, const FunctionSignature &sig, const FunctionCallArguments &call,
                const vector<bool> &filled, const FunctionBinding &binding, FunctionVariables &variables) {
	FunctionMismatch::TemplateDeduction deduction;
	for (idx_t i = 0; i < binding.argument_to_parameters.size(); i++) {
		auto &param_type = sig.GetParameter(binding.argument_to_parameters[i]).GetTypeName();
		auto &argument = call.GetArgument(i);
		auto value_variable = FindValueVariable(param_type, sig);
		if (value_variable && argument.type.id() != LogicalTypeId::UNKNOWN &&
		    argument.type.id() != LogicalTypeId::SQLNULL) {
			auto value_mismatch = InferValue(*value_variable, GetArgumentConstant(context, argument), i, variables);
			if (value_mismatch) {
				return value_mismatch;
			}
			continue;
		}
		if (!InferVariables(context, sig, param_type, argument.type, variables, deduction)) {
			deduction.argument = i;
			return FunctionMismatch::Reason(std::move(deduction));
		}
	}
	for (idx_t i = 0; i < sig.GetParameterCount(); i++) {
		auto &param = sig.GetParameter(i);
		if (filled[i] || !param.HasDefaultValue()) {
			continue;
		}
		auto value_variable = FindValueVariable(param.GetTypeName(), sig);
		if (value_variable) {
			auto value_mismatch = InferValue(*value_variable, *param.GetDefaultValue(), optional_idx(), variables);
			if (value_mismatch) {
				return value_mismatch;
			}
			continue;
		}
		if (!InferVariables(context, sig, param.GetTypeName(), param.GetDefaultValue()->type(), variables, deduction)) {
			return FunctionMismatch::Reason(std::move(deduction));
		}
	}

	// every variable of the types the call uses has to be determined by the arguments and defaults
	auto unresolved = [](const TypeVariable &variable) {
		return FunctionMismatch::Reason(
		    FunctionMismatch::TemplateUnresolved {variable.GetName().GetIdentifierName(), variable.IsValue()});
	};
	for (idx_t i = 0; i < sig.GetParameterCount(); i++) {
		if (sig.GetParameter(i).IsVariadic() && !filled[i]) {
			// a variadic parameter the call passes nothing to is not used
			continue;
		}
		auto variable = FindUndeterminedVariable(sig, sig.GetParameter(i).GetTypeName(), variables);
		if (variable) {
			return unresolved(*variable);
		}
	}
	if (sig.GetReturnType().IsType()) {
		auto variable = FindUndeterminedVariable(sig, sig.GetReturnType().GetTypeName(), variables);
		if (variable) {
			return unresolved(*variable);
		}
	}
	return nullopt;
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
	auto result = Specificity::EQUAL;
	for (idx_t i = 0; i < left.argument_to_parameters.size(); i++) {
		auto &left_type = left_signature.GetParameter(left.argument_to_parameters[i]).GetTypeName();
		auto &right_type = right_signature.GetParameter(right.argument_to_parameters[i]).GetTypeName();
		result = CombineSpecificity(result, CompareSpecificity(left_signature, left_type, right_signature, right_type));
	}
	return result;
}

//! How useful a reason is to report, lower is more useful. A wrong name points at an argument the call passes, and
//! often causes the counts to be off as well
static idx_t MismatchRank(const FunctionMismatch::Reason &reason) {
	switch (static_cast<FunctionMismatch::Type>(reason.index())) {
	case FunctionMismatch::Type::UNKNOWN_NAMED_ARGUMENT:
	case FunctionMismatch::Type::ALREADY_PASSED_BY_POSITION:
		return 0;
	case FunctionMismatch::Type::MISSING_PARAMETER:
		return 1;
	case FunctionMismatch::Type::TOO_MANY_ARGUMENTS:
		return 2;
	default:
		return 3;
	}
}

std::variant<FunctionBinding, FunctionMismatch> FunctionBinder::MatchOverload(optional_ptr<ClientContext> context,
                                                                              idx_t overload,
                                                                              shared_ptr<const SimpleFunction> function,
                                                                              const FunctionCallArguments &call,
                                                                              bool explicit_casts) {
	// every mismatch is counted towards the distance, the most useful one is the reason
	idx_t mismatches = 0;
	optional<FunctionMismatch::Reason> reason;
	auto mismatch = [&](FunctionMismatch::Reason candidate, idx_t count = 1) {
		if (!reason || MismatchRank(candidate) < MismatchRank(*reason)) {
			reason = std::move(candidate);
		}
		mismatches += count;
	};
	const auto &sig = function->GetSignature();

	// The number of arguments the call passes, and how many of them it passes by position
	const auto received_count = call.GetArgumentCount();
	const auto received_positional_count = call.GetPositionalCount();

	// The number of arguments the function accepts at most, and how many of them by position
	const auto accepted_positional_count = sig.GetPositionalParameterCount();
	const auto args_param = sig.GetArgs();
	const auto kwargs_param = sig.GetKwargs();

	idx_t accepted_count = NumericLimits<idx_t>::Maximum();
	if (!args_param && !kwargs_param) {
		accepted_count = sig.GetParameterCount();
	}

	// Every parameter without a default has to be filled by the call, either from its position or by its name.
	// Counting the arguments instead would let a named argument stand in for a required positional parameter, so
	// that f(a, *, opt := NULL) matched a call f(opt := 1) that cannot fill "a" just as well as f(*, opt := NULL).
	for (idx_t i = 0; i < sig.GetParameterCount(); i++) {
		auto &param = sig.GetParameter(i);
		if (param.IsVariadic() || param.HasDefaultValue()) {
			continue;
		}
		if (param.AcceptsPosition() && i < received_positional_count) {
			continue;
		}
		bool filled = false;
		if (param.AcceptsName()) {
			for (idx_t arg_idx = received_positional_count; arg_idx < received_count; arg_idx++) {
				if (call.GetArgument(arg_idx).name == param.GetName()) {
					filled = true;
					break;
				}
			}
		}
		if (!filled) {
			// This parameter cannot be filled by the call, so this function cannot be a match.
			mismatch(FunctionMismatch::MissingParameter {i, param.IsNamed() ? param.GetName() : Identifier()});
		}
	}

	if (received_count > accepted_count) {
		// We have more arguments than the function can take, so this function cannot be a match. The limit reported
		// is of the positional parameters, if there are too many arguments passed by position
		const bool positional = received_positional_count > accepted_positional_count;
		mismatch(FunctionMismatch::TooManyArguments {accepted_count,
		                                             positional ? accepted_positional_count : accepted_count,
		                                             positional},
		         received_count - accepted_count);
	} else if (received_positional_count > accepted_positional_count && !args_param) {
		// There is no "*args" to receive the extra positional arguments
		mismatch(FunctionMismatch::TooManyArguments {accepted_positional_count, accepted_positional_count, true},
		         received_positional_count - accepted_positional_count);
	}

	for (idx_t i = received_positional_count; i < received_count; i++) {
		auto param_idx = sig.GetParameterIndexByName(call.GetArgument(i).name);
		if (!param_idx.IsValid()) {
			if (!kwargs_param) {
				// no parameter with this name, and no "**kwargs" to receive it
				mismatch(FunctionMismatch::UnknownNamedArgument {i});
			}
		} else if (sig.GetParameter(param_idx.GetIndex()).AcceptsPosition() &&
		           param_idx.GetIndex() < received_positional_count) {
			mismatch(FunctionMismatch::AlreadyPassedByPosition {i});
		}
	}

	if (reason) {
		// the wrong shape is further away than arguments that cannot be cast, of which there are at most this many
		return FunctionMismatch(overload, std::move(function), received_count + mismatches, std::move(*reason));
	}

	// the shape matches, so every argument has a parameter that receives it: its position, "*args", its name or
	// "**kwargs"
	optional_idx args_idx;
	optional_idx kwargs_idx;
	for (idx_t i = 0; i < sig.GetParameterCount(); i++) {
		switch (sig.GetParameter(i).GetKind()) {
		case FunctionParameterKind::VAR_POSITIONAL:
			args_idx = i;
			break;
		case FunctionParameterKind::VAR_KEYWORD:
			kwargs_idx = i;
			break;
		default:
			break;
		}
	}

	auto get_parameter_index = [&](idx_t i) {
		if (i >= received_positional_count) {
			auto by_name = sig.GetParameterIndexByName(call.GetArgument(i).name);
			return by_name.IsValid() ? by_name.GetIndex() : kwargs_idx.GetIndex();
		}
		return i < accepted_positional_count ? i : args_idx.GetIndex();
	};

	// The shape matches: bind the types the signature declares by name. A type variable stands for any type until
	// the arguments determine it, or for any type of its kind
	auto parameter_types = BindParameterPatterns(context, sig);
	const bool has_variables = !sig.GetVariables().empty();
	vector<VariableUse> variable_uses;
	if (has_variables) {
		for (auto &param : sig.GetParameters()) {
			variable_uses.push_back(GetVariableUse(sig, param.GetTypeName()));
		}
	}
	// a parameter reads as the kind of type a constrained variable stands for
	auto parameter_type_name = [&](idx_t param_idx) {
		const bool unconstrained = has_variables && variable_uses[param_idx].unconstrained;
		return unconstrained ? sig.GetParameter(param_idx).GetTypeName().ToString()
		                     : parameter_types[param_idx].ToString();
	};

	idx_t cost = 0;
	bool has_unknown_argument = false;
	// the arguments whose cost depends on what the type variables are determined to be, and what they cost as any type
	vector<pair<idx_t, idx_t>> variable_arguments;
	for (idx_t i = 0; i < received_count; i++) {
		auto &arg_type = call.GetArgument(i).type;
		const bool is_named = i >= received_positional_count;
		if (!is_named && arg_type.id() == LogicalTypeId::UNKNOWN) {
			has_unknown_argument = true;
			continue;
		}
		if (explicit_casts) {
			continue;
		}
		auto param_idx = get_parameter_index(i);
		const bool unconstrained = has_variables && variable_uses[param_idx].unconstrained;
		int64_t cast_cost;
		if (unconstrained && arg_type.id() == LogicalTypeId::STRING_LITERAL) {
			// a string literal can be cast to a type whatever its variables are determined to be
			auto literal_target = BindTypeNameInternal(
			    context, sig.GetParameter(param_idx).GetTypeName(), sig,
			    [](const Identifier &, LogicalTypeId constraint) {
				    return constraint == LogicalTypeId::INVALID ? LogicalType(LogicalType::VARCHAR)
				                                                : LogicalType(constraint);
			    },
			    BindValuePattern);
			cast_cost = ImplicitCastCost(context, arg_type, literal_target);
		} else {
			cast_cost = ImplicitCastCost(context, arg_type, parameter_types[param_idx]);
		}
		if (cast_cost < 0) {
			mismatch(FunctionMismatch::IncompatibleArgument {i, parameter_type_name(param_idx)});
			continue;
		}
		if (unconstrained && arg_type.id() != LogicalTypeId::SQLNULL) {
			variable_arguments.emplace_back(i, static_cast<idx_t>(cast_cost));
			continue;
		}
		cost += static_cast<idx_t>(cast_cost);
	}
	if (reason) {
		return FunctionMismatch(overload, std::move(function), mismatches, std::move(*reason));
	}

	// Only candidates whose casts match need a materialized argument-to-parameter mapping.
	FunctionBinding binding;
	binding.overload = overload;
	binding.function = function;
	binding.argument_to_parameters.reserve(received_count);
	for (idx_t i = 0; i < received_count; i++) {
		binding.argument_to_parameters.push_back(get_parameter_index(i));
	}
	FunctionVariables variables;
	if (has_variables) {
		// the shape and the casts match, so a variable that cannot be determined is the closest miss there is
		vector<bool> filled(sig.GetParameterCount(), false);
		for (auto param_idx : binding.argument_to_parameters) {
			filled[param_idx] = true;
		}
		auto variable_mismatch = DeduceVariables(context, sig, call, filled, binding, variables);
		if (variable_mismatch) {
			return FunctionMismatch(overload, std::move(function), 0, std::move(*variable_mismatch));
		}
		// only the types that use a variable are rebuilt, the others keep their aliases and extension info
		for (idx_t param_idx = 0; param_idx < sig.GetParameterCount(); param_idx++) {
			if (!variable_uses[param_idx].UsesAny()) {
				continue;
			}
			if (sig.GetParameter(param_idx).IsVariadic() && !filled[param_idx]) {
				// the call passes nothing to the parameter, so nothing determines its type
				continue;
			}
			auto &param_type = sig.GetParameter(param_idx).GetTypeName();
			optional_idx own_type;
			for (idx_t i = 0; i < binding.argument_to_parameters.size() && !own_type.IsValid(); i++) {
				if (binding.argument_to_parameters[i] == param_idx &&
				    IsTypeOfValues(context, sig, param_type, call.GetArgument(i).type)) {
					own_type = i;
				}
			}
			// an argument that is of the type already is taken as it is, the others are cast to that type
			parameter_types[param_idx] = own_type.IsValid() ? call.GetArgument(own_type.GetIndex()).type
			                                                : BindTypeName(context, param_type, sig, variables);
		}
		// An argument passed as a type variable is cast to the type the variable is determined to be. An overload
		// that takes any type is selected last, the more so the less the parameter says about the type
		for (auto &[i, pattern_cost] : variable_arguments) {
			auto param_idx = binding.argument_to_parameters[i];
			auto cast_cost = ImplicitCastCost(context, call.GetArgument(i).type, parameter_types[param_idx]);
			// the arguments determine the variable, so an argument is cast to it whether the cast is implicit or not
			cost += cast_cost < 0 ? pattern_cost : static_cast<idx_t>(cast_cost);
			cost += TYPE_VARIABLE_COST - variable_uses[param_idx].depth;
		}
	}
	binding.return_type = BindReturnType(context, sig, variables);
	for (auto &variable : sig.GetVariables()) {
		auto &variable_name = variable.GetName().GetIdentifierName();
		if (variable.IsValue()) {
			auto entry = variables.values.find(variable_name);
			if (entry != variables.values.end()) {
				binding.variable_values[variable_name] = entry->second;
			}
		} else if (variables.types.find(variable_name) != variables.types.end()) {
			binding.variable_types[variable_name] =
			    GetVariableType(variable.GetName(), sig.GetConstraint(variable.GetName()), variables);
		}
	}
	binding.parameter_types = std::move(parameter_types);

	// all arguments are implicitly castable - with a parameter of an unknown type, the cost is 0
	binding.cost = has_unknown_argument ? 0 : cost;
	return binding;
}

FunctionBindResult FunctionBinder::SelectFunction(optional_ptr<ClientContext> context, const BaseFunctionSet &functions,
                                                  const FunctionCallArguments &call, bool explicit_casts) {
	// Look for duplicate names
	identifier_set_t seen_names;
	for (idx_t i = call.GetPositionalCount(); i < call.GetArgumentCount(); i++) {
		if (!seen_names.insert(call.GetArgument(i).name).second) {
			// no overload can take the same argument twice
			auto kind = functions.GetOverloadCount() > 0 ? functions.GetOverload(0).GetKind() : FunctionKind::SCALAR;
			return FunctionBindError::CreateDuplicateArgument(functions.GetName(), call, i, kind);
		}
	}

	// Try to match each overload, keeping the ones that match at the lowest cost
	vector<FunctionBinding> cheapest;
	vector<FunctionMismatch> mismatches;
	for (idx_t overload = 0; overload < functions.GetOverloadCount(); overload++) {
		auto match = MatchOverload(context, overload, functions.GetSharedOverload(overload), call, explicit_casts);
		auto binding = std::get_if<FunctionBinding>(&match);
		if (!binding) {
			// Once an overload matches, unsuccessful candidates cannot contribute to the result or diagnostics.
			if (cheapest.empty()) {
				mismatches.push_back(std::move(std::get<FunctionMismatch>(match)));
			}
			continue;
		}

		// We only keep the cheap(est) matches
		if (!cheapest.empty() && cheapest[0] < *binding) {
			continue;
		}
		if (!cheapest.empty() && *binding < cheapest[0]) {
			cheapest.clear();
		}
		cheapest.push_back(std::move(*binding));
	}

	if (cheapest.empty()) {
		// Sort mismatches by closest distance
		std::stable_sort(mismatches.begin(), mismatches.end());
		return FunctionBindError::CreateNoMatch(functions.GetName(), call, std::move(mismatches));
	}
	if (cheapest.size() == 1) {
		return std::move(cheapest[0]);
	}
	// several overloads match at the lowest cost
	for (idx_t i = 0; i < call.GetPositionalCount(); i++) {
		if (call.GetArgument(i).type.IsUnknown()) {
			// the types of the prepared statement parameters may decide between them
			throw ParameterNotResolvedException();
		}
	}
	// the overload that declares the most about its arguments is the best match, if there is one
	for (idx_t i = 0; i < cheapest.size(); i++) {
		bool most_specific = true;
		for (idx_t j = 0; j < cheapest.size() && most_specific; j++) {
			most_specific = i == j || CompareSpecificity(cheapest[i], cheapest[j]) == Specificity::MORE;
		}
		if (most_specific) {
			return std::move(cheapest[i]);
		}
	}
	return FunctionBindError::CreateAmbiguousMatch(functions.GetName(), call, std::move(cheapest));
}

FunctionBinding FunctionBinder::MatchFunction(optional_ptr<ClientContext> context,
                                              shared_ptr<const SimpleFunction> function,
                                              const FunctionCallArguments &call) {
	auto name = function->GetName();
	auto match = MatchOverload(context, 0, std::move(function), call, true);
	auto binding = std::get_if<FunctionBinding>(&match);
	if (!binding) {
		vector<FunctionMismatch> mismatches;
		mismatches.push_back(std::move(std::get<FunctionMismatch>(match)));
		FunctionBindError::CreateNoMatch(std::move(name), call, std::move(mismatches)).Throw();
	}
	return std::move(*binding);
}

vector<LogicalType> FunctionBinding::GetArgumentTypes() const {
	auto &sig = function->GetSignature();
	vector<idx_t> received(sig.GetParameterCount(), 0);
	for (auto param_idx : argument_to_parameters) {
		received[param_idx]++;
	}
	vector<LogicalType> result;
	for (idx_t param_idx = 0; param_idx < sig.GetParameterCount(); param_idx++) {
		auto count = received[param_idx];
		if (count == 0 && !sig.GetParameter(param_idx).IsVariadic()) {
			// the call matches the function, so every parameter it leaves out has a default
			count = 1;
		}
		result.insert(result.end(), count, parameter_types[param_idx]);
	}
	return result;
}

template <class BOUND, class T>
BOUND FunctionBinder::CreateBoundFunction(const FunctionBinding &binding) {
	return BOUND(binding.GetFunction<T>(), binding.GetArgumentTypes(), binding.return_type);
}

BoundScalarFunction FunctionBinder::BindScalarFunction(optional_ptr<ClientContext> context,
                                                       const ScalarFunction &function,
                                                       const FunctionCallArguments &call) {
	auto binding = MatchFunction(context, make_shared_ptr<ScalarFunction>(function), call);
	return CreateBoundFunction<BoundScalarFunction, ScalarFunction>(binding);
}

BoundAggregateFunction FunctionBinder::BindAggregateFunction(optional_ptr<ClientContext> context,
                                                             const AggregateFunction &function,
                                                             const FunctionCallArguments &call) {
	auto binding = MatchFunction(context, make_shared_ptr<AggregateFunction>(function), call);
	return CreateBoundFunction<BoundAggregateFunction, AggregateFunction>(binding);
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
	return CreateBoundFunction<BoundWindowFunction, WindowFunction>(binding);
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

//! The types of the arguments a table function receives by position - the types plan serialization records
static vector<LogicalType> PositionalTypes(const FunctionSignature &signature, const vector<LogicalType> &types) {
	return vector<LogicalType>(types.begin(), types.begin() + NumericCast<vector<LogicalType>::difference_type>(
	                                                              signature.GetPositionalParameterCount()));
}

vector<LogicalType> FunctionBinder::BindParameterPatterns(optional_ptr<ClientContext> context,
                                                          const FunctionSignature &signature) {
	vector<LogicalType> result;
	result.reserve(signature.GetParameterCount());
	for (auto &param : signature.GetParameters()) {
		result.push_back(BindTypePattern(context, param.GetTypeName(), signature));
	}
	return result;
}

LogicalType FunctionBinder::BindReturnPattern(optional_ptr<ClientContext> context, const FunctionSignature &signature) {
	auto &return_type = signature.GetReturnType();
	return return_type.IsType() ? BindTypePattern(context, return_type.GetTypeName(), signature)
	                            : BindReturnType(context, signature, FunctionVariables());
}

LogicalType FunctionBinder::BindReturnType(optional_ptr<ClientContext> context, const FunctionSignature &signature,
                                           const FunctionVariables &variables) {
	auto &return_type = signature.GetReturnType();
	switch (return_type.GetKind()) {
	case ReturnType::Kind::TYPE:
		return BindTypeName(context, return_type.GetTypeName(), signature, variables);
	case ReturnType::Kind::ANY:
		return LogicalType::ANY;
	case ReturnType::Kind::TABLE:
	case ReturnType::Kind::NONE:
		return LogicalType::INVALID;
	case ReturnType::Kind::EXPRESSION:
		throw NotImplementedException("A return type that is computed by an expression");
	default:
		throw InternalException("Unknown ReturnType kind");
	}
}

BoundTableFunction FunctionBinder::BindTableFunction(optional_ptr<ClientContext> context, const TableFunction &function,
                                                     const FunctionCallArguments &call) {
	auto binding = MatchFunction(context, make_shared_ptr<const TableFunction>(function), call);
	auto definition = binding.GetFunction<TableFunction>();
	auto arguments = PositionalTypes(definition->GetSignature(), binding.parameter_types);
	return BoundTableFunction(std::move(definition), std::move(arguments), binding.parameter_types);
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
		auto result = SelectFunction(context, functions, {positional, named});
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
			auto retry_result = SelectFunction(context, functions, {positional, named});
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

//! Resolve any ANY in the target type by substituting the matching part of the source, so we never cast a value to a
//! type that still contains ANY - ANY has no physical representation, so building such a vector throws. This happens
//! e.g. when an ARRAY argument fills a LIST(ANY) parameter (the file list of the multi-file readers).
static LogicalType ResolveAnyInParameterType(const LogicalType &target, const LogicalType &source) {
	if (target.id() == LogicalTypeId::ANY) {
		return source;
	}
	if (target.id() == LogicalTypeId::LIST &&
	    (source.id() == LogicalTypeId::LIST || source.id() == LogicalTypeId::ARRAY)) {
		auto &source_child =
		    source.id() == LogicalTypeId::LIST ? ListType::GetChildType(source) : ArrayType::GetChildType(source);
		return LogicalType::LIST(ResolveAnyInParameterType(ListType::GetChildType(target), source_child));
	}
	return target;
}

Value FunctionBinder::CastToParameterType(ClientContext &context, Value value, const LogicalType &parameter_type) {
	if (RequiresCast(value.type(), parameter_type) == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
		return value.CastAs(context, ResolveAnyInParameterType(parameter_type, value.type()));
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

//! The arguments of a call in the order of the signature: [standard | *args | keyword-only | **kwargs], with the
//! defaults of the parameters the call leaves out inserted
struct ResolvedArguments {
	vector<unique_ptr<Expression>> arguments;
	//! The type of the parameter each argument fills, with the templates substituted
	vector<LogicalType> types;
	//! The leading arguments that are passed by position, and the names of the rest - keyword-only or "**kwargs"
	idx_t positional_count = 0;
	vector<Identifier> named_argument_names;
};

//! Drain the arguments of a call into the order of the signature of the function it binds to
static ResolvedArguments ResolveArguments(const SimpleFunction &function, const FunctionBinding &binding,
                                          vector<unique_ptr<Expression>> &arguments,
                                          vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	const auto &sig = function.GetSignature();
	const auto received_positional_count = arguments.size();

	// the arguments every parameter receives, in the order they were passed
	vector<vector<idx_t>> received(sig.GetParameterCount());
	for (idx_t i = 0; i < binding.argument_to_parameters.size(); i++) {
		received[binding.argument_to_parameters[i]].push_back(i);
	}

	ResolvedArguments result;
	for (idx_t param_idx = 0; param_idx < sig.GetParameterCount(); param_idx++) {
		const auto &param = sig.GetParameter(param_idx);
		for (auto i : received[param_idx]) {
			if (param.GetKind() == FunctionParameterKind::VAR_KEYWORD) {
				result.named_argument_names.push_back(named_arguments[i - received_positional_count].first);
			}
			auto &argument =
			    i < received_positional_count ? arguments[i] : named_arguments[i - received_positional_count].second;
			result.arguments.push_back(std::move(argument));
		}
		if (!param.IsVariadic() && received[param_idx].empty()) {
			// the call matches the function, so every parameter it leaves out has a default
			auto default_arg = make_uniq<BoundConstantExpression>(*param.GetDefaultValue(), param.GetName());
			result.arguments.push_back(std::move(default_arg));
		}
		if (param.GetKind() == FunctionParameterKind::KEYWORD_ONLY) {
			result.named_argument_names.push_back(param.GetName());
		}
	}
	result.types = binding.GetArgumentTypes();
	for (auto &type : result.types) {
		if (TypeVisitor::Contains(type, LogicalTypeId::UNKNOWN)) {
			// no argument determines the type of an unresolved parameter - the call is bound again at EXECUTE
			throw ParameterNotResolvedException();
		}
	}
	result.positional_count = result.arguments.size() - result.named_argument_names.size();
	arguments.clear();
	named_arguments.clear();
	return result;
}

//! Fold the arguments of a table or pragma function to the constants it is called with. The arguments passed by
//! position fill "parameters", so "range(1, col1 := 5)" reaches the callback exactly as "range(1, 5)" does. The
//! keyword-only and "**kwargs" arguments go into "named_parameters" - an option "**kwargs" declares by the name of the
//! option, cast to its type like an explicit cast, as it takes no part in selection
static void FoldArguments(ClientContext &context, const SimpleFunction &function, ResolvedArguments &resolved,
                          vector<Value> &parameters, named_argument_map_t &named_parameters) {
	auto &signature = function.GetSignature();
	auto option_schema = signature.GetTypedKwargs();
	for (idx_t i = 0; i < resolved.arguments.size(); i++) {
		auto &argument = *resolved.arguments[i];
		if (i < resolved.positional_count) {
			// A TABLE parameter is filled by the subquery itself rather than by a value, so its slot holds a plain
			// NULL. A NULL of type TABLE would not do: TABLE has no physical type, so it cannot be deserialized
			parameters.push_back(resolved.types[i].id() == LogicalTypeId::TABLE ? Value()
			                                                                    : FoldArgument(context, argument));
			continue;
		}
		auto &name = resolved.named_argument_names[i - resolved.positional_count];
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
		named_parameters.insert(make_pair(option.name, PlaceOption(context, argument, option)));
	}
}

unique_ptr<ResolvedTableFunction>
FunctionBinder::BindTableFunction(const TableFunctionSet &functions,
                                  vector<unique_ptr<Expression>> &positional_arguments,
                                  vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error) {
	VerifyNamedArgumentsAccepted(functions, named_arguments);
	auto selection = SelectFunction(context, functions, {positional_arguments, named_arguments});
	if (!selection) {
		error = selection.GetError();
		return nullptr;
	}
	auto &binding = selection.GetBinding();
	auto definition = binding.GetFunction<TableFunction>();
	auto resolved = ResolveArguments(*definition, binding, positional_arguments, named_arguments);
	CastToFunctionArguments(definition->GetName(), resolved.types, resolved.arguments);
	vector<Value> parameters;
	named_argument_map_t named_parameters;
	FoldArguments(context, *definition, resolved, parameters, named_parameters);
	// copied out of the set: binding the table function fills in the bound return types
	return make_uniq<ResolvedTableFunction>(
	    BoundTableFunction(definition, PositionalTypes(definition->GetSignature(), binding.parameter_types),
	                       binding.parameter_types),
	    std::move(parameters), std::move(named_parameters));
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
	auto selection = SelectFunction(context, functions, {input_arguments, named_arguments});
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
	auto resolved = ResolveArguments(*definition, binding, input_arguments, named_arguments);
	// Input columns remain in the subquery; only named options are cast and folded here.
	const auto positional_count =
	    NumericCast<vector<unique_ptr<Expression>>::difference_type>(resolved.positional_count);
	resolved.arguments.erase(resolved.arguments.begin(), resolved.arguments.begin() + positional_count);
	resolved.types.erase(resolved.types.begin(), resolved.types.begin() + positional_count);
	resolved.positional_count = 0;
	CastToFunctionArguments(definition->GetName(), resolved.types, resolved.arguments);
	vector<Value> parameters;
	named_argument_map_t named_parameters;
	FoldArguments(context, *definition, resolved, parameters, named_parameters);
	return make_uniq<ResolvedTableFunction>(
	    BoundTableFunction(definition, PositionalTypes(definition->GetSignature(), binding.parameter_types),
	                       binding.parameter_types),
	    std::move(parameters), std::move(named_parameters));
}

unique_ptr<BoundPragmaInfo> FunctionBinder::BindPragmaFunction(
    const PragmaFunctionSet &functions, vector<unique_ptr<Expression>> &positional_arguments,
    vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error) {
	VerifyNamedArgumentsAccepted(functions, named_arguments);
	auto selection = SelectFunction(context, functions, {positional_arguments, named_arguments});
	if (!selection) {
		error = selection.GetError();
		return nullptr;
	}
	auto &binding = selection.GetBinding();
	auto definition = binding.GetFunction<PragmaFunction>();
	auto resolved = ResolveArguments(*definition, binding, positional_arguments, named_arguments);
	CastToFunctionArguments(definition->GetName(), resolved.types, resolved.arguments);
	vector<Value> parameters;
	named_argument_map_t named_parameters;
	FoldArguments(context, *definition, resolved, parameters, named_parameters);
	return make_uniq<BoundPragmaInfo>(*definition, std::move(parameters), std::move(named_parameters),
	                                  binding.parameter_types);
}

//! Whether the type has no concrete type information (e.g. DECIMAL without width / scale) - ANY, UNKNOWN and INVALID
//! are excluded, as casts to these have special meaning (e.g. for prepared statement parameters)
static bool IsIncompleteCastTarget(const LogicalType &type) {
	if (type.IsComplete()) {
		return false;
	}
	return !TypeVisitor::Contains(type, [](const LogicalType &child) {
		return child.id() == LogicalTypeId::ANY || child.id() == LogicalTypeId::UNKNOWN ||
		       child.id() == LogicalTypeId::INVALID;
	});
}

void FunctionBinder::CastToFunctionArguments(const Identifier &function_name, vector<LogicalType> &argument_types,
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
		if (children[i]->GetReturnType().id() == LogicalTypeId::UNKNOWN && !target_type.IsComplete()) {
			// bind callbacks only see concrete types - an unresolved parameter is bound again at EXECUTE
			throw ParameterNotResolvedException();
		}
		target_type.Verify();
		// don't cast lambda children, their slot only ever holds a placeholder
		if (children[i]->GetReturnType().id() == LogicalTypeId::LAMBDA) {
			continue;
		}
		const bool target_has_any = TypeVisitor::Contains(target_type, LogicalTypeId::ANY);
		if (target_has_any && children[i]->GetReturnType().id() == LogicalTypeId::ARRAY &&
		    target_type.id() == LogicalTypeId::LIST) {
			// an array passed to a LIST(ANY) parameter is cast to a list of its child type
			children[i] = BoundCastExpression::AddArrayCastToList(context, std::move(children[i]));
		}
		// check if the type of child matches the type of function argument
		// if not we need to add a cast
		auto cast_result = RequiresCast(children[i]->GetReturnType(), target_type);
		// except for one special case: if the function accepts ANY argument
		// in that case we don't add a cast
		if (cast_result == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
			if (target_has_any) {
				// a type that is not concrete cannot be cast to (e.g. a NULL or a VARIANT passed to LIST(ANY)) - the
				// function has to handle the argument as-is
				continue;
			}
			if (IsIncompleteCastTarget(target_type)) {
				throw InternalException("Function %s has incomplete argument type %s - concrete argument types must "
				                        "be resolved in the resolve_types callback",
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

//! The type of the NULL constant a function call is folded to - SQLNULL if the return type could not be determined
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
		// the type the function declares, whatever the arguments determine it to be
		const auto return_type = BindReturnPattern(&context, bound_function.GetSignature());
		if (HasUntypedNullArgument(regular_args, keyword_args)) {
			return make_uniq<BoundConstantExpression>(Value(NullReturnType(return_type)), std::move(alias),
			                                          query_location);
		}
		if (HasNullConstantArgument(context, regular_args, keyword_args, binder)) {
			if (return_type.IsComplete()) {
				auto null_function = CreateBoundFunction<BoundScalarFunction, ScalarFunction>(binding);
				for (auto &child : keyword_args) {
					regular_args.push_back(std::move(child.second));
				}
				PropagateReturnCollation(null_function, null_function.GetProperties(), regular_args, string());
				return make_uniq<BoundConstantExpression>(Value(null_function.GetReturnType()), std::move(alias),
				                                          query_location);
			}
			auto resolved = ResolveScalarFunctionTypes(binding, regular_args, keyword_args);
			PropagateReturnCollation(resolved.bound_function, resolved.bound_function.GetProperties(), regular_args,
			                         resolved.pushed_collation);
			return make_uniq<BoundConstantExpression>(Value(NullReturnType(resolved.bound_function.GetReturnType())),
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
		auto &target_type = argument_types[i].IsComplete() ? argument_types[i] : source_type;
		ExtractCollationFromType(source_type, target_type, collations);
	}
	if (is_index_key && collations.collation.empty() && collations.fields.empty()) {
		// index keys only use explicit collations, comparison keys and the default collation would change them
		return string();
	}
	// push collations to the children
	for (idx_t i = 0; i < children.size(); i++) {
		auto &arg = children[i];
		auto &arg_type = argument_types[i];
		auto &target_type = arg_type.IsComplete() ? arg_type : arg->GetReturnType();
		// apply the collations to the (possibly nested) varchar leaves of the argument type
		auto collated_type = ApplyCollationToType(target_type, collations);
		arg = BoundCastExpression::AddCastToType(context, std::move(arg), collated_type);
		if (RequiresCollationPropagation(arg->GetReturnType())) {
			// if this is a varchar type - propagate the collation
			arg->SetReturnType(collated_type);
		}
		// now push the actual collation handling
		if (ExpressionBinder::PushCollation(context, arg, collated_type, type)) {
			// Normalization can change field types, so do not cast back to the original argument type.
			arg_type = arg->GetReturnType();
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
		throw InternalException("Function %s added arguments in its bind callback - argument types must be "
		                        "resolved in the resolve_types callback",
		                        bound_function.GetName());
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
			                        "callback - argument types must be resolved in the resolve_types callback",
			                        bound_function.GetName(), i + 1, source_type, target_type);
		}
	}
}

//! Incomplete argument types (e.g. a DECIMAL without width and scale) accept any type of that kind - resolve them to
//! the type of the argument
static void ResolveIncompleteArgumentTypes(BoundSimpleFunction &bound_function,
                                           const vector<unique_ptr<Expression>> &arguments) {
	auto &argument_types = bound_function.GetArguments();
	for (idx_t i = 0; i < arguments.size() && i < argument_types.size(); i++) {
		auto &source_type = arguments[i]->GetReturnType();
		if (!IsIncompleteCastTarget(argument_types[i]) || source_type.id() != argument_types[i].id() ||
		    !source_type.IsComplete()) {
			continue;
		}
		argument_types[i] = source_type;
	}
}

static vector<LogicalType> CaptureLogicalArguments(const BoundSimpleFunction &function,
                                                   const vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(function.GetArguments().size() == arguments.size());
	vector<LogicalType> result;
	result.reserve(arguments.size());
	for (idx_t argument_index = 0; argument_index < arguments.size(); argument_index++) {
		auto target_type = function.GetArguments()[argument_index];
		result.push_back(target_type.IsComplete() ? target_type : arguments[argument_index]->GetReturnType());
	}
	return result;
}

//! Let the signature resolve the return type from the resolved arguments of the call
static void ResolveReturnType(ClientContext &context, const SimpleFunction &function,
                              BoundSimpleFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
	auto &signature = function.GetSignature();
	auto resolver = signature.GetReturnType().GetResolver();
	if (!resolver) {
		return;
	}
	ResolveReturnTypeInput input(context, bound_function, signature, arguments);
	bound_function.SetReturnType(resolver(input));
}

ResolvedScalarFunctionTypes
FunctionBinder::ResolveScalarFunctionTypes(const FunctionBinding &binding, vector<unique_ptr<Expression>> &arguments,
                                           vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	auto &function = binding.GetFunction();
	// Make a BoundScalarFunction out of the ScalarFunction, so we can store bind info and other properties in it.
	ResolvedScalarFunctionTypes result {CreateBoundFunction<BoundScalarFunction, ScalarFunction>(binding), {}};
	auto &bound_function = result.bound_function;

	// Reorder named args and expand the variadic arguments
	auto resolved = ResolveArguments(function, binding, arguments, named_arguments);
	bound_function.SetNamedArguments(resolved.positional_count, std::move(resolved.named_argument_names));
	arguments = std::move(resolved.arguments);

	bound_function.SetLogicalArguments(CaptureLogicalArguments(bound_function, arguments));
	ResolveIncompleteArgumentTypes(bound_function, arguments);

	if (bound_function.HasResolveTypesCallback()) {
		ResolveScalarFunctionTypesInput input(context, bound_function, arguments);
		bound_function.GetResolveTypesCallback()(input);
		VerifyArgumentCount(bound_function, arguments.size());
	}
	const bool is_index_key = arguments_collated || (binder && binder->IsBindingIndexKey());
	result.pushed_collation =
	    PushArgumentCollations(context, bound_function, bound_function.GetProperties(), arguments, is_index_key);
	CastToFunctionArguments(bound_function.GetName(), bound_function.GetArguments(), arguments);
	ResolveReturnType(context, function, bound_function, arguments);
	return result;
}

pair<BoundScalarFunction, unique_ptr<FunctionData>>
FunctionBinder::ResolveScalarFunction(const FunctionBinding &binding, vector<unique_ptr<Expression>> &arguments,
                                      vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments) {
	auto resolved = ResolveScalarFunctionTypes(binding, arguments, named_arguments);
	auto &bound_function = resolved.bound_function;

	// The bind callback is called with arguments that are cast to the argument types of the bound function
	unique_ptr<FunctionData> bind_info;
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
		BindScalarFunctionInput input(context, bound_function, arguments, binder);
		bind_info = bound_function.GetBindCallback()(input);
		VerifyArgumentCount(bound_function, arguments.size());
		VerifyBindArgumentTypes(bound_function, arguments);
	}

	if (bound_function.HasModifiedDatabasesCallback() && binder) {
		auto &properties = binder->GetStatementProperties();
		FunctionModifiedDatabasesInput input(bind_info, properties);
		bound_function.GetModifiedDatabasesCallback()(context, input);
	}

	PropagateReturnCollation(bound_function, bound_function.GetProperties(), arguments, resolved.pushed_collation);
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
	auto binding = MatchFunction(&context, function, {children, keyword_args});
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
		                                  result_func->GetChildrenMutable());
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
	auto bound_function = CreateBoundFunction<BoundAggregateFunction, AggregateFunction>(binding);

	// Reorder named args and expand the variadic arguments
	auto resolved = ResolveArguments(function, binding, children, named_arguments);
	bound_function.SetNamedArguments(resolved.positional_count, std::move(resolved.named_argument_names));
	children = std::move(resolved.arguments);

	bound_function.SetLogicalArguments(CaptureLogicalArguments(bound_function, children));

	unique_ptr<FunctionData> bind_info;

	ResolveReturnType(context, function, bound_function, children);
	if (bound_function.GetCallbacks().HasBindCallback()) {
		BindAggregateFunctionInput input(context, bound_function, children);
		bind_info = bound_function.GetCallbacks().GetBindCallback()(input);

		// we may have lost some arguments in the bind
		children.resize(MinValue(bound_function.GetArguments().size(), children.size()));
		VerifyArgumentCount(bound_function, children.size());
	}

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
	auto binding = MatchFunction(&context, function, {children, keyword_args});
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
		binding = MatchFunction(&context, function, {children, keyword_args});
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
	auto bound_function = CreateBoundFunction<BoundWindowFunction, WindowFunction>(binding);

	// Reorder named args and expand the variadic arguments
	auto resolved = ResolveArguments(function, binding, children, named_arguments);
	bound_function.SetNamedArguments(resolved.positional_count, std::move(resolved.named_argument_names));
	children = std::move(resolved.arguments);

	auto logical_arguments = CaptureLogicalArguments(bound_function, children);
	auto argument_types = bound_function.GetArguments();

	unique_ptr<FunctionData> bind_info;

	ResolveReturnType(context, function, bound_function, children);
	if (bound_function.HasBindCallback()) {
		BindWindowFunctionInput input(context, bound_function, children, order_types, arg_order_types);
		bind_info = bound_function.GetBindCallback()(input);
		// we may have lost some arguments in the bind
		children.resize(MinValue(bound_function.GetArguments().size(), children.size()));
		VerifyArgumentCount(bound_function, children.size());
	}

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
	auto binding = MatchFunction(&context, function, {children, keyword_args});
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
