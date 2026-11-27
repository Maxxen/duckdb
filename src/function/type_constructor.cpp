#include "duckdb/function/type_constructor.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {

namespace {

LogicalType BindIdentityType(BindLogicalTypeInput &input) {
	return input.base_type;
}

} // namespace

FunctionSignature TypeConstructor::Signature() {
	return FunctionSignature(vector<FunctionParameter>(), LogicalType::TYPE());
}

TypeConstructor::TypeConstructor(Identifier name, FunctionSignature signature_p, bind_logical_type_function_t bind)
    : SimpleFunction(std::move(name), std::move(signature_p)), bind_function(bind) {
	D_ASSERT(bind_function);
	signature.Verify();
}

TypeConstructor::TypeConstructor(FunctionSignature signature_p, bind_logical_type_function_t bind)
    : TypeConstructor(Identifier(), std::move(signature_p), bind) {
}

TypeConstructor TypeConstructor::Identity(Identifier name) {
	return TypeConstructor(std::move(name), Signature(), BindIdentityType);
}

TypeConstructor TypeConstructor::Unchecked(Identifier name, bind_logical_type_function_t bind) {
	// accepts anything, the bind function validates the arguments itself
	auto signature = Signature();
	signature.AddPack("T").AddArgs("args", TypeName::PACK("T"));
	signature.AddNamedPack("KV").AddKwargs("kwargs", TypeName::NAMED_PACK("KV"));
	return TypeConstructor(std::move(name), std::move(signature), bind);
}

TypeConstructorSet::TypeConstructorSet() : FunctionSet<TypeConstructor>(Identifier()) {
}

TypeConstructorSet::TypeConstructorSet(Identifier name) : FunctionSet<TypeConstructor>(std::move(name)) {
}

namespace {

//! The type an argument is matched against. A modifier is always a constant, so an integer one matches as an
//! integer literal exactly as it would in a function call - that is what lets DECIMAL(10) bind to a UTINYINT
//! parameter, as the literal is known to fit. Unlike ExpressionBinder::GetExpressionReturnType this does not treat
//! strings as literals: a string literal is implicitly castable to anything, which would let DECIMAL('32') through.
//! Only overload matching uses this; errors report the value's own type.
LogicalType MatchType(const TypeArgument &arg) {
	auto &value = arg.GetValue();
	auto &type = value.type();
	if (type.IsIntegral() && !value.IsNull()) {
		return LogicalType::INTEGER_LITERAL(value);
	}
	return type;
}

//! How a constructor reads in an error: MAP(key TYPE, value TYPE). Deliberately not SimpleFunction::ToString(),
//! which quotes identifiers and appends the "-> TYPE" return that every constructor trivially shares.
string ConstructorToString(const Identifier &type_name, const TypeConstructor &constructor) {
	const auto &sig = constructor.GetSignature();
	vector<string> parts;
	for (auto &param : sig.GetParameters()) {
		switch (param.GetKind()) {
		case FunctionParameterKind::VAR_POSITIONAL:
			parts.push_back(param.GetTypeName().ToString() + "...");
			continue;
		case FunctionParameterKind::VAR_KEYWORD:
			parts.push_back(param.GetName().GetIdentifierName() + " := " + param.GetTypeName().ToString() + "...");
			continue;
		default:
			break;
		}
		string part = param.GetName().GetIdentifierName() + " " + param.GetTypeName().ToString();
		if (param.HasDefaultValue()) {
			part += " := " + param.GetDefaultValue()->ToString();
		}
		parts.push_back(std::move(part));
	}
	return type_name.GetIdentifierName() + "(" + StringUtil::Join(parts, ", ") + ")";
}

//! How a modifier is referred to in errors: by its parameter name where it has one, by its position otherwise.
string ArgumentName(const Identifier &name, idx_t position) {
	if (name.empty()) {
		return StringUtil::Format("%llu", position + 1);
	}
	return StringUtil::Format("%s", name);
}

//! Cast a modifier to the type its parameter declares. Overload selection already established the cast is possible.
Value CastArgument(const Identifier &type_name, const string &arg_name, const Value &value, const LogicalType &target,
                   QueryLocation location) {
	if (value.IsNull()) {
		throw BinderException(location, "Type parameter %s for type %s cannot be NULL", arg_name, type_name);
	}
	if (target.id() == LogicalTypeId::ANY || value.type() == target) {
		return value;
	}
	string error;
	auto result = value.DefaultTryCastAs(target, &error);
	if (!result) {
		throw BinderException(location,
		                      "Type parameter %s for type %s must be of type %s, but %s could not be converted: %s",
		                      arg_name, type_name, target.ToString(), value.ToString(), error);
	}
	return *result;
}

//! The modifiers a constructor is bound with: the call arguments in the order of the parameters that receive them,
//! with the default of every parameter the call leaves out, each cast to the type it is passed as. What "*args" and
//! "**kwargs" receive follows in call order, with its name preserved.
vector<TypeArgument> NormalizeArguments(const Identifier &type_name, const FunctionBinding &binding,
                                        const vector<TypeArgument> &arguments, QueryLocation type_location) {
	const auto &sig = binding.GetFunction().GetSignature();
	vector<TypeArgument> result;
	for (auto &bound : binding.layout.arguments) {
		auto &param = sig.GetParameter(bound.parameter_index);
		if (!bound.argument_index.IsValid()) {
			// a parameter filled in from its default has no source text - point at the type instead
			auto arg_name = ArgumentName(param.GetName(), bound.parameter_index);
			result.emplace_back(param.GetName().GetIdentifierName(),
			                    CastArgument(type_name, arg_name, *param.GetDefaultValue(), bound.type, type_location),
			                    type_location);
			continue;
		}
		// the positional arguments of the call come before the named ones, so this is the argument's index in the call
		auto position = bound.argument_index.GetIndex();
		auto &arg = arguments[position];
		auto location = arg.GetQueryLocation();
		if (param.IsVariadic()) {
			// referred to by the name it was passed with, or by its position in the call
			auto arg_name = ArgumentName(Identifier(arg.GetName()), position);
			result.emplace_back(arg.GetName(), CastArgument(type_name, arg_name, arg.GetValue(), bound.type, location),
			                    location);
			continue;
		}
		auto arg_name = ArgumentName(param.GetName(), bound.parameter_index);
		result.emplace_back(param.GetName().GetIdentifierName(),
		                    CastArgument(type_name, arg_name, arg.GetValue(), bound.type, location), location);
	}
	return result;
}

} // namespace

string TypeConstructor::ToString() const {
	return ConstructorToString(GetName(), *this);
}

LogicalType TypeConstructorSet::Bind(optional_ptr<ClientContext> context, const LogicalType &base_type,
                                     const vector<TypeArgument> &arguments, QueryLocation query_location,
                                     optional_ptr<const DecimalWidthLimit> decimal_width_limit) const {
	D_ASSERT(!functions.empty());

	// Fast path for the common case: a single constructor taking no modifiers, which is every type that is not
	// parameterised. It also gets a dedicated error, as the generic one would list a single empty candidate.
	if (functions.size() == 1) {
		const auto &sig = functions[0]->GetSignature();
		if (sig.GetParameterCount() == 0) {
			if (!arguments.empty()) {
				throw BinderException(query_location, "Type %s does not take any type parameters", name);
			}
			vector<TypeArgument> modifiers;
			BindLogicalTypeInput input {context, base_type, modifiers, query_location, decimal_width_limit};
			return functions[0]->GetBindFunction()(input);
		}
	}

	vector<LogicalType> positional;
	vector<pair<Identifier, LogicalType>> named;
	for (auto &arg : arguments) {
		if (arg.HasName()) {
			named.emplace_back(Identifier(arg.GetName()), MatchType(arg));
		} else if (named.empty()) {
			positional.push_back(MatchType(arg));
		} else {
			throw BinderException(arg.GetQueryLocation(),
			                      "Type parameter %s for type %s cannot follow a named type parameter",
			                      arg.GetValue().ToString(), name);
		}
	}

	// The candidates are selected with the regular function machinery - its errors name the call, not a function
	auto selection = FunctionBinder::SelectFunction(context, *this, {positional, named});
	if (!selection) {
		ErrorData error = selection.GetError();
		error.AddQueryLocation(query_location);
		error.Throw();
	}
	auto &binding = selection.GetBinding();
	auto constructor_ptr = binding.GetFunction<TypeConstructor>();
	auto &constructor = *constructor_ptr;

	auto modifiers = NormalizeArguments(name, binding, arguments, query_location);
	BindLogicalTypeInput input {context, base_type, modifiers, query_location, decimal_width_limit};
	return constructor.GetBindFunction()(input);
}

} // namespace duckdb
