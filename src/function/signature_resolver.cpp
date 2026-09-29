#include "duckdb/function/signature_resolver.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/function/type_constructor.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// LogicalType -> TypeName
//===--------------------------------------------------------------------===//
namespace {

class TypeConverter {
public:
	explicit TypeConverter(vector<TypeVariable> &variables_p) : variables(variables_p) {
	}

	TypeName Convert(const LogicalType &type) {
		if (type.id() == LogicalTypeId::INVALID) {
			return TypeName();
		}
		auto alias = type.GetAlias();
		if (!alias.empty()) {
			vector<TypeParam> params;
			if (type.HasExtensionInfo()) {
				for (auto &modifier : type.GetExtensionInfo()->modifiers) {
					params.push_back(ConvertValue(modifier.value));
				}
			}
			return TypeName(QualifiedName(Identifier(alias)), std::move(params));
		}
		switch (type.id()) {
		case LogicalTypeId::TEMPLATE:
			return DeclareTemplate(TemplateType::GetName(type));
		case LogicalTypeId::ANY:
			return ConvertAny(type);
		case LogicalTypeId::DECIMAL:
			if (!type.AuxInfo() || DecimalType::GetWidth(type) == 0) {
				return Family("DECIMAL",
				              {Variable("W", TypeVariableKind::INTEGER), Variable("S", TypeVariableKind::INTEGER)});
			}
			return Builtin(type.id(), {TypeParam::Integer(DecimalType::GetWidth(type)),
			                           TypeParam::Integer(DecimalType::GetScale(type))});
		case LogicalTypeId::LIST:
			if (!type.AuxInfo()) {
				return Family("LIST", {Variable("T", TypeVariableKind::TYPE)});
			}
			return Builtin(type.id(), {TypeParam::Type(Convert(ListType::GetChildType(type)))});
		case LogicalTypeId::ARRAY: {
			if (!type.AuxInfo()) {
				return Family("ARRAY",
				              {Variable("T", TypeVariableKind::TYPE), Variable("N", TypeVariableKind::INTEGER)});
			}
			auto child = TypeParam::Type(Convert(ArrayType::GetChildType(type)));
			if (ArrayType::IsAnySize(type)) {
				return Builtin(type.id(), {std::move(child), Variable("N", TypeVariableKind::INTEGER)});
			}
			return Builtin(type.id(),
			               {std::move(child), TypeParam::Integer(NumericCast<int64_t>(ArrayType::GetSize(type)))});
		}
		case LogicalTypeId::MAP:
			if (!type.AuxInfo()) {
				return Family("MAP", {Variable("K", TypeVariableKind::TYPE), Variable("V", TypeVariableKind::TYPE)});
			}
			return Builtin(type.id(), {TypeParam::Type(Convert(MapType::KeyType(type))),
			                           TypeParam::Type(Convert(MapType::ValueType(type)))});
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::TUPLE: {
			if (!type.AuxInfo()) {
				return Family(TypeName::BuiltinName(type.id()),
				              {Variable("T", TypeVariableKind::TYPE, TypeVariableArity::PACK)});
			}
			vector<TypeParam> params;
			for (auto &child : StructType::GetChildTypes(type)) {
				auto name = type.id() == LogicalTypeId::TUPLE ? Identifier() : child.first;
				params.push_back(TypeParam::Type(Convert(child.second), std::move(name)));
			}
			return Builtin(type.id(), std::move(params));
		}
		case LogicalTypeId::UNION: {
			if (!type.AuxInfo()) {
				return Family("UNION", {Variable("T", TypeVariableKind::TYPE, TypeVariableArity::PACK)});
			}
			vector<TypeParam> params;
			for (idx_t i = 0; i < UnionType::GetMemberCount(type); i++) {
				params.push_back(
				    TypeParam::Type(Convert(UnionType::GetMemberType(type, i)), UnionType::GetMemberName(type, i)));
			}
			return Builtin(type.id(), std::move(params));
		}
		case LogicalTypeId::ENUM: {
			if (!type.AuxInfo()) {
				return Family("ENUM", {Variable("E", TypeVariableKind::STRING, TypeVariableArity::PACK)});
			}
			vector<TypeParam> params;
			for (idx_t i = 0; i < EnumType::GetSize(type); i++) {
				params.push_back(TypeParam::String(EnumType::GetString(type, i).GetString()));
			}
			return Builtin(type.id(), std::move(params));
		}
		case LogicalTypeId::VARCHAR: {
			auto collation = type.AuxInfo() ? StringType::GetCollation(type) : string();
			if (collation.empty()) {
				return Builtin(type.id(), {});
			}
			return Builtin(type.id(), {TypeParam::String(collation, Identifier("collation"))});
		}
		case LogicalTypeId::GEOMETRY:
			if (type.AuxInfo() && GeoType::HasCRS(type)) {
				return Builtin(type.id(), {TypeParam::String(GeoType::GetCRS(type).GetDefinition())});
			}
			return Builtin(type.id(), {});
		default:
			break;
		}
		auto name = TypeName::BuiltinName(type.id());
		if (!name || type.id() == LogicalTypeId::SQLNULL) {
			if (type.AuxInfo()) {
				throw InternalException("Type '%s' cannot be used in a function signature", type.ToString());
			}
			return Pseudo(type.id(), {});
		}
		return Builtin(type.id(), {});
	}

private:
	TypeParam ConvertValue(const Value &value) {
		if (value.type().IsIntegral()) {
			return TypeParam::Integer(value.GetValue<int64_t>());
		}
		if (value.type().id() == LogicalTypeId::VARCHAR) {
			return TypeParam::String(StringValue::Get(value));
		}
		throw InternalException("Type modifier '%s' cannot be used in a function signature", value.ToString());
	}

	TypeName ConvertAny(const LogicalType &type) {
		if (!type.AuxInfo()) {
			return Pseudo(type.id(), {});
		}
		auto target = AnyType::GetTargetType(type);
		auto cast_score = AnyType::GetCastScore(type);
		return Pseudo(type.id(),
		              {TypeParam::Type(Convert(target)), TypeParam::Integer(NumericCast<int64_t>(cast_score))});
	}

	static TypeName Builtin(LogicalTypeId id, vector<TypeParam> params) {
		return TypeName(QualifiedName(Identifier(TypeName::BuiltinName(id))), std::move(params));
	}

	static TypeName Pseudo(LogicalTypeId id, vector<TypeParam> params) {
		return TypeName(
		    QualifiedName(vector<Identifier> {TypeName::PseudoTypeQualifier()}, Identifier(EnumUtil::ToString(id))),
		    std::move(params));
	}

	static TypeName Family(const char *name, vector<TypeParam> params) {
		return TypeName(QualifiedName(Identifier(name)), std::move(params));
	}

	TypeName DeclareTemplate(const string &name) {
		Identifier identifier(name);
		for (auto &variable : variables) {
			if (variable.GetName() == identifier) {
				if (variable.GetKind() != TypeVariableKind::TYPE || variable.GetArity() != TypeVariableArity::SINGLE) {
					throw InternalException("Template type '%s' conflicts with type variable '%s'", name,
					                        variable.ToString());
				}
				return TypeName(QualifiedName(identifier));
			}
		}
		variables.emplace_back(identifier, TypeVariableKind::TYPE);
		return TypeName(QualifiedName(identifier));
	}

	//! Declares a fresh type variable, named by the prefix and the first free number
	TypeParam Variable(const string &prefix, TypeVariableKind kind,
	                   TypeVariableArity arity = TypeVariableArity::SINGLE) {
		for (idx_t i = 0;; i++) {
			Identifier name(prefix + to_string(i));
			bool taken = false;
			for (auto &variable : variables) {
				if (variable.GetName() == name) {
					taken = true;
					break;
				}
			}
			if (!taken) {
				variables.emplace_back(name, kind, arity);
				return TypeParam::Type(TypeName(QualifiedName(name)), Identifier(), arity == TypeVariableArity::PACK);
			}
		}
	}

private:
	vector<TypeVariable> &variables;
};

} // namespace

TypeName TypeName::FromLogicalType(const LogicalType &type, vector<TypeVariable> &variables) {
	return TypeConverter(variables).Convert(type);
}

TypeName TypeName::FromLogicalType(const LogicalType &type) {
	vector<TypeVariable> variables;
	auto result = FromLogicalType(type, variables);
	if (!variables.empty()) {
		throw InternalException("Type '%s' introduces type variables", type.ToString());
	}
	return result;
}

TypeName FunctionSignature::ConvertType(const LogicalType &type) {
	return TypeName::FromLogicalType(type, type_variables);
}

FunctionSignature &FunctionSignature::InsertParameter(idx_t position, FunctionParameter parameter) {
	if (parameter.unconverted_type) {
#ifdef D_ASSERT_IS_ENABLED
		parameter.original_type = *parameter.unconverted_type;
#endif
		parameter.type = ConvertType(*parameter.unconverted_type);
		parameter.unconverted_type = nullptr;
	}
	parameters.insert(parameters.begin() + NumericCast<int64_t>(position), std::move(parameter));
	return *this;
}

FunctionSignature &FunctionSignature::SetParameters(vector<FunctionParameter> parameters_p) {
	parameters.clear();
	for (auto &param : parameters_p) {
		InsertParameter(parameters.size(), std::move(param));
	}
	if (!GetKwargs()) {
		// the options belong to the "**kwargs" parameter
		typed_kwargs = nullptr;
	}
	RemoveUnusedTypeVariables();
	return *this;
}

//===--------------------------------------------------------------------===//
// TypeName -> LogicalType
//===--------------------------------------------------------------------===//
namespace {

void CountOccurrences(const TypeName &type, identifier_map_t<idx_t> &occurrences) {
	if (type.IsPlainName()) {
		occurrences[type.GetName()]++;
		return;
	}
	for (auto &param : type.GetParams()) {
		if (param.GetKind() == TypeParamKind::TYPE) {
			CountOccurrences(param.GetType(), occurrences);
		}
	}
}

Value IntegerArgument(int64_t value) {
	if (value >= NumericLimits<int32_t>::Minimum() && value <= NumericLimits<int32_t>::Maximum()) {
		return Value::INTEGER(NumericCast<int32_t>(value));
	}
	return Value::BIGINT(value);
}

} // namespace

SignatureResolver::SignatureResolver(optional_ptr<ClientContext> context_p, const FunctionSignature &signature_p,
                                     Identifier catalog_p, Identifier schema_p,
                                     optional_ptr<SignatureTypeCache> cache_p)
    : context(context_p), signature(signature_p), catalog(std::move(catalog_p)), schema(std::move(schema_p)),
      cache(cache_p) {
	if (signature.GetTypeVariables().empty()) {
		return;
	}
	for (auto &param : signature.GetParameters()) {
		CountOccurrences(param.GetType(), occurrences);
	}
	CountOccurrences(signature.GetReturnType(), occurrences);
}

SignatureResolver::SignatureResolver(optional_ptr<ClientContext> context_p, const SimpleFunction &function,
                                     optional_ptr<SignatureTypeCache> cache_p)
    : SignatureResolver(context_p, function.GetSignature(), function.GetCatalogName(), function.GetSchemaName(),
                        cache_p) {
}

bool SignatureResolver::ReferencesVariables(const TypeName &type) const {
	if (signature.GetTypeVariables().empty()) {
		return false;
	}
	if (type.IsPlainName()) {
		return signature.GetTypeVariable(type.GetName()) != nullptr;
	}
	for (auto &param : type.GetParams()) {
		if (param.GetKind() == TypeParamKind::TYPE && ReferencesVariables(param.GetType())) {
			return true;
		}
	}
	return false;
}

bool SignatureResolver::IsOwnedBySystemCatalog() const {
	return catalog.empty() || catalog == Identifier::SystemCatalog();
}

bool SignatureResolver::IsVariable(const TypeParam &param) const {
	return param.GetKind() == TypeParamKind::TYPE && param.GetType().IsPlainName() &&
	       signature.GetTypeVariable(param.GetType().GetName());
}

bool SignatureResolver::IsFamilyOfUnusedVariables(const TypeName &type) const {
	if (type.GetParams().empty()) {
		return false;
	}
	for (auto &param : type.GetParams()) {
		if (!IsVariable(param)) {
			return false;
		}
		auto entry = occurrences.find(param.GetType().GetName());
		if (entry == occurrences.end() || entry->second != 1) {
			return false;
		}
	}
	return true;
}

LogicalType SignatureResolver::ResolveFamily(const TypeName &type, LogicalTypeId id) const {
	switch (id) {
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE:
	case LogicalTypeId::UNION:
	case LogicalTypeId::ENUM:
		break;
	default:
		return LogicalType::INVALID;
	}
	if (IsFamilyOfUnusedVariables(type)) {
		return LogicalType(id);
	}
	// an ARRAY whose size is a variable accepts arrays of any size
	auto &params = type.GetParams();
	if (id == LogicalTypeId::ARRAY && params.size() == 2 && IsVariable(params[1])) {
		return LogicalType::ARRAY(Resolve(params[0].GetType()), optional_idx());
	}
	return LogicalType::INVALID;
}

LogicalType SignatureResolver::Resolve(const TypeName &type) const {
	if (!type.IsValid()) {
		return LogicalType::INVALID;
	}
	if (!cache || ReferencesVariables(type)) {
		return ResolveInternal(type);
	}
	auto entry = cache->find(type);
	if (entry != cache->end()) {
		return entry->second;
	}
	auto result = ResolveInternal(type);
	if (result.id() != LogicalTypeId::INVALID) {
		cache->emplace(type, result);
	}
	return result;
}

LogicalType SignatureResolver::ResolveInternal(const TypeName &type) const {
	if (type.IsPlainName()) {
		auto variable = signature.GetTypeVariable(type.GetName());
		if (variable) {
			if (variable->GetKind() != TypeVariableKind::TYPE || variable->GetArity() != TypeVariableArity::SINGLE) {
				throw NotImplementedException("Type variable '%s' cannot be used as a type", variable->ToString());
			}
			return LogicalType::TEMPLATE(variable->GetName().GetIdentifierName());
		}
	}
	auto &params = type.GetParams();
	if (type.IsPseudoType()) {
		auto id = EnumUtil::FromString<LogicalTypeId>(type.GetName().GetIdentifierName());
		if (id == LogicalTypeId::ANY && params.size() == 2) {
			return LogicalType::ANY_PARAMS(Resolve(params[0].GetType()), NumericCast<idx_t>(params[1].GetInteger()));
		}
		return LogicalType(id);
	}
	if (!type.IsQualified()) {
		if (params.empty() && IsOwnedBySystemCatalog()) {
			// the lookup would end in the default types of the system catalog, which bind the same without a context
			auto builtin = DefaultTypeGenerator::TryBindWithoutParameters(type.GetName());
			if (builtin.id() != LogicalTypeId::INVALID) {
				return builtin;
			}
		}
		auto family = ResolveFamily(type, DefaultTypeGenerator::GetDefaultType(type.GetName()));
		if (family.id() != LogicalTypeId::INVALID) {
			return family;
		}
	}
	vector<TypeArgument> arguments;
	for (auto &param : params) {
		auto name = param.GetName().GetIdentifierName();
		switch (param.GetKind()) {
		case TypeParamKind::TYPE:
			if (param.ExpandsPack()) {
				throw NotImplementedException("Type '%s' expands a pack, which cannot be resolved", type.ToString());
			}
			{
				auto child = Resolve(param.GetType());
				if (child.id() == LogicalTypeId::INVALID) {
					return child;
				}
				arguments.emplace_back(std::move(name), Value::TYPE(std::move(child)));
				break;
			}
		case TypeParamKind::INTEGER:
			arguments.emplace_back(std::move(name), IntegerArgument(param.GetInteger()));
			break;
		case TypeParamKind::STRING:
			arguments.emplace_back(std::move(name), Value(param.GetString()));
			break;
		}
	}
	return LookupType(type, arguments);
}

LogicalType SignatureResolver::LookupType(const TypeName &type, const vector<TypeArgument> &arguments) const {
	auto &name = type.GetQualifiedName();
	// the catalog can only be read within a transaction - outside of one only the default types are reachable
	if (!context || !context->transaction.HasActiveTransaction()) {
		if (!type.IsQualified()) {
			vector<pair<string, Value>> params;
			for (auto &arg : arguments) {
				params.emplace_back(arg.GetName(), arg.GetValue());
			}
			auto result = DefaultTypeGenerator::TryDefaultBind(name.Name().GetIdentifierName(), params);
			if (result.id() != LogicalTypeId::INVALID) {
				return result;
			}
		}
		if (try_resolve) {
			return LogicalType::INVALID;
		}
		throw InvalidInputException("Type '%s' cannot be resolved without a client context and an active transaction",
		                            type.ToString());
	}
	auto client_context = context;
	auto &client = *client_context;
	optional_ptr<TypeCatalogEntry> entry;
	if (type.IsQualified()) {
		entry = Catalog::GetEntry<TypeCatalogEntry>(client, name, OnEntryNotFound::THROW_EXCEPTION);
	} else {
		if (!catalog.empty() && !schema.empty()) {
			entry = Catalog::GetEntry<TypeCatalogEntry>(client, QualifiedName(catalog, schema, name.Name()),
			                                            OnEntryNotFound::RETURN_NULL);
		}
		if (!entry) {
			entry = Catalog::GetEntry<TypeCatalogEntry>(
			    client, QualifiedName(Identifier::SystemCatalog(), Identifier::DefaultSchema(), name.Name()),
			    OnEntryNotFound::THROW_EXCEPTION);
		}
	}
	return entry->constructors.Bind(context, entry->user_type, arguments);
}

LogicalType SignatureResolver::TryResolve(const TypeName &type) const {
	try_resolve = true;
	auto result = Resolve(type);
	try_resolve = false;
	return result;
}

string SignatureResolver::TypeToString(const TypeName &type) const {
	auto resolved = context ? Resolve(type) : TryResolve(type);
	if (resolved.id() == LogicalTypeId::INVALID) {
		return type.ToString();
	}
	return resolved.ToString();
}

LogicalType SignatureResolver::ResolveParameter(idx_t index) const {
	return Resolve(signature.GetParameter(index).GetType());
}

vector<LogicalType> SignatureResolver::ResolveParameters() const {
	vector<LogicalType> result;
	result.reserve(signature.GetParameterCount());
	for (idx_t i = 0; i < signature.GetParameterCount(); i++) {
		result.push_back(ResolveParameter(i));
	}
	return result;
}

LogicalType SignatureResolver::ResolveReturnType() const {
	return Resolve(signature.GetReturnType());
}

LogicalType SignatureResolver::ResolveVarArgs() const {
	auto args = signature.GetArgs();
	return args ? Resolve(args->GetType()) : LogicalType::INVALID;
}

LogicalType SignatureResolver::ResolveType(optional_ptr<ClientContext> context, const TypeName &type) {
	FunctionSignature signature;
	return SignatureResolver(context, signature).Resolve(type);
}

} // namespace duckdb
