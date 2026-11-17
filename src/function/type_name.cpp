#include "duckdb/function/type_name.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/type_constructor.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// TypeNameNode
//===--------------------------------------------------------------------===//
static hash_t HashTypeName(const Identifier &name, const vector<TypeNameArgument> &arguments) {
	auto hash = name.Hash();
	for (auto &argument : arguments) {
		hash = CombineHash(hash, argument.Hash());
	}
	return hash;
}

TypeNameNode::TypeNameNode(Identifier name_p, vector<TypeNameArgument> arguments_p)
    : name(std::move(name_p)), arguments(std::move(arguments_p)), hash(HashTypeName(name, arguments)) {
}

//===--------------------------------------------------------------------===//
// TypeNameArgument
//===--------------------------------------------------------------------===//
TypeNameArgument::TypeNameArgument(TypeName type, Identifier label_p)
    : value(std::move(type)), label(std::move(label_p)) {
}

TypeNameArgument::TypeNameArgument(Literal literal, Identifier label_p)
    : value(std::move(literal)), label(std::move(label_p)) {
}

string TypeNameArgument::ToString() const {
	return IsType() ? GetType().ToString() : GetLiteral().ToString();
}

hash_t TypeNameArgument::Hash() const {
	return CombineHash(label.Hash(), IsType() ? GetType().Hash() : GetLiteral().Hash());
}

bool TypeNameArgument::operator==(const TypeNameArgument &other) const {
	if (label != other.label || IsType() != other.IsType()) {
		return false;
	}
	return IsType() ? GetType() == other.GetType() : GetLiteral() == other.GetLiteral();
}

bool TypeNameArgument::operator!=(const TypeNameArgument &other) const {
	return !(*this == other);
}

//===--------------------------------------------------------------------===//
// LogicalType -> TypeName
//===--------------------------------------------------------------------===//
namespace {

//! The name of a type that takes no arguments. The names of the types of the catalog are shared, so that naming one
//! allocates nothing
TypeName BareTypeName(LogicalTypeId id) {
	static const auto shared_names = [] {
		vector<optional<TypeName>> names(NumericLimits<uint8_t>::Maximum() + 1);
		auto add = [&](LogicalTypeId shared_id) {
			// the name the type prints as, if the catalog knows the type by it
			Identifier name(EnumUtil::ToString(shared_id));
			auto builtin_name = TypeExpression::GetBuiltinTypeName(shared_id);
			if (builtin_name && DefaultTypeGenerator::GetDefaultType(name) != shared_id) {
				name = Identifier(builtin_name);
			}
			names[static_cast<uint8_t>(shared_id)] = TypeName(std::move(name));
		};
		for (auto &type : LogicalType::AllTypes()) {
			add(type.id());
		}
		for (auto shared_id : {LogicalTypeId::SQLNULL, LogicalTypeId::ANY, LogicalTypeId::LAMBDA, LogicalTypeId::TABLE,
		                       LogicalTypeId::TYPE}) {
			add(shared_id);
		}
		return names;
	}();
	auto &shared_name = shared_names[static_cast<uint8_t>(id)];
	if (shared_name) {
		return *shared_name;
	}
	// an ID that is not a type of the catalog is named by itself, and does not bind
	return TypeName(Identifier(EnumUtil::ToString(id)));
}

TypeNameArgument TypeArgumentOf(const LogicalType &type, Identifier label = Identifier()) {
	return TypeNameArgument(TypeName(type), std::move(label));
}

TypeNameArgument LiteralArgumentOf(const Value &value, const string &label = string()) {
	// the literal the parser produces for the value, as a type expression holds it
	auto expression = ConstantExpression::FromValue(value);
	if (expression->GetExpressionClass() == ExpressionClass::CONSTANT) {
		return TypeNameArgument(expression->Cast<ConstantExpression>().GetLiteral(), Identifier(label));
	}
	return TypeNameArgument(Literal::String(value.ToString()), Identifier(label));
}

TypeName NameOfType(const LogicalType &type) {
	if (type.id() == LogicalTypeId::TEMPLATE) {
		// a template is the type variable of that name, which the signature declares
		return TypeName(Identifier(TemplateType::GetName(type)));
	}

	vector<TypeNameArgument> arguments;
	auto alias = type.GetAlias();
	if (!alias.empty()) {
		// a type that carries an alias is a type of the catalog under that name
		if (type.HasExtensionInfo()) {
			for (auto &modifier : type.GetExtensionInfo()->modifiers) {
				arguments.push_back(LiteralArgumentOf(modifier.value));
			}
		}
		return TypeName(Identifier(alias), std::move(arguments));
	}

	switch (type.id()) {
	case LogicalTypeId::DECIMAL:
		if (type.HasParameters()) {
			arguments.emplace_back(Literal::Integer(DecimalType::GetWidth(type)));
			arguments.emplace_back(Literal::Integer(DecimalType::GetScale(type)));
		}
		break;
	case LogicalTypeId::VARCHAR: {
		auto collation = StringType::GetCollation(type);
		if (!collation.empty()) {
			arguments.emplace_back(Literal::String(std::move(collation)), Identifier("collation"));
		}
		break;
	}
	case LogicalTypeId::GEOMETRY:
		if (GeoType::HasCRS(type)) {
			arguments.emplace_back(Literal::String(GeoType::GetCRS(type).GetDefinition()));
		}
		break;
	case LogicalTypeId::ENUM:
		if (type.HasParameters()) {
			for (idx_t i = 0; i < EnumType::GetSize(type); i++) {
				arguments.emplace_back(Literal::String(EnumType::GetString(type, i).GetString()));
			}
		}
		break;
	case LogicalTypeId::LIST:
		if (type.HasParameters()) {
			arguments.push_back(TypeArgumentOf(ListType::GetChildType(type)));
		}
		break;
	case LogicalTypeId::ARRAY:
		if (type.HasParameters()) {
			arguments.push_back(TypeArgumentOf(ArrayType::GetChildType(type)));
			if (!ArrayType::IsAnySize(type)) {
				arguments.emplace_back(Literal::Integer(NumericCast<int64_t>(ArrayType::GetSize(type))));
			}
		}
		break;
	case LogicalTypeId::STRUCT:
		if (type.HasParameters()) {
			for (auto &child : StructType::GetChildTypes(type)) {
				arguments.push_back(TypeArgumentOf(child.second, child.first));
			}
		}
		break;
	case LogicalTypeId::TUPLE:
		if (type.HasParameters()) {
			for (auto &child : StructType::GetChildTypes(type)) {
				arguments.push_back(TypeArgumentOf(child.second));
			}
		}
		break;
	case LogicalTypeId::MAP:
		if (type.HasParameters()) {
			arguments.push_back(TypeArgumentOf(MapType::KeyType(type)));
			arguments.push_back(TypeArgumentOf(MapType::ValueType(type)));
		}
		break;
	case LogicalTypeId::UNION:
		if (type.HasParameters()) {
			for (idx_t i = 0; i < UnionType::GetMemberCount(type); i++) {
				arguments.push_back(TypeArgumentOf(UnionType::GetMemberType(type, i), UnionType::GetMemberName(type, i)));
			}
		}
		break;
	default:
		break;
	}
	auto bare_name = BareTypeName(type.id());
	if (arguments.empty()) {
		return bare_name;
	}
	return TypeName(bare_name.GetName(), std::move(arguments));
}

} // namespace

//===--------------------------------------------------------------------===//
// TypeName
//===--------------------------------------------------------------------===//
TypeName::TypeName(Identifier name) : TypeName(std::move(name), vector<TypeNameArgument>()) {
}

TypeName::TypeName(Identifier name, vector<TypeNameArgument> arguments)
    : node(make_shared_ptr<const TypeNameNode>(std::move(name), std::move(arguments))) {
}

TypeName::TypeName(const LogicalType &type) : node(NameOfType(type).node) {
}

const Identifier &TypeName::GetName() const {
	return node->name;
}

const vector<TypeNameArgument> &TypeName::GetArguments() const {
	return node->arguments;
}

bool TypeName::HasArguments() const {
	return !node->arguments.empty();
}

hash_t TypeName::Hash() const {
	return node->hash;
}

bool TypeName::operator==(const TypeName &other) const {
	if (node == other.node) {
		return true;
	}
	return node->hash == other.node->hash && node->name == other.node->name &&
	       node->arguments == other.node->arguments;
}

bool TypeName::operator!=(const TypeName &other) const {
	return !(*this == other);
}

string TypeName::ToString() const {
	auto &name = node->name;
	auto &arguments = node->arguments;
	if (arguments.empty()) {
		return name.GetIdentifierName();
	}
	// LIST and ARRAY are written as a suffix of their element
	if (name == "LIST" && arguments.size() == 1) {
		return arguments[0].ToString() + "[]";
	}
	if (name == "ARRAY" && arguments.size() <= 2) {
		return arguments[0].ToString() + "[" + (arguments.size() == 2 ? arguments[1].ToString() : "ANY") + "]";
	}
	string result = name.GetIdentifierName() + "(";
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		auto &label = arguments[i].GetLabel();
		if (!label.empty()) {
			result += SQLIdentifier(label) + " ";
		}
		result += arguments[i].ToString();
	}
	return result + ")";
}

//===--------------------------------------------------------------------===//
// TypeName -> LogicalType
//===--------------------------------------------------------------------===//
//! The value variable of the signature an argument of a type name refers to, if it refers to one
static optional_ptr<const TypeVariable> FindValueVariable(const TypeNameArgument &argument,
                                                          const FunctionSignature &signature) {
	if (!argument.IsType() || argument.GetType().HasArguments()) {
		return nullptr;
	}
	auto variable = signature.FindVariable(argument.GetType().GetName());
	return variable && variable->IsValue() ? variable : nullptr;
}

LogicalType FunctionBinder::BindTypeName(optional_ptr<ClientContext> context, const TypeName &name) {
	return BindTypeName(context, name, FunctionSignature());
}

LogicalType FunctionBinder::BindTypeName(optional_ptr<ClientContext> context, const TypeName &name,
                                         const FunctionSignature &signature) {
	auto &identifier = name.GetName();
	auto &arguments = name.GetArguments();
	if (arguments.empty()) {
		auto variable = signature.FindVariable(identifier);
		if (variable) {
			if (variable->IsValue()) {
				throw InternalException("Value variable %s is used as a type", identifier.GetIdentifierName());
			}
			// a type variable is passed on as the template it declares, which the call then determines
			auto &variable_name = identifier.GetIdentifierName();
			auto constraint = signature.GetConstraint(identifier);
			return constraint == LogicalTypeId::INVALID ? LogicalType::TEMPLATE(variable_name)
			                                            : LogicalType::TEMPLATE(variable_name, constraint);
		}
		if (identifier == "ANY") {
			return LogicalType::ANY;
		}
	}
	if (identifier == "ARRAY" && arguments.size() == 2 && FindValueVariable(arguments[1], signature)) {
		// the size of an array is the only value a call determines so far: an array of any size
		return LogicalType::ARRAY(BindTypeName(context, arguments[0].GetType(), signature), optional_idx());
	}

	vector<TypeArgument> bound_arguments;
	for (auto &argument : arguments) {
		if (FindValueVariable(argument, signature)) {
			throw NotImplementedException("Value variable %s can only be the size of an array",
			                              argument.GetType().GetName().GetIdentifierName());
		}
		auto value = argument.IsType() ? Value::TYPE(BindTypeName(context, argument.GetType(), signature))
		                               : argument.GetLiteral().ToValue();
		bound_arguments.emplace_back(argument.GetLabel().GetIdentifierName(), std::move(value));
	}

	if (context) {
		auto entry = Catalog::GetEntry<TypeCatalogEntry>(*context, QualifiedName(identifier),
		                                                 OnEntryNotFound::RETURN_NULL);
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

	if (arguments.empty() && identifier == "LAMBDA") {
		return LogicalType::LAMBDA;
	}
	if (arguments.empty() && identifier == "TABLE") {
		return LogicalType::TABLE;
	}
	throw CatalogException("Type with name %s does not exist", identifier.GetIdentifierName());
}

} // namespace duckdb
