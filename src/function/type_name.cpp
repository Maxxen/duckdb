#include "duckdb/function/type_name.hpp"

#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/types/hash.hpp"
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

TypeNameNode::TypeNameNode(Identifier name_p, vector<TypeNameArgument> arguments_p, LogicalTypeId builtin_type_p)
    : name(std::move(name_p)), arguments(std::move(arguments_p)), hash(HashTypeName(name, arguments)),
      builtin_type(builtin_type_p) {
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

//! The names of the built-in types, by type. They are shared, so that naming one allocates nothing
const vector<shared_ptr<const TypeNameNode>> &BuiltinTypeNames() {
	static const auto shared_names = [] {
		vector<shared_ptr<const TypeNameNode>> names(NumericLimits<uint8_t>::Maximum() + 1);
		auto add = [&](LogicalTypeId id) {
			// the name the type prints as, if the catalog knows the type by it
			Identifier name(EnumUtil::ToString(id));
			auto builtin_name = TypeExpression::GetBuiltinTypeName(id);
			if (builtin_name && DefaultTypeGenerator::GetDefaultType(name) != id) {
				name = Identifier(builtin_name);
			}
			names[static_cast<uint8_t>(id)] =
			    make_shared_ptr<const TypeNameNode>(std::move(name), vector<TypeNameArgument>(), id);
		};
		for (auto &type : LogicalType::AllTypes()) {
			add(type.id());
		}
		for (auto id : {LogicalTypeId::SQLNULL, LogicalTypeId::ANY, LogicalTypeId::LAMBDA, LogicalTypeId::TABLE,
		                LogicalTypeId::TYPE}) {
			add(id);
		}
		return names;
	}();
	return shared_names;
}

//! The shared name of a type that takes no arguments, nullptr if it is not a built-in one
shared_ptr<const TypeNameNode> BuiltinTypeName(LogicalTypeId id) {
	return BuiltinTypeNames()[static_cast<uint8_t>(id)];
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

shared_ptr<const TypeNameNode> NameOfType(const LogicalType &type) {
	vector<TypeNameArgument> arguments;
	auto alias = type.GetAlias();
	if (!alias.empty()) {
		// a type that carries an alias is a type of the catalog under that name
		if (type.HasExtensionInfo()) {
			for (auto &modifier : type.GetExtensionInfo()->modifiers) {
				arguments.push_back(LiteralArgumentOf(modifier.value));
			}
		}
		return make_shared_ptr<const TypeNameNode>(Identifier(alias), std::move(arguments));
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
			if (ArrayType::IsAnySize(type)) {
				throw InternalException("An array of any size has no name: declare its size as a value variable of "
				                        "the signature, e.g. foo<N: INTEGER>(x FLOAT[N])");
			}
			arguments.push_back(TypeArgumentOf(ArrayType::GetChildType(type)));
			arguments.emplace_back(Literal::Integer(NumericCast<int64_t>(ArrayType::GetSize(type))));
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
				arguments.push_back(
				    TypeArgumentOf(UnionType::GetMemberType(type, i), UnionType::GetMemberName(type, i)));
			}
		}
		break;
	default:
		break;
	}
	auto builtin_name = BuiltinTypeName(type.id());
	if (!builtin_name) {
		// an ID that is not a type of the catalog is named by itself, and does not bind
		return make_shared_ptr<const TypeNameNode>(Identifier(EnumUtil::ToString(type.id())), std::move(arguments));
	}
	if (arguments.empty()) {
		return builtin_name;
	}
	return make_shared_ptr<const TypeNameNode>(builtin_name->name, std::move(arguments));
}

} // namespace

//===--------------------------------------------------------------------===//
// TypeName
//===--------------------------------------------------------------------===//
//! A name without arguments that is the name of a built-in type is that type's shared name
static shared_ptr<const TypeNameNode> NameOfIdentifier(Identifier name) {
	auto builtin_type = DefaultTypeGenerator::GetDefaultType(name);
	auto builtin_name = builtin_type == LogicalTypeId::INVALID ? nullptr : BuiltinTypeName(builtin_type);
	if (builtin_name) {
		return builtin_name;
	}
	return make_shared_ptr<const TypeNameNode>(std::move(name), vector<TypeNameArgument>());
}

TypeName::TypeName(shared_ptr<const TypeNameNode> node_p) : node(std::move(node_p)) {
}

TypeName::TypeName(Identifier name) : node(NameOfIdentifier(std::move(name))) {
}

TypeName::TypeName(Identifier name, vector<TypeNameArgument> arguments)
    : node(arguments.empty() ? NameOfIdentifier(std::move(name))
                             : make_shared_ptr<const TypeNameNode>(std::move(name), std::move(arguments))) {
}

TypeName::TypeName(const LogicalType &type) : node(NameOfType(type)) {
}

TypeName::TypeName(LogicalTypeId type_id) : TypeName(LogicalType(type_id)) {
}

TypeName TypeName::VARIABLE(const string &name) {
	return TypeName(Identifier(name));
}

TypeName TypeName::LIST(TypeName child) {
	return TypeName(Identifier("LIST"), {std::move(child)});
}

TypeName TypeName::ARRAY(TypeName child, TypeName size) {
	return TypeName(Identifier("ARRAY"), {std::move(child), std::move(size)});
}

TypeName TypeName::ARRAY(TypeName child, idx_t size) {
	return TypeName(Identifier("ARRAY"), {std::move(child), Literal::Integer(NumericCast<int64_t>(size))});
}

TypeName TypeName::MAP(TypeName key, TypeName value) {
	return TypeName(Identifier("MAP"), {std::move(key), std::move(value)});
}

TypeName TypeName::DECIMAL(TypeName width, TypeName scale) {
	return TypeName(Identifier("DECIMAL"), {std::move(width), std::move(scale)});
}

TypeName TypeName::STRUCT(vector<pair<string, TypeName>> children) {
	vector<TypeNameArgument> arguments;
	for (auto &child : children) {
		arguments.emplace_back(std::move(child.second), Identifier(child.first));
	}
	return TypeName(Identifier("STRUCT"), std::move(arguments));
}

TypeName TypeName::TUPLE(vector<TypeName> children) {
	vector<TypeNameArgument> arguments;
	for (auto &child : children) {
		arguments.emplace_back(std::move(child));
	}
	return TypeName(Identifier("TUPLE"), std::move(arguments));
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

LogicalTypeId TypeName::GetBuiltinType() const {
	return node->builtin_type;
}

bool TypeName::Contains(const Identifier &name) const {
	if (node->name == name) {
		return true;
	}
	for (auto &argument : node->arguments) {
		if (argument.IsType() && argument.GetType().Contains(name)) {
			return true;
		}
	}
	return false;
}

hash_t TypeName::Hash() const {
	return node->hash;
}

bool TypeName::operator==(const TypeName &other) const {
	if (node == other.node) {
		return true;
	}
	return node->hash == other.node->hash && node->name == other.node->name && node->arguments == other.node->arguments;
}

bool TypeName::operator!=(const TypeName &other) const {
	return !(*this == other);
}

string TypeName::ToString() const {
	auto &name = node->name;
	auto &arguments = node->arguments;
	if (arguments.empty()) {
		// a built-in type prints as the type does
		return node->builtin_type == LogicalTypeId::INVALID ? name.GetIdentifierName()
		                                                    : LogicalType(node->builtin_type).ToString();
	}
	// LIST and ARRAY are written as a suffix of their element
	if (name == "LIST" && arguments.size() == 1) {
		return arguments[0].ToString() + "[]";
	}
	if (name == "ARRAY" && arguments.size() == 2) {
		return arguments[0].ToString() + "[" + arguments[1].ToString() + "]";
	}
	// a DECIMAL is written without a space, as the type prints
	auto separator = name == "DECIMAL" ? "," : ", ";
	string result = name.GetIdentifierName() + "(";
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (i > 0) {
			result += separator;
		}
		auto &label = arguments[i].GetLabel();
		if (!label.empty()) {
			result += SQLIdentifier(label) + " ";
		}
		result += arguments[i].ToString();
	}
	return result + ")";
}

} // namespace duckdb
