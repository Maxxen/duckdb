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

//! The expansion of a pattern is another thing than the pattern, and than the pattern over the expansion: "*(T[])" is
//! neither "T[]" nor "(*T)[]"
static hash_t HashExpansion(TypeMultiplicity kind, const TypeName &pattern) {
	if (kind == TypeMultiplicity::SINGLE) {
		throw InternalException("The expansion of '%s' is positional or named", pattern.ToString());
	}
	return CombineHash(duckdb::Hash(pattern.Hash()), duckdb::Hash(static_cast<uint8_t>(kind)));
}

TypeNameNode::TypeNameNode(Identifier name_p, vector<TypeNameArgument> arguments_p, LogicalTypeId builtin_type_p)
    : hash(HashTypeName(name_p, arguments_p)),
      value(Name {std::move(name_p), std::move(arguments_p), builtin_type_p}) {
}

TypeNameNode::TypeNameNode(TypeMultiplicity kind, TypeName pattern_p)
    : hash(HashExpansion(kind, pattern_p)), value(Expansion {kind, std::move(pattern_p)}) {
}

void TypeNameNode::ThrowNotAName() const {
	throw InternalException("An expansion is not a type name: it has a pattern, no name and no arguments");
}

void TypeNameNode::ThrowNotAnExpansion() const {
	throw InternalException("Type name '%s' is not an expansion", GetName().name.GetIdentifierName());
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
	return make_shared_ptr<const TypeNameNode>(builtin_name->GetName().name, std::move(arguments));
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

TypeName TypeName::PACK(const string &name) {
	return EXPAND(VARIABLE(name));
}

TypeName TypeName::NAMED_PACK(const string &name) {
	return EXPAND_NAMED(VARIABLE(name));
}

TypeName TypeName::EXPAND(TypeName pattern) {
	return TypeName(make_shared_ptr<const TypeNameNode>(TypeMultiplicity::POSITIONAL_PACK, std::move(pattern)));
}

TypeName TypeName::EXPAND_NAMED(TypeName pattern) {
	return TypeName(make_shared_ptr<const TypeNameNode>(TypeMultiplicity::NAMED_PACK, std::move(pattern)));
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

TypeName TypeName::DECIMAL(TypeNameArgument width, TypeNameArgument scale) {
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

bool TypeName::Contains(const Identifier &name) const {
	if (IsExpansion()) {
		return GetPattern().Contains(name);
	}
	if (GetName() == name) {
		return true;
	}
	for (auto &argument : GetArguments()) {
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
	if (node->hash != other.node->hash || IsExpansion() != other.IsExpansion()) {
		return false;
	}
	if (IsExpansion()) {
		return GetExpansionKind() == other.GetExpansionKind() && GetPattern() == other.GetPattern();
	}
	auto &name = node->GetName();
	auto &other_name = other.node->GetName();
	return name.name == other_name.name && name.arguments == other_name.arguments;
}

bool TypeName::operator!=(const TypeName &other) const {
	return !(*this == other);
}

string TypeName::ToString() const {
	if (IsExpansion()) {
		// written with its stars - around the pattern, unless that is a name by itself
		auto &pattern = GetPattern();
		const bool is_bare = !pattern.IsExpansion() && !pattern.HasArguments();
		string stars = GetExpansionKind() == TypeMultiplicity::NAMED_PACK ? "**" : "*";
		return is_bare ? stars + pattern.ToString() : stars + "(" + pattern.ToString() + ")";
	}
	auto &name = GetName();
	auto &arguments = GetArguments();
	if (arguments.empty()) {
		const auto builtin_type = GetBuiltinType();
		if (builtin_type != LogicalTypeId::INVALID) {
			// a built-in type prints as the type does
			return LogicalType(builtin_type).ToString();
		}
		return name.GetIdentifierName();
	}
	// LIST and ARRAY are written as a suffix of their element - in parentheses if the element is an expansion, which
	// the suffix is not part of
	const bool is_list = name == "LIST" && arguments.size() == 1;
	if (is_list || (name == "ARRAY" && arguments.size() == 2)) {
		auto element = arguments[0].ToString();
		if (arguments[0].IsType() && arguments[0].GetType().IsExpansion()) {
			element = "(" + element + ")";
		}
		return is_list ? element + "[]" : element + "[" + arguments[1].ToString() + "]";
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
