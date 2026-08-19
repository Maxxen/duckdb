#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/types/hash.hpp"
namespace duckdb {

TypeExpression::TypeExpression(QualifiedName qualified_name_p, vector<unique_ptr<ParsedExpression>> children_p)
    : ParsedExpression(ExpressionType::TYPE, ExpressionClass::TYPE), qualified_name(std::move(qualified_name_p)),
      children(std::move(children_p)) {
	D_ASSERT(!qualified_name.Name().empty());
}

TypeExpression::TypeExpression(Identifier type_name, vector<unique_ptr<ParsedExpression>> children)
    : TypeExpression(QualifiedName(std::move(type_name)), std::move(children)) {
}

TypeExpression::TypeExpression(const string &type_name, vector<unique_ptr<ParsedExpression>> children)
    : TypeExpression(Identifier(type_name), std::move(children)) {
}

TypeExpression::TypeExpression() : ParsedExpression(ExpressionType::TYPE, ExpressionClass::TYPE) {
}

string TypeExpression::ToString() const {
	string result;
	auto &type_name = qualified_name.Name();
	if (!qualified_name.Catalog().empty()) {
		result += SQLIdentifier(qualified_name.Catalog()) + ".";
	}
	if (!qualified_name.Schema().empty()) {
		result += SQLIdentifier(qualified_name.Schema()) + ".";
	}

	auto &params = children;

	// LIST and ARRAY have special syntax
	if (result.empty() && type_name == "LIST" && params.size() == 1) {
		return params[0]->ToString() + "[]";
	}
	if (result.empty() && type_name == "ARRAY" && params.size() == 2) {
		auto &type_param = params[0];
		auto &size_param = params[1];
		return type_param->ToString() + "[" + size_param->ToString() + "]";
	}
	// So does STRUCT, MAP and UNION
	if (result.empty() && type_name == "STRUCT") {
		if (params.empty()) {
			return "STRUCT";
		}
		string struct_result = "STRUCT(";
		for (idx_t i = 0; i < params.size(); i++) {
			struct_result += SQLIdentifier(params[i]->GetAlias()) + " " + params[i]->ToString();
			if (i < params.size() - 1) {
				struct_result += ", ";
			}
		}
		struct_result += ")";
		return struct_result;
	}
	if (result.empty() && type_name == "UNION") {
		if (params.empty()) {
			return "UNION";
		}
		string union_result = "UNION(";
		for (idx_t i = 0; i < params.size(); i++) {
			union_result += SQLIdentifier(params[i]->GetAlias()) + " " + params[i]->ToString();
			if (i < params.size() - 1) {
				union_result += ", ";
			}
		}
		union_result += ")";
		return union_result;
	}

	if (result.empty() && type_name == "MAP" && params.size() == 2) {
		return "MAP(" + params[0]->ToString() + ", " + params[1]->ToString() + ")";
	}

	if (result.empty() && type_name == "VARCHAR" && !params.empty()) {
		if (params.back()->HasAlias() && params.back()->GetAlias() == "collation") {
			// Special case for VARCHAR with collation
			auto collate_expr = params.back()->Cast<ConstantExpression>();
			return StringUtil::Format("VARCHAR COLLATE %s", SQLIdentifier(StringValue::Get(collate_expr.GetValue())));
		}
	}

	if (result.empty() && type_name == "INTERVAL" && !params.empty()) {
		// We ignore interval types parameters.
		return "INTERVAL";
	}

	auto type_id = TransformStringToLogicalTypeId(type_name.GetIdentifierName());
	if (type_id != LogicalTypeId::UNBOUND && type_id != LogicalTypeId::SQLNULL) {
		// Built-in type name
		result += type_name.GetIdentifierName();
	} else {
		result += SQLIdentifier(type_name);
	}

	if (!params.empty()) {
		result += "(";
		for (idx_t i = 0; i < params.size(); i++) {
			result += params[i]->ToString();
			if (i < params.size() - 1) {
				result += ", ";
			}
		}
		result += ")";
	}
	return result;
}

//===--------------------------------------------------------------------===//
// LogicalType -> TypeExpression
//===--------------------------------------------------------------------===//

namespace {

unique_ptr<ParsedExpression> TypeChild(const LogicalType &type, const Identifier &name) {
	auto child = TypeExpression::FromLogicalType(type);
	if (!name.empty()) {
		child->SetAlias(name);
	}
	return std::move(child);
}

unique_ptr<ParsedExpression> ValueChild(Value value, const char *label = nullptr) {
	auto child = make_uniq_base<ParsedExpression, ConstantExpression>(std::move(value));
	if (label) {
		child->SetAlias(Identifier(label));
	}
	return child;
}

//! The name a built-in type is spelled with. Must be a name DefaultTypeGenerator knows.
const char *BuiltinTypeName(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::TINYINT:
		return "tinyint";
	case LogicalTypeId::SMALLINT:
		return "smallint";
	case LogicalTypeId::INTEGER:
		return "integer";
	case LogicalTypeId::BIGINT:
		return "bigint";
	case LogicalTypeId::HUGEINT:
		return "hugeint";
	case LogicalTypeId::UTINYINT:
		return "utinyint";
	case LogicalTypeId::USMALLINT:
		return "usmallint";
	case LogicalTypeId::UINTEGER:
		return "uinteger";
	case LogicalTypeId::UBIGINT:
		return "ubigint";
	case LogicalTypeId::UHUGEINT:
		return "uhugeint";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::BIGNUM:
		return "bignum";
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIME:
		return "time";
	case LogicalTypeId::TIME_NS:
		return "time_ns";
	case LogicalTypeId::TIME_TZ:
		return "timetz";
	case LogicalTypeId::TIMESTAMP:
		return "timestamp_us";
	case LogicalTypeId::TIMESTAMP_SEC:
		return "timestamp_s";
	case LogicalTypeId::TIMESTAMP_MS:
		return "timestamp_ms";
	case LogicalTypeId::TIMESTAMP_NS:
		return "timestamp_ns";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestamptz";
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return "timestamptz_ns";
	case LogicalTypeId::INTERVAL:
		return "interval";
	case LogicalTypeId::VARCHAR:
		return "varchar";
	case LogicalTypeId::BLOB:
		return "blob";
	case LogicalTypeId::BIT:
		return "bit";
	case LogicalTypeId::UUID:
		return "uuid";
	case LogicalTypeId::SQLNULL:
		return "null";
	case LogicalTypeId::TYPE:
		return "type";
	case LogicalTypeId::VARIANT:
		return "variant";
	case LogicalTypeId::GEOMETRY:
		return "geometry";
	case LogicalTypeId::DECIMAL:
		return "decimal";
	case LogicalTypeId::ENUM:
		return "enum";
	case LogicalTypeId::LIST:
		return "list";
	case LogicalTypeId::ARRAY:
		return "array";
	case LogicalTypeId::STRUCT:
		return "struct";
	case LogicalTypeId::TUPLE:
		return "tuple";
	case LogicalTypeId::MAP:
		return "map";
	case LogicalTypeId::UNION:
		return "union";
	default:
		return nullptr;
	}
}

} // namespace

unique_ptr<TypeExpression> TypeExpression::FromLogicalType(const LogicalType &type) {
	if (type.IsUnbound()) {
		return UnboundType::CopyTypeExpression(type);
	}

	vector<unique_ptr<ParsedExpression>> children;

	// A type that carries an alias is a user-defined type: name it, and let the binder resolve it again.
	auto alias = type.GetAlias();
	if (!alias.empty()) {
		if (type.HasExtensionInfo()) {
			for (auto &modifier : type.GetExtensionInfo()->modifiers) {
				children.push_back(ValueChild(modifier.value));
			}
		}
		return make_uniq<TypeExpression>(Identifier(alias), std::move(children));
	}

	auto name = BuiltinTypeName(type.id());
	if (!name) {
		throw InternalException("Type '%s' cannot be expressed as a type expression", type.ToString());
	}

	switch (type.id()) {
	case LogicalTypeId::DECIMAL:
		children.push_back(ValueChild(Value::UTINYINT(DecimalType::GetWidth(type))));
		children.push_back(ValueChild(Value::UTINYINT(DecimalType::GetScale(type))));
		break;
	case LogicalTypeId::VARCHAR: {
		auto collation = StringType::GetCollation(type);
		if (!collation.empty()) {
			children.push_back(ValueChild(Value(collation), "collation"));
		}
		break;
	}
	case LogicalTypeId::GEOMETRY:
		if (GeoType::HasCRS(type)) {
			children.push_back(ValueChild(Value(GeoType::GetCRS(type).GetDefinition())));
		}
		break;
	case LogicalTypeId::ENUM:
		for (idx_t i = 0; i < EnumType::GetSize(type); i++) {
			children.push_back(ValueChild(Value(EnumType::GetString(type, i).GetString())));
		}
		break;
	case LogicalTypeId::LIST:
		children.push_back(TypeChild(ListType::GetChildType(type), Identifier()));
		break;
	case LogicalTypeId::ARRAY:
		if (ArrayType::IsAnySize(type)) {
			throw InternalException("An any-size ARRAY cannot be expressed as a type expression");
		}
		children.push_back(TypeChild(ArrayType::GetChildType(type), Identifier()));
		children.push_back(ValueChild(Value::UBIGINT(ArrayType::GetSize(type))));
		break;
	case LogicalTypeId::STRUCT:
		for (auto &child : StructType::GetChildTypes(type)) {
			children.push_back(TypeChild(child.second, child.first));
		}
		break;
	case LogicalTypeId::TUPLE:
		for (auto &child : StructType::GetChildTypes(type)) {
			children.push_back(TypeChild(child.second, Identifier()));
		}
		break;
	case LogicalTypeId::MAP:
		children.push_back(TypeChild(MapType::KeyType(type), Identifier()));
		children.push_back(TypeChild(MapType::ValueType(type), Identifier()));
		break;
	case LogicalTypeId::UNION:
		for (idx_t i = 0; i < UnionType::GetMemberCount(type); i++) {
			children.push_back(TypeChild(UnionType::GetMemberType(type, i), UnionType::GetMemberName(type, i)));
		}
		break;
	default:
		break;
	}

	return make_uniq<TypeExpression>(Identifier(name), std::move(children));
}

void TypeExpression::Verify() const {
	D_ASSERT(!qualified_name.Name().empty());
}

} // namespace duckdb
