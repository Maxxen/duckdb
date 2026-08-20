#include "duckdb/common/type_descriptor.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// TypeParameter
//===--------------------------------------------------------------------===//
TypeParameter TypeParameter::NonType(Value value) {
	TypeParameter result;
	result.value = std::move(value);
	return result;
}

TypeParameter TypeParameter::NonType(Identifier label, Value value) {
	TypeParameter result;
	result.label = std::move(label);
	result.value = std::move(value);
	return result;
}

TypeParameter TypeParameter::Type(TypeDescriptor type) {
	TypeParameter result;
	result.type = make_uniq<TypeDescriptor>(std::move(type));
	return result;
}

TypeParameter TypeParameter::Type(Identifier label, TypeDescriptor type) {
	TypeParameter result;
	result.label = std::move(label);
	result.type = make_uniq<TypeDescriptor>(std::move(type));
	return result;
}

TypeParameter::TypeParameter(const TypeParameter &other) : label(other.label), value(other.value) {
	if (other.type) {
		type = make_uniq<TypeDescriptor>(*other.type);
	}
}

TypeParameter &TypeParameter::operator=(const TypeParameter &other) {
	if (this == &other) {
		return *this;
	}
	label = other.label;
	value = other.value;
	type = other.type ? make_uniq<TypeDescriptor>(*other.type) : nullptr;
	return *this;
}

TypeParameter::~TypeParameter() {
}

const Value &TypeParameter::GetValue() const {
	if (IsType()) {
		throw InternalException("TypeParameter::GetValue called on a type parameter");
	}
	return value;
}

const TypeDescriptor &TypeParameter::GetType() const {
	if (!IsType()) {
		throw InternalException("TypeParameter::GetType called on a non-type parameter");
	}
	return *type;
}

bool TypeParameter::operator==(const TypeParameter &other) const {
	if (label != other.label || IsType() != other.IsType()) {
		return false;
	}
	if (IsType()) {
		return *type == *other.type;
	}
	return ValueOperations::NotDistinctFrom(value, other.value);
}

//===--------------------------------------------------------------------===//
// TypeDescriptor
//===--------------------------------------------------------------------===//
TypeDescriptor::TypeDescriptor(QualifiedName name_p) : name(std::move(name_p)) {
}

TypeDescriptor::TypeDescriptor(QualifiedName name_p, vector<TypeParameter> parameters_p)
    : name(std::move(name_p)), parameters(std::move(parameters_p)) {
}

bool TypeDescriptor::operator==(const TypeDescriptor &other) const {
	if (name.Path() != other.name.Path() || parameters.size() != other.parameters.size()) {
		return false;
	}
	for (idx_t i = 0; i < parameters.size(); i++) {
		if (parameters[i] != other.parameters[i]) {
			return false;
		}
	}
	return true;
}

unique_ptr<TypeExpression> TypeDescriptor::ToTypeExpression() const {
	vector<unique_ptr<ParsedExpression>> children;
	children.reserve(parameters.size());
	for (auto &param : parameters) {
		unique_ptr<ParsedExpression> child;
		if (param.IsType()) {
			child = param.GetType().ToTypeExpression();
		} else {
			child = make_uniq_base<ParsedExpression, ConstantExpression>(param.GetValue());
		}
		if (param.HasLabel()) {
			child->SetAlias(param.Label());
		}
		children.push_back(std::move(child));
	}
	return make_uniq<TypeExpression>(name, std::move(children));
}

string TypeDescriptor::ToString() const {
	return ToTypeExpression()->ToString();
}

//===--------------------------------------------------------------------===//
// DefaultBind
//===--------------------------------------------------------------------===//
LogicalType TypeDescriptor::DefaultBind() const {
	auto &catalog = name.Catalog();
	auto &schema = name.Schema();
	// a built-in type is either unqualified or lives at system.main - anything else needs the catalog
	if ((!catalog.empty() && catalog != Identifier::SystemCatalog()) ||
	    (!schema.empty() && schema != Identifier::DefaultSchema())) {
		throw BinderException("Type \"%s\" cannot be resolved without a catalog connection", ToString());
	}

	vector<pair<string, Value>> args;
	args.reserve(parameters.size());
	for (auto &param : parameters) {
		auto value = param.IsType() ? Value::TYPE(param.GetType().DefaultBind()) : param.GetValue();
		args.emplace_back(param.Label().GetIdentifierName(), std::move(value));
	}

	auto result = DefaultTypeGenerator::TryDefaultBind(name.Name().GetIdentifierName(), args);
	if (result.id() == LogicalTypeId::INVALID) {
		throw BinderException("Type \"%s\" is not a built-in type", name.Name());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// FromLogicalType
//===--------------------------------------------------------------------===//
namespace {

//! The canonical built-in name for an id, as registered in DefaultTypeGenerator's table. Returns
//! nullptr for ids that have no built-in name, or whose name alone does not determine the type.
const char *BuiltinTypeName(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::SQLNULL:
		return "NULL";
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
		return "TINYINT";
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
		return "BIGINT";
	case LogicalTypeId::HUGEINT:
		return "HUGEINT";
	case LogicalTypeId::UTINYINT:
		return "UTINYINT";
	case LogicalTypeId::USMALLINT:
		return "USMALLINT";
	case LogicalTypeId::UINTEGER:
		return "UINTEGER";
	case LogicalTypeId::UBIGINT:
		return "UBIGINT";
	case LogicalTypeId::UHUGEINT:
		return "UHUGEINT";
	case LogicalTypeId::FLOAT:
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::BIGNUM:
		return "BIGNUM";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIME:
		return "TIME";
	case LogicalTypeId::TIME_NS:
		return "TIME_NS";
	case LogicalTypeId::TIME_TZ:
		return "TIMETZ";
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP_US";
	case LogicalTypeId::TIMESTAMP_SEC:
		return "TIMESTAMP_S";
	case LogicalTypeId::TIMESTAMP_MS:
		return "TIMESTAMP_MS";
	case LogicalTypeId::TIMESTAMP_NS:
		return "TIMESTAMP_NS";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMPTZ";
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return "TIMESTAMPTZ_NS";
	case LogicalTypeId::INTERVAL:
		return "INTERVAL";
	case LogicalTypeId::VARCHAR:
		return "VARCHAR";
	case LogicalTypeId::BLOB:
		return "BLOB";
	case LogicalTypeId::BIT:
		return "BIT";
	case LogicalTypeId::UUID:
		return "UUID";
	case LogicalTypeId::VARIANT:
		return "VARIANT";
	case LogicalTypeId::GEOMETRY:
		return "GEOMETRY";
	case LogicalTypeId::TYPE:
		return "TYPE";
	case LogicalTypeId::DECIMAL:
		return "DECIMAL";
	case LogicalTypeId::ENUM:
		return "ENUM";
	case LogicalTypeId::STRUCT:
		return "STRUCT";
	case LogicalTypeId::TUPLE:
		return "TUPLE";
	case LogicalTypeId::LIST:
		return "LIST";
	case LogicalTypeId::ARRAY:
		return "ARRAY";
	case LogicalTypeId::MAP:
		return "MAP";
	case LogicalTypeId::UNION:
		return "UNION";
	default:
		return nullptr;
	}
}

//! Whether the type's parameters live in its type info, so a bare LogicalType(id) cannot describe it
bool RequiresTypeInfo(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE:
	case LogicalTypeId::MAP:
	case LogicalTypeId::UNION:
		return true;
	default:
		return false;
	}
}

bool Fail(string &error, const string &message) {
	error = message;
	return false;
}

bool Emit(unique_ptr<TypeDescriptor> &result, const char *name, vector<TypeParameter> params) {
	result = make_uniq<TypeDescriptor>(QualifiedName(Identifier(name)), std::move(params));
	return true;
}

//! The describing core. Reports failure through "error" rather than throwing, so that the
//! non-throwing entry point does not need exceptions for control flow.
bool TryDescribe(const LogicalType &type, unique_ptr<TypeDescriptor> &result, string &error);

bool TryDescribeChild(const LogicalType &child, const Identifier &label, vector<TypeParameter> &params, string &error) {
	unique_ptr<TypeDescriptor> described;
	if (!TryDescribe(child, described, error)) {
		return false;
	}
	params.push_back(label.empty() ? TypeParameter::Type(std::move(*described))
	                               : TypeParameter::Type(label, std::move(*described)));
	return true;
}

bool TryDescribe(const LogicalType &type, unique_ptr<TypeDescriptor> &result, string &error) {
	// A type that carries an alias is a nominal type: it is named by that alias, and its parameters are its
	// extension modifiers. A caller describing a type in order to *define* it strips the alias first, so that
	// the definition does not name the very entry being defined.
	auto alias = type.GetAlias();
	if (!alias.empty()) {
		vector<TypeParameter> alias_params;
		if (type.HasExtensionInfo()) {
			for (auto &modifier : type.GetExtensionInfo()->modifiers) {
				alias_params.push_back(modifier.label.empty()
				                           ? TypeParameter::NonType(modifier.value)
				                           : TypeParameter::NonType(Identifier(modifier.label), modifier.value));
			}
		}
		result = make_uniq<TypeDescriptor>(QualifiedName(Identifier(alias)), std::move(alias_params));
		return true;
	}

	auto name = BuiltinTypeName(type.id());
	if (!name) {
		return Fail(error, StringUtil::Format("%s has no built-in type name", EnumUtil::ToString(type.id())));
	}

	// a parameterised type carries its parameters in its type info. DefaultTypeGenerator stores a bare
	// LogicalType(id) as the base a bind_function is applied to - that is a template, not a type, and there
	// is nothing to describe.
	if (RequiresTypeInfo(type.id()) && !type.AuxInfo()) {
		return Fail(error, StringUtil::Format("%s carries no type information", EnumUtil::ToString(type.id())));
	}

	vector<TypeParameter> params;
	switch (type.id()) {
	case LogicalTypeId::DECIMAL:
		params.push_back(TypeParameter::NonType(Value::UTINYINT(DecimalType::GetWidth(type))));
		params.push_back(TypeParameter::NonType(Value::UTINYINT(DecimalType::GetScale(type))));
		return Emit(result, name, std::move(params));
	case LogicalTypeId::LIST:
		if (!TryDescribeChild(ListType::GetChildType(type), Identifier(), params, error)) {
			return false;
		}
		return Emit(result, name, std::move(params));
	case LogicalTypeId::ARRAY:
		if (ArrayType::IsAnySize(type)) {
			return Fail(error, StringUtil::Format("ARRAY type \"%s\" has no fixed size", type.ToString()));
		}
		if (!TryDescribeChild(ArrayType::GetChildType(type), Identifier(), params, error)) {
			return false;
		}
		params.push_back(TypeParameter::NonType(Value::BIGINT(NumericCast<int64_t>(ArrayType::GetSize(type)))));
		return Emit(result, name, std::move(params));
	case LogicalTypeId::MAP:
		if (!TryDescribeChild(MapType::KeyType(type), Identifier(), params, error) ||
		    !TryDescribeChild(MapType::ValueType(type), Identifier(), params, error)) {
			return false;
		}
		return Emit(result, name, std::move(params));
	case LogicalTypeId::STRUCT:
		if (StructType::IsUnnamed(type)) {
			return Fail(error, StringUtil::Format("STRUCT type \"%s\" has unnamed fields", type.ToString()));
		}
		for (auto &child : StructType::GetChildTypes(type)) {
			if (!TryDescribeChild(child.second, child.first, params, error)) {
				return false;
			}
		}
		return Emit(result, name, std::move(params));
	case LogicalTypeId::TUPLE:
		for (auto &child : StructType::GetChildTypes(type)) {
			if (!TryDescribeChild(child.second, Identifier(), params, error)) {
				return false;
			}
		}
		return Emit(result, name, std::move(params));
	case LogicalTypeId::UNION: {
		auto member_count = UnionType::GetMemberCount(type);
		for (idx_t i = 0; i < member_count; i++) {
			if (!TryDescribeChild(UnionType::GetMemberType(type, i), UnionType::GetMemberName(type, i), params,
			                      error)) {
				return false;
			}
		}
		return Emit(result, name, std::move(params));
	}
	case LogicalTypeId::ENUM: {
		auto size = EnumType::GetSize(type);
		for (idx_t i = 0; i < size; i++) {
			params.push_back(TypeParameter::NonType(Value(EnumType::GetString(type, i).GetString())));
		}
		return Emit(result, name, std::move(params));
	}
	case LogicalTypeId::VARCHAR: {
		auto collation = StringType::GetCollation(type);
		if (!collation.empty()) {
			params.push_back(TypeParameter::NonType(Identifier("collation"), Value(collation)));
		}
		return Emit(result, name, std::move(params));
	}
	case LogicalTypeId::GEOMETRY:
		if (GeoType::HasCRS(type)) {
			params.push_back(TypeParameter::NonType(Value(GeoType::GetCRS(type).GetDefinition())));
		}
		return Emit(result, name, std::move(params));
	default:
		return Emit(result, name, {});
	}
}

} // namespace

TypeDescriptor TypeDescriptor::FromLogicalType(const LogicalType &type) {
	unique_ptr<TypeDescriptor> result;
	string error;
	if (!TryDescribe(type, result, error)) {
		throw NotImplementedException("Cannot describe type without a catalog: %s", error);
	}
	return std::move(*result);
}

unique_ptr<TypeDescriptor> TypeDescriptor::TryFromLogicalType(const LogicalType &type) {
	unique_ptr<TypeDescriptor> result;
	string error;
	if (!TryDescribe(type, result, error)) {
		return nullptr;
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Serialization
//===--------------------------------------------------------------------===//

void TypeParameter::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<Identifier>(100, "label", label);
	serializer.WritePropertyWithDefault<Value>(101, "value", value, Value());
	serializer.WritePropertyWithDefault<unique_ptr<TypeDescriptor>>(102, "type", type);
}

TypeParameter TypeParameter::Deserialize(Deserializer &deserializer) {
	TypeParameter result;
	deserializer.ReadPropertyWithDefault<Identifier>(100, "label", result.label);
	deserializer.ReadPropertyWithExplicitDefault<Value>(101, "value", result.value, Value());
	deserializer.ReadPropertyWithDefault<unique_ptr<TypeDescriptor>>(102, "type", result.type);
	return result;
}

void TypeDescriptor::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<QualifiedName>(100, "name", name);
	serializer.WritePropertyWithDefault<vector<TypeParameter>>(101, "parameters", parameters);
}

unique_ptr<TypeDescriptor> TypeDescriptor::Deserialize(Deserializer &deserializer) {
	auto name = deserializer.ReadProperty<QualifiedName>(100, "name");
	auto parameters = deserializer.ReadPropertyWithDefault<vector<TypeParameter>>(101, "parameters");
	return make_uniq<TypeDescriptor>(std::move(name), std::move(parameters));
}

} // namespace duckdb
