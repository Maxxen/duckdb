#include "duckdb/parser/type_name.hpp"

#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// TypeVariable
//===--------------------------------------------------------------------===//
TypeVariable::TypeVariable(Identifier name_p, TypeVariableKind kind_p, TypeVariableArity arity_p)
    : name(std::move(name_p)), kind(kind_p), arity(arity_p) {
}

string TypeVariable::ToString() const {
	string result = SQLIdentifier::ToString(name);
	if (arity == TypeVariableArity::PACK) {
		result += "...";
	}
	switch (kind) {
	case TypeVariableKind::INTEGER:
		result += " INTEGER";
		break;
	case TypeVariableKind::STRING:
		result += " VARCHAR";
		break;
	default:
		break;
	}
	return result;
}

bool TypeVariable::operator==(const TypeVariable &other) const {
	return name == other.name && kind == other.kind && arity == other.arity;
}

bool TypeVariable::operator!=(const TypeVariable &other) const {
	return !(*this == other);
}

//===--------------------------------------------------------------------===//
// TypeParam
//===--------------------------------------------------------------------===//
TypeParam::TypeParam(TypeParamKind kind_p, Identifier name_p) : kind(kind_p), name(std::move(name_p)) {
}

TypeParam TypeParam::Type(TypeName type_p, Identifier name_p, bool expand_pack_p) {
	TypeParam result(TypeParamKind::TYPE, std::move(name_p));
	result.type = make_shared_ptr<const TypeName>(std::move(type_p));
	result.expand_pack = expand_pack_p;
	return result;
}

TypeParam TypeParam::Integer(int64_t value, Identifier name_p) {
	TypeParam result(TypeParamKind::INTEGER, std::move(name_p));
	result.integer = value;
	return result;
}

TypeParam TypeParam::String(string value, Identifier name_p) {
	TypeParam result(TypeParamKind::STRING, std::move(name_p));
	result.str = std::move(value);
	return result;
}

const TypeName &TypeParam::GetType() const {
	if (kind != TypeParamKind::TYPE) {
		throw InternalException("TypeParam::GetType called on a non-type parameter");
	}
	return *type;
}

int64_t TypeParam::GetInteger() const {
	if (kind != TypeParamKind::INTEGER) {
		throw InternalException("TypeParam::GetInteger called on a non-integer parameter");
	}
	return integer;
}

const string &TypeParam::GetString() const {
	if (kind != TypeParamKind::STRING) {
		throw InternalException("TypeParam::GetString called on a non-string parameter");
	}
	return str;
}

string TypeParam::ToString() const {
	string value;
	switch (kind) {
	case TypeParamKind::TYPE:
		value = type->ToString();
		if (expand_pack) {
			value += "...";
		}
		break;
	case TypeParamKind::INTEGER:
		value = to_string(integer);
		break;
	case TypeParamKind::STRING:
		value = SQLString::ToString(str);
		break;
	}
	if (name.empty()) {
		return value;
	}
	if (kind == TypeParamKind::TYPE) {
		return SQLIdentifier::ToString(name) + " " + value;
	}
	return SQLIdentifier::ToString(name) + " := " + value;
}

bool TypeParam::operator==(const TypeParam &other) const {
	if (kind != other.kind || name != other.name || expand_pack != other.expand_pack) {
		return false;
	}
	switch (kind) {
	case TypeParamKind::TYPE:
		return *type == *other.type;
	case TypeParamKind::INTEGER:
		return integer == other.integer;
	case TypeParamKind::STRING:
		return str == other.str;
	}
	return false;
}

bool TypeParam::operator!=(const TypeParam &other) const {
	return !(*this == other);
}

hash_t TypeParam::Hash() const {
	auto result = duckdb::Hash(static_cast<uint8_t>(kind));
	result = CombineHash(result, IdentifierHashFunction()(name));
	switch (kind) {
	case TypeParamKind::TYPE:
		return CombineHash(result, type->Hash());
	case TypeParamKind::INTEGER:
		return CombineHash(result, duckdb::Hash(integer));
	case TypeParamKind::STRING:
		return CombineHash(result, duckdb::Hash(str.c_str(), str.size()));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// TypeName
//===--------------------------------------------------------------------===//
TypeName::TypeName(QualifiedName name_p, vector<TypeParam> params_p)
    : name(std::move(name_p)), params(std::move(params_p)) {
}

const Identifier &TypeName::PseudoTypeQualifier() {
	static const Identifier QUALIFIER("$pseudo");
	return QUALIFIER;
}

bool TypeName::IsPseudoType() const {
	auto &path = name.Path();
	return path.size() == 2 && path[0] == PseudoTypeQualifier();
}

bool TypeName::IsPseudoType(LogicalTypeId id) const {
	return IsPseudoType() && name.Name() == Identifier(EnumUtil::ToString(id));
}

bool TypeName::ContainsInvalid() const {
	if (!IsValid()) {
		return true;
	}
	for (auto &param : params) {
		if (param.GetKind() == TypeParamKind::TYPE && param.GetType().ContainsInvalid()) {
			return true;
		}
	}
	return false;
}

bool TypeName::ContainsPseudoType(LogicalTypeId id) const {
	if (IsPseudoType(id)) {
		return true;
	}
	for (auto &param : params) {
		if (param.GetKind() == TypeParamKind::TYPE && param.GetType().ContainsPseudoType(id)) {
			return true;
		}
	}
	return false;
}

string TypeName::ToString() const {
	string result;
	if (IsPseudoType()) {
		result = name.Name().GetIdentifierName();
	} else {
		result = name.QualificationToString();
		auto &type_name = name.Name().GetIdentifierName();
		if (!IsQualified() && DefaultTypeGenerator::GetDefaultType(name.Name()) != LogicalTypeId::INVALID) {
			result += type_name;
		} else {
			result += SQLIdentifier::ToString(type_name);
		}
	}
	if (!params.empty()) {
		vector<string> param_strings;
		for (auto &param : params) {
			param_strings.push_back(param.ToString());
		}
		result += "(" + StringUtil::Join(param_strings, ", ") + ")";
	}
	return result;
}

bool TypeName::operator==(const TypeName &other) const {
	return name == other.name && params == other.params;
}

bool TypeName::operator!=(const TypeName &other) const {
	return !(*this == other);
}

hash_t TypeName::Hash() const {
	auto result = name.Hash();
	for (auto &param : params) {
		result = CombineHash(result, param.Hash());
	}
	return result;
}

const char *TypeName::BuiltinName(LogicalTypeId id) {
	switch (id) {
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
	case LogicalTypeId::SQLNULL:
		return "NULL";
	case LogicalTypeId::TYPE:
		return "TYPE";
	case LogicalTypeId::VARIANT:
		return "VARIANT";
	case LogicalTypeId::GEOMETRY:
		return "GEOMETRY";
	case LogicalTypeId::DECIMAL:
		return "DECIMAL";
	case LogicalTypeId::ENUM:
		return "ENUM";
	case LogicalTypeId::LIST:
		return "LIST";
	case LogicalTypeId::ARRAY:
		return "ARRAY";
	case LogicalTypeId::STRUCT:
		return "STRUCT";
	case LogicalTypeId::TUPLE:
		return "TUPLE";
	case LogicalTypeId::MAP:
		return "MAP";
	case LogicalTypeId::UNION:
		return "UNION";
	default:
		return nullptr;
	}
}

} // namespace duckdb
