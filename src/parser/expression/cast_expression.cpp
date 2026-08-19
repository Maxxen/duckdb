#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> CastExpression::TypeExpressionFrom(const LogicalType &type) {
	if (type.IsUnbound()) {
		return UnboundType::GetTypeExpression(type)->Copy();
	}
	return make_uniq_base<ParsedExpression, ConstantExpression>(Value::TYPE(type));
}

CastExpression::CastExpression(unique_ptr<ParsedExpression> target, unique_ptr<ParsedExpression> child_p,
                               bool try_cast_p)
    : ParsedExpression(ExpressionType::OPERATOR_CAST, ExpressionClass::CAST), cast_type(std::move(target)),
      try_cast(try_cast_p) {
	D_ASSERT(child_p);
	D_ASSERT(cast_type);
	this->child = std::move(child_p);
}

CastExpression::CastExpression(const LogicalType &target, unique_ptr<ParsedExpression> child_p, bool try_cast_p)
    : CastExpression(TypeExpressionFrom(target), std::move(child_p), try_cast_p) {
}

CastExpression::CastExpression() : ParsedExpression(ExpressionType::OPERATOR_CAST, ExpressionClass::CAST) {
}

void CastExpression::SetTargetType(unique_ptr<ParsedExpression> target) {
	D_ASSERT(target);
	cast_type = std::move(target);
}

LogicalType CastExpression::GetTargetLogicalType() const {
	D_ASSERT(cast_type);
	if (cast_type->GetExpressionClass() == ExpressionClass::CONSTANT) {
		auto &constant = cast_type->Cast<ConstantExpression>();
		if (constant.GetValue().type().id() == LogicalTypeId::TYPE) {
			return TypeValue::GetType(constant.GetValue());
		}
	}
	return LogicalType::UNBOUND(cast_type->Copy());
}

string CastExpression::ToString() const {
	return ToString<CastExpression, ParsedExpression>(*this);
}

void CastExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "child", child);
	// the target is written as a LogicalType so the format does not depend on how it is held in memory
	serializer.WriteProperty<LogicalType>(201, "cast_type", GetTargetLogicalType());
	serializer.WritePropertyWithDefault<bool>(202, "try_cast", try_cast);
}

unique_ptr<ParsedExpression> CastExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CastExpression>(new CastExpression());
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "child", result->child);
	auto cast_type = deserializer.ReadProperty<LogicalType>(201, "cast_type");
	result->cast_type = TypeExpressionFrom(cast_type);
	deserializer.ReadPropertyWithDefault<bool>(202, "try_cast", result->try_cast);
	return std::move(result);
}

} // namespace duckdb
