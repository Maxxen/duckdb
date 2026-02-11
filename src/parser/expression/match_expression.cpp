#include "duckdb/parser/expression/match_expression.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Match Pattern
//----------------------------------------------------------------------------------------------------------------------

void MatchPattern::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "type", GetType());
}

unique_ptr<MatchPattern> MatchPattern::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<MatchPatternType>(100, "type");
	switch (type) {
	case MatchPatternType::WILDCARD:
		return MatchPatternWildcard::Deserialize(deserializer);
	case MatchPatternType::IDENTIFIER:
		return MatchPatternIdentifier::Deserialize(deserializer);
	case MatchPatternType::LITERAL:
		return MatchPatternLiteral::Deserialize(deserializer);
	case MatchPatternType::LIST:
		return MatchPatternList::Deserialize(deserializer);
	case MatchPatternType::REST:
		return MatchPatternRest::Deserialize(deserializer);
	//case MatchPatternType::STRUCT:
	//	return MatchPatternStruct::Deserialize(deserializer);
	default:
		throw SerializationException("Unknown MatchPatternType during deserialization");
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Match Pattern Wildcard
//----------------------------------------------------------------------------------------------------------------------

string MatchPatternWildcard::ToString() const {
	return "_";
}

bool MatchPatternWildcard::Equals(const MatchPattern &other) const {
	return other.GetType() == MatchPatternType::WILDCARD;
}

unique_ptr<MatchPattern> MatchPatternWildcard::Copy() const {
	return make_uniq<MatchPatternWildcard>();
}

void MatchPatternWildcard::Serialize(Serializer &serializer) const {
	MatchPattern::Serialize(serializer);
	// no properties to serialize
}

unique_ptr<MatchPattern> MatchPatternWildcard::Deserialize(Deserializer &deserializer) {
	return make_uniq<MatchPatternWildcard>();
}

//----------------------------------------------------------------------------------------------------------------------
// Match Pattern Rest
//----------------------------------------------------------------------------------------------------------------------

string MatchPatternRest::ToString() const {
	return "..";
}

bool MatchPatternRest::Equals(const MatchPattern &other) const {
	return other.GetType() == MatchPatternType::REST;
}

unique_ptr<MatchPattern> MatchPatternRest::Copy() const {
	return make_uniq<MatchPatternRest>();
}

void MatchPatternRest::Serialize(Serializer &serializer) const {
	MatchPattern::Serialize(serializer);
	// no properties to serialize
}

unique_ptr<MatchPattern> MatchPatternRest::Deserialize(Deserializer &deserializer) {
	return make_uniq<MatchPatternRest>();
}

//----------------------------------------------------------------------------------------------------------------------
// Match Pattern Identifier
//----------------------------------------------------------------------------------------------------------------------

string MatchPatternIdentifier::ToString() const {
	return KeywordHelper::WriteOptionallyQuoted(identifier);
}

bool MatchPatternIdentifier::Equals(const MatchPattern &other) const {
	if (other.GetType() != MatchPatternType::IDENTIFIER) {
		return false;
	}
	auto &other_identifier = other.Cast<MatchPatternIdentifier>();
	return identifier == other_identifier.identifier;
}

unique_ptr<MatchPattern> MatchPatternIdentifier::Copy() const {
	return make_uniq<MatchPatternIdentifier>(identifier);
}

void MatchPatternIdentifier::Serialize(Serializer &serializer) const {
	MatchPattern::Serialize(serializer);
	serializer.WriteProperty(200, "identifier", identifier);
}

unique_ptr<MatchPattern> MatchPatternIdentifier::Deserialize(Deserializer &deserializer) {
	auto identifier = deserializer.ReadProperty<string>(200, "identifier");
	return make_uniq<MatchPatternIdentifier>(identifier);
}

//----------------------------------------------------------------------------------------------------------------------
// Match Pattern Literal
//----------------------------------------------------------------------------------------------------------------------

string MatchPatternLiteral::ToString() const {
	return value.ToString();
}

bool MatchPatternLiteral::Equals(const MatchPattern &other) const {
	if (other.GetType() != MatchPatternType::LITERAL) {
		return false;
	}
	auto &other_literal = other.Cast<MatchPatternLiteral>();
	return value == other_literal.value;
}

unique_ptr<MatchPattern> MatchPatternLiteral::Copy() const {
	return make_uniq<MatchPatternLiteral>(value);
}

void MatchPatternLiteral::Serialize(Serializer &serializer) const {
	MatchPattern::Serialize(serializer);
	serializer.WriteProperty(200, "value", value);
}

unique_ptr<MatchPattern> MatchPatternLiteral::Deserialize(Deserializer &deserializer) {
	auto value = deserializer.ReadProperty<Value>(200, "value");
	return make_uniq<MatchPatternLiteral>(value);
}

//----------------------------------------------------------------------------------------------------------------------
// Match Pattern List
//----------------------------------------------------------------------------------------------------------------------

string MatchPatternList::ToString() const {
	string result = "[";
	for (idx_t i = 0; i < elements.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += elements[i]->ToString();
	}
	result += "]";
	return result;
}

bool MatchPatternList::Equals(const MatchPattern &other) const {
	if (other.GetType() != MatchPatternType::LIST) {
		return false;
	}
	auto &other_list = other.Cast<MatchPatternList>();
	if (elements.size() != other_list.elements.size()) {
		return false;
	}
	for (idx_t i = 0; i < elements.size(); i++) {
		if (!elements[i]->Equals(*other_list.elements[i])) {
			return false;
		}
	}
	return true;
}

unique_ptr<MatchPattern> MatchPatternList::Copy() const {
	vector<unique_ptr<MatchPattern>> element_copies;
	for (auto &element : elements) {
		element_copies.push_back(element->Copy());
	}
	return make_uniq<MatchPatternList>(std::move(element_copies));
}

void MatchPatternList::Serialize(Serializer &serializer) const {
	MatchPattern::Serialize(serializer);
	serializer.WriteProperty(200, "elements", elements);
}

unique_ptr<MatchPattern> MatchPatternList::Deserialize(Deserializer &deserializer) {
	auto elements = deserializer.ReadProperty<vector<unique_ptr<MatchPattern>>>(200, "elements");
	return make_uniq<MatchPatternList>(std::move(elements));
}

//----------------------------------------------------------------------------------------------------------------------
// Match Pattern Struct
//----------------------------------------------------------------------------------------------------------------------

string MatchPatternStruct::ToString() const {
	string result = "{";
	for (idx_t i = 0; i < fields.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += KeywordHelper::WriteQuoted(fields[i].first, '\'') + ": " + fields[i].second->ToString();
	}
	result += "}";
	return result;
}

bool MatchPatternStruct::Equals(const MatchPattern &other) const {
	if (other.GetType() != MatchPatternType::STRUCT) {
		return false;
	}
	auto &other_struct = other.Cast<MatchPatternStruct>();
	if (fields.size() != other_struct.fields.size()) {
		return false;
	}
	for (idx_t i = 0; i < fields.size(); i++) {
		if (fields[i].first != other_struct.fields[i].first) {
			return false;
		}
		if (!fields[i].second->Equals(*other_struct.fields[i].second)) {
			return false;
		}
	}
	return true;
}

unique_ptr<MatchPattern> MatchPatternStruct::Copy() const {
	child_list_t<unique_ptr<MatchPattern>> field_copies;
	for (auto &field : fields) {
		field_copies.emplace_back(field.first, field.second->Copy());
	}
	return make_uniq<MatchPatternStruct>(std::move(field_copies));
}

void MatchPatternStruct::Serialize(Serializer &serializer) const {
	MatchPattern::Serialize(serializer);

	serializer.WriteList(200, "fields", fields.size(), [&](Serializer::List &list, idx_t i) {
		list.WriteObject([&](Serializer &field_serializer) {
			auto &field = fields[i];
			field_serializer.WriteProperty(100, "name", field.first);
			field_serializer.WriteProperty(101, "pattern", field.second);
		});
	});
}

unique_ptr<MatchPattern> MatchPatternStruct::Deserialize(Deserializer &deserializer) {

	child_list_t<unique_ptr<MatchPattern>> fields;
	deserializer.ReadList(200, "fields", [&](Deserializer::List &list, idx_t index) {
		list.ReadObject([&](Deserializer &field) {
			auto field_name = field.ReadProperty<string>(100, "name");
			auto field_pattern = field.ReadProperty<unique_ptr<MatchPattern>>(101, "pattern");
			fields.emplace_back(std::move(field_name), std::move(field_pattern));
		});
	});

	auto result = make_uniq<MatchPatternStruct>(std::move(fields));
	return std::move(result);
}


//----------------------------------------------------------------------------------------------------------------------
// MatchArm
//----------------------------------------------------------------------------------------------------------------------

MatchArm::MatchArm() {
}

string MatchArm::ToString() const {
	string result = "WHEN " + pattern->ToString();
	if (guard) {
		result += " IF " + guard->ToString();
	}
	result += " THEN " + expression->ToString();
	return result;
}

bool MatchArm::Equals(const MatchArm &other) const {
	return pattern->Equals(*other.pattern) && expression->Equals(*other.expression);
}

unique_ptr<MatchArm> MatchArm::Copy() const {
	auto copy = make_uniq<MatchArm>();
	copy->pattern = pattern->Copy();
	copy->expression = expression->Copy();
	return copy;
}

void MatchArm::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "pattern", pattern);
	serializer.WritePropertyWithDefault(101, "guard", expression);
	serializer.WriteProperty(102, "expression", expression);
}

unique_ptr<MatchArm> MatchArm::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<MatchArm>();
	result->pattern = deserializer.ReadProperty<unique_ptr<MatchPattern>>(100, "pattern");
	result->guard = deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(101, "guard");
	result->expression = deserializer.ReadProperty<unique_ptr<ParsedExpression>>(102, "expression");
	return result;
}

//----------------------------------------------------------------------------------------------------------------------
// MatchExpression
//----------------------------------------------------------------------------------------------------------------------

MatchExpression::MatchExpression() : ParsedExpression(ExpressionType::MATCH_EXPR, ExpressionClass::MATCH) {
}

string MatchExpression::ToString() const {
	string result = "MATCH CASE";
	result += "(" + arg->ToString() + ")";
	for (auto &arm : arms) {
		result += " " + arm->ToString();
	}
	result += " ELSE " + else_expr->ToString();
	result += " END";
	return result;
}

bool MatchExpression::Equal(const MatchExpression &a, const MatchExpression &b) {
	if (!a.arg->Equals(*b.arg)) {
		return false;
	}
	if (a.arms.size() != b.arms.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.arms.size(); i++) {
		if (!a.arms[i]->pattern->Equals(*b.arms[i]->pattern)) {
			return false;
		}
		if (!a.arms[i]->expression->Equals(*b.arms[i]->expression)) {
			return false;
		}
		if (!a.else_expr->Equals(*b.else_expr)) {
			return false;
		}
	}
	return true;
}

unique_ptr<ParsedExpression> MatchExpression::Copy() const {
	auto copy = make_uniq<MatchExpression>();
	copy->CopyProperties(*this);

	copy->arg = arg->Copy();
	for (auto &arm : arms) {
		copy->arms.push_back(arm->Copy());
	}

	copy->else_expr = else_expr->Copy();

	return std::move(copy);
}

void MatchExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WriteProperty(200, "arg", arg);
	serializer.WriteProperty(201, "arms", arms);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(202, "else_expr", else_expr);
}

unique_ptr<ParsedExpression> MatchExpression::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<MatchExpression>();
	deserializer.ReadDeletedProperty<unique_ptr<ParsedExpression>>(200, "arg");
	deserializer.ReadProperty(201, "arms", result->arms);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(202, "else_expr", result->else_expr);
	return std::move(result);
}

} // namespace duckdb
