#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

struct LogicalTypeModifier {
public:
	explicit LogicalTypeModifier(Value value_p) : value(std::move(value_p)) {
	}
	string ToString() const {
		return label.empty() ? value.ToString() : label.GetIdentifierName();
	}

public:
	Value value;
	Identifier label;

	void Serialize(Serializer &serializer) const;
	static LogicalTypeModifier Deserialize(Deserializer &source);
};

struct ExtensionTypeInfo {
	vector<LogicalTypeModifier> modifiers;
	unordered_map<string, Value> properties;

public:
	void Serialize(Serializer &serializer) const;
	static unique_ptr<ExtensionTypeInfo> Deserialize(Deserializer &source);
	static bool Equals(optional_ptr<ExtensionTypeInfo> rhs, optional_ptr<ExtensionTypeInfo> lhs);
};

} // namespace duckdb
