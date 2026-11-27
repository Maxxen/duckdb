#include "duckdb/function/function_binder.hpp"

#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

TypePack::TypePack(vector<Element> elements_p) : elements(std::move(elements_p)) {
}

TypePack TypePack::Positional(vector<LogicalType> types) {
	vector<Element> elements;
	elements.reserve(types.size());
	for (auto &type : types) {
		elements.push_back(Element {std::move(type), nullopt});
	}
	return TypePack(std::move(elements));
}

TypePack TypePack::Named(child_list_t<LogicalType> labelled_types) {
	vector<Element> elements;
	elements.reserve(labelled_types.size());
	for (auto &entry : labelled_types) {
		if (entry.first.empty()) {
			throw InternalException("An element of a named pack has no label");
		}
		elements.push_back(Element {std::move(entry.second), std::move(entry.first)});
	}
	return TypePack(std::move(elements));
}

TypePack TypePack::ParametersOf(const LogicalType &type) {
	vector<Element> elements;
	if (type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::TUPLE) {
		// the same elements as below, read without making a value of every type: a pack is made for every struct
		// argument of a call
		auto &children = StructType::GetChildTypes(type);
		elements.reserve(children.size());
		for (auto &child : children) {
			elements.push_back(Element {child.second, child.first.empty() ? optional<Identifier>() : child.first});
		}
		return TypePack(std::move(elements));
	}
	const auto count = type.GetParameterCount();
	elements.reserve(count);
	for (idx_t i = 0; i < count; i++) {
		auto parameter = type.GetParameter(i);
		if (parameter.type().id() != LogicalTypeId::TYPE || parameter.IsNull()) {
			throw InternalException("Parameter %llu of %s is not a type, so it is not an element of a pack of types", i,
			                        type.ToString());
		}
		// a type names a parameter that has no label by the empty identifier
		auto &name = type.GetParameterName(i);
		optional<Identifier> label;
		if (!name.empty()) {
			label = name;
		}
		elements.push_back(Element {TypeValue::GetType(parameter), std::move(label)});
	}
	return TypePack(std::move(elements));
}

idx_t TypePack::Count() const {
	return elements.size();
}

const LogicalType &TypePack::GetType(idx_t index) const {
	return elements[index].type;
}

optional_ptr<const Identifier> TypePack::GetLabel(idx_t index) const {
	auto &label = elements[index].label;
	return label ? &*label : nullptr;
}

string TypePack::ToString() const {
	string result = "(";
	for (idx_t i = 0; i < elements.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		if (elements[i].label) {
			result += SQLIdentifier(*elements[i].label) + " ";
		}
		result += elements[i].type.ToString();
	}
	return result + ")";
}

bool TypePack::operator==(const TypePack &other) const {
	if (elements.size() != other.elements.size()) {
		return false;
	}
	for (idx_t i = 0; i < elements.size(); i++) {
		auto &element = elements[i];
		auto &other_element = other.elements[i];
		if (element.label != other_element.label || !element.type.EqualsIncludingCollation(other_element.type)) {
			return false;
		}
	}
	return true;
}

bool TypePack::operator!=(const TypePack &other) const {
	return !(*this == other);
}

} // namespace duckdb
