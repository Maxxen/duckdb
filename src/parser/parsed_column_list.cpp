#include "duckdb/parser/parsed_column_list.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"

namespace duckdb {

ParsedColumnList::ParsedColumnList(bool allow_duplicate_names) : allow_duplicate_names(allow_duplicate_names) {
}

ParsedColumnList::ParsedColumnList(vector<ParsedColumnDefinition> columns, bool allow_duplicate_names)
    : allow_duplicate_names(allow_duplicate_names) {
	for (auto &col : columns) {
		AddColumn(std::move(col));
	}
}

void ParsedColumnList::AddColumn(ParsedColumnDefinition column) {
	auto oid = columns.size();
	AddToNameMap(column, oid);
	columns.push_back(std::move(column));
}

void ParsedColumnList::AddToNameMap(ParsedColumnDefinition &col, column_t oid) {
	if (!allow_duplicate_names) {
		if (name_map.find(col.Name()) != name_map.end()) {
			throw CatalogException("Column with name %s already exists!", col.Name());
		}
	}
	name_map[col.Name()] = oid;
}

const ParsedColumnDefinition &ParsedColumnList::GetColumn(LogicalIndex logical) const {
	if (logical.index >= columns.size()) {
		throw InternalException("Logical column index %lld out of range", logical.index);
	}
	return columns[logical.index];
}

const ParsedColumnDefinition &ParsedColumnList::GetColumn(const string &name) const {
	auto entry = name_map.find(name);
	if (entry == name_map.end()) {
		throw InternalException("Column with name \"%s\" does not exist", name);
	}
	auto logical_index = entry->second;
	D_ASSERT(logical_index < columns.size());
	return columns[logical_index];
}

vector<string> ParsedColumnList::GetColumnNames() const {
	vector<string> names;
	names.reserve(columns.size());
	for (auto &column : columns) {
		names.push_back(column.Name());
	}
	return names;
}

bool ParsedColumnList::ColumnExists(const string &name) const {
	return name_map.find(name) != name_map.end();
}

LogicalIndex ParsedColumnList::GetColumnIndex(string &column_name) const {
	auto entry = name_map.find(column_name);
	if (entry == name_map.end()) {
		return LogicalIndex(DConstants::INVALID_INDEX);
	}
	column_name = columns[entry->second].Name();
	return LogicalIndex(entry->second);
}

ParsedColumnList ParsedColumnList::Copy() const {
	ParsedColumnList result(allow_duplicate_names);
	for (auto &col : columns) {
		result.AddColumn(col.Copy());
	}
	return result;
}

ParsedColumnList::ParsedColumnListIterator ParsedColumnList::Logical() const {
	return ParsedColumnListIterator(*this);
}

} // namespace duckdb
