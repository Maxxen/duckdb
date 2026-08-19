//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_column_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_column_definition.hpp"

namespace duckdb {

//! A set of parsed column definitions, as produced by the parser.
//!
//! Unlike ColumnList this is purely a declaration-order list: it carries no physical/storage
//! mapping. The binder resolves it into a bound ColumnList, at which point physical column indices
//! and storage oids are assigned.
class ParsedColumnList {
public:
	class ParsedColumnListIterator;

public:
	DUCKDB_API explicit ParsedColumnList(bool allow_duplicate_names = false);
	DUCKDB_API explicit ParsedColumnList(vector<ParsedColumnDefinition> columns, bool allow_duplicate_names = false);

	DUCKDB_API void AddColumn(ParsedColumnDefinition column);

	DUCKDB_API const ParsedColumnDefinition &GetColumn(LogicalIndex index) const;
	DUCKDB_API const ParsedColumnDefinition &GetColumn(const Identifier &name) const;
	DUCKDB_API vector<string> GetColumnNames() const;

	DUCKDB_API bool ColumnExists(const Identifier &name) const;
	DUCKDB_API LogicalIndex GetColumnIndex(Identifier &column_name) const;

	idx_t LogicalColumnCount() const {
		return columns.size();
	}
	bool empty() const { // NOLINT: match stl API
		return columns.empty();
	}

	ParsedColumnList Copy() const;
	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static ParsedColumnList Deserialize(Deserializer &deserializer);

	DUCKDB_API ParsedColumnListIterator Logical() const;

	void SetAllowDuplicates(bool allow_duplicates) {
		allow_duplicate_names = allow_duplicates;
	}

private:
	vector<ParsedColumnDefinition> columns;
	//! A map of column name to logical column index
	identifier_map_t<column_t> name_map;
	//! Allow duplicate names or not
	bool allow_duplicate_names;

private:
	void AddToNameMap(ParsedColumnDefinition &column);

public:
	// logical iterator
	class ParsedColumnListIterator {
	public:
		explicit ParsedColumnListIterator(const ParsedColumnList &list) : list(list) {
		}

	private:
		const ParsedColumnList &list;

	private:
		class ParsedColumnListIteratorInternal {
		public:
			ParsedColumnListIteratorInternal(const ParsedColumnList &list, idx_t pos, idx_t end)
			    : list(list), pos(pos), end(end) {
			}

			const ParsedColumnList &list;
			idx_t pos;
			idx_t end;

		public:
			ParsedColumnListIteratorInternal &operator++() {
				pos++;
				return *this;
			}
			bool operator!=(const ParsedColumnListIteratorInternal &other) const {
				return pos != other.pos || end != other.end || &list != &other.list;
			}
			const ParsedColumnDefinition &operator*() const {
				return list.GetColumn(LogicalIndex(pos));
			}
		};

	public:
		idx_t Size() {
			return list.LogicalColumnCount();
		}

		ParsedColumnListIteratorInternal begin() { // NOLINT: match stl API
			return ParsedColumnListIteratorInternal(list, 0, Size());
		}
		ParsedColumnListIteratorInternal end() { // NOLINT: match stl API
			return ParsedColumnListIteratorInternal(list, Size(), Size());
		}
	};
};

} // namespace duckdb
