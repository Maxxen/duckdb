//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/catalog/default/default_generator.hpp"

namespace duckdb {
class SchemaCatalogEntry;

class DefaultTypeGenerator : public DefaultGenerator {
public:
	DefaultTypeGenerator(Catalog &catalog, SchemaCatalogEntry &schema);

	SchemaCatalogEntry &schema;

public:
	DUCKDB_API static LogicalTypeId GetDefaultType(const Identifier &name);
	DUCKDB_API static LogicalType TryDefaultBind(const string &name, const vector<pair<string, Value>> &params);
	//! The type a default type name resolves to without type parameters, when that needs no constructor - INVALID if
	//! the name is not a default type or has constructors of its own
	DUCKDB_API static LogicalType TryBindWithoutConstructor(const Identifier &name);

	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const Identifier &entry_name) override;
	vector<Identifier> GetDefaultEntries() override;
};

} // namespace duckdb
