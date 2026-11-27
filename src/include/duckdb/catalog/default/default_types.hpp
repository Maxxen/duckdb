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
class FunctionSignature;
class SchemaCatalogEntry;

class DefaultTypeGenerator : public DefaultGenerator {
public:
	DefaultTypeGenerator(Catalog &catalog, SchemaCatalogEntry &schema);

	SchemaCatalogEntry &schema;

public:
	DUCKDB_API static LogicalTypeId GetDefaultType(const Identifier &name);
	DUCKDB_API static LogicalType TryDefaultBind(const string &name, const vector<pair<string, Value>> &params);
	//! The signatures of the constructors of a built-in type: the parameters the type is written with, e.g.
	//! TUPLE(*args TYPE). A built-in type has at least one, a name that is not a built-in type has none
	DUCKDB_API static vector<FunctionSignature> GetDefaultConstructorSignatures(const Identifier &name);

	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const Identifier &entry_name) override;
	vector<Identifier> GetDefaultEntries() override;
};

} // namespace duckdb
