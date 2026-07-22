#include "capi_v2_internal.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

namespace {

TableDescription *ToTableDescription(duckdb_v2_table_description_handle desc) {
	return reinterpret_cast<TableDescription *>(desc);
}

} // namespace

} // namespace duckdb

DUCKDB_V2_ERROR duckdb_v2_table_description_create(duckdb_v2_connection_handle conn, duckdb_v2_qname_handle name,
                                                   duckdb_v2_table_description_handle *out_desc,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || !name || !out_desc) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_table_description_create");
		}
		*out_desc = nullptr;
		auto &context = *duckdb::ToConn(conn)->context;
		auto &path = reinterpret_cast<duckdb::QualifiedName *>(name)->Path();
		// The qname invariant guarantees one to three non-empty parts. A two-part
		// name is read as SQL reads it: the first part tries as a schema and as an
		// attached database, and BindSchemaOrCatalog rejects the ambiguous case.
		duckdb::Identifier catalog;
		duckdb::Identifier schema;
		if (path.size() == 3) {
			catalog = path[0];
			schema = path[1];
		} else if (path.size() == 2) {
			schema = path[0];
			// The attached-database lookup reads catalog state and needs a transaction.
			context.RunFunctionInTransaction([&]() { duckdb::Binder::BindSchemaOrCatalog(context, catalog, schema); });
		}
		auto description = context.TableInfo(catalog, schema, path.back());
		if (!description) {
			throw duckdb::CatalogException("Table with name %s does not exist!",
			                               reinterpret_cast<duckdb::QualifiedName *>(name)->ToString());
		}
		*out_desc = reinterpret_cast<duckdb_v2_table_description_handle>(description.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_description_get_qname(duckdb_v2_table_description_handle desc,
                                                      duckdb_v2_qname_handle *out_qname,
                                                      duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!desc || !out_qname) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_table_description_get_qname");
		}
		auto &resolved = duckdb::ToTableDescription(desc)->qualified_name;
		*out_qname = reinterpret_cast<duckdb_v2_qname_handle>(new duckdb::QualifiedName(resolved));
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_description_get_schema(duckdb_v2_table_description_handle desc,
                                                       duckdb_v2_schema_handle *out_schema,
                                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!desc || !out_schema) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_table_description_get_schema");
		}
		*out_schema = nullptr;
		auto schema = duckdb::make_uniq<duckdb::SchemaWrapperV2>();
		for (auto &column : duckdb::ToTableDescription(desc)->columns) {
			schema->fields.push_back({column.Name().GetIdentifierName(), column.GetType()});
		}
		*out_schema = reinterpret_cast<duckdb_v2_schema_handle>(schema.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_description_column_is_generated(duckdb_v2_table_description_handle desc, idx_t index,
                                                                bool *out_is_generated,
                                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!desc || !out_is_generated) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_table_description_column_is_generated");
		}
		auto &columns = duckdb::ToTableDescription(desc)->columns;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException("column index %llu out of range for a table with %llu columns", index,
			                                    columns.size());
		}
		*out_is_generated = columns[index].Generated();
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_description_column_has_default(duckdb_v2_table_description_handle desc, idx_t index,
                                                               bool *out_has_default,
                                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!desc || !out_has_default) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_table_description_column_has_default");
		}
		auto &columns = duckdb::ToTableDescription(desc)->columns;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException("column index %llu out of range for a table with %llu columns", index,
			                                    columns.size());
		}
		auto &column = columns[index];
		*out_has_default = !column.Generated() && column.HasDefaultValue();
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_description_is_readonly(duckdb_v2_table_description_handle desc, bool *out_readonly,
                                                        duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!desc || !out_readonly) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_table_description_is_readonly");
		}
		*out_readonly = duckdb::ToTableDescription(desc)->readonly;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_description_destroy(duckdb_v2_table_description_handle *desc) {
	if (!desc || !*desc) {
		return static_cast<DUCKDB_V2_ERROR>(DUCKDB_V2_ERROR_NONE);
	}
	delete duckdb::ToTableDescription(*desc);
	*desc = nullptr;
	return static_cast<DUCKDB_V2_ERROR>(DUCKDB_V2_ERROR_NONE);
}
