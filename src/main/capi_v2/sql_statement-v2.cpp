#include "capi_v2_internal.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"

#include <algorithm>

DUCKDB_V2_API_CALL_t duckdb_v2_parse_sql(duckdb_v2_connection_handle conn, const char *sql,
                                         duckdb_v2_statement_iterator_handle *out_iterator,
                                         duckdb_v2_error_info_handle *err) {
	if (out_iterator) {
		*out_iterator = nullptr;
	}
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn || !sql || !out_iterator) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_parse_sql");
		}
		auto *connection = duckdb::ToConn(conn);
		// Set up a lazy iterator over the connection's parser options and
		// extensions, but parse nothing here. Each statement is parsed on demand by
		// statement_iterator_next, so a statement that registers grammar (LOAD an
		// extension) is parsed and executed before a following statement uses it.
		// Statements are raw parser output: statement-level rewrites (pragma
		// reparsing, expansion unpacking, transaction wrapping) happen in
		// statement_execute, so a statement group is never split across the API
		// boundary. A parse error is not raised here; it surfaces from the next()
		// that reaches the failing statement.
		auto wrapper = duckdb::make_uniq<duckdb::StatementIteratorWrapperV2>(connection->context, std::string(sql));
		*out_iterator = reinterpret_cast<_duckdb_v2_statement_iterator *>(wrapper.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_statement_iterator_next(duckdb_v2_statement_iterator_handle iterator,
                                                       duckdb_v2_sql_statement_handle *out_statement,
                                                       duckdb_v2_error_info_handle *err) {
	if (out_statement) {
		*out_statement = nullptr;
	}
	return duckdb::WithErrorHandler(err, [&]() {
		if (!iterator || !out_statement) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_statement_iterator_next");
		}
		auto *wrapper = duckdb::ToStatementIterator(iterator);
		if (wrapper->finished) {
			// Spent by a prior exhaustion or parse error: *out_statement stays NULL.
			return;
		}
		duckdb::unique_ptr<duckdb::SQLStatement> statement;
		try {
			// Construct the ParseIterator on first use: its constructor runs UTF-8
			// validation and can throw, routed through the same rendering below.
			if (!wrapper->iterator) {
				wrapper->iterator = duckdb::make_uniq<duckdb::ParseIterator>(*wrapper->context, wrapper->query);
			}
			// Parse the next statement on demand. Peek returns false at exhaustion;
			// *out_statement then stays NULL, idempotently.
			if (!wrapper->iterator->Peek()) {
				wrapper->finished = true;
				return;
			}
			statement = wrapper->iterator->GetStatement();
		} catch (const std::exception &ex) {
			// A parse error surfaces here, when iteration reaches the failing
			// statement, not up front. It terminates iteration (a following next()
			// reports clean exhaustion, not the error again). Route it through the
			// engine's public ProcessError like the eager query path so the rendered
			// shape honors errors_as_json (JSON, else LINE/caret), then re-throw to
			// route back through WithErrorHandler.
			wrapper->finished = true;
			duckdb::ErrorData error(ex);
			wrapper->context->ProcessError(error, wrapper->query);
			error.Throw();
		}
		*out_statement = reinterpret_cast<_duckdb_v2_sql_statement *>(statement.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_sql_statement_destroy(duckdb_v2_sql_statement_handle *statement) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!statement) {
			return;
		}
		if (*statement) {
			delete duckdb::ToSqlStatement(*statement);
			*statement = nullptr;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_statement_iterator_destroy(duckdb_v2_statement_iterator_handle *iterator) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!iterator) {
			return;
		}
		if (*iterator) {
			delete duckdb::ToStatementIterator(*iterator);
			*iterator = nullptr;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_statement_bind(duckdb_v2_connection_handle conn,
                                              duckdb_v2_sql_statement_handle statement,
                                              duckdb_v2_schema_handle *out_schema,
                                              duckdb_v2_schema_handle *out_parameters,
                                              duckdb_v2_error_info_handle *err) {
	if (out_schema) {
		*out_schema = nullptr;
	}
	if (out_parameters) {
		*out_parameters = nullptr;
	}
	if (!conn || !statement || !out_schema) {
		return duckdb::WithErrorHandler(
		    err, [&]() { throw duckdb::InvalidInputException("null argument to duckdb_v2_statement_bind"); });
	}
	auto *connection = duckdb::ToConn(conn);
	return duckdb::WithErrorHandler(err, [&]() {
		// Borrowed, not consumed: bind a copy. Preprocess it so the schema matches
		// what execution would bind (pragma reparse, expansion).
		duckdb::vector<duckdb::unique_ptr<duckdb::SQLStatement>> fragments;
		fragments.push_back(duckdb::ToSqlStatement(statement)->Copy());
		connection->context->PreprocessStatements(fragments);
		if (fragments.size() != 1) {
			// A statement that expands into a group (a dynamic PIVOT, or
			// statement-expanding DDL) is not bindable: bind binds one engine statement.
			throw duckdb::InvalidInputException(
			    "statement expands into multiple engine statements; bind is not supported, execute it instead");
		}
		// BindStatement, not Prepare: bind phase only (read-only, no optimize / physical
		// plan) and must not cancel a live result (Prepare's InitialCleanup would). It
		// throws on a bind error, routed through WithErrorHandler.
		auto signature = connection->context->BindStatement(std::move(fragments[0]));
		// Assemble into locals, publish to the out-params only at the end once nothing
		// can throw, so a failure leaves them NULL.
		auto output = duckdb::make_uniq<duckdb::SchemaWrapperV2>();
		for (duckdb::idx_t i = 0; i < signature.types.size(); i++) {
			output->fields.push_back({signature.names[i].GetIdentifierName(), signature.types[i]});
		}
		duckdb::unique_ptr<duckdb::SchemaWrapperV2> params;
		if (out_parameters) {
			params = duckdb::make_uniq<duckdb::SchemaWrapperV2>();
			// BindStatement returns parameters unordered; order by binding index for the
			// positional input-schema contract that pairs with statement_execute's values.
			// Positional params may be gapped ($1, $3 with no $2), so not a dense 1..N; the
			// field name carries the binding index.
			std::sort(signature.parameters.begin(), signature.parameters.end(),
			          [](const auto &a, const auto &b) { return a.index < b.index; });
			for (auto &parameter : signature.parameters) {
				params->fields.push_back({parameter.identifier.GetIdentifierName(), parameter.type});
			}
		}
		*out_schema = reinterpret_cast<_duckdb_v2_schema *>(output.release());
		if (out_parameters) {
			*out_parameters = reinterpret_cast<_duckdb_v2_schema *>(params.release());
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_statement_add_collection(duckdb_v2_sql_statement_handle statement,
                                                        duckdb_v2_identifier_t name,
                                                        duckdb_v2_column_data_collection_handle collection,
                                                        const duckdb_v2_identifier_t *column_names, idx_t column_count,
                                                        duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!statement) {
			throw duckdb::InvalidInputException("null statement argument to duckdb_v2_statement_add_collection");
		}
		if (!collection) {
			throw duckdb::InvalidInputException("null collection argument to duckdb_v2_statement_add_collection");
		}
		// name is a borrowed view and must be non-empty: the empty view {NULL, 0}
		// is rejected here (a deviation from the general str-input rule), as is
		// the malformed null-pointer-with-nonzero-length form.
		if (!name.ptr || name.len == 0) {
			throw duckdb::InvalidInputException(
			    "name must be a non-empty string view in duckdb_v2_statement_add_collection");
		}
		if (column_count > 0 && !column_names) {
			throw duckdb::InvalidInputException(
			    "column_names is null but column_count is nonzero in duckdb_v2_statement_add_collection");
		}
		auto &stmt = *duckdb::ToSqlStatement(statement);
		auto &cdc = *reinterpret_cast<duckdb::ColumnDataCollection *>(collection);
		auto alias = duckdb::ToIdentifier(name);

		// Explicit names, when given, must cover every column of the collection.
		if (column_count > 0 && column_count != cdc.ColumnCount()) {
			throw duckdb::InvalidInputException("column_count (" + std::to_string(column_count) +
			                                    ") does not match the collection's column count (" +
			                                    std::to_string(cdc.ColumnCount()) + ")");
		}
		duckdb::vector<duckdb::Identifier> expected_names;
		for (idx_t i = 0; i < column_count; i++) {
			expected_names.push_back(duckdb::ToIdentifier(column_names[i]));
		}

		// Locate the CTE map (throws on an unsupported statement type) and refuse a
		// name already present, whether from the SQL text or a prior call.
		auto &cte_map = duckdb::CommonTableExpressionMap::GetForStatement(stmt);
		if (cte_map.map.contains(alias)) {
			throw duckdb::InvalidInputException("a CTE named \"%s\" already exists on the statement",
			                                    alias.GetIdentifierName());
		}

		// Borrow the collection into a ColumnDataRef and bind it as the engine's
		// non-materialized collection CTE. Empty expected_names keeps col1..colN.
		auto table_ref = duckdb::ColumnDataRef::Create(cdc, alias, expected_names);
		cte_map.map.insert(alias, duckdb::CommonTableExpressionInfo::WrapNonMaterialized(std::move(table_ref)));
	});
}
