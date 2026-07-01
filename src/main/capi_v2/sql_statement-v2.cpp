#include "capi_v2_internal.hpp"

#include "duckdb/parser/parser.hpp"

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
		auto wrapper = duckdb::make_uniq<duckdb::StatementIteratorWrapperV2>();
		// Raw parse only: the connection supplies the parser options and
		// parser extensions, nothing else. No preprocessing, no binding, no
		// catalog access, no transaction. Statement-level rewrites (pragma
		// reparsing, expansion unpacking, transaction wrapping) happen in
		// statement_execute, so a statement group is never split across the
		// API boundary.
		//
		// A parse error goes through the engine's public ProcessError, like the
		// eager query path: honors errors_as_json (JSON, else LINE/caret), then
		// re-thrown to route back through WithErrorHandler.
		duckdb::Parser parser(connection->context->GetParserOptions());
		try {
			parser.ParseQuery(std::string(sql));
		} catch (const duckdb::Exception &ex) {
			duckdb::ErrorData error(ex);
			connection->context->ProcessError(error, std::string(sql));
			error.Throw();
		}
		wrapper->statements = std::move(parser.statements);
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
		if (wrapper->cursor >= wrapper->statements.size()) {
			// Exhausted, idempotently: *out_statement stays NULL.
			return;
		}
		auto statement = std::move(wrapper->statements[wrapper->cursor++]);
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
