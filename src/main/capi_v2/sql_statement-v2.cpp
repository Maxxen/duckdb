#include "capi_v2_internal.hpp"

#include "duckdb/parser/parser.hpp"

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
		// connection_query, so a statement group is never split across the
		// API boundary.
		duckdb::Parser parser(connection->context->GetParserOptions());
		parser.ParseQuery(std::string(sql));
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
