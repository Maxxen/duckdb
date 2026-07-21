#include "capi_v2_test_helpers.hpp"

#include <string>
#include <vector>

// Collection binding: statement_add_collection + column_data_collection_reset.
// Fixtures dogfood the V2 API: collections are built through the V2 context scope
// and filled through the V2 chunk write surface.

namespace {

// RAII holder for a collection handle; destroyed on scope exit.
struct V2Cdc {
	duckdb_v2_column_data_collection_handle handle = nullptr;
	V2Cdc() = default;
	V2Cdc(const V2Cdc &) = delete;
	V2Cdc &operator=(const V2Cdc &) = delete;
	~V2Cdc() {
		duckdb_v2_column_data_collection_destroy(&handle);
	}
	operator duckdb_v2_column_data_collection_handle() const {
		return handle;
	}
};

// Creates an empty collection over the given type ids, borrowing a context via
// execute_with_context. The collection outlives the callback.
duckdb_v2_column_data_collection_handle MakeCdc(duckdb_v2_connection_handle conn,
                                                const std::vector<DUCKDB_V2_LOGICAL_TYPE_ID> &ids) {
	duckdb_v2_column_data_collection_handle cdc = nullptr;
	DUCKDB_V2_API_CALL_t rc = DUCKDB_V2_ERROR_NONE;
	V2WithContext(conn, [&](duckdb_v2_context_handle ctx) {
		std::vector<duckdb_v2_logical_type_handle> types;
		for (auto id : ids) {
			types.push_back(V2TypeOf(id));
		}
		rc = duckdb_v2_column_data_collection_create(ctx, types.data(), types.size(), &cdc, nullptr);
		for (auto &t : types) {
			duckdb_v2_logical_type_destroy(&t);
		}
	});
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(cdc != nullptr);
	return cdc;
}

// One INTEGER, VARCHAR row for the fillers below.
struct IVRow {
	int32_t i = 0;
	bool i_null = false;
	std::string s;
	bool s_null = false;
};

// Appends one chunk of INTEGER, VARCHAR rows through a fresh append state.
void AppendIV(duckdb_v2_column_data_collection_handle cdc, const std::vector<IVRow> &rows) {
	duckdb_v2_logical_type_handle types[2] = {V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER),
	                                          V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)};
	duckdb_v2_data_chunk_handle chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 2, &chunk, nullptr);
	duckdb_v2_logical_type_destroy(&types[0]);
	duckdb_v2_logical_type_destroy(&types[1]);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_handle ivec = nullptr;
	duckdb_v2_vector_handle svec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &ivec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 1, &svec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(ivec, rows.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(svec, rows.size(), nullptr) == DUCKDB_V2_ERROR_NONE);

	void *iraw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(ivec, &iraw, nullptr) == DUCKDB_V2_ERROR_NONE);
	for (idx_t r = 0; r < rows.size(); r++) {
		static_cast<int32_t *>(iraw)[r] = rows[r].i;
		if (rows[r].i_null) {
			REQUIRE(duckdb_v2_vector_set_null(ivec, r, nullptr) == DUCKDB_V2_ERROR_NONE);
		}
		if (rows[r].s_null) {
			REQUIRE(duckdb_v2_vector_set_null(svec, r, nullptr) == DUCKDB_V2_ERROR_NONE);
		} else {
			REQUIRE(V2VectorAssignString(svec, r, rows[r].s.data(), rows[r].s.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
		}
	}

	duckdb_v2_column_data_collection_append_state_handle st = nullptr;
	REQUIRE(duckdb_v2_column_data_collection_append_state_create(cdc, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_column_data_collection_append(cdc, st, chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_column_data_collection_append_state_destroy(&st);
	duckdb_v2_data_chunk_destroy(&chunk);
}

// Appends one chunk of INTEGER values through a fresh append state.
void AppendInts(duckdb_v2_column_data_collection_handle cdc, const std::vector<int32_t> &vals) {
	auto t = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_data_chunk_handle chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(&t, 1, &chunk, nullptr);
	duckdb_v2_logical_type_destroy(&t);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, vals.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	void *raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(vec, &raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	for (idx_t r = 0; r < vals.size(); r++) {
		static_cast<int32_t *>(raw)[r] = vals[r];
	}

	duckdb_v2_column_data_collection_append_state_handle st = nullptr;
	REQUIRE(duckdb_v2_column_data_collection_append_state_create(cdc, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_column_data_collection_append(cdc, st, chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_column_data_collection_append_state_destroy(&st);
	duckdb_v2_data_chunk_destroy(&chunk);
}

// Parses a single-statement SQL string and returns the owned statement handle.
// The iterator is destroyed immediately; the yielded statement is independent.
duckdb_v2_sql_statement_handle ParseOne(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(conn, sql, &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_handle stmt = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_statement_iterator_destroy(&iter);
	REQUIRE(stmt != nullptr);
	return stmt;
}

// Reads a single-row BIGINT scalar (count, sum) from a query.
int64_t ScalarBigint(duckdb_v2_connection_handle conn, const char *sql) {
	V2Result r;
	REQUIRE(V2Query(conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	int64_t out = reinterpret_cast<const int64_t *>(view.data)[SelAt(view.sel, 0)];
	duckdb_v2_data_chunk_destroy(&chunk);
	while (auto c = V2StepChunk(r)) {
		duckdb_v2_data_chunk_destroy(&c);
	}
	return out;
}

// Reads the single-row, non-null VARCHAR scalar from an executed result and
// drains the rest.
std::string ResultScalarVarchar(V2Result &r) {
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t row = SelAt(view.sel, 0);
	REQUIRE(RowValid(view, row));
	std::string out = V2StrTo(V2StringView(reinterpret_cast<const duckdb_v2_string *>(view.data)[row]));
	duckdb_v2_data_chunk_destroy(&chunk);
	while (auto c = V2StepChunk(r)) {
		duckdb_v2_data_chunk_destroy(&c);
	}
	return out;
}

// Reads a single-row, non-null VARCHAR scalar (a string_agg fingerprint) from a query.
std::string ScalarVarchar(duckdb_v2_connection_handle conn, const char *sql) {
	V2Result r;
	REQUIRE(V2Query(conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	return ResultScalarVarchar(r);
}

// Attaches a collection under `name`, executes, and returns the changed-row count.
int64_t AttachExecuteChanged(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle stmt, const char *name,
                             duckdb_v2_column_data_collection_handle cdc) {
	REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str(name), cdc, nullptr, 0, nullptr) == DUCKDB_V2_ERROR_NONE);
	V2Result r;
	REQUIRE(duckdb_v2_statement_execute(conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	return V2DrainChangedRows(r);
}

// Asserts the call was refused with INVALID_INPUT and left a message on the slot.
void RequireRefused(DUCKDB_V2_API_CALL_t rc, duckdb_v2_error_info_handle &err) {
	REQUIRE(rc == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(err, &msg) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(msg.len > 0);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(err == nullptr);
}

const char *long_name_text = "a string well beyond twelve bytes";

} // namespace

// ===========================================================================
// 1. INSERT flush happy path.
// ===========================================================================

TEST_CASE("V2: INSERT flush over an attached collection", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, name VARCHAR)");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	AppendIV(cdc, {{10, false, "alpha", false},
	               {20, false, long_name_text, false},
	               {0, true, "gamma", false},
	               {40, false, "", true}});

	auto stmt = ParseOne(fx.conn, "INSERT INTO t FROM buf");
	REQUIRE(AttachExecuteChanged(fx.conn, stmt, "buf", cdc) == 4);
	duckdb_v2_sql_statement_destroy(&stmt);

	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM t") == 4);
	REQUIRE(ScalarVarchar(fx.conn, "SELECT string_agg(coalesce(id::VARCHAR, 'NULL') || ':' || coalesce(name, 'NULL'), "
	                               "'|' ORDER BY id NULLS FIRST) FROM t") ==
	        std::string("NULL:gamma|10:alpha|20:") + long_name_text + "|40:NULL");
}

// ===========================================================================
// 2. Contents-at-execution pin.
// ===========================================================================

TEST_CASE("V2: re-execute after reset and refill inserts the new contents", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, name VARCHAR)");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	auto stmt = ParseOne(fx.conn, "INSERT INTO t FROM buf");
	REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) == DUCKDB_V2_ERROR_NONE);

	AppendIV(cdc, {{1, false, "one", false}});
	{
		V2Result r;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2DrainChangedRows(r) == 1);
	}

	// Reset drops the buffered row; the refill is what the next execution scans.
	REQUIRE(duckdb_v2_column_data_collection_reset(cdc, nullptr) == DUCKDB_V2_ERROR_NONE);
	AppendIV(cdc, {{2, false, "two", false}, {3, false, "three", false}});
	{
		V2Result r;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2DrainChangedRows(r) == 2);
	}
	duckdb_v2_sql_statement_destroy(&stmt);

	REQUIRE(ScalarVarchar(fx.conn, "SELECT string_agg(name, '|' ORDER BY id) FROM t") == "one|two|three");
}

TEST_CASE("V2: re-execute without reset inserts the same rows twice", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, name VARCHAR)");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	AppendIV(cdc, {{7, false, "seven", false}});

	auto stmt = ParseOne(fx.conn, "INSERT INTO t FROM buf");
	REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) == DUCKDB_V2_ERROR_NONE);
	for (idx_t i = 0; i < 2; i++) {
		V2Result r;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2DrainChangedRows(r) == 1);
	}
	duckdb_v2_sql_statement_destroy(&stmt);

	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM t WHERE id = 7") == 2);
}

// ===========================================================================
// 3. Reset invalidates append states; a fresh state appends again.
// ===========================================================================

TEST_CASE("V2: reset invalidates append states, a fresh state appends again", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, name VARCHAR)");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	AppendIV(cdc, {{1, false, "first", false}});
	auto stmt = ParseOne(fx.conn, "INSERT INTO t FROM buf");
	REQUIRE(AttachExecuteChanged(fx.conn, stmt, "buf", cdc) == 1);

	// Reset invalidates outstanding append/scan states. The documented pattern is
	// to create a new append state after reset, which AppendIV does per call.
	REQUIRE(duckdb_v2_column_data_collection_reset(cdc, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t after_reset = 99;
	REQUIRE(duckdb_v2_column_data_collection_row_count(cdc, &after_reset, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(after_reset == 0);

	AppendIV(cdc, {{2, false, "second", false}});
	idx_t refilled = 0;
	REQUIRE(duckdb_v2_column_data_collection_row_count(cdc, &refilled, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(refilled == 1);
	{
		V2Result r;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2DrainChangedRows(r) == 1);
	}
	duckdb_v2_sql_statement_destroy(&stmt);

	REQUIRE(ScalarVarchar(fx.conn, "SELECT string_agg(name, '|' ORDER BY id) FROM t") == "first|second");
}

// ===========================================================================
// 4. MERGE INTO over the buffer (upsert).
// ===========================================================================

TEST_CASE("V2: MERGE INTO upsert over an attached collection", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(k INTEGER PRIMARY KEY, v VARCHAR)");
	V2ExecSQL(fx.conn, "INSERT INTO t VALUES (1, 'old-one'), (2, 'old-two')");

	// One matched key (updated) and one new key (inserted).
	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	AppendIV(cdc, {{1, false, "new-one", false}, {3, false, "new-three", false}});

	auto stmt = ParseOne(fx.conn, "MERGE INTO t USING buf AS b(k, v) ON t.k = b.k "
	                              "WHEN MATCHED THEN UPDATE SET v = b.v "
	                              "WHEN NOT MATCHED THEN INSERT VALUES (b.k, b.v)");
	REQUIRE(AttachExecuteChanged(fx.conn, stmt, "buf", cdc) == 2);
	duckdb_v2_sql_statement_destroy(&stmt);

	REQUIRE(ScalarVarchar(fx.conn, "SELECT string_agg(k || ':' || v, '|' ORDER BY k) FROM t") ==
	        "1:new-one|2:old-two|3:new-three");
}

// ===========================================================================
// 5. SELECT join over the buffer, and two collections under two names.
// ===========================================================================

TEST_CASE("V2: SELECT join over an attached collection", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, label VARCHAR)");
	V2ExecSQL(fx.conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});
	AppendInts(cdc, {2, 3});

	auto stmt = ParseOne(fx.conn, "SELECT string_agg(t.label, '|' ORDER BY t.id) "
	                              "FROM t JOIN buf AS b(id) ON t.id = b.id");
	REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) == DUCKDB_V2_ERROR_NONE);
	V2Result r;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto got = ResultScalarVarchar(r);
	duckdb_v2_sql_statement_destroy(&stmt);
	REQUIRE(got == "b|c");
}

TEST_CASE("V2: two collections joined under two names", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;

	V2Cdc names;
	names.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	AppendIV(names, {{1, false, "x", false}, {2, false, "y", false}, {3, false, "z", false}});

	V2Cdc keys;
	keys.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});
	AppendInts(keys, {2, 3});

	auto stmt = ParseOne(fx.conn, "SELECT string_agg(a.name, ',' ORDER BY a.k) "
	                              "FROM buf1 AS a(k, name) JOIN buf2 AS b(k) ON a.k = b.k");
	REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf1"), names, nullptr, 0, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf2"), keys, nullptr, 0, nullptr) == DUCKDB_V2_ERROR_NONE);

	V2Result r;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto got = ResultScalarVarchar(r);
	duckdb_v2_sql_statement_destroy(&stmt);
	REQUIRE(got == "y,z");
}

// ===========================================================================
// 6. SQL column aliasing and the col1..colN default names.
// ===========================================================================

TEST_CASE("V2: SQL column aliasing and col1..colN defaults", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	AppendIV(cdc, {{5, false, "five", false}});

	// Aliased columns resolve.
	{
		auto stmt = ParseOne(fx.conn, "SELECT b.y || ':' || b.x FROM buf AS b(x, y)");
		REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		V2Result r;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		auto got = ResultScalarVarchar(r);
		duckdb_v2_sql_statement_destroy(&stmt);
		REQUIRE(got == "five:5");
	}

	// Default names col1..colN bind without an alias, and the result schema
	// carries them.
	{
		auto stmt = ParseOne(fx.conn, "SELECT col1, col2 FROM buf");
		REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		V2Result r;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		V2RequireColumn(r, 0, "col1", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
		V2RequireColumn(r, 1, "col2", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
		V2DrainRowCount(r);
		duckdb_v2_sql_statement_destroy(&stmt);
	}
}

TEST_CASE("V2: add_collection binds explicit column names", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	AppendIV(cdc, {{5, false, "five", false}});

	// The columns resolve by the given names without an alias, and the result
	// schema reports them.
	{
		duckdb_v2_identifier_t names[] = {V2Str("k"), V2Str("v")};
		auto stmt = ParseOne(fx.conn, "SELECT v || ':' || k AS combined, k FROM buf");
		REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, names, 2, nullptr) == DUCKDB_V2_ERROR_NONE);
		V2Result r;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		V2RequireColumn(r, 1, "k", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
		auto got = ResultScalarVarchar(r);
		duckdb_v2_sql_statement_destroy(&stmt);
		REQUIRE(got == "five:5");
	}

	// A column_count that does not match the collection's column count is refused.
	{
		duckdb_v2_identifier_t one[] = {V2Str("k")};
		auto stmt = ParseOne(fx.conn, "SELECT * FROM buf");
		duckdb_v2_error_info_handle err = nullptr;
		RequireRefused(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, one, 1, &err), err);
		duckdb_v2_sql_statement_destroy(&stmt);
	}
}

// ===========================================================================
// 7. Refusals; each carries a message on the err slot.
// ===========================================================================

TEST_CASE("V2: add_collection refusals carry an error message", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER)");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});

	// Duplicate name from a prior call.
	{
		auto stmt = ParseOne(fx.conn, "INSERT INTO t FROM buf");
		REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		duckdb_v2_error_info_handle err = nullptr;
		RequireRefused(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, &err), err);
		duckdb_v2_sql_statement_destroy(&stmt);
	}

	// Duplicate name already present in the SQL text as a CTE.
	{
		auto stmt = ParseOne(fx.conn, "WITH buf AS (SELECT 1 AS x) SELECT * FROM buf");
		duckdb_v2_error_info_handle err = nullptr;
		RequireRefused(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, &err), err);
		duckdb_v2_sql_statement_destroy(&stmt);
	}

	// Empty name view {NULL, 0}.
	{
		auto stmt = ParseOne(fx.conn, "INSERT INTO t FROM buf");
		duckdb_v2_error_info_handle err = nullptr;
		RequireRefused(duckdb_v2_statement_add_collection(stmt, duckdb_v2_str {nullptr, 0}, cdc, nullptr, 0, &err),
		               err);
		duckdb_v2_sql_statement_destroy(&stmt);
	}

	// Malformed name view: null pointer with nonzero length.
	{
		auto stmt = ParseOne(fx.conn, "INSERT INTO t FROM buf");
		duckdb_v2_error_info_handle err = nullptr;
		RequireRefused(duckdb_v2_statement_add_collection(stmt, duckdb_v2_str {nullptr, 5}, cdc, nullptr, 0, &err),
		               err);
		duckdb_v2_sql_statement_destroy(&stmt);
	}

	// Null statement.
	{
		duckdb_v2_error_info_handle err = nullptr;
		RequireRefused(duckdb_v2_statement_add_collection(nullptr, V2Str("buf"), cdc, nullptr, 0, &err), err);
	}

	// Null collection.
	{
		auto stmt = ParseOne(fx.conn, "INSERT INTO t FROM buf");
		duckdb_v2_error_info_handle err = nullptr;
		RequireRefused(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), nullptr, nullptr, 0, &err), err);
		duckdb_v2_sql_statement_destroy(&stmt);
	}

	// Unsupported statement type (SET).
	{
		auto stmt = ParseOne(fx.conn, "SET memory_limit = '1GB'");
		duckdb_v2_error_info_handle err = nullptr;
		RequireRefused(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, &err), err);
		duckdb_v2_sql_statement_destroy(&stmt);
	}
}

// ===========================================================================
// 8. Empty collection: INSERT executes, zero rows changed.
// ===========================================================================

TEST_CASE("V2: empty collection inserts zero rows", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, name VARCHAR)");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});

	auto stmt = ParseOne(fx.conn, "INSERT INTO t FROM buf");
	REQUIRE(AttachExecuteChanged(fx.conn, stmt, "buf", cdc) == 0);
	duckdb_v2_sql_statement_destroy(&stmt);

	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM t") == 0);
}

// ===========================================================================
// 9. statement_bind reflects the collection's schema.
// ===========================================================================

TEST_CASE("V2: statement_bind reflects the attached collection schema", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	AppendIV(cdc, {{1, false, "one", false}});

	auto stmt = ParseOne(fx.conn, "SELECT * FROM buf");
	REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &schema, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(schema, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);
	for (idx_t i = 0; i < 2; i++) {
		duckdb_v2_str field_name = {nullptr, 0};
		duckdb_v2_logical_type_handle field_type = nullptr; // borrowed
		REQUIRE(duckdb_v2_schema_get_field(schema, i, &field_name, &field_type, nullptr) == DUCKDB_V2_ERROR_NONE);
		DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		REQUIRE(duckdb_v2_logical_type_get_id(field_type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(field_name == (i == 0 ? "col1" : "col2"));
		REQUIRE(id == (i == 0 ? DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER : DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR));
	}
	duckdb_v2_schema_destroy(&schema);
	duckdb_v2_sql_statement_destroy(&stmt);
}

// ===========================================================================
// 10. statement_prepare + prepared_execute over the buffer.
// ===========================================================================

TEST_CASE("V2: statement_prepare and prepared_execute over an attached collection", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, name VARCHAR)");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	AppendIV(cdc, {{1, false, "one", false}, {2, false, "two", false}});

	auto stmt = ParseOne(fx.conn, "INSERT INTO t FROM buf");
	REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_prepared_statement_handle prepared = nullptr;
	REQUIRE(duckdb_v2_statement_prepare(fx.conn, stmt, false, &prepared, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_destroy(&stmt);
	{
		V2Result r;
		REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2DrainChangedRows(r) == 2);
	}
	duckdb_v2_prepared_statement_destroy(&prepared);

	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM t") == 2);
}

// ===========================================================================
// 11. Lifetime: result before collection; statement destroyed while the
//     collection lives on and is reusable via a fresh parse.
// ===========================================================================

TEST_CASE("V2: lifetime ordering, result before collection, statement reuse", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, name VARCHAR)");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	AppendIV(cdc, {{1, false, "one", false}});

	// First statement: execute, drain, destroy the result, then destroy the
	// statement, all while the collection stays alive.
	auto stmt1 = ParseOne(fx.conn, "INSERT INTO t FROM buf");
	REQUIRE(duckdb_v2_statement_add_collection(stmt1, V2Str("buf"), cdc, nullptr, 0, nullptr) == DUCKDB_V2_ERROR_NONE);
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt1, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2DrainChangedRows(r) == 1);
		REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);
	}
	duckdb_v2_sql_statement_destroy(&stmt1);

	// The collection outlives the first statement and is reusable via a fresh parse.
	auto stmt2 = ParseOne(fx.conn, "INSERT INTO t FROM buf");
	REQUIRE(AttachExecuteChanged(fx.conn, stmt2, "buf", cdc) == 1);
	duckdb_v2_sql_statement_destroy(&stmt2);

	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM t") == 2);
}

// ===========================================================================
// Expansion interaction. The only statement expansion reachable over in-memory
// data is an auto-PIVOT, which parses to one MultiStatement (an enum-creation
// plus a SELECT) that carries no single CTE map; the supported statement types
// do not themselves expand. So a collection binding cannot be combined with
// statement expansion, and add_collection refuses the MultiStatement.
// ===========================================================================

TEST_CASE("V2: an expanding auto-PIVOT parses to a MultiStatement that add_collection refuses",
          "[capi_v2][collection_binding]") {
	V2EnvFixture fx;

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
	AppendIV(cdc, {{1, false, "a", false}, {2, false, "b", false}});

	auto stmt = ParseOne(fx.conn, "PIVOT buf ON col2 USING sum(col1)");
	duckdb_v2_error_info_handle err = nullptr;
	RequireRefused(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, &err), err);
	duckdb_v2_sql_statement_destroy(&stmt);
}

// ===========================================================================
// Exhaustive type coverage. A value read out of a source column with the total
// cell reader (vector_get_value), written into a collection with the total cell
// writer (vector_set_value), and pushed through the binding into a destination
// table, must survive with full fidelity. This is the value path a binding uses
// for the long tail: it is total over every logical type, so one code path
// covers every column type below.
// ===========================================================================

namespace {

// One column type to round-trip: a type expression, the literals seeding the
// source rows (a trailing NULL row is always added), and any type-setup DDL.
struct TypeCase {
	const char *label;
	std::string setup;                 // optional DDL run before the tables; empty for none
	std::string type_sql;              // the column type expression
	std::vector<std::string> literals; // non-null source literals; a NULL row is appended
};

// Destroys every held value on scope exit, so a failing REQUIRE cannot leak the
// owned cell values read out of the source column.
struct V2ValueBag {
	std::vector<duckdb_v2_value_handle> values;
	V2ValueBag() = default;
	V2ValueBag(const V2ValueBag &) = delete;
	V2ValueBag &operator=(const V2ValueBag &) = delete;
	~V2ValueBag() {
		for (auto &v : values) {
			duckdb_v2_value_destroy(&v);
		}
	}
};

// Reads every row of `SELECT c FROM src` into owned value handles and copies the
// column's logical type out (owned). The result is fully drained and destroyed
// before returning, so the connection is free for the collection build that
// follows.
duckdb_v2_logical_type_handle ReadColumn(duckdb_v2_connection_handle conn, V2ValueBag &bag) {
	V2Result r;
	REQUIRE(V2Query(conn, "SELECT c FROM src", &r) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_result_get_schema(r, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str field_name = {nullptr, 0};
	duckdb_v2_logical_type_handle borrowed = nullptr; // borrowed; valid until schema destroyed
	REQUIRE(duckdb_v2_schema_get_field(schema, 0, &field_name, &borrowed, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_handle owned = nullptr;
	REQUIRE(duckdb_v2_logical_type_copy(borrowed, &owned, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_schema_destroy(&schema);
	while (auto chunk = V2StepChunk(r)) {
		idx_t size = 0;
		REQUIRE(duckdb_v2_data_chunk_get_size(chunk, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vector_handle vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
		for (idx_t row = 0; row < size; row++) {
			duckdb_v2_value_handle v = nullptr;
			REQUIRE(duckdb_v2_vector_get_value(vec, row, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
			bag.values.push_back(v);
		}
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	REQUIRE(owned != nullptr);
	return owned;
}

// Builds a single-column collection over `col_type` and fills it from `values`
// through the total cell writer, in one fresh chunk.
duckdb_v2_column_data_collection_handle BuildCollection(duckdb_v2_connection_handle conn,
                                                        duckdb_v2_logical_type_handle col_type,
                                                        const std::vector<duckdb_v2_value_handle> &values) {
	duckdb_v2_column_data_collection_handle cdc = nullptr;
	V2WithContext(conn, [&](duckdb_v2_context_handle ctx) {
		REQUIRE(duckdb_v2_column_data_collection_create(ctx, &col_type, 1, &cdc, nullptr) == DUCKDB_V2_ERROR_NONE);
	});
	REQUIRE(cdc != nullptr);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_data_chunk_create(&col_type, 1, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, values.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	for (idx_t row = 0; row < values.size(); row++) {
		REQUIRE(duckdb_v2_vector_set_value(vec, row, values[row], nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	duckdb_v2_column_data_collection_append_state_handle st = nullptr;
	REQUIRE(duckdb_v2_column_data_collection_append_state_create(cdc, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_column_data_collection_append(cdc, st, chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_column_data_collection_append_state_destroy(&st);
	duckdb_v2_data_chunk_destroy(&chunk);
	return cdc;
}

// Runs the source -> value handles -> collection -> INSERT -> destination round
// trip for one column type and asserts byte-for-byte fidelity: the destination
// row count matches and the symmetric difference is empty both ways. EXCEPT
// compares NULLs equal, so it doubles as an IS NOT DISTINCT FROM check.
void RequireRoundTrip(const TypeCase &tc) {
	V2EnvFixture fx;
	if (!tc.setup.empty()) {
		V2ExecSQL(fx.conn, tc.setup.c_str());
	}
	V2ExecSQL(fx.conn, ("CREATE TABLE src(c " + tc.type_sql + ")").c_str());
	V2ExecSQL(fx.conn, ("CREATE TABLE dst(c " + tc.type_sql + ")").c_str());

	std::string insert = "INSERT INTO src(c) VALUES ";
	for (const auto &lit : tc.literals) {
		insert += "(" + lit + "), ";
	}
	insert += "(NULL)";
	V2ExecSQL(fx.conn, insert.c_str());
	const idx_t row_count = tc.literals.size() + 1;

	V2ValueBag bag;
	duckdb_v2_logical_type_handle col_type = ReadColumn(fx.conn, bag);
	REQUIRE(bag.values.size() == row_count);

	V2Cdc cdc;
	cdc.handle = BuildCollection(fx.conn, col_type, bag.values);
	duckdb_v2_logical_type_destroy(&col_type);

	auto stmt = ParseOne(fx.conn, "INSERT INTO dst FROM buf");
	REQUIRE(AttachExecuteChanged(fx.conn, stmt, "buf", cdc) == static_cast<int64_t>(row_count));
	duckdb_v2_sql_statement_destroy(&stmt);

	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM dst") == static_cast<int64_t>(row_count));
	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM (SELECT * FROM src EXCEPT SELECT * FROM dst)") == 0);
	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM (SELECT * FROM dst EXCEPT SELECT * FROM src)") == 0);
}

// A string comfortably past the 12-byte inline cutoff for the string-backed kinds.
const char *inline_overflow_text = "a string well beyond twelve bytes for the non-inline heap path";

// The exhaustive type table: one row per column type. Every entry seeds at least
// one non-null literal and always carries a NULL row (added by RequireRoundTrip);
// variable-width kinds include a value past the inline cutoff, and nested kinds
// carry interior NULLs at both parent and child level.
//
// JSON is omitted: its logical type is a VARCHAR alias registered by the json
// extension, which is not linked into this test build.
std::vector<TypeCase> AllTypeCases() {
	std::string long_text = std::string("'") + inline_overflow_text + "'";
	return {
	    // Boolean and every integer width.
	    {"BOOLEAN", "", "BOOLEAN", {"true", "false"}},
	    {"TINYINT", "", "TINYINT", {"-128", "127", "0"}},
	    {"SMALLINT", "", "SMALLINT", {"-32768", "32767", "0"}},
	    {"INTEGER", "", "INTEGER", {"-2147483648", "2147483647", "0"}},
	    {"BIGINT", "", "BIGINT", {"-9223372036854775808", "9223372036854775807", "0"}},
	    {"HUGEINT",
	     "",
	     "HUGEINT",
	     {"-170141183460469231731687303715884105727", "170141183460469231731687303715884105727", "0"}},
	    {"UTINYINT", "", "UTINYINT", {"0", "255"}},
	    {"USMALLINT", "", "USMALLINT", {"0", "65535"}},
	    {"UINTEGER", "", "UINTEGER", {"0", "4294967295"}},
	    {"UBIGINT", "", "UBIGINT", {"0", "18446744073709551615"}},
	    {"UHUGEINT", "", "UHUGEINT", {"0", "340282366920938463463374607431768211455"}},

	    // Floating point and every decimal backing width (int16, int32, int64, int128).
	    {"FLOAT", "", "FLOAT", {"1.5", "-2.25", "0"}},
	    {"DOUBLE", "", "DOUBLE", {"3.141592653589793", "-2.5", "0"}},
	    {"DECIMAL(4,2)", "", "DECIMAL(4,2)", {"12.34", "-99.99", "0"}},
	    {"DECIMAL(9,2)", "", "DECIMAL(9,2)", {"1234567.89", "-1.00", "0"}},
	    {"DECIMAL(18,4)", "", "DECIMAL(18,4)", {"12345678901234.5678", "-0.0001", "0"}},
	    {"DECIMAL(38,10)",
	     "",
	     "DECIMAL(38,10)",
	     {"1234567890123456789012345678.1234567890", "-9999999999999999999999999999.9999999999", "0"}},

	    // Strings and byte kinds, each with a value past the inline cutoff.
	    {"VARCHAR", "", "VARCHAR", {"'short'", long_text, "('a' || chr(0) || 'b')", "''"}},
	    {"BLOB",
	     "",
	     "BLOB",
	     {"'\\xAA\\xBB\\x00\\xFF'::BLOB",
	      "'\\x00\\x11\\x22\\x33\\x44\\x55\\x66\\x77\\x88\\x99\\xAA\\xBB\\xCC\\xDD\\xEE\\xFF'::BLOB"}},
	    {"BIT", "", "BIT", {"'101'::BIT", "repeat('10', 60)::BIT"}},
	    {"VARINT",
	     "",
	     "VARINT",
	     {"'123456789012345678901234567890'::VARINT", "'-98765432109876543210987654321098765432109876543210'::VARINT",
	      "'0'::VARINT"}},
	    {"UUID",
	     "",
	     "UUID",
	     {"'12345678-1234-5678-1234-567812345678'::UUID", "'00000000-0000-0000-0000-000000000000'::UUID"}},

	    // Temporal kinds across every resolution.
	    {"DATE", "", "DATE", {"'2021-06-15'::DATE", "'1970-01-01'::DATE"}},
	    {"TIME", "", "TIME", {"'12:34:56'::TIME", "'00:00:00'::TIME"}},
	    {"TIME_NS", "", "TIME_NS", {"'12:34:56.123456789'::TIME_NS", "'00:00:00'::TIME_NS"}},
	    {"TIMETZ", "", "TIMETZ", {"'12:34:56+02'::TIMETZ", "'00:00:00+00'::TIMETZ"}},
	    {"TIMESTAMP", "", "TIMESTAMP", {"'2021-06-15 12:34:56'::TIMESTAMP"}},
	    {"TIMESTAMP_S", "", "TIMESTAMP_S", {"'2021-06-15 12:34:56'::TIMESTAMP_S"}},
	    {"TIMESTAMP_MS", "", "TIMESTAMP_MS", {"'2021-06-15 12:34:56.123'::TIMESTAMP_MS"}},
	    {"TIMESTAMP_NS", "", "TIMESTAMP_NS", {"'2021-06-15 12:34:56.123456789'::TIMESTAMP_NS"}},
	    {"TIMESTAMPTZ", "", "TIMESTAMPTZ", {"'2021-06-15 12:34:56+00'::TIMESTAMPTZ"}},
	    {"INTERVAL", "", "INTERVAL", {"'1 year 2 months 3 days 04:05:06.789'::INTERVAL", "INTERVAL '-5' MONTH"}},

	    // A user-declared enumeration.
	    {"ENUM", "CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok')", "mood", {"'happy'", "'sad'", "'ok'"}},

	    // Nested kinds with interior NULLs at parent and child level.
	    {"LIST", "", "INTEGER[]", {"[1, 2, 3]", "[NULL, 5]", "[]"}},
	    {"ARRAY", "", "INTEGER[3]", {"[1, 2, 3]", "[NULL, 5, 6]"}},
	    {"STRUCT", "", "STRUCT(a INTEGER, b VARCHAR)", {"{'a': 1, 'b': 'x'}", "{'a': NULL, 'b': NULL}"}},
	    {"MAP", "", "MAP(INTEGER, VARCHAR)", {"MAP([1, 2], ['a', 'b'])", "MAP([], [])"}},
	    {"UNION",
	     "",
	     "UNION(i INTEGER, s VARCHAR)",
	     {"union_value(i := 1)", std::string("union_value(s := ") + long_text + ")"}},

	    // Deeper nestings: a list of structs, and a struct carrying a list and a map.
	    {"LIST(STRUCT)",
	     "",
	     "STRUCT(a INTEGER, b VARCHAR)[]",
	     {"[{'a': 1, 'b': 'x'}, NULL, {'a': NULL, 'b': NULL}]", "[]"}},
	    {"STRUCT(LIST, MAP)",
	     "",
	     "STRUCT(l INTEGER[], m MAP(VARCHAR, INTEGER))",
	     {"{'l': [1, NULL, 3], 'm': MAP(['k1', 'k2'], [10, NULL])}", "{'l': NULL, 'm': NULL}"}},

	    // VARIANT: V1 and the typed-chunk writers reject it because they cannot
	    // coerce scalars into it, but the value path boxes and unboxes the cell
	    // as-is, so the round trip holds here (this assertion is the pin).
	    {"VARIANT",
	     "",
	     "VARIANT",
	     {"'{\"a\":1,\"b\":[1,2,3]}'::VARIANT", "'42'::VARIANT",
	      std::string("'\"") + inline_overflow_text + "\"'::VARIANT"}},
	};
}

} // namespace

TEST_CASE("V2: values of every logical type round-trip through the collection binding",
          "[capi_v2][collection_binding]") {
	for (const auto &tc : AllTypeCases()) {
		DYNAMIC_SECTION("type: " << tc.label) {
			RequireRoundTrip(tc);
		}
	}
}

// ===========================================================================
// Column subset appends. INSERT INTO t(<subset>) FROM buf, where the collection
// carries only the subset's columns in the subset's order and the engine fills
// each omitted column from its DEFAULT. This is composition over the existing
// binding: a column list in the caller's SQL, no new surface.
// ===========================================================================

namespace {

// One (VARCHAR, INTEGER) row for the subset fillers below.
struct BIRow {
	std::string b;
	bool b_null = false;
	int32_t id = 0;
	bool id_null = false;
};

// Appends one chunk of (VARCHAR, INTEGER) rows through a fresh append state. The
// column order matches a non-table-order subset such as (col_b, id).
void AppendVarcharInt(duckdb_v2_column_data_collection_handle cdc, const std::vector<BIRow> &rows) {
	duckdb_v2_logical_type_handle types[2] = {nullptr, nullptr};
	types[0] = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	types[1] = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_data_chunk_handle chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 2, &chunk, nullptr);
	duckdb_v2_logical_type_destroy(&types[0]);
	duckdb_v2_logical_type_destroy(&types[1]);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_handle bvec = nullptr;
	duckdb_v2_vector_handle ivec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &bvec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 1, &ivec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(bvec, rows.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(ivec, rows.size(), nullptr) == DUCKDB_V2_ERROR_NONE);

	void *iraw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(ivec, &iraw, nullptr) == DUCKDB_V2_ERROR_NONE);
	for (idx_t r = 0; r < rows.size(); r++) {
		static_cast<int32_t *>(iraw)[r] = rows[r].id;
		if (rows[r].id_null) {
			REQUIRE(duckdb_v2_vector_set_null(ivec, r, nullptr) == DUCKDB_V2_ERROR_NONE);
		}
		if (rows[r].b_null) {
			REQUIRE(duckdb_v2_vector_set_null(bvec, r, nullptr) == DUCKDB_V2_ERROR_NONE);
		} else {
			REQUIRE(V2VectorAssignString(bvec, r, rows[r].b.data(), rows[r].b.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
		}
	}

	duckdb_v2_column_data_collection_append_state_handle st = nullptr;
	REQUIRE(duckdb_v2_column_data_collection_append_state_create(cdc, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_column_data_collection_append(cdc, st, chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_column_data_collection_append_state_destroy(&st);
	duckdb_v2_data_chunk_destroy(&chunk);
}

// Executes the statement and asserts the engine rejected it with a message on
// the slot. Column-subset mismatches (a generated or unknown target column, a
// column-count or type mismatch against the list) come from the engine at INSERT
// bind/execute, not from any new code; the rejection surfaces either as the
// execute return or while draining the result, so this catches both.
void RequireInsertRejected(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle stmt,
                           const char *label = "") {
	INFO("case: " << label);
	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = duckdb_v2_statement_execute(conn, stmt, nullptr, nullptr, 0, &r, &err);
	while (rc == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		rc = duckdb_v2_result_step(r, &chunk, &status, &err);
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
		if (rc != DUCKDB_V2_ERROR_NONE || status == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED ||
		    status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED) {
			break;
		}
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
			REQUIRE(duckdb_v2_result_wait(r, nullptr) == DUCKDB_V2_ERROR_NONE);
		}
	}
	duckdb_v2_result_destroy(&r);
	REQUIRE(rc != DUCKDB_V2_ERROR_NONE);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(err, &msg) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(msg.len > 0);
	duckdb_v2_error_info_destroy(&err);
}

} // namespace

TEST_CASE("V2: column subset in non-table order leaves omitted columns at their default",
          "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, col_b VARCHAR, col_c INTEGER, col_d VARCHAR)");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});
	// A NULL in a subset column (col_b) and a NULL in the key (id).
	AppendVarcharInt(cdc, {{"alpha", false, 1, false}, {"", true, 2, false}, {"gamma", false, 0, true}});

	auto stmt = ParseOne(fx.conn, "INSERT INTO t(col_b, id) FROM buf");
	REQUIRE(AttachExecuteChanged(fx.conn, stmt, "buf", cdc) == 3);
	duckdb_v2_sql_statement_destroy(&stmt);

	// The two omitted columns default to NULL for every row.
	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM t WHERE col_c IS NULL AND col_d IS NULL") == 3);
	REQUIRE(ScalarVarchar(fx.conn, "SELECT string_agg(coalesce(id::VARCHAR, 'NULL') || '/' || coalesce(col_b, 'NULL'), "
	                               "'|' ORDER BY id NULLS LAST) FROM t") == "1/alpha|2/NULL|NULL/gamma");
}

TEST_CASE("V2: column subset fills an omitted foldable DEFAULT", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, filler INTEGER DEFAULT 42)");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});
	AppendInts(cdc, {1, 2, 3});

	auto stmt = ParseOne(fx.conn, "INSERT INTO t(id) FROM buf");
	REQUIRE(AttachExecuteChanged(fx.conn, stmt, "buf", cdc) == 3);
	duckdb_v2_sql_statement_destroy(&stmt);

	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM t WHERE filler = 42") == 3);
}

TEST_CASE("V2: column subset fills an omitted non-foldable DEFAULT at flush", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE SEQUENCE seq START 100");
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, s BIGINT DEFAULT nextval('seq'))");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});
	AppendInts(cdc, {10, 20, 30});

	auto stmt = ParseOne(fx.conn, "INSERT INTO t(id) FROM buf");
	REQUIRE(AttachExecuteChanged(fx.conn, stmt, "buf", cdc) == 3);
	duckdb_v2_sql_statement_destroy(&stmt);

	// The engine evaluates the sequence per row at flush: three distinct values from 100.
	REQUIRE(ScalarBigint(fx.conn, "SELECT count(DISTINCT s) FROM t") == 3);
	REQUIRE(ScalarBigint(fx.conn, "SELECT min(s) FROM t") == 100);
	REQUIRE(ScalarBigint(fx.conn, "SELECT max(s) FROM t") == 102);
}

TEST_CASE("V2: reordered subset re-executes after reset and refill", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, col_b VARCHAR, col_c INTEGER)");

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});
	auto stmt = ParseOne(fx.conn, "INSERT INTO t(col_b, id) FROM buf");
	REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) == DUCKDB_V2_ERROR_NONE);

	AppendVarcharInt(cdc, {{"one", false, 1, false}});
	{
		V2Result r;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2DrainChangedRows(r) == 1);
	}

	// Reset drops the first batch; the refill is what the second execution scans.
	REQUIRE(duckdb_v2_column_data_collection_reset(cdc, nullptr) == DUCKDB_V2_ERROR_NONE);
	AppendVarcharInt(cdc, {{"two", false, 2, false}, {"three", false, 3, false}});
	{
		V2Result r;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2DrainChangedRows(r) == 2);
	}
	duckdb_v2_sql_statement_destroy(&stmt);

	REQUIRE(ScalarVarchar(fx.conn, "SELECT string_agg(col_b, '|' ORDER BY id) FROM t") == "one|two|three");
	// The omitted column defaults to NULL across both batches.
	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM t WHERE col_c IS NULL") == 3);
}

TEST_CASE("V2: column subset errors surface from the engine", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;

	// A subset naming a generated column.
	{
		V2ExecSQL(fx.conn, "CREATE TABLE gen(id INTEGER, g INTEGER GENERATED ALWAYS AS (id + 1))");
		V2Cdc cdc;
		cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});
		AppendInts(cdc, {1});
		auto stmt = ParseOne(fx.conn, "INSERT INTO gen(g) FROM buf");
		REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		RequireInsertRejected(fx.conn, stmt, "generated column");
		duckdb_v2_sql_statement_destroy(&stmt);
	}

	// A subset naming a column that does not exist.
	{
		V2ExecSQL(fx.conn, "CREATE TABLE missing(id INTEGER)");
		V2Cdc cdc;
		cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});
		AppendInts(cdc, {1});
		auto stmt = ParseOne(fx.conn, "INSERT INTO missing(nope) FROM buf");
		REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		RequireInsertRejected(fx.conn, stmt, "unknown column");
		duckdb_v2_sql_statement_destroy(&stmt);
	}

	// A collection whose column count does not match the INSERT column list.
	{
		V2ExecSQL(fx.conn, "CREATE TABLE arity(id INTEGER, b VARCHAR)");
		V2Cdc cdc;
		cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER}); // one column, list names two
		AppendInts(cdc, {1});
		auto stmt = ParseOne(fx.conn, "INSERT INTO arity(id, b) FROM buf");
		REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		RequireInsertRejected(fx.conn, stmt, "count mismatch");
		duckdb_v2_sql_statement_destroy(&stmt);
	}

	// A collection whose column type cannot cast to the target column.
	{
		V2ExecSQL(fx.conn, "CREATE TABLE mistyped(s STRUCT(a INTEGER))");
		V2Cdc cdc;
		cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});
		AppendInts(cdc, {1});
		auto stmt = ParseOne(fx.conn, "INSERT INTO mistyped(s) FROM buf");
		REQUIRE(duckdb_v2_statement_add_collection(stmt, V2Str("buf"), cdc, nullptr, 0, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		RequireInsertRejected(fx.conn, stmt, "type mismatch");
		duckdb_v2_sql_statement_destroy(&stmt);
	}
}

// ===========================================================================
// 12. Collection append validation and the remaining statement types.
// ===========================================================================

TEST_CASE("V2: collection append refuses a mismatched chunk", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;

	V2Cdc cdc;
	cdc.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});

	duckdb_v2_column_data_collection_append_state_handle st = nullptr;
	REQUIRE(duckdb_v2_column_data_collection_append_state_create(cdc, &st, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The wrong column count is refused before anything is copied.
	{
		auto t = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
		duckdb_v2_data_chunk_handle chunk = nullptr;
		REQUIRE(duckdb_v2_data_chunk_create(&t, 1, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_logical_type_destroy(&t);
		REQUIRE(duckdb_v2_column_data_collection_append(cdc, st, chunk, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		duckdb_v2_data_chunk_destroy(&chunk);
	}

	// A wrong column type likewise.
	{
		duckdb_v2_logical_type_handle types[2] = {V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER),
		                                          V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT)};
		duckdb_v2_data_chunk_handle chunk = nullptr;
		REQUIRE(duckdb_v2_data_chunk_create(types, 2, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_logical_type_destroy(&types[0]);
		duckdb_v2_logical_type_destroy(&types[1]);
		REQUIRE(duckdb_v2_column_data_collection_append(cdc, st, chunk, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	duckdb_v2_column_data_collection_append_state_destroy(&st);

	// The refusals left the collection empty.
	idx_t count = 999;
	REQUIRE(duckdb_v2_column_data_collection_row_count(cdc, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);
}

TEST_CASE("V2: collection binding drives UPDATE and DELETE", "[capi_v2][collection_binding]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(id INTEGER, label VARCHAR)");
	V2ExecSQL(fx.conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");

	// An UPDATE joined against the bound collection.
	V2Cdc hits;
	hits.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});
	AppendInts(hits, {1, 3});
	auto update = ParseOne(fx.conn, "UPDATE t SET label = 'hit' FROM buf AS b(k) WHERE t.id = b.k");
	REQUIRE(AttachExecuteChanged(fx.conn, update, "buf", hits) == 2);
	duckdb_v2_sql_statement_destroy(&update);
	REQUIRE(ScalarVarchar(fx.conn, "SELECT string_agg(label, '|' ORDER BY id) FROM t") == "hit|b|hit");

	// A DELETE with a USING clause over the bound collection.
	V2Cdc gone;
	gone.handle = MakeCdc(fx.conn, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER});
	AppendInts(gone, {2});
	auto del = ParseOne(fx.conn, "DELETE FROM t USING buf AS b(k) WHERE t.id = b.k");
	REQUIRE(AttachExecuteChanged(fx.conn, del, "buf", gone) == 1);
	duckdb_v2_sql_statement_destroy(&del);
	REQUIRE(ScalarBigint(fx.conn, "SELECT count(*) FROM t") == 2);
	REQUIRE(ScalarVarchar(fx.conn, "SELECT string_agg(label, '|' ORDER BY id) FROM t") == "hit|hit");
}
