#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api_helpers.hpp"
#include "test_helpers.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: the batch Appender (chunk oriented).
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb_api;

// A VARCHAR value beyond the 12-byte inline cutoff, exercising the string heap.
const std::string ap_long_string = "a string well beyond twelve bytes";

// Reads a single-row BIGINT scalar (count, sum) from a query.
int64_t ApScalarBigint(Connection &conn, const std::string &sql) {
	auto result = conn.Execute(sql);
	auto chunk = result.FetchChunk();
	REQUIRE(chunk);
	auto view = chunk.GetVector(0).GetView();
	REQUIRE(view.IsValid(0));
	int64_t out = view.Data<int64_t>()[view.SelAt(0)];
	while (result.FetchChunk()) {
	}
	return out;
}

// Reads a single-row, non-null VARCHAR scalar (a string_agg fingerprint).
std::string ApScalarVarchar(Connection &conn, const std::string &sql) {
	auto result = conn.Execute(sql);
	auto chunk = result.FetchChunk();
	REQUIRE(chunk);
	auto view = chunk.GetVector(0).GetView();
	REQUIRE(view.IsValid(0));
	auto slot = view.Data<StringLayout>()[view.SelAt(0)];
	std::string out(slot.AsStringView());
	while (result.FetchChunk()) {
	}
	return out;
}

// Builds an empty chunk over the appender's column types. Construction borrows a
// context; the chunk is context-free afterwards, so the caller fills and appends
// it outside the transaction.
std::unique_ptr<DataChunk> MakeChunk(Appender &app, Connection &conn) {
	std::unique_ptr<DataChunk> chunk;
	chunk = std::make_unique<DataChunk>(app.ColumnTypes());
	return chunk;
}

// Appends rows to a single-INTEGER-column appender by building one chunk.
void AppendInts(Appender &app, Connection &conn, const std::vector<int32_t> &values) {
	auto chunk = MakeChunk(app, conn);
	auto vec = chunk->GetVector(0);
	vec.SetSize(values.size());
	auto *data = vec.GetDataMutable<int32_t>();
	for (idx_t i = 0; i < values.size(); i++) {
		data[i] = values[i];
	}
	app.AppendChunk(*chunk);
}

// One (INTEGER, VARCHAR) row; a nullopt string is appended as NULL.
struct IntStr {
	int32_t i;
	std::optional<std::string> s;
};

// Appends (INTEGER, VARCHAR) rows to a two-column appender by building one chunk.
void AppendIntStr(Appender &app, Connection &conn, const std::vector<IntStr> &rows) {
	auto chunk = MakeChunk(app, conn);
	auto ivec = chunk->GetVector(0);
	auto svec = chunk->GetVector(1);
	ivec.SetSize(rows.size());
	svec.SetSize(rows.size());
	auto *ints = ivec.GetDataMutable<int32_t>();
	auto svalidity = svec.GetValidityMutable(); // fresh mask: all valid
	for (idx_t i = 0; i < rows.size(); i++) {
		ints[i] = rows[i].i;
		if (rows[i].s) {
			svec.AssignString(i, *rows[i].s);
		} else {
			svalidity.SetInvalid(i);
		}
	}
	app.AppendChunk(*chunk);
}

// The two column types shared by the query-constructor tests below (INTEGER,
// VARCHAR), rebuilt per call because LogicalType is move-only.
std::vector<LogicalType> IntVarcharTypes() {
	std::vector<LogicalType> types;
	types.push_back(LogicalType::INTEGER());
	types.push_back(LogicalType::VARCHAR());
	return types;
}

} // namespace

TEST_CASE("Stable C++API: Appender common leaf types, long VARCHAR, and NULLs", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE leaves(b BOOLEAN, i8 TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, "
	             "u8 UTINYINT, u16 USMALLINT, u32 UINTEGER, u64 UBIGINT, f FLOAT, d DOUBLE, s VARCHAR)")
	    .Drain();

	Appender appender(conn, QualifiedName::Parse("leaves"));

	// A two-row chunk: row 0 carries a value per column, row 1 is all NULL.
	auto chunk = MakeChunk(appender, conn);
	const idx_t rows = 2;
	for (idx_t c = 0; c < 12; c++) {
		chunk->GetVector(c).SetSize(rows);
	}
	chunk->GetVector(0).GetDataMutable<bool>()[0] = true;
	chunk->GetVector(1).GetDataMutable<int8_t>()[0] = -8;
	chunk->GetVector(2).GetDataMutable<int16_t>()[0] = -16;
	chunk->GetVector(3).GetDataMutable<int32_t>()[0] = -32;
	chunk->GetVector(4).GetDataMutable<int64_t>()[0] = -64;
	chunk->GetVector(5).GetDataMutable<uint8_t>()[0] = 8;
	chunk->GetVector(6).GetDataMutable<uint16_t>()[0] = 16;
	chunk->GetVector(7).GetDataMutable<uint32_t>()[0] = 32;
	chunk->GetVector(8).GetDataMutable<uint64_t>()[0] = 64;
	chunk->GetVector(9).GetDataMutable<float>()[0] = 1.5f;
	chunk->GetVector(10).GetDataMutable<double>()[0] = 2.5;
	chunk->GetVector(11).AssignString(0, ap_long_string);
	// Row 1 NULL in every column.
	for (idx_t c = 0; c < 12; c++) {
		chunk->GetVector(c).GetValidityMutable().SetInvalid(1);
	}
	appender.AppendChunk(*chunk);
	appender.Flush(conn);

	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM leaves") == 2);
	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM leaves WHERE b AND i8 = -8 AND i16 = -16 AND i32 = -32 "
	                             "AND i64 = -64 AND u8 = 8 AND u16 = 16 AND u32 = 32 AND u64 = 64 "
	                             "AND f = 1.5 AND d = 2.5 AND s = '" +
	                                 ap_long_string + "'") == 1);
	// The NULL row is NULL in every column.
	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM leaves WHERE b IS NULL AND i8 IS NULL AND i16 IS NULL "
	                             "AND i32 IS NULL AND i64 IS NULL AND u8 IS NULL AND u16 IS NULL AND u32 IS NULL "
	                             "AND u64 IS NULL AND f IS NULL AND d IS NULL AND s IS NULL") == 1);
}

TEST_CASE("Stable C++API: Appender buffers multiple chunks before one flush", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(i INTEGER)").Drain();

	Appender appender(conn, QualifiedName::Parse("t"));
	AppendInts(appender, conn, {1, 2, 3});
	AppendInts(appender, conn, {4, 5});
	appender.Flush(conn);

	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM t") == 5);
	REQUIRE(ApScalarBigint(conn, "SELECT sum(i)::BIGINT FROM t") == 15);
}

TEST_CASE("Stable C++API: Appender two flushes accumulate", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(i INTEGER)").Drain();

	Appender appender(conn, QualifiedName::Parse("t"));
	AppendInts(appender, conn, {1});
	appender.Flush(conn);
	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM t") == 1);

	AppendInts(appender, conn, {2, 3});
	appender.Flush(conn);
	// A flush with an empty buffer is a no-op.
	appender.Flush(conn);

	REQUIRE(ApScalarVarchar(conn, "SELECT string_agg(i::VARCHAR, '|' ORDER BY i) FROM t") == "1|2|3");
}

TEST_CASE("Stable C++API: Appender AppendChunk refuses a mismatched chunk", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(i INTEGER, s VARCHAR)").Drain();

	Appender appender(conn, QualifiedName::Parse("t"));

	// A chunk whose types match ColumnTypes() buffers fine.
	AppendIntStr(appender, conn, {{10, "bulk"}, {11, "bulk"}, {12, "bulk"}});

	// A chunk with the wrong column types is refused before anything is copied.
	std::unique_ptr<DataChunk> wrong;
	std::vector<LogicalType> types;
	types.push_back(LogicalType::BIGINT());
	types.push_back(LogicalType::VARCHAR());
	wrong = std::make_unique<DataChunk>(types);
	wrong->GetVector(0).SetSize(1);
	wrong->GetVector(1).SetSize(1);
	REQUIRE_THROWS_MATCHES(appender.AppendChunk(*wrong), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// The appender stays usable after the refusal.
	AppendIntStr(appender, conn, {{99, "tail"}});
	appender.Flush(conn);

	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM t") == 4);
	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM t WHERE s = 'bulk'") == 3);
	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM t WHERE i = 99 AND s = 'tail'") == 1);
}

TEST_CASE("Stable C++API: Appender Clear drops buffered chunks", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(i INTEGER)").Drain();

	Appender appender(conn, QualifiedName::Parse("t"));
	AppendInts(appender, conn, {1});
	appender.Flush(conn);

	// Buffered but not flushed: Clear discards it.
	AppendInts(appender, conn, {2});
	appender.Clear();
	AppendInts(appender, conn, {3});
	appender.Flush(conn);

	REQUIRE(ApScalarVarchar(conn, "SELECT string_agg(i::VARCHAR, '|' ORDER BY i) FROM t") == "1|3");
}

TEST_CASE("Stable C++API: Appender is reusable after Flush; missing table refuses", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(i INTEGER)").Drain();

	Appender appender(conn, QualifiedName::Parse("t"));
	AppendInts(appender, conn, {1});
	appender.Flush(conn);
	// A flush with an empty buffer is a no-op.
	appender.Flush(conn);
	// The appender stays usable after a flush: append and flush again.
	AppendInts(appender, conn, {2});
	appender.Flush(conn);
	REQUIRE(ApScalarVarchar(conn, "SELECT string_agg(i::VARCHAR, '|' ORDER BY i) FROM t") == "1|2");

	// A missing table refuses at construction with the engine's catalog error.
	REQUIRE_THROWS_MATCHES(Appender(conn, QualifiedName::Parse("no_such_table")), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_DATABASE_CATALOG));
}

TEST_CASE("Stable C++API: Appender skips generated columns", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE g(a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2))").Drain();

	// ColumnTypes() carries only the non-generated column; the engine computes b
	// at flush.
	Appender appender(conn, QualifiedName::Parse("g"));
	REQUIRE(appender.ColumnTypes().size() == 1);
	AppendInts(appender, conn, {21});
	appender.Flush(conn);

	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM g WHERE a = 21 AND b = 42") == 1);
}

TEST_CASE("Stable C++API: Appender does not match a view of the same name", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE VIEW v AS SELECT 1 AS a").Drain();

	// A view carries columns but is not an appendable base table; the resolution
	// refuses it rather than building an INSERT that would fail at flush.
	REQUIRE_THROWS_MATCHES(Appender(conn, QualifiedName::Parse("v")), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_DATABASE_CATALOG));
}

TEST_CASE("Stable C++API: Appender resolution follows the search path", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE SCHEMA s").Drain();
	conn.Execute("CREATE TABLE s.only_here(i INTEGER)").Drain();

	// The table lives only in schema s, which is not on the search path: the
	// unqualified name does not resolve, exactly as it would not in SQL.
	REQUIRE_THROWS_MATCHES(Appender(conn, QualifiedName::Parse("only_here")), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_DATABASE_CATALOG));

	// The qualified name reaches it, and the flush INSERT stays pinned to the
	// resolved table.
	Appender appender(conn, QualifiedName::Parse("s.only_here"));
	AppendInts(appender, conn, {5});
	appender.Flush(conn);

	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM s.only_here WHERE i = 5") == 1);
}

TEST_CASE("Stable C++API: Appender refuses a table in a read-only database", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto path = duckdb::TestCreatePath("cpp_appender_readonly.db");
	duckdb::DeleteDatabase(path);

	// Create the file backed table through a writable attach, then reattach read
	// only. The two-part name promotes its first part to the attached database.
	conn.Execute("ATTACH '" + path + "' AS rw").Drain();
	conn.Execute("CREATE TABLE rw.rt(i INTEGER)").Drain();
	conn.Execute("DETACH rw").Drain();
	conn.Execute("ATTACH '" + path + "' AS ro (READ_ONLY)").Drain();

	REQUIRE_THROWS_MATCHES(Appender(conn, QualifiedName::Parse("ro.rt")), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	conn.Execute("DETACH ro").Drain();
	duckdb::DeleteDatabase(path);
}

TEST_CASE("Stable C++API: Appender appends to a temporary table", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TEMP TABLE tmp(i INTEGER)").Drain();

	// A temp table's catalog is temp; the fully qualified flush targets it.
	Appender appender(conn, QualifiedName::Parse("tmp"));
	AppendInts(appender, conn, {9});
	appender.Flush(conn);

	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM tmp WHERE i = 9") == 1);
}

TEST_CASE("Stable C++API: Appender destructor does not flush; unflushed rows are dropped", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(i INTEGER)").Drain();

	// Rows left in the buffer when the appender is destroyed are dropped, not
	// flushed: persisting them is an explicit Flush.
	{
		Appender appender(conn, QualifiedName::Parse("t"));
		AppendInts(appender, conn, {7});
	}
	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM t") == 0);

	// The same rows persist when Flush is called before the appender goes away.
	{
		Appender appender(conn, QualifiedName::Parse("t"));
		AppendInts(appender, conn, {7});
		appender.Flush(conn);
	}
	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM t WHERE i = 7") == 1);
}

TEST_CASE("Stable C++API: Appender Flush on a busy connection fails and is retryable", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(i INTEGER)").Drain();

	Appender appender(conn, QualifiedName::Parse("t"));
	AppendInts(appender, conn, {7});
	{
		// A live, undrained result occupies the connection's execution slot.
		auto blocker = conn.Execute("SELECT i FROM range(10) t(i)");
		REQUIRE_THROWS_MATCHES(appender.Flush(conn), Exception, HasErrorCode(DUCKDB_V2_ERROR_RESOURCE_IN_USE));
	}
	// The buffered rows survived the busy failure; the retry inserts them.
	appender.Flush(conn);

	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM t WHERE i = 7") == 1);
}

TEST_CASE("Stable C++API: Appender flushes on a different connection than it was built with", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto builder = db.Connect();
	auto flusher = db.Connect();
	builder.Execute("CREATE TABLE t(i INTEGER)").Drain();

	// Built with one connection, flushed with another to the same database. The
	// appender holds no connection of its own between calls.
	Appender appender(builder, QualifiedName::Parse("t"));
	AppendInts(appender, builder, {1, 2});
	appender.Flush(flusher);

	// The flush connection can be busy-independent of the builder: a live result
	// on `builder` does not block a flush on `flusher`.
	auto blocker = builder.Execute("SELECT i FROM range(10) t(i)");
	AppendInts(appender, flusher, {3});
	appender.Flush(flusher);
	while (blocker.FetchChunk()) {
	}

	REQUIRE(ApScalarVarchar(flusher, "SELECT string_agg(i::VARCHAR, '|' ORDER BY i) FROM t") == "1|2|3");
}

TEST_CASE("Stable C++API: Appender query constructor drives a plain INSERT", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(id INTEGER, s VARCHAR)").Drain();

	// The caller brings the statement and the buffer types; the buffered rows bind
	// as appended_data(col1, col2).
	Appender appender(conn, "INSERT INTO t FROM appended_data", IntVarcharTypes());
	AppendIntStr(appender, conn, {{1, "one"}, {2, ap_long_string}});
	appender.Flush(conn);

	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM t") == 2);
	REQUIRE(ApScalarBigint(conn, "SELECT count(*) FROM t WHERE id = 2 AND s = '" + ap_long_string + "'") == 1);
}

TEST_CASE("Stable C++API: Appender query constructor honors a custom data name and column rename",
          "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(id INTEGER, s VARCHAR)").Drain();

	// A caller-chosen buffer name, and the col1..colN columns renamed in SQL.
	Appender appender(conn, "INSERT INTO t SELECT key, label FROM rows AS r(key, label)", IntVarcharTypes(), "rows");
	AppendIntStr(appender, conn, {{1, "one"}, {2, "two"}});
	appender.Flush(conn);

	REQUIRE(ApScalarVarchar(conn, "SELECT string_agg(id || ':' || s, '|' ORDER BY id) FROM t") == "1:one|2:two");
}

TEST_CASE("Stable C++API: Appender query constructor drives an UPDATE the table constructor cannot express",
          "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(id INTEGER, v VARCHAR)").Drain();
	conn.Execute("INSERT INTO t VALUES (1, 'old'), (2, 'keep')").Drain();

	// UPDATE ... FROM the buffer: a statement type the table constructor has no
	// way to reach.
	Appender appender(conn, "UPDATE t SET v = d.col2 FROM appended_data AS d WHERE t.id = d.col1", IntVarcharTypes());
	AppendIntStr(appender, conn, {{1, "new"}});
	appender.Flush(conn);

	REQUIRE(ApScalarVarchar(conn, "SELECT string_agg(id || ':' || v, '|' ORDER BY id) FROM t") == "1:new|2:keep");
}

TEST_CASE("Stable C++API: Appender query constructor drives a MERGE INTO upsert", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(k INTEGER PRIMARY KEY, v VARCHAR)").Drain();
	conn.Execute("INSERT INTO t VALUES (1, 'old-one'), (2, 'old-two')").Drain();

	// One matched key (updated) and one new key (inserted).
	Appender appender(conn,
	                  "MERGE INTO t USING appended_data AS b(k, v) ON t.k = b.k "
	                  "WHEN MATCHED THEN UPDATE SET v = b.v "
	                  "WHEN NOT MATCHED THEN INSERT VALUES (b.k, b.v)",
	                  IntVarcharTypes());
	AppendIntStr(appender, conn, {{1, "new-one"}, {3, "new-three"}});
	appender.Flush(conn);

	REQUIRE(ApScalarVarchar(conn, "SELECT string_agg(k || ':' || v, '|' ORDER BY k) FROM t") ==
	        "1:new-one|2:old-two|3:new-three");
}

TEST_CASE("Stable C++API: Appender query constructor refusals", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(id INTEGER, s VARCHAR)").Drain();

	// No column types.
	REQUIRE_THROWS_MATCHES(Appender(conn, "INSERT INTO t FROM appended_data", std::vector<LogicalType>()), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	// No statement in the query.
	REQUIRE_THROWS_MATCHES(Appender(conn, "", IntVarcharTypes()), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	// More than one statement.
	REQUIRE_THROWS_MATCHES(Appender(conn, "INSERT INTO t FROM appended_data; SELECT 1", IntVarcharTypes()), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	// An unsupported statement type is refused by the collection binding.
	REQUIRE_THROWS_MATCHES(Appender(conn, "CREATE TABLE x AS FROM appended_data", IntVarcharTypes()), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: Appender query constructor names the buffer columns", "[cpp_api][appender]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(id INTEGER, s VARCHAR)").Drain();

	// Naming the buffer columns lets the query reference them directly, no alias.
	Appender appender(conn, "INSERT INTO t SELECT key, label FROM buf", IntVarcharTypes(), "buf", {"key", "label"});
	AppendIntStr(appender, conn, {{1, "one"}, {2, "two"}});
	appender.Flush(conn);

	REQUIRE(ApScalarVarchar(conn, "SELECT string_agg(id || ':' || s, '|' ORDER BY id) FROM t") == "1:one|2:two");
}
