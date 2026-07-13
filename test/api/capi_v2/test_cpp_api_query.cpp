#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api_helpers.hpp"
#include "test_helpers.hpp"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>

// ---------------------------------------------------------------------------
// Stable C++ API tests: statements, streaming results, prepared statements, column data.
// ---------------------------------------------------------------------------

namespace {

// A rendered box always contains the light-vertical box-drawing glyph (U+2502).
constexpr const char *kBoxVertical = "\342\224\202";
// The truncation ellipsis the renderer emits when a column is capped (U+2026).
constexpr const char *kEllipsis = "\342\200\246";

bool Contains(const std::string &haystack, const std::string &needle) {
	return haystack.find(needle) != std::string::npos;
}

} // namespace

TEST_CASE("Stable C++API: Column Data Single Scan", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Produce 4100 rows of (a, b) where b == a * 10. This spans several internal
	// chunks (> STANDARD_VECTOR_SIZE), so both append and scan iterate multiple times.
	constexpr int64_t ROW_COUNT = 4100;
	auto result = conn.Execute("SELECT i AS a, i * 10 AS b FROM range(4100) t(i)");
	REQUIRE(result.GetSchema().GetFieldCount() == 2);

	// Drain the stream up front: chunks are appended twice below, and the
	// result must be consumed outside WithTransaction (the transaction
	// callback holds the context lock the stream needs).
	std::vector<DataChunk> chunks;
	while (auto chunk = result.FetchChunk()) {
		chunks.push_back(std::move(chunk));
	}

	conn.WithTransaction([&](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::BIGINT()); // range() produces BIGINT
		types.push_back(LogicalType::BIGINT());

		ColumnDataCollection collection(ctx, types);
		REQUIRE(collection.GetRowCount() == 0);

		// Append every chunk of the query result into the collection.
		{
			auto append_state = collection.GetAppendState();
			for (auto &chunk : chunks) {
				collection.Append(append_state, chunk);
			}
		}
		REQUIRE(collection.GetRowCount() == idx_t(ROW_COUNT));

		// Scan the collection back single-threaded and verify the values round-trip.
		// The scan target is a fresh standalone chunk; Scan sets its cardinality.
		{
			auto scan_state = collection.GetSingleScanState();
			DataChunk out(ctx, types);

			idx_t scanned = 0;
			int64_t sum_a = 0;
			while (collection.Scan(scan_state, out)) {
				const auto rows = out.GetRowCount();
				const auto va = out.GetVector(0).GetView();
				const auto vb = out.GetVector(1).GetView();
				const auto a = va.Data<int64_t>();
				const auto b = vb.Data<int64_t>();
				for (idx_t r = 0; r < rows; r++) {
					REQUIRE(b[vb.SelAt(r)] == a[va.SelAt(r)] * 10);
					sum_a += a[va.SelAt(r)];
				}
				scanned += rows;
			}

			REQUIRE(scanned == idx_t(ROW_COUNT));
			REQUIRE(sum_a == (ROW_COUNT - 1) * ROW_COUNT / 2); // sum of 0..4099
		}

		// Combine consumes another collection and merges its rows into this one.
		{
			ColumnDataCollection other(ctx, types);
			auto append_state = other.GetAppendState();
			for (auto &chunk : chunks) {
				other.Append(append_state, chunk);
			}
			REQUIRE(other.GetRowCount() == idx_t(ROW_COUNT));

			collection.Combine(std::move(other));
			REQUIRE(collection.GetRowCount() == idx_t(ROW_COUNT * 2));
		}
	});
}
TEST_CASE("Stable C++API: Column Data Multi Scan", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	constexpr int64_t ROW_COUNT = 4100;
	auto result = conn.Execute("SELECT i AS a, i * 10 AS b FROM range(4100) t(i)");
	REQUIRE(result.GetSchema().GetFieldCount() == 2);

	// Drain the stream before entering the transaction callback (which
	// holds the context lock the stream needs).
	std::vector<DataChunk> chunks;
	while (auto chunk = result.FetchChunk()) {
		chunks.push_back(std::move(chunk));
	}

	conn.WithTransaction([&](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::BIGINT());
		types.push_back(LogicalType::BIGINT());

		ColumnDataCollection collection(ctx, types);

		{
			auto append_state = collection.GetAppendState();
			for (auto &chunk : chunks) {
				collection.Append(append_state, chunk);
			}
		}
		REQUIRE(collection.GetRowCount() == idx_t(ROW_COUNT));

		// Drive the parallel scan API single-threaded: a shared state coordinates
		// the scan, while a per-worker state tracks the current worker's position.
		auto shared_state = collection.GetSharedScanState();
		auto worker_state = collection.GetWorkerScanState();
		DataChunk out(ctx, types);

		idx_t scanned = 0;
		int64_t sum_a = 0;
		while (collection.Scan(shared_state, worker_state, out)) {
			const auto rows = out.GetRowCount();
			const auto va = out.GetVector(0).GetView();
			const auto vb = out.GetVector(1).GetView();
			const auto a = va.Data<int64_t>();
			const auto b = vb.Data<int64_t>();
			for (idx_t r = 0; r < rows; r++) {
				REQUIRE(b[vb.SelAt(r)] == a[va.SelAt(r)] * 10);
				sum_a += a[va.SelAt(r)];
			}
			scanned += rows;
		}

		REQUIRE(scanned == idx_t(ROW_COUNT));
		REQUIRE(sum_a == (ROW_COUNT - 1) * ROW_COUNT / 2);
	});
}
TEST_CASE("Stable C++API: step loop drains a multi-chunk result", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto result = conn.Execute("SELECT i FROM range(100000) t(i)");

	idx_t total_rows = 0;
	idx_t chunk_count = 0;
	while (true) {
		auto step = result.Step();
		if (step.status == QueryResult::StepStatus::CHUNK) {
			REQUIRE(step.chunk);
			total_rows += step.chunk.GetRowCount();
			chunk_count++;
			continue;
		}
		REQUIRE(!step.chunk); // non-empty iff status is CHUNK
		if (step.status == QueryResult::StepStatus::WAITING) {
			result.Wait();
			continue;
		}
		REQUIRE(step.status == QueryResult::StepStatus::FINISHED);
		break;
	}
	REQUIRE(total_rows == 100000);
	REQUIRE(chunk_count > 1);

	// FINISHED is sticky, and waiting on a terminal result is a no-op.
	auto step = result.Step();
	REQUIRE(step.status == QueryResult::StepStatus::FINISHED);
	REQUIRE(!step.chunk);
	result.Wait();
}
TEST_CASE("Stable C++API: Drain applies side effects and reports rows changed", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	REQUIRE(conn.Execute("CREATE TABLE t (i INTEGER)").Drain() == 0);
	REQUIRE(conn.Execute("INSERT INTO t VALUES (1), (2), (3)").Drain() == 3);
	REQUIRE(conn.Execute("DELETE FROM t WHERE i = 1").Drain() == 1);
	REQUIRE(conn.Execute("SELECT i FROM range(1000) t(i)").Drain() == 0); // rows drained and discarded

	auto result = conn.Execute("SELECT i FROM t");
	idx_t rows = 0;
	while (auto chunk = result.FetchChunk()) {
		rows += chunk.GetRowCount();
	}
	REQUIRE(rows == 2);
}
TEST_CASE("Stable C++API: a busy connection refuses new work with RESOURCE_IN_USE", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto live = conn.Execute("SELECT i FROM range(100000) t(i)");

	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT 1"), Exception, HasErrorCode(DUCKDB_V2_ERROR_RESOURCE_IN_USE));

	// Draining the live result frees the connection.
	while (live.FetchChunk()) {
	}
	auto second = conn.Execute("SELECT 1");
	REQUIRE(second.FetchChunk());
}
TEST_CASE("Stable C++API: Interrupt cancels a running query", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto result = conn.Execute("SELECT i FROM range(10000000) t(i)");
	conn.Interrupt();

	// Steps observe the cancellation as the sticky CANCELLED status.
	auto status = QueryResult::StepStatus::WAITING;
	for (int i = 0; i < 1000 && status != QueryResult::StepStatus::CANCELLED; i++) {
		status = result.Step().status;
	}
	REQUIRE(status == QueryResult::StepStatus::CANCELLED);

	// FetchChunk reports the same event on the error channel.
	REQUIRE_THROWS_MATCHES(result.FetchChunk(), Exception, HasErrorCode(DUCKDB_V2_ERROR_RUNTIME_INTERRUPT));

	// The cancelled result freed the connection.
	REQUIRE(conn.Execute("SELECT 1").Drain() == 0);
}
TEST_CASE("Stable C++API: GetQueryProgress reports idle values when no query is active", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto progress = conn.GetQueryProgress();
	REQUIRE(progress.percentage == -1.0);
	REQUIRE(progress.rows_processed == 0);
	REQUIRE(progress.total_rows_to_process == 0);
}
TEST_CASE("Stable C++API: ParseSQL iterates statements into Execute", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto statements = conn.ParseSQL("SELECT 42; SELECT 84; SELECT 126");
	int statement_count = 0;
	while (auto statement = statements.Next()) {
		auto result = conn.Execute(statement);
		REQUIRE(result.FetchChunk());
		result.Drain();
		statement_count++;
	}
	REQUIRE(statement_count == 3);

	// Exhaustion is idempotent.
	REQUIRE(!statements.Next());

	// The string-taking Execute is single-statement sugar.
	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT 1; SELECT 2"), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
	REQUIRE_THROWS_MATCHES(conn.Execute(""), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}
TEST_CASE("Stable C++API: Bind", "[cpp_api][statement_bind]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(a INTEGER, b VARCHAR)").Drain();

	auto iter = conn.ParseSQL("SELECT a, b FROM t WHERE a = $1");
	auto stmt = iter.Next();
	REQUIRE(static_cast<bool>(stmt));

	auto sig = conn.Bind(stmt);
	REQUIRE(sig.output.GetFieldCount() == 2);
	REQUIRE(sig.output.GetFieldName(0) == "a");
	REQUIRE(sig.output.GetFieldType(0) == LogicalType::INTEGER());
	REQUIRE(sig.output.GetFieldName(1) == "b");
	REQUIRE(sig.output.GetFieldType(1) == LogicalType::VARCHAR());
	REQUIRE(sig.parameters.GetFieldCount() == 1);
	REQUIRE(sig.parameters.GetFieldName(0) == "1");
	REQUIRE(sig.parameters.GetFieldType(0) == LogicalType::INTEGER());

	// Non-consuming: the statement is still alive and re-bindable.
	REQUIRE(static_cast<bool>(stmt));
	auto sig2 = conn.Bind(stmt);
	REQUIRE(sig2.output.GetFieldCount() == 2);

	// Dynamic PIVOT is rejected with INVALID_INPUT.
	conn.Execute("CREATE TABLE sales(product VARCHAR, quarter VARCHAR, amount INTEGER)").Drain();
	auto piter = conn.ParseSQL("PIVOT sales ON quarter USING sum(amount)");
	auto pstmt = piter.Next();
	REQUIRE_THROWS_MATCHES(conn.Bind(pstmt), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}
TEST_CASE("Stable C++API: QueryResult GetSchema", "[cpp_api][query_result]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto result = conn.Execute("SELECT 1 AS a, 'x' AS b");
	auto schema = result.GetSchema();
	REQUIRE(schema.GetFieldCount() == 2);
	REQUIRE(schema.GetFieldName(0) == "a");
	REQUIRE(schema.GetFieldName(1) == "b");
	REQUIRE(schema.GetFieldType(0) == LogicalType::INTEGER());
}
TEST_CASE("Stable C++API: QueryResult result and statement types", "[cpp_api][query_result]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE rt(i INTEGER)").Drain();

	// Consume-vs-drain decided purely from GetResultType, no SQL inspection.
	auto run = [&](const char *sql) {
		auto result = conn.Execute(sql);
		auto types = std::make_pair(result.GetResultType(), result.GetStatementType());
		if (types.first == QueryResult::ResultType::QUERY_RESULT) {
			while (auto chunk = result.FetchChunk()) {
			}
		} else {
			result.Drain();
		}
		return types;
	};

	auto select = run("SELECT * FROM rt");
	REQUIRE(select.first == QueryResult::ResultType::QUERY_RESULT);
	REQUIRE(select.second == QueryResult::StatementType::SELECT);

	auto insert = run("INSERT INTO rt VALUES (1), (2)");
	REQUIRE(insert.first == QueryResult::ResultType::CHANGED_ROWS);
	REQUIRE(insert.second == QueryResult::StatementType::INSERT);

	auto ddl = run("CREATE TABLE rt2(i INTEGER)");
	REQUIRE(ddl.first == QueryResult::ResultType::NOTHING);
	REQUIRE(ddl.second == QueryResult::StatementType::CREATE);

	// The drain path applied the INSERT's side effects.
	auto verify = conn.Execute("SELECT count(*) FROM rt");
	auto chunk = verify.FetchChunk();
	auto view = chunk.GetVector(0).GetView();
	REQUIRE(view.Data<int64_t>()[view.SelAt(0)] == 2);
}
TEST_CASE("Stable C++API: prepared statements", "[cpp_api][prepared_statement]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE scores(id INTEGER, score INTEGER)").Drain();
	conn.Execute("INSERT INTO scores VALUES (1, 40), (2, 55), (3, 70), (4, 90)").Drain();

	// Value is move-only, so a parameter list is built by move, not brace-init.
	auto Params = [](std::initializer_list<int64_t> values) {
		std::vector<Value> params;
		for (auto value : values) {
			params.push_back(Value::Bigint(value));
		}
		return params;
	};

	SECTION("bind once, execute many with different parameters") {
		// Parse and bind once, then reuse across executions (binding neither executes
		// nor consumes): the parse-once, bind-once, execute-many pattern.
		auto iter = conn.ParseSQL("SELECT id, score FROM scores WHERE score >= $1 ORDER BY id");
		auto stmt = iter.Next();

		auto sig = conn.Bind(stmt);
		REQUIRE(sig.output.GetFieldCount() == 2);
		REQUIRE(sig.output.GetFieldName(0) == "id");
		REQUIRE(sig.output.GetFieldType(0) == LogicalType::INTEGER());
		REQUIRE(sig.output.GetFieldName(1) == "score");
		REQUIRE(sig.parameters.GetFieldCount() == 1);
		REQUIRE(sig.parameters.GetFieldName(0) == "1");                    // $1 -> "1"
		REQUIRE(sig.parameters.GetFieldType(0) == LogicalType::INTEGER()); // inferred from score >= $1

		// Execute with one value, then another: different results, same statement, no
		// re-parse and no re-bind.
		auto high = Collect2<int32_t, int32_t>(conn.Execute(stmt, Params({50})), 0, 1);
		REQUIRE(high.size() == 3);
		REQUIRE(high[0].first == 2);
		REQUIRE(high[0].second == 55);
		REQUIRE(high[2].first == 4);

		auto higher = Collect2<int32_t, int32_t>(conn.Execute(stmt, Params({80})), 0, 1);
		REQUIRE(higher.size() == 1);
		REQUIRE(higher[0].first == 4);
		REQUIRE(higher[0].second == 90);

		// Still alive and re-bindable after executing.
		REQUIRE(static_cast<bool>(stmt));
		REQUIRE(conn.Bind(stmt).parameters.GetFieldCount() == 1);
	}

	SECTION("positional parameters bind in order") {
		auto iter = conn.ParseSQL("SELECT id, score FROM scores WHERE score >= $1 AND score < $2 ORDER BY id");
		auto stmt = iter.Next();
		REQUIRE(conn.Bind(stmt).parameters.GetFieldCount() == 2);

		// $1 = 50, $2 = 80 -> 50 <= score < 80.
		auto rows = Collect2<int32_t, int32_t>(conn.Execute(stmt, Params({50, 80})), 0, 1);
		REQUIRE(rows.size() == 2);
		REQUIRE(rows[0].first == 2);
		REQUIRE(rows[0].second == 55);
		REQUIRE(rows[1].first == 3);
		REQUIRE(rows[1].second == 70);
	}

	SECTION("a prepared DML statement reused to insert rows") {
		conn.Execute("CREATE TABLE log(v INTEGER)").Drain();
		auto iter = conn.ParseSQL("INSERT INTO log VALUES ($1)");
		auto stmt = iter.Next();

		auto sig = conn.Bind(stmt);
		REQUIRE(sig.parameters.GetFieldCount() == 1);
		REQUIRE(sig.output.GetFieldCount() == 1); // the changed-rows count column

		// Each execution inserts one row and reports one changed row.
		REQUIRE(conn.Execute(stmt, Params({10})).Drain() == 1);
		REQUIRE(conn.Execute(stmt, Params({20})).Drain() == 1);
		REQUIRE(conn.Execute(stmt, Params({30})).Drain() == 1);

		auto summary = Collect2<int64_t, int32_t>(conn.Execute("SELECT count(*) AS c, max(v) AS m FROM log"), 0, 1);
		REQUIRE(summary.size() == 1);
		REQUIRE(summary[0].first == 3);   // three rows inserted
		REQUIRE(summary[0].second == 30); // last value
	}
}
TEST_CASE("Stable C++API: PreparedStatement handle", "[cpp_api][prepared_statement]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(x INTEGER)").Drain();
	conn.Execute("INSERT INTO t VALUES (1),(2),(3),(4)").Drain();

	// Value is move-only, so a parameter list is built by move.
	auto Params = [](std::initializer_list<int64_t> values) {
		std::vector<Value> params;
		for (auto value : values) {
			params.push_back(Value::Bigint(value));
		}
		return params;
	};

	auto Prepare = [&](const std::string &sql, bool require_cacheable = false) {
		auto iter = conn.ParseSQL(sql);
		auto stmt = iter.Next();
		return conn.Prepare(stmt, require_cacheable);
	};

	SECTION("prepare once, execute many with different parameters") {
		auto prepared = Prepare("SELECT x FROM t WHERE x > $1 ORDER BY x");
		auto first = Collect2<int32_t, int32_t>(prepared.Execute(Params({2})), 0, 0);
		REQUIRE(first.size() == 2);
		REQUIRE(first[0].first == 3);
		REQUIRE(first[1].first == 4);
		// Same handle, different value: non-consuming.
		auto second = Collect2<int32_t, int32_t>(prepared.Execute(Params({0})), 0, 0);
		REQUIRE(second.size() == 4);
		REQUIRE(second[0].first == 1);
		REQUIRE(second[3].first == 4);
	}

	SECTION("ReusesPlan reports the honest cache signal") {
		REQUIRE(Prepare("SELECT 42").ReusesPlan() == true);
		REQUIRE(Prepare("SELECT $1::INTEGER + $2::INTEGER").ReusesPlan() == true);
		REQUIRE(Prepare("SELECT $1 + $2").ReusesPlan() == false);
		REQUIRE(Prepare("SELECT * FROM t WHERE x = $1").ReusesPlan() == false);
	}

	SECTION("require_cacheable accepts a cacheable plan and rejects an uncacheable one") {
		// Cacheable: succeeds.
		auto ok = Prepare("SELECT $1::INTEGER + $2::INTEGER", true);
		REQUIRE(ok.ReusesPlan() == true);
		// Uncacheable: throws INVALID_INPUT.
		bool threw = false;
		try {
			Prepare("SELECT * FROM t WHERE x = $1", true);
		} catch (const Exception &ex) {
			threw = true;
			REQUIRE(ex.GetCode() == DUCKDB_V2_ERROR_INVALID_INPUT);
		}
		REQUIRE(threw);
	}

	SECTION("no-parameter execute") {
		auto prepared = Prepare("SELECT x FROM t ORDER BY x");
		auto rows = Collect2<int32_t, int32_t>(prepared.Execute(), 0, 0);
		REQUIRE(rows.size() == 4);
	}

	SECTION("prepared DML reused reports rows changed") {
		conn.Execute("CREATE TABLE log(v INTEGER)").Drain();
		auto prepared = Prepare("INSERT INTO log VALUES ($1)");
		REQUIRE(prepared.Execute(Params({10})).Drain() == 1);
		REQUIRE(prepared.Execute(Params({20})).Drain() == 1);
		auto summary = Collect2<int64_t, int32_t>(conn.Execute("SELECT count(*) AS c, max(v) AS m FROM log"), 0, 1);
		REQUIRE(summary[0].first == 2);
		REQUIRE(summary[0].second == 20);
	}
}

TEST_CASE("Stable C++API: named parameters", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE scores(id INTEGER, score INTEGER)").Drain();
	conn.Execute("INSERT INTO scores VALUES (1, 40), (2, 55), (3, 70), (4, 90)").Drain();

	SECTION("Connection::Execute binds by name") {
		auto iter = conn.ParseSQL("SELECT id, score FROM scores WHERE score >= $min ORDER BY id");
		auto stmt = iter.Next();
		std::vector<NamedParam> params;
		params.push_back(NamedParam {"min", Value::Bigint(70)});
		auto rows = Collect2<int32_t, int32_t>(conn.Execute(stmt, params), 0, 1);
		REQUIRE(rows.size() == 2);
		REQUIRE(rows[0].first == 3);
		REQUIRE(rows[1].first == 4);
	}

	SECTION("PreparedStatement::Execute binds by name") {
		auto iter = conn.ParseSQL("SELECT id FROM scores WHERE score >= $min ORDER BY id");
		auto prepared = conn.Prepare(iter.Next());
		std::vector<NamedParam> params;
		params.push_back(NamedParam {"min", Value::Bigint(90)});
		auto rows = Collect2<int32_t, int32_t>(prepared.Execute(params), 0, 0);
		REQUIRE(rows.size() == 1);
		REQUIRE(rows[0].first == 4);
	}

	SECTION("an empty NamedParam name binds positionally") {
		auto iter = conn.ParseSQL("SELECT id FROM scores WHERE score >= $1 ORDER BY id");
		auto stmt = iter.Next();
		std::vector<NamedParam> params;
		params.push_back(NamedParam {"", Value::Bigint(90)}); // empty name -> positional $1
		auto rows = Collect2<int32_t, int32_t>(conn.Execute(stmt, params), 0, 0);
		REQUIRE(rows.size() == 1);
		REQUIRE(rows[0].first == 4);
	}

	SECTION("a wrong key set throws INVALID_INPUT") {
		auto iter = conn.ParseSQL("SELECT id FROM scores WHERE score >= $min");
		auto stmt = iter.Next();
		std::vector<NamedParam> params;
		params.push_back(NamedParam {"wrong", Value::Bigint(1)});
		REQUIRE_THROWS_MATCHES(conn.Execute(stmt, params), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
	}
}

// ---------------------------------------------------------------------------
// RenderBox: engine-side box rendering (BoxRenderer, the CLI renderer),
// consuming the result by transfer.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: RenderBox renders glyphs, the type row, and NULL cells", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto text = conn.Execute("SELECT * FROM (VALUES (1, 'x'), (2, NULL)) t(a, b)").RenderBox();

	// Box-drawing glyphs: this is the engine renderer, not a hand-rolled one.
	REQUIRE(Contains(text, kBoxVertical));
	// The type row shows the engine's short type names.
	REQUIRE(Contains(text, "int32"));   // column a (INTEGER)
	REQUIRE(Contains(text, "varchar")); // column b (VARCHAR)
	// A value cell and a default-rendered NULL cell.
	REQUIRE(Contains(text, "x"));
	REQUIRE(Contains(text, "NULL"));
}

TEST_CASE("Stable C++API: RenderBox footer counts all rows even when max_rows bounds display", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// 100 rows, display bounded to 5: max_rows bounds DISPLAY, not the read.
	auto text = conn.Execute("SELECT i AS n FROM range(100) t(i)").RenderBox(/*max_rows=*/5);
	REQUIRE(Contains(text, "100 rows")); // exact total, matching the CLI
	REQUIRE(Contains(text, "5 shown"));  // display bounded to max_rows
	REQUIRE_FALSE(Contains(text, "100 shown"));
}

TEST_CASE("Stable C++API: RenderBox max_rows changes display and 0 selects the default", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto few = conn.Execute("SELECT i AS n FROM range(100) t(i)").RenderBox(/*max_rows=*/3);
	REQUIRE(Contains(few, "3 shown"));

	// 0 selects the renderer default (20).
	auto def = conn.Execute("SELECT i AS n FROM range(100) t(i)").RenderBox(/*max_rows=*/0);
	REQUIRE(Contains(def, "20 shown"));

	REQUIRE(few != def);
}

TEST_CASE("Stable C++API: RenderBox max_width and max_col_width change the layout", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Five wide columns; explicit widths keep the test off the terminal probe.
	const char *sql = "SELECT repeat('a', 15) AS c1, repeat('b', 15) AS c2, repeat('c', 15) AS c3, "
	                  "repeat('d', 15) AS c4, repeat('e', 15) AS c5";

	// A tight max_width forces the renderer to hide columns: the hidden-column
	// ellipsis and an "N columns" footer appear.
	auto narrow = conn.Execute(sql).RenderBox(0, /*max_width=*/40);
	REQUIRE(Contains(narrow, kEllipsis));
	REQUIRE(Contains(narrow, "5 columns"));

	// A generous max_width fits every column: nothing is hidden.
	auto wide = conn.Execute(sql).RenderBox(0, /*max_width=*/200);
	REQUIRE_FALSE(Contains(wide, "5 columns"));
	REQUIRE(narrow != wide);

	// At the tight width, raising max_col_width wraps cells instead of hiding
	// columns: the layout changes and every column is shown again.
	auto wrapped = conn.Execute(sql).RenderBox(0, /*max_width=*/40, /*max_col_width=*/6);
	REQUIRE_FALSE(Contains(wrapped, "5 columns"));
	REQUIRE(wrapped != narrow);
}

TEST_CASE("Stable C++API: RenderBox render_mode selects rows vs columns layout", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	const char *sql = "SELECT 1 AS a, 2 AS b, 3 AS c";
	auto rows = conn.Execute(sql).RenderBox(0, 0, 0, "", /*render_mode=*/0);
	auto cols = conn.Execute(sql).RenderBox(0, 0, 0, "", /*render_mode=*/1);
	REQUIRE(rows != cols);
}

TEST_CASE("Stable C++API: RenderBox custom null_value overrides the default NULL text", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto def = conn.Execute("SELECT CAST(NULL AS INTEGER) AS b").RenderBox();
	REQUIRE(Contains(def, "NULL"));

	auto custom = conn.Execute("SELECT CAST(NULL AS INTEGER) AS b").RenderBox(0, 0, 0, "<nil>");
	REQUIRE(Contains(custom, "<nil>"));
	// The type row renders "int32", the value renders "<nil>": no "NULL" anywhere.
	REQUIRE_FALSE(Contains(custom, "NULL"));
}

TEST_CASE("Stable C++API: RenderBox consumes the result and frees the connection", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto r = conn.Execute("SELECT i FROM range(5) t(i)");
	auto text = r.RenderBox();
	REQUIRE(Contains(text, kBoxVertical));

	// The one-live-result slot was released: a new query runs on the same connection.
	auto next = conn.Execute("SELECT 42 AS answer");
	auto chunk = next.FetchChunk();
	REQUIRE(chunk);
	REQUIRE(chunk.GetRowCount() == 1);
}

TEST_CASE("Stable C++API: RenderBox on a partially consumed result renders the remainder", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// More than one chunk so a single fetch leaves a remainder.
	auto r = conn.Execute("SELECT i FROM range(5000) t(i)");
	auto first = r.FetchChunk();
	REQUIRE(first);
	auto consumed = first.GetRowCount();
	REQUIRE(consumed > 0);
	REQUIRE(consumed < 5000);

	auto text = r.RenderBox(); // renders only what remains
	auto remainder = idx_t(5000) - consumed;
	REQUIRE(Contains(text, std::to_string(remainder) + " rows"));
}

TEST_CASE("Stable C++API: RenderBox handles zero-row and no-row-output results", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Zero-row SELECT: the schema is known and the footer reports 0 rows.
	auto empty = conn.Execute("SELECT i AS n FROM range(0) t(i)").RenderBox();
	REQUIRE(Contains(empty, kBoxVertical));
	REQUIRE(Contains(empty, "0 rows"));

	// A DDL statement produces no row output; rendering must not crash and the
	// side effect (the table) is applied by the drain inside RenderBox.
	auto ddl = conn.Execute("CREATE TABLE rb_ddl(x INTEGER)").RenderBox();
	REQUIRE_FALSE(ddl.empty());
	// Proof the CREATE took effect (the result was drained, not abandoned).
	REQUIRE(conn.Execute("INSERT INTO rb_ddl VALUES (1)").Drain() == 1);
}

TEST_CASE("Stable C++API: RenderBox limit yields an approximate '? rows' footer", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// The .show() idiom: wrap the query with LIMIT n and pass n as limit, so the
	// footer honestly reads "? rows" once the bound is filled rather than
	// reporting the truncated count as the exact total.
	auto bounded = conn.Execute("SELECT i FROM range(21) t(i)").RenderBox(0, 0, 0, "", 0, /*limit=*/21);
	REQUIRE(Contains(bounded, "? rows"));
	REQUIRE_FALSE(Contains(bounded, "21 rows"));

	// With limit 0 the count is known and exact, so no "? rows" appears.
	auto exact = conn.Execute("SELECT i FROM range(21) t(i)").RenderBox();
	REQUIRE_FALSE(Contains(exact, "? rows"));
}
