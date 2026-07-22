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
// Stable C++ API tests: Arrow stream export.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: Arrow stream export", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	SECTION("default batch yields all rows and a stable schema") {
		auto result = conn.Execute("SELECT i FROM range(100000) t(i)");
		auto stream = result.ToArrowStream();
		// ToArrowStream consumes the result.
		REQUIRE_FALSE(static_cast<bool>(result));
		REQUIRE(static_cast<bool>(stream));

		ArrowSchema schema {};
		stream.GetSchema(schema);
		REQUIRE(schema.release != nullptr);
		REQUIRE(schema.n_children == 1);
		schema.release(&schema);

		int64_t rows = 0;
		idx_t arrays = 0;
		ArrowArray array {};
		while (stream.Next(array)) {
			rows += array.length;
			arrays++;
			array.release(&array);
		}
		REQUIRE(rows == 100000);
		// The default batch (131072) exceeds the row count: a single array.
		REQUIRE(arrays == 1);
	}

	SECTION("batch_size coalesces into multiple arrays") {
		auto result = conn.Execute("SELECT i FROM range(100000) t(i)");
		auto stream = result.ToArrowStream(10000);

		int64_t rows = 0;
		idx_t arrays = 0;
		int64_t first_len = 0;
		ArrowArray array {};
		while (stream.Next(array)) {
			if (arrays == 0) {
				first_len = array.length;
			}
			rows += array.length;
			arrays++;
			array.release(&array);
		}
		REQUIRE(rows == 100000);
		REQUIRE(arrays == 10);
		// Each array coalesces many engine chunks (10000 > STANDARD_VECTOR_SIZE).
		REQUIRE(first_len == 10000);
	}

	SECTION("export over a partially consumed result yields the remainder") {
		auto result = conn.Execute("SELECT i FROM range(5000) t(i)");
		// Consume the first engine chunk (2048 rows).
		auto first = result.FetchChunk();
		REQUIRE(static_cast<bool>(first));
		auto consumed = first.GetRowCount();
		REQUIRE(consumed == 2048);

		auto stream = result.ToArrowStream();

		// The stream must continue from the consumed offset, not restart: the
		// first remaining value (a BIGINT inside the struct array) must equal it.
		ArrowArray first_remaining {};
		REQUIRE(stream.Next(first_remaining));
		REQUIRE(first_remaining.n_children == 1);
		const auto *col = first_remaining.children[0];
		const auto *values = reinterpret_cast<const int64_t *>(col->buffers[1]);
		REQUIRE(values[col->offset] == static_cast<int64_t>(consumed));
		int64_t rows = first_remaining.length;
		first_remaining.release(&first_remaining);

		ArrowArray array {};
		while (stream.Next(array)) {
			rows += array.length;
			array.release(&array);
		}
		REQUIRE(rows == static_cast<int64_t>(5000 - consumed));
	}

	SECTION("the stream owns the connection until destroyed") {
		{
			auto result = conn.Execute("SELECT i FROM range(10) t(i)");
			auto stream = result.ToArrowStream();
			// The connection is busy while the stream is live.
			REQUIRE_THROWS_MATCHES(conn.Execute("SELECT 1"), Exception, HasErrorCode(DUCKDB_V2_ERROR_RESOURCE_IN_USE));
		}
		// Stream destroyed: the connection is free again.
		REQUIRE(static_cast<bool>(conn.Execute("SELECT 1").FetchChunk()));
	}

	SECTION("operations on a moved-from stream throw") {
		auto result = conn.Execute("SELECT i FROM range(10) t(i)");
		auto stream = result.ToArrowStream();
		auto moved = std::move(stream);
		REQUIRE_FALSE(static_cast<bool>(stream));
		REQUIRE(static_cast<bool>(moved));

		ArrowSchema schema {};
		REQUIRE_THROWS_MATCHES(stream.GetSchema(schema), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
		ArrowArray array {};
		REQUIRE_THROWS_MATCHES(stream.Next(array), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
}
