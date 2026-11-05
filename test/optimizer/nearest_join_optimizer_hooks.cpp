#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_nearest_join.hpp"

using namespace duckdb;

struct NearestJoinHookTest : OptimizerExtensionInfo {
	idx_t nearest_joins = 0;
	string function_name;
	optional_idx left_argument;
	optional_idx right_argument;
	bool right_is_get = false;
	bool right_has_filters = false;

	void Reset() {
		nearest_joins = 0;
		function_name.clear();
		left_argument = optional_idx();
		right_argument = optional_idx();
		right_is_get = false;
		right_has_filters = false;
	}

	void Inspect(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_NEAREST_JOIN) {
			auto &join = op.Cast<LogicalNearestJoin>();
			nearest_joins++;
			idx_t left_idx, right_idx;
			auto function = join.TrySplitRanking(left_idx, right_idx);
			if (function) {
				function_name = function->Function().GetName().GetIdentifierName();
				left_argument = left_idx;
				right_argument = right_idx;
			}
			// look through the projections of a subquery target
			reference<LogicalOperator> right = *join.children[1];
			while (right.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
				right = *right.get().children[0];
			}
			if (right.get().type == LogicalOperatorType::LOGICAL_GET) {
				right_is_get = true;
				right_has_filters = right.get().Cast<LogicalGet>().table_filters.HasFilters();
			}
		}
		for (auto &child : op.children) {
			Inspect(*child);
		}
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		auto &info = static_cast<NearestJoinHookTest &>(*input.info);
		info.Reset();
		info.Inspect(*plan);
	}
};

TEST_CASE("Optimizer extensions see a normalized LogicalNearestJoin", "[optimizer][nearest]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE q AS SELECT i AS id, [i, 0, 0]::FLOAT[3] AS e FROM range(3) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE p AS SELECT i AS id, [i, 1, 0]::FLOAT[3] AS e FROM range(10) t(i)"));
	auto info = make_shared_ptr<NearestJoinHookTest>();
	OptimizerExtension extension;
	extension.optimizer_info = info;
	extension.optimize_function = NearestJoinHookTest::Optimize;
	OptimizerExtension::Register(DBConfig::GetConfig(*con.context), std::move(extension));

	// a distance function of one left and one right argument can be split
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM q JOIN p NEAREST 2 BY DISTANCE array_distance(q.e, p.e)"));
	REQUIRE(info->nearest_joins == 1);
	REQUIRE(info->function_name == "array_distance");
	REQUIRE(info->left_argument.GetIndex() == 0);
	REQUIRE(info->right_argument.GetIndex() == 1);
	REQUIRE(info->right_is_get);
	REQUIRE(!info->right_has_filters);

	// the argument order is reported as written
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM q JOIN p NEAREST 2 BY DISTANCE array_distance(p.e, q.e)"));
	REQUIRE(info->left_argument.GetIndex() == 1);
	REQUIRE(info->right_argument.GetIndex() == 0);

	// filters of a pre-filtered target are pushed into the scan of the target
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM q JOIN (SELECT * FROM p WHERE id > 3) t "
	                          "NEAREST 2 BY SIMILARITY array_cosine_similarity(q.e, t.e)"));
	REQUIRE(info->function_name == "array_cosine_similarity");
	REQUIRE(info->right_is_get);
	REQUIRE(info->right_has_filters);

	// rankings that mix both sides in one argument cannot be split
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM q JOIN p NEAREST 1 BY DISTANCE abs(q.id - p.id)"));
	REQUIRE(info->nearest_joins == 1);
	REQUIRE(info->function_name.empty());

	// rankings referencing only one side cannot be split
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM q JOIN p NEAREST 1 BY DISTANCE array_distance(p.e, p.e)"));
	REQUIRE(info->function_name.empty());
}
