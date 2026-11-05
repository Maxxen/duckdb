#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/execution/operator/scan/physical_empty_result.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/physical_planner_extension.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_nearest_join.hpp"

using namespace duckdb;

enum class NearestJoinHookMode { DECLINE, EMPTY_RESULT, WRONG_TYPES };

struct NearestJoinPlannerInfo : PhysicalPlannerExtensionInfo {
	NearestJoinHookMode mode = NearestJoinHookMode::DECLINE;
	idx_t nearest_joins = 0;
	bool saw_cte = false;
	string function_name;
	optional_idx left_argument;
	optional_idx right_argument;

	static optional_ptr<PhysicalOperator> Plan(PhysicalPlannerExtensionInput &input, LogicalOperator &op) {
		auto &info = input.info->Cast<NearestJoinPlannerInfo>();
		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
		    op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
			info.saw_cte = true;
		}
		if (op.type != LogicalOperatorType::LOGICAL_NEAREST_JOIN) {
			return nullptr;
		}
		auto &join = op.Cast<LogicalNearestJoin>();
		info.nearest_joins++;
		// the ranking is already resolved to positions here
		idx_t left_idx, right_idx;
		auto function = join.TrySplitRanking(left_idx, right_idx);
		if (function) {
			info.function_name = function->Function().GetName().GetIdentifierName();
			info.left_argument = left_idx;
			info.right_argument = right_idx;
		}
		switch (info.mode) {
		case NearestJoinHookMode::DECLINE:
			return nullptr;
		case NearestJoinHookMode::EMPTY_RESULT:
			return input.planner.Make<PhysicalEmptyResult>(op.types, op.estimated_cardinality);
		case NearestJoinHookMode::WRONG_TYPES:
			return input.planner.Make<PhysicalEmptyResult>(vector<LogicalType> {LogicalType::BOOLEAN},
			                                               op.estimated_cardinality);
		}
		return nullptr;
	}
};

TEST_CASE("Physical planner extensions can replace a LogicalNearestJoin", "[optimizer][nearest]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE q AS SELECT i AS id, [i, 0, 0]::FLOAT[3] AS e FROM range(3) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE p AS SELECT i AS id, [i, 1, 0]::FLOAT[3] AS e FROM range(10) t(i)"));
	auto info = make_shared_ptr<NearestJoinPlannerInfo>();
	PhysicalPlannerExtension extension;
	extension.plan_function = NearestJoinPlannerInfo::Plan;
	extension.planner_info = info;
	PhysicalPlannerExtension::Register(DBConfig::GetConfig(*con.context), std::move(extension));

	const string query = "SELECT q.id, p.id FROM q JOIN p NEAREST 1 BY DISTANCE array_distance(p.e, q.e) ORDER BY ALL";

	// declining falls back to the built-in plan
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2}));
	REQUIRE(info->nearest_joins == 1);
	REQUIRE(info->function_name == "array_distance");
	REQUIRE(info->left_argument.GetIndex() == 1);
	REQUIRE(info->right_argument.GetIndex() == 0);

	// the extension's operator replaces the join
	info->mode = NearestJoinHookMode::EMPTY_RESULT;
	result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 0);

	// the extension's operator must produce the types of the logical operator
	info->mode = NearestJoinHookMode::WRONG_TYPES;
	REQUIRE_FAIL(con.Query(query));

	// operators that register planner state are not handed to extensions
	info->mode = NearestJoinHookMode::DECLINE;
	REQUIRE_NO_FAIL(con.Query("WITH c AS MATERIALIZED (SELECT * FROM p) SELECT count(*) FROM c, c c2"));
	REQUIRE(!info->saw_cte);
}
