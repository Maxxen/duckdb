//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_planner_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/extension_callback_manager.hpp"

namespace duckdb {
struct DBConfig;
class ClientContext;
class LogicalOperator;
class PhysicalOperator;
class PhysicalPlanGenerator;

//! The PhysicalPlannerExtensionInfo holds static information relevant to the physical planner extension
struct PhysicalPlannerExtensionInfo {
	virtual ~PhysicalPlannerExtensionInfo() {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct PhysicalPlannerExtensionInput {
	ClientContext &context;
	//! The planner, used to plan the children of the operator (CreatePlan) and to create physical operators (Make)
	PhysicalPlanGenerator &planner;
	optional_ptr<PhysicalPlannerExtensionInfo> info;
};

//! Plans a logical operator, or returns nullptr to leave it to the next extension and finally to DuckDB.
//! The function runs after the column bindings have been resolved: the expressions of the operator refer to the
//! columns of its children by position. The returned operator must produce exactly the types of the logical
//! operator, in order. Declining is only allowed before any child of the operator has been planned.
typedef optional_ptr<PhysicalOperator> (*physical_plan_function_t)(PhysicalPlannerExtensionInput &input,
                                                                   LogicalOperator &op);

class PhysicalPlannerExtension {
public:
	//! The plan function of the physical planner extension, called for every logical operator that is planned
	//! (except CTEs, CTE references and duplicate eliminated joins and scans)
	physical_plan_function_t plan_function = nullptr;

	//! Additional info passed to the plan function
	shared_ptr<PhysicalPlannerExtensionInfo> planner_info;

	static void Register(DBConfig &config, PhysicalPlannerExtension extension);
	static ExtensionCallbackIteratorHelper<PhysicalPlannerExtension> Iterate(ClientContext &context) {
		return ExtensionCallbackManager::Get(context).PhysicalPlannerExtensions();
	}
};

} // namespace duckdb
