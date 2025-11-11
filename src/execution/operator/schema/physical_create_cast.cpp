#include "duckdb/execution/operator/schema/physical_create_cast.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

PhysicalCreateCast::PhysicalCreateCast(PhysicalPlan &physical_plan, unique_ptr<CreateCastInfo> info_p,
                                       idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_CAST, {LogicalType::BIGINT}, estimated_cardinality),
      info(std::move(info_p)) {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCreateCast::GetData(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSourceInput &input) const {
	auto &target = info->target;
	auto &source = info->source;
	auto &dbconfig = DBConfig::GetConfig(context.client);

	dbconfig.GetCastFunctions().RegisterCastFunction(source, target, DefaultCasts::ReinterpretCast, info->cast_cost);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
