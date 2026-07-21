#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"

namespace duckdb {

CommonTableExpressionInfo::~CommonTableExpressionInfo() {
}

unique_ptr<CommonTableExpressionInfo> CommonTableExpressionInfo::WrapNonMaterialized(unique_ptr<TableRef> table_ref) {
	// Wrap the reference in SELECT * and store it as a non-materialized CTE.
	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(make_uniq<StarExpression>());
	select->from_table = std::move(table_ref);

	auto cte_info = make_uniq<CommonTableExpressionInfo>();
	cte_info->query_node = std::move(select);
	cte_info->materialized = CTEMaterialize::CTE_MATERIALIZE_NEVER;
	return cte_info;
}

CommonTableExpressionInfo::CommonTableExpressionInfo(unique_ptr<SelectStatement> query,
                                                     unique_ptr<QueryNode> query_node_p) {
	if (query_node_p) {
		this->query_node = std::move(query_node_p);
	} else if (query) {
		this->query_node = std::move(query->node);
	}
}

unique_ptr<CommonTableExpressionInfo> CommonTableExpressionInfo::Copy() {
	auto result = make_uniq<CommonTableExpressionInfo>();
	result->aliases = aliases;
	if (query_node) {
		result->query_node = query_node->Copy();
	}
	for (auto &key : key_targets) {
		result->key_targets.push_back(key->Copy());
	}
	for (auto &agg : payload_aggregates) {
		result->payload_aggregates.push_back(agg->Copy());
	}
	result->materialized = materialized;
	result->is_trigger_generated = is_trigger_generated;
	return result;
}

CTEMaterialize CommonTableExpressionInfo::GetMaterializedForSerialization(Serializer &serializer) const {
	if (serializer.ShouldSerialize(StorageVersion::V1_5_0)) {
		return materialized;
	}
	return CTEMaterialize::CTE_MATERIALIZE_DEFAULT;
}

unique_ptr<SelectStatement> CommonTableExpressionInfo::GetQueryForSerialization(Serializer &serializer) const {
	if (!query_node ||
	    (query_node->type != QueryNodeType::SELECT_NODE && query_node->type != QueryNodeType::SET_OPERATION_NODE &&
	     query_node->type != QueryNodeType::RECURSIVE_CTE_NODE)) {
		throw SerializationException(
		    "DML CTEs (INSERT/UPDATE/DELETE) require storage version v2.0.0 or higher and cannot be "
		    "serialized to older storage formats");
	}
	auto select = make_uniq<SelectStatement>();
	select->node = query_node->Copy();
	return select;
}

} // namespace duckdb
