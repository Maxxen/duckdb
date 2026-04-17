#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

AggregateFunctionInfo::~AggregateFunctionInfo() {
}

hash_t BaseAggregateFunction::Hash() const {
	hash_t hash = signature.GetReturnType().Hash();
	for (auto &param : signature.GetParameters()) {
		hash = duckdb::CombineHash(hash, param.Hash());
	}
	return hash;
}

bool BoundAggregateFunction::IsBoundFrom(const AggregateFunction &aggregate_function) const {
	if (name != aggregate_function.name) {
		return false;
	}
	if (catalog_name != aggregate_function.catalog_name) {
		return false;
	}
	if (schema_name != aggregate_function.schema_name) {
		return false;
	}
	return true;
}

bool BoundAggregateFunction::operator==(const BoundAggregateFunction &other) const {
	return name == other.name && catalog_name == other.catalog_name && schema_name == other.schema_name &&
	       arguments == other.arguments && return_type == other.return_type;
}

void BoundAggregateFunction::ReplaceDefinition(const AggregateFunction &aggregate_function) {
	// Copy over all properties
	name = aggregate_function.name;
	catalog_name = aggregate_function.catalog_name;
	schema_name = aggregate_function.schema_name;

	// As well as all function callbacks
	SetBindCallback(aggregate_function.GetBindCallback());
	SetStateInitCallback(aggregate_function.GetStateInitCallback());
	SetStateSizeCallback(aggregate_function.GetStateSizeCallback());
	SetStateDestructorCallback(aggregate_function.GetStateDestructorCallback());
	SetStateUpdateCallback(aggregate_function.GetStateUpdateCallback());
	SetStateSimpleUpdateCallback(aggregate_function.GetStateSimpleUpdateCallback());
	SetStateCombineCallback(aggregate_function.GetStateCombineCallback());
	SetStateFinalizeCallback(aggregate_function.GetStateFinalizeCallback());

	SetWindowCallback(aggregate_function.GetWindowCallback());
	SetWindowInitCallback(aggregate_function.GetWindowInitCallback());

	SetStatisticsCallback(aggregate_function.GetStatisticsCallback());
	SetSerializeCallback(aggregate_function.GetSerializeCallback());
	SetDeserializeCallback(aggregate_function.GetDeserializeCallback());
	SetExportTypeCallback(aggregate_function.GetExportTypeCallback());

	// SetExtraFunctionInfo(aggregate_function.GetExtraFunctionInfo());

	SetOrderDependent(aggregate_function.GetOrderDependent());
	SetDistinctDependent(aggregate_function.GetDistinctDependent());
}

} // namespace duckdb
