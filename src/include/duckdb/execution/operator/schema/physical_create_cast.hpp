//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_cast_info.hpp"

namespace duckdb {

//! PhysicalCreateCast represents a CREATE CAST command
class PhysicalCreateCast : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_CAST;

public:
	PhysicalCreateCast(PhysicalPlan &physical_plan, unique_ptr<CreateCastInfo> info, idx_t estimated_cardinality);

	unique_ptr<CreateCastInfo> info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
