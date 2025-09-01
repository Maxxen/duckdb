//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class GlobalSinkState;
class LocalSinkState;
struct OperatorSinkInput;
class WindowCollection;


class WindowFunctionGlobalStateInput {
public:
	ClientContext &client;
	const idx_t payload_count;
	const ValidityMask &partition_mask;
	const ValidityMask &order_mask;
};

class WindowFunctionLocalStateInput {
public:
	ExecutionContext &context;
	const GlobalSinkState &gstate;
};

class WindowFunctionSinkInput {
public:
	ExecutionContext &context;
	DataChunk &sink_chunk;
	DataChunk &coll_chunk;
	const idx_t input_idx;
	OperatorSinkInput &sink;
};

class WindowFunctionFinalizeInput {
public:
	ExecutionContext &context;
	optional_ptr<WindowCollection> collection;
	OperatorSinkInput &sink;
};

class WindowFunctionEvaluateInput {
public:
	ExecutionContext &context;
	DataChunk &eval_chunk;
	Vector &result;
	idx_t count;
	idx_t row_idx;
	OperatorSinkInput &sink;
};

typedef unique_ptr<GlobalSinkState>(*window_get_global_state)(WindowFunctionGlobalStateInput &input);

typedef unique_ptr<LocalSinkState> (*window_get_local_state)(WindowFunctionLocalStateInput &input);

typedef void (*window_sink)(WindowFunctionSinkInput &input);

typedef void (*window_finalize)(WindowFunctionFinalizeInput &input);

typedef void (*window_evalaute)(WindowFunctionEvaluateInput &input);

class WindowFunction : public BaseScalarFunction {
public:
	bool operator==(const WindowFunction &rhs) const {
		return false;
	}
	bool operator!=(const WindowFunction &rhs) const {
		return !(*this == rhs);
	}
public:
	window_get_global_state get_global_state = nullptr;
	window_get_local_state get_local_state = nullptr;
	window_sink sink = nullptr;
	window_finalize finalize = nullptr;
	window_evalaute evaluate = nullptr;
};

} // namespace duckdb
