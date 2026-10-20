#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundLambdaExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	for (auto &capture : expr.Captures()) {
		result->AddChild(*capture);
	}
	return result;
}

void ExpressionExecutor::Execute(const BoundLambdaExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	// a lambda evaluates to the values it captures - the function it is passed to executes its body over them
	D_ASSERT(result.GetType() == expr.GetCapturesType());
	auto &captures = expr.Captures();
	if (captures.empty()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, false);
		return;
	}
	auto &entries = StructVector::GetEntries(result);
	bool all_constant = true;
	idx_t entry_size = 0;
	for (idx_t i = 0; i < captures.size(); i++) {
		Execute(*captures[i], state->child_states[i].get(), sel, count, entries[i]);
		if (entries[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			all_constant = false;
		}
		entry_size = MaxValue<idx_t>(entry_size, entries[i].size());
	}
	result.BufferMutable().SetVectorTypeOnly(all_constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	result.BufferMutable().SetVectorSizeOnly(entry_size);
}

} // namespace duckdb
