//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/verification_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {
class ClientContext;
class Expression;

//! VerificationStatistics holds the statistics the statistics propagator derived for each expression, so that
//! executed vectors can be verified against them. Only used when debug_verify_stats is set - the state is
//! registered on the ClientContext by the statistics propagator and looked up by the ExpressionExecutor.
class VerificationStatistics : public ClientContextState {
public:
	static constexpr const char *STATE_KEY = "verification_statistics";

	//! Store the statistics of an expression, overwriting any previous entry for the same node
	void SetStats(const Expression &expr, unique_ptr<BaseStatistics> stats);
	//! Return the statistics of an expression, if any
	optional_ptr<const BaseStatistics> GetStats(const Expression &expr) const;

	//! Get the state registered on the context, creating it if it does not exist yet
	static VerificationStatistics &GetOrCreate(ClientContext &context);
	//! Get the state registered on the context, if any
	static shared_ptr<VerificationStatistics> TryGet(ClientContext &context);

private:
	mutable mutex lock;
	reference_map_t<const Expression, unique_ptr<BaseStatistics>> stats_map;
};

} // namespace duckdb
