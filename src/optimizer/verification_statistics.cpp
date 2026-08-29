#include "duckdb/optimizer/verification_statistics.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

void VerificationStatistics::SetStats(const Expression &expr, unique_ptr<BaseStatistics> stats) {
	lock_guard<mutex> guard(lock);
	stats_map[expr] = std::move(stats);
}

optional_ptr<const BaseStatistics> VerificationStatistics::GetStats(const Expression &expr) const {
	lock_guard<mutex> guard(lock);
	auto entry = stats_map.find(expr);
	if (entry == stats_map.end()) {
		return nullptr;
	}
	return entry->second.get();
}

VerificationStatistics &VerificationStatistics::GetOrCreate(ClientContext &context) {
	return *context.registered_state->GetOrCreate<VerificationStatistics>(STATE_KEY);
}

shared_ptr<VerificationStatistics> VerificationStatistics::TryGet(ClientContext &context) {
	return context.registered_state->Get<VerificationStatistics>(STATE_KEY);
}

} // namespace duckdb
