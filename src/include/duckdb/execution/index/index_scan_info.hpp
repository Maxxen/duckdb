//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/index_scan_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

//! Opaque, index-specific query parameters produced by a BoundIndex bind callback at
//! optimizer-time and consumed by the matching scan/probe callback at runtime.
//! Concrete index types derive from this to carry their query (e.g. ART bounds, HNSW
//! query vector + k, RTREE bounding box). A set `limit` marks an OrderLimit query kind.
struct IndexScanInfo {
	virtual ~IndexScanInfo() = default;

	//! The k of an OrderLimit query. INVALID_INDEX for a Predicate query (no limit).
	idx_t limit = DConstants::INVALID_INDEX;

	bool HasLimit() const {
		return limit != DConstants::INVALID_INDEX;
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

//! The result of asking an index whether it can serve a given query. A null query_params
//! (i.e. a null IndexMatch) means the index is not applicable.
struct IndexMatch {
	//! The index-specific query parameters to carry to runtime.
	unique_ptr<IndexScanInfo> query_params;
	//! Whether the match is exact. If false, the original predicate/join condition must be
	//! retained above the index scan/join as a residual recheck (e.g. RTREE bbox prefilter).
	bool is_exact = true;
	//! Estimated number of row-ids the index will produce.
	idx_t estimated_cardinality = 0;
	//! Estimated cost, used to choose among multiple applicable indexes (lower is better).
	double estimated_cost = 0;
	//! For probe joins: the outer-input column binding that provides the probe key. Lets the core
	//! join matcher verify the key originates from the index join's child.
	ColumnBinding probe_key_binding;
};

//! What kind of ordering an OrderLimit trigger presents to the index.
enum class IndexOrderKind : uint8_t { ASC_VALUE, DESC_VALUE, DISTANCE_ASC };

} // namespace duckdb
