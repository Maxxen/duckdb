#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

#include <bitpacking.h>

namespace duckdb {

//------------------------------------------------------------------------------
// GEOMETRY/GEOGRAPHY -> VARCHAR
//------------------------------------------------------------------------------
// TODO: Format as "Well-Known-Text"
static bool SpatialVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {

	const auto is_geog = source.GetType().id() == LogicalTypeId::GEOGRAPHY;
	const auto text = is_geog ? "GEOGRAPHY" : "GEOMETRY";
	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](const string_t&) {
		// Uppercase exponent using ":G" format, in accordance with the WKT spec
		//duckdb_fmt::format("{:G}", wkb_blob);
		return StringVector::AddString(result, text);
	});

	return true;
}

BoundCastInfo DefaultCasts::SpatialCastSwitch(BindCastInput &input, const LogicalType &source,
                                            const LogicalType &target) {

	switch (source.id()) {
		case LogicalTypeId::GEOMETRY:
			switch (target.id()) {
				case LogicalTypeId::GEOGRAPHY: return DefaultCasts::ReinterpretCast;
				case LogicalTypeId::BLOB: return DefaultCasts::ReinterpretCast;
				case LogicalTypeId::VARCHAR: return SpatialVarcharCast;
				default: return DefaultCasts::TryVectorNullCast;
			}
		case LogicalTypeId::GEOGRAPHY:
			switch (target.id()) {
				case LogicalTypeId::GEOMETRY: return DefaultCasts::ReinterpretCast;
				case LogicalTypeId::BLOB: return DefaultCasts::ReinterpretCast;
				case LogicalTypeId::VARCHAR: return SpatialVarcharCast;
				default: return DefaultCasts::TryVectorNullCast;
			}
		default:
			D_ASSERT(false);
			return DefaultCasts::TryVectorNullCast;
	}
}

} // namespace duckdb
