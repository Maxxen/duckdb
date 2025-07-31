#include "duckdb/common/types/geometry.hpp"
#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {

static bool GeometryToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](const string_t &input) -> string_t {
		return Geometry::ToString(result, input.GetData(), input.GetSize());
	});
	return true;
}

BoundCastInfo DefaultCasts::GeometryCastSwitch(BindCastInput &input, const LogicalType &source,
                                               const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::GEOMETRY);
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		return GeometryToVarcharCast;
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
