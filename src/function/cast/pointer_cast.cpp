#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

BoundCastInfo DefaultCasts::PointerCastSwitch(BindCastInput &input, const LogicalType &source,
                                              const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// pointer to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<uintptr_t, duckdb::CastFromPointer>);
	case LogicalTypeId::UNION:
		return DefaultCasts::PromoteToUnionCast(input, source, target);
	default:
		return nullptr;
	}
}

} // namespace duckdb
