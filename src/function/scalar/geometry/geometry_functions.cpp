#include "duckdb/function/scalar/geometry_functions.hpp"

#include "duckdb/common/types/geometry.hpp"

namespace duckdb {

static void ExtentFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	const auto count = input.size();

	auto &geom_vec = input.data[0];
	UnifiedVectorFormat geom_format;
	geom_vec.ToUnifiedFormat(count, geom_format);
	const auto geom_data = UnifiedVectorFormat::GetData<string_t>(geom_format);

	const auto &extent_parts = StructVector::GetEntries(result);
	const auto xmin_data = FlatVector::GetData<double>(*extent_parts[0]);
	const auto ymin_data = FlatVector::GetData<double>(*extent_parts[1]);
	const auto xmax_data = FlatVector::GetData<double>(*extent_parts[2]);
	const auto ymax_data = FlatVector::GetData<double>(*extent_parts[3]);

	for (idx_t out_idx = 0; out_idx < count; ++out_idx) {
		const auto row_idx = geom_format.sel->get_index(out_idx);

		if (!geom_format.validity.RowIsValid(row_idx)) {
			FlatVector::SetNull(result, out_idx, true);
			continue;
		}

		auto bounds = GeometryExtent::Empty();
		if (!Geometry::GetExtent(geom_data[row_idx], bounds)) {
			FlatVector::SetNull(result, out_idx, true);
			continue;
		}

		xmin_data[out_idx] = bounds.min_x;
		ymin_data[out_idx] = bounds.min_y;
		xmax_data[out_idx] = bounds.max_x;
		ymax_data[out_idx] = bounds.max_y;
	}

	if (input.AllConstant() || count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction StExtentFun::GetFunction() {

	auto extent_type = LogicalType::STRUCT({{"xmin", LogicalType::DOUBLE},
	                                        {"ymin", LogicalType::DOUBLE},
	                                        {"xmax", LogicalType::DOUBLE},
	                                        {"ymax", LogicalType::DOUBLE}});

	ScalarFunction fun("st_extent", {LogicalType::GEOMETRY()}, std::move(extent_type), ExtentFunction);
	return fun;
}

static unique_ptr<FunctionData> IntersectExtentBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	const auto lhs_has_crs = GeoType::HasCRS(arguments[0]->return_type);
	const auto rhs_has_crs = GeoType::HasCRS(arguments[1]->return_type);
	if (lhs_has_crs != rhs_has_crs) {
		throw BinderException("st_intersect_extent requires both geometries to have the same CRS");
	}
	if (lhs_has_crs || rhs_has_crs) {
		// If both geometries have a CRS, we need to check if they are the same
		const auto lhs_crs = GeoType::GetCRS(arguments[0]->return_type);
		const auto rhs_crs = GeoType::GetCRS(arguments[1]->return_type);
		if (lhs_crs != rhs_crs) {
			throw BinderException("st_intersect_extent requires both geometries to have the same CRS");
		}
	}

	bound_function.arguments[0] = arguments[0]->return_type;
	bound_function.arguments[1] = arguments[1]->return_type;

	return nullptr;
}
static void IntersectFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, bool>(
	    input.data[0], input.data[1], result, input.size(), [&](const string_t &a, const string_t &b) {
		    auto bounds_a = GeometryExtent::Empty();
		    auto bounds_b = GeometryExtent::Empty();
		    if (!Geometry::GetExtent(a, bounds_a) || !Geometry::GetExtent(b, bounds_b)) {
			    return false;
		    }
		    return bounds_a.Intersects(bounds_b);
	    });
}

ScalarFunction StIntersectExtentFun::GetFunction() {
	ScalarFunction fun("st_intersect_extent", {LogicalType::GEOMETRY(), LogicalType::GEOMETRY()}, LogicalType::BOOLEAN,
	                   IntersectFunction, IntersectExtentBind);
	return fun;
}

} // namespace duckdb
