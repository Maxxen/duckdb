#include "duckdb/function/scalar/geometry_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/geometry_crs.hpp"

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
		const auto &lhs_crs = GeoType::GetCRS(arguments[0]->return_type);
		const auto &rhs_crs = GeoType::GetCRS(arguments[1]->return_type);
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

ScalarFunctionSet StIntersectExtentFun::GetFunctions() {
	ScalarFunction fun({LogicalType::GEOMETRY(), LogicalType::GEOMETRY()}, LogicalType::BOOLEAN, IntersectFunction,
	                   IntersectExtentBind);

	ScalarFunctionSet set("ST_Intersect_Extent");
	set.AddFunction(fun);

	return set;
}

static unique_ptr<FunctionData> BindCRSFunction(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	// Check if the CRS is set in the first argument
	bound_function.arguments[0] = arguments[0]->return_type;
	return nullptr;
}

static LogicalType GetCRSLogicalType() {
	return LogicalType::STRUCT({
	    {"type", LogicalType::VARCHAR},
	    {"name", LogicalType::VARCHAR},
	    {"value", LogicalType::VARCHAR},
	});
}

static Value GetCRSValue(const LogicalType &logical_type) {

	if (!GeoType::HasCRS(logical_type)) {
		// Return null
		return Value(GetCRSLogicalType());
	}

	auto &crs = GeoType::GetCRS(logical_type);

	const char *type_str;
	switch (crs.GetType()) {
	case CoordinateReferenceSystemType::PROJJSON:
		type_str = "projjson";
		break;
	case CoordinateReferenceSystemType::WKT2_2019:
		type_str = "wkt2:2019";
		break;
	case CoordinateReferenceSystemType::AUTH_CODE:
		type_str = "authority_code";
		break;
	case CoordinateReferenceSystemType::SRID:
		type_str = "srid";
		break;
	case CoordinateReferenceSystemType::UNKNOWN:
	default:
		type_str = "unknown";
		break;
	}

	auto type_value = Value(type_str);
	auto name_value = crs.GetName().empty() ? Value(LogicalTypeId::VARCHAR) : Value(crs.GetName());
	auto text_value = Value(crs.GetValue());

	auto crs_value =
	    Value::STRUCT(GetCRSLogicalType(), {std::move(type_value), std::move(name_value), std::move(text_value)});

	return crs_value;
}

static void CRSFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &type = args.data[0].GetType();
	result.Reference(GetCRSValue(type));
}

static unique_ptr<Expression> BindCRSFunctionExpression(FunctionBindExpressionInput &input) {
	const auto &return_type = input.children[0]->return_type;
	if (return_type.id() == LogicalTypeId::UNKNOWN || return_type.id() == LogicalTypeId::SQLNULL) {
		// parameter - unknown return type
		return nullptr;
	}

	return make_uniq<BoundConstantExpression>(GetCRSValue(return_type));
}

ScalarFunctionSet StCrsFun::GetFunctions() {
	ScalarFunctionSet set("ST_CRS");

	const auto crs_type = GetCRSLogicalType();

	for (auto &type : {LogicalTypeId::GEOMETRY, LogicalTypeId::GEOGRAPHY}) {
		ScalarFunction geom_func({type}, crs_type, CRSFunction, BindCRSFunction);
		geom_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
		geom_func.bind_expression = BindCRSFunctionExpression;
		set.AddFunction(geom_func);
	}
	return set;
}

//----------------------------------------------------------------------------------------------------------------------
// ST_GeomFromWKB
//----------------------------------------------------------------------------------------------------------------------
static void GeomFromWKBFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(input.data[0], result, input.size(), [&](const string_t &wkb) {
		string_t res;
		if (!Geometry::FromWKB(wkb, res, result, true)) {
			throw InvalidInputException("Failed to convert WKB: %s", wkb.GetString());
		}
		return res;
	});
}

ScalarFunctionSet StGeomfromwkbFun::GetFunctions() {
	// Create the function that converts WKB to GEOMETRY
	ScalarFunctionSet set;
	ScalarFunction fun({LogicalType::BLOB}, LogicalType::GEOMETRY(), GeomFromWKBFunction);
	set.AddFunction(fun);
	return set;
}

ScalarFunctionSet StGeogfromwkbFun::GetFunctions() {
	ScalarFunctionSet set;
	ScalarFunction fun({LogicalType::BLOB}, LogicalType::GEOGRAPHY(), GeomFromWKBFunction);
	set.AddFunction(fun);
	return set;
}

} // namespace duckdb
