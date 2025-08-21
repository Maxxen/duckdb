#include "geo_parquet.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

//------------------------------------------------------------------------------
// GeoParquetFileMetadata
//------------------------------------------------------------------------------
GeoParquetFileMetadata::GeoParquetFileMetadata(GeoParquetVersion version) : version(version) {
}

static constexpr auto OGC_WGS84_PROJJSON =
    R">>({"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"WGS 84 (CRS84)","datum":{"type":"GeodeticReferenceFrame","name":"World Geodetic System 1984","ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563}},"coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"},{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"}]},"scope":"unknown","area":"World","bbox":{"south_latitude":-90,"west_longitude":-180,"north_latitude":90,"east_longitude":180},"id":{"authority":"OGC","code":"CRS84"}})>>";

unique_ptr<GeoParquetFileMetadata> GeoParquetFileMetadata::TryRead(const duckdb_parquet::FileMetaData &file_meta_data,
                                                                   const ClientContext &context) {
	// Conversion not enabled!
	if (!IsGeoParquetConversionEnabled(context)) {
		return nullptr;
	}

	for (auto &kv : file_meta_data.key_value_metadata) {
		if (kv.key == "geo") {
			const auto geo_metadata = yyjson_read(kv.value.c_str(), kv.value.size(), 0);
			if (!geo_metadata) {
				// Could not parse the JSON
				return nullptr;
			}

			try {
				// Check the root object
				const auto root = yyjson_doc_get_root(geo_metadata);
				if (!yyjson_is_obj(root)) {
					throw InvalidInputException("Geoparquet metadata is not an object");
				}

				// Check and parse the version
				const auto version_val = yyjson_obj_get(root, "version");
				if (!yyjson_is_str(version_val)) {
					throw InvalidInputException("Geoparquet metadata does not have a version");
				}

				auto version_str = StringUtil::Lower(yyjson_get_str(version_val));
				const auto version_major = StringUtil::Split(version_str, '.')[0];
				if (version_major.empty()) {
					throw InvalidInputException("Geoparquet metadata version is empty");
				}
				const auto version_minor = StringUtil::Split(version_str, '.')[1];
				if (version_minor.empty()) {
					throw InvalidInputException("Geoparquet metadata version is missing minor version");
				}
				int version_major_int = 0;
				int version_minor_int = 0;
				try {
					version_major_int = std::stoi(version_major);
					version_minor_int = std::stoi(version_minor);
				} catch (...) {
					throw InvalidInputException("Geoparquet metadata version '%s' is not a valid version", version_str);
				}

				GeoParquetVersion version;
				if (version_major_int != 1) {
					throw InvalidInputException("Geoparquet version %s is not supported", version_str);
				}
				if (version_minor_int == 0) {
					version = GeoParquetVersion::V100;
				} else {
					// Treat everything above 1.0 as 1.1
					version = GeoParquetVersion::V110;
				}

				auto result = make_uniq<GeoParquetFileMetadata>(version);

				// Check and parse the primary geometry column
				const auto primary_geometry_column_val = yyjson_obj_get(root, "primary_column");
				if (!yyjson_is_str(primary_geometry_column_val)) {
					throw InvalidInputException("Geoparquet metadata does not have a primary column");
				}
				result->primary_geometry_column = yyjson_get_str(primary_geometry_column_val);

				// Check and parse the geometry columns
				const auto columns_val = yyjson_obj_get(root, "columns");
				if (!yyjson_is_obj(columns_val)) {
					throw InvalidInputException("Geoparquet metadata does not have a columns object");
				}

				// Iterate over all geometry columns
				yyjson_obj_iter iter = yyjson_obj_iter_with(columns_val);
				yyjson_val *column_key;

				while ((column_key = yyjson_obj_iter_next(&iter))) {
					const auto column_val = yyjson_obj_iter_get_val(column_key);
					const auto column_name = yyjson_get_str(column_key);

					auto &column = result->geometry_columns[column_name];

					if (!yyjson_is_obj(column_val)) {
						throw InvalidInputException("Geoparquet column '%s' is not an object", column_name);
					}

					// Parse the encoding
					const auto encoding_val = yyjson_obj_get(column_val, "encoding");
					if (!yyjson_is_str(encoding_val)) {
						throw InvalidInputException("Geoparquet column '%s' does not have an encoding", column_name);
					}
					const auto encoding_str = yyjson_get_str(encoding_val);
					if (strcmp(encoding_str, "WKB") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::WKB;
					} else if (strcmp(encoding_str, "point") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::POINT;
					} else if (strcmp(encoding_str, "linestring") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::LINESTRING;
					} else if (strcmp(encoding_str, "polygon") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::POLYGON;
					} else if (strcmp(encoding_str, "multipoint") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::MULTIPOINT;
					} else if (strcmp(encoding_str, "multilinestring") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::MULTILINESTRING;
					} else if (strcmp(encoding_str, "multipolygon") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::MULTIPOLYGON;
					} else {
						throw InvalidInputException("Geoparquet column '%s' has an unsupported encoding", column_name);
					}

					// Parse the geometry types
					const auto geometry_types_val = yyjson_obj_get(column_val, "geometry_types");
					if (!yyjson_is_arr(geometry_types_val)) {
						throw InvalidInputException("Geoparquet column '%s' does not have geometry types", column_name);
					}
					// We dont care about the geometry types for now.

					// Try parse the crs
					column.projjson = OGC_WGS84_PROJJSON; // Default to OGC:CRS84 if not specified

					const auto crs_val = yyjson_obj_get(column_val, "crs");
					if (yyjson_is_obj(crs_val)) {
						// Print the object as a str
						size_t crs_json_len = 0;
						auto crs_json = yyjson_val_write_opts(crs_val, 0, nullptr, &crs_json_len, nullptr);
						if (!crs_json) {
							throw InvalidInputException("Geoparquet column '%s' could not read CRS", column_name);
						}
						column.projjson = string(crs_json, crs_json_len);
						free(crs_json);
					}

					// Try parse the edges
					const auto edges_val = yyjson_obj_get(column_val, "edges");
					if (yyjson_is_str(edges_val)) {
						auto edges_str = yyjson_get_str(edges_val);
						if (strcmp(edges_str, "planar") == 0) {
							column.logical_type = LogicalType::GEOMETRY(column.projjson); // Default planar geometry
						} else if (strcmp(edges_str, "spherical") == 0) {
							column.logical_type = LogicalType::GEOGRAPHY(column.projjson);
						} else {
							throw InvalidInputException("Geoparquet column '%s' has an unsupported edge type: %s",
							                            column_name, edges_str);
						}
					} else {
						column.logical_type =
						    LogicalType::GEOMETRY(column.projjson); // Default to planar geometry if not specified
					}

					// TODO: Parse the bounding box, other metadata that might be useful.
					// (Only encoding and geometry types are required to be present)
				}

				// Return the result
				// Make sure to free the JSON document
				yyjson_doc_free(geo_metadata);
				return result;

			} catch (...) {
				// Make sure to free the JSON document in case of an exception
				yyjson_doc_free(geo_metadata);
				throw;
			}
		}
	}
	return nullptr;
}

void GeoParquetFileMetadata::Write(duckdb_parquet::FileMetaData &file_meta_data) const {

	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	// Add the version
	switch (version) {
	case GeoParquetVersion::V100:
		yyjson_mut_obj_add_strncpy(doc, root, "version", "1.0.0", 5);
		break;
	case GeoParquetVersion::V110:
		yyjson_mut_obj_add_strncpy(doc, root, "version", "1.1.0", 5);
		break;
	default:
		yyjson_mut_doc_free(doc);
		throw NotImplementedException("Unsupported GeoParquet version: %d", static_cast<int>(version));
	}

	// Add the primary column
	yyjson_mut_obj_add_strncpy(doc, root, "primary_column", primary_geometry_column.c_str(),
	                           primary_geometry_column.size());

	// Add the columns
	const auto json_columns = yyjson_mut_obj_add_obj(doc, root, "columns");

	for (auto &column : geometry_columns) {
		const auto column_json = yyjson_mut_obj_add_obj(doc, json_columns, column.first.c_str());
		yyjson_mut_obj_add_str(doc, column_json, "encoding", "WKB");

		// Write the edges type
		switch (column.second.logical_type.id()) {
		case LogicalTypeId::GEOMETRY: {
			yyjson_mut_obj_add_str(doc, column_json, "edges", "planar");
		} break;
		case LogicalTypeId::GEOGRAPHY: {
			yyjson_mut_obj_add_str(doc, column_json, "edges", "spherical");
		} break;
		default:
			throw NotImplementedException("Unsupported logical type for GeoParquet column: %s",
			                              column.second.logical_type.ToString());
		}
		const auto geometry_types = yyjson_mut_obj_add_arr(doc, column_json, "geometry_types");
		for (auto &type_name : column.second.stats.types.Format(true)) {
			yyjson_mut_arr_add_strcpy(doc, geometry_types, type_name.c_str());
		}
		const auto bbox = yyjson_mut_obj_add_arr(doc, column_json, "bbox");
		yyjson_mut_arr_add_real(doc, bbox, column.second.stats.bbox.min_x);
		yyjson_mut_arr_add_real(doc, bbox, column.second.stats.bbox.min_y);
		yyjson_mut_arr_add_real(doc, bbox, column.second.stats.bbox.max_x);
		yyjson_mut_arr_add_real(doc, bbox, column.second.stats.bbox.max_y);

		if (column.second.stats.types.Any(VertexType::XYZ) || column.second.stats.types.Any(VertexType::XYZM)) {
			yyjson_mut_arr_add_real(doc, bbox, column.second.stats.bbox.min_z);
			yyjson_mut_arr_add_real(doc, bbox, column.second.stats.bbox.max_z);
		}

		yyjson_doc *crs_doc = nullptr;

		// Try to get the CRS from the settings
		if (!column.second.projjson.empty()) {
			crs_doc = yyjson_read(column.second.projjson.c_str(), column.second.projjson.size(), 0);
		}

		// Try to get it from the logical type
		if (!crs_doc) {
			if (GeoType::HasCRS(column.second.logical_type)) {
				auto &crs = column.second.projjson;
				crs_doc = yyjson_read(crs.c_str(), crs.size(), 0);
				if (!crs_doc) {
					// TODO: Use the `spatial` extension to attempt to convert the CRS to PROJJSON format automatically
					yyjson_mut_doc_free(doc);
					throw InvalidInputException("GeoParquet requires the CRS field to be in PROJJSON format!");
				}
			}
		}

		if (crs_doc) {
			const auto crs_root = yyjson_doc_get_root(crs_doc);
			const auto crs_val = yyjson_val_mut_copy(doc, crs_root);
			const auto crs_key = yyjson_mut_strcpy(doc, "projjson");
			yyjson_mut_obj_add(column_json, crs_key, crs_val);

			// Free the CRS document
			yyjson_doc_free(crs_doc);
		}
	}

	yyjson_write_err err;
	size_t len;
	char *json = yyjson_mut_write_opts(doc, 0, nullptr, &len, &err);
	if (!json) {
		yyjson_mut_doc_free(doc);
		throw SerializationException("Failed to write JSON string: %s", err.msg);
	}

	// Create a string from the JSON
	duckdb_parquet::KeyValue kv;
	kv.__set_key("geo");
	kv.__set_value(string(json, len));

	// Free the JSON and the document
	free(json);
	yyjson_mut_doc_free(doc);

	file_meta_data.key_value_metadata.push_back(kv);
	file_meta_data.__isset.key_value_metadata = true;
}

bool GeoParquetFileMetadata::IsGeometryColumn(const string &column_name) const {
	return geometry_columns.find(column_name) != geometry_columns.end();
}

void GeoParquetFileMetadata::AddGeoParquetStats(const string &column_name, const LogicalType &type,
                                                const duckdb_parquet::GeospatialStatistics &stats) {
	lock_guard<mutex> glock(write_lock);

	if (primary_geometry_column.empty()) {
		primary_geometry_column = column_name;
	}

	auto &column = geometry_columns[column_name];

	column.logical_type = type;
	column.geometry_encoding = GeoParquetColumnEncoding::WKB; // Default to WKB encoding

	// Try to extract the stats
	column.stats.bbox.min_x = std::min(stats.bbox.xmin, stats.bbox.xmax);
	column.stats.bbox.min_y = std::min(stats.bbox.ymin, stats.bbox.ymax);
	column.stats.bbox.max_x = std::max(stats.bbox.xmin, stats.bbox.xmax);
	column.stats.bbox.max_y = std::max(stats.bbox.ymin, stats.bbox.ymax);

	// Add Z bounds, if present
	// M values are not supported in GeoParquet, so we ignore them
	if (stats.bbox.__isset.zmin && stats.bbox.__isset.zmax) {
		column.stats.bbox.min_z = std::min(stats.bbox.zmin, stats.bbox.zmax);
		column.stats.bbox.max_z = std::max(stats.bbox.zmin, stats.bbox.zmax);
	}

	// Add the geometry types, but only if they are _XY or _XYZ.
	// GeoParquet doesnt currently support _XYM or _XYZM types.
	if (stats.__isset.geospatial_types) {
		for (auto &gtype : stats.geospatial_types) {
			switch (gtype) {
			case 1:
				column.stats.types.Add(GeometryType::POINT, VertexType::XY);
				break;
			case 1001:
				column.stats.types.Add(GeometryType::POINT, VertexType::XYZ);
				break;
			case 2:
				column.stats.types.Add(GeometryType::LINESTRING, VertexType::XY);
				break;
			case 1002:
				column.stats.types.Add(GeometryType::LINESTRING, VertexType::XYZ);
				break;
			case 3:
				column.stats.types.Add(GeometryType::POLYGON, VertexType::XY);
				break;
			case 1003:
				column.stats.types.Add(GeometryType::POLYGON, VertexType::XYZ);
				break;
			case 4:
				column.stats.types.Add(GeometryType::MULTIPOINT, VertexType::XY);
				break;
			case 1004:
				column.stats.types.Add(GeometryType::MULTIPOINT, VertexType::XYZ);
				break;
			case 5:
				column.stats.types.Add(GeometryType::MULTILINESTRING, VertexType::XY);
				break;
			case 1005:
				column.stats.types.Add(GeometryType::MULTILINESTRING, VertexType::XYZ);
				break;
			case 6:
				column.stats.types.Add(GeometryType::MULTIPOLYGON, VertexType::XY);
				break;
			case 1006:
				column.stats.types.Add(GeometryType::MULTIPOLYGON, VertexType::XYZ);
				break;
			case 7:
				column.stats.types.Add(GeometryType::GEOMETRYCOLLECTION, VertexType::XY);
				break;
			case 1007:
				column.stats.types.Add(GeometryType::GEOMETRYCOLLECTION, VertexType::XYZ);
				break;
			default:
				throw InvalidInputException("GeoParquet only supports XY and XYZ geometries of POINT, "
				                            "LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON and "
				                            "GEOMETRYCOLLECTION types. Unsupported type: %d",
				                            gtype);
			}
		}
	}
}

bool GeoParquetFileMetadata::IsGeoParquetConversionEnabled(const ClientContext &context) {
	Value geoparquet_enabled;
	if (!context.TryGetCurrentSetting("enable_geoparquet_conversion", geoparquet_enabled)) {
		return false;
	}
	if (!geoparquet_enabled.GetValue<bool>()) {
		// Disabled by setting
		return false;
	}
	return true;
}

} // namespace duckdb
