#include "duckdb/common/types/geometry_crs.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Identify
//----------------------------------------------------------------------------------------------------------------------

// Try to identify the type of the coordinate system from the text
static CoordinateReferenceSystemType TryIdentifyCoordinateSystemType(const string &text) {
	if (text.empty()) {
		return CoordinateReferenceSystemType::UNKNOWN;
	}

	// Trim whitespace for pattern matching
	const char *beg = text.c_str();
	const char *end = beg + text.size();

	while (beg != end && std::isspace(*beg)) {
		beg++;
	}

	while (end != beg && std::isspace(*end)) {
		end--;
	}

	if (beg == end) {
		return CoordinateReferenceSystemType::UNKNOWN;
	}

	// Look for PROJJSON pattern
	if (*beg == '{' && *end == '}') {
		// Try to parse as JSON
		// Note: We do not do full JSON parsing here, just a simple check for curly braces
		return CoordinateReferenceSystemType::PROJJSON;
	}

	// Look for SRID pattern (all digits)
	auto all_digits = true;
	for (auto ptr = beg; ptr != end; ptr++) {
		if (!std::isdigit(*ptr)) {
			all_digits = false;
			break;
		}
	}
	if (all_digits) {
		return CoordinateReferenceSystemType::SRID;
	}

	// Look for AUTH:CODE pattern
	for (auto colon_pos = beg; colon_pos != end; colon_pos++) {
		if (*colon_pos == ':') {
			bool auth_valid = true;
			for (auto ptr = beg; ptr != colon_pos; ptr++) {
				if (!std::isalpha(*ptr)) {
					auth_valid = false;
					break;
				}
			}

			bool code_valid = true;
			for (auto ptr = colon_pos + 1; ptr != end; ptr++) {
				if (!std::isdigit(*ptr)) {
					code_valid = false;
					break;
				}
			}

			if (auth_valid && code_valid) {
				return CoordinateReferenceSystemType::AUTH_CODE;
			}
			break;
		}
	}

	// Look for WKT2 pattern
	const char *wkt2_keywords[] = {"GEOGCRS[",       "PROJCRS[", "VERTCRS[",     "COMPOUNDCRS[",    "ENGINEERINGCRS[",
	                               "PARAMETRICCRS[", "TIMECRS[", "GEODETICCRS[", "DERIVEDPROJCRS[", "BOUNDCRS["};

	for (auto &keyword : wkt2_keywords) {
		const auto keyword_len = strlen(keyword);
		if (static_cast<size_t>(end - beg + 1) >= keyword_len && strncmp(beg, keyword, keyword_len) == 0) {
			return CoordinateReferenceSystemType::WKT2_2019;
		}
	}

	return CoordinateReferenceSystemType::UNKNOWN;
}

CoordinateReferenceSystem::CoordinateReferenceSystem()
    : type(CoordinateReferenceSystemType::UNKNOWN), name(""), text("") {
}

CoordinateReferenceSystem::CoordinateReferenceSystem(const string &crs) {
	// Try to identify the type of the coordinate system from the text
	type = TryIdentifyCoordinateSystemType(crs);
	name = "";

	if (type == CoordinateReferenceSystemType::AUTH_CODE) {
		// Normalize to uppercase
		text = StringUtil::Upper(crs);
	} else {
		text = crs;
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Serialization
//----------------------------------------------------------------------------------------------------------------------

void CoordinateReferenceSystem::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<CoordinateReferenceSystemType>(200, "type", type,
	                                                                   CoordinateReferenceSystemType::UNKNOWN);
	serializer.WritePropertyWithDefault<string>(201, "name", name);
	serializer.WritePropertyWithDefault<string>(202, "text", text);
}

CoordinateReferenceSystem CoordinateReferenceSystem::Deserialize(Deserializer &deserializer) {
	CoordinateReferenceSystem crs;
	deserializer.ReadPropertyWithExplicitDefault<CoordinateReferenceSystemType>(200, "type", crs.type,
	                                                                            CoordinateReferenceSystemType::UNKNOWN);
	deserializer.ReadPropertyWithDefault<string>(201, "name", crs.name);
	deserializer.ReadPropertyWithDefault<string>(202, "text", crs.text);
	return crs;
}

} // namespace duckdb
