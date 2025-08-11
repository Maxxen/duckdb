#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "fast_float/fast_float.h"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Helper Classes
//----------------------------------------------------------------------------------------------------------------------
class BinaryWriter {
public:
	template<class T>
	void Write(const T& value) {
		auto ptr = reinterpret_cast<const char*>(&value);
		buffer.insert(buffer.end(), ptr, ptr + sizeof(T));
	}

	template<class T>
	struct Reserved {
		size_t offset;
		T value;
	};

	template<class T>
	Reserved<T> Reserve() {
		auto offset = buffer.size();
		buffer.resize(buffer.size() + sizeof(T));
		return { offset, T() };
	}

	template<class T>
	void Write(const Reserved<T> &reserved) {
		if (reserved.offset + sizeof(T) > buffer.size()) {
			throw InternalException("Write out of bounds in BinaryWriter");
		}
		auto ptr = reinterpret_cast<const char*>(&reserved.value);
		// We've reserved 0 bytes, so we can safely memcpy
		memcpy(buffer.data() + reserved.offset, ptr, sizeof(T));
	}

	void Write(const char* data, size_t size) {
		D_ASSERT(data != nullptr);
		buffer.insert(buffer.end(), data, data + size);
	}

	const vector<char>& GetBuffer() const {
		return buffer;
	}

private:
	vector<char> buffer;
};

class TextReader {
public:
	TextReader(const char* data, size_t size) : beg(data), pos(data), end(data + size) { }

	bool TryMatch(const char* str) {
		auto ptr = pos;
		while (*str && pos < end && tolower(*pos) == tolower(*str)) {
			pos++;
			str++;
		}
		if (*str == '\0') {
			SkipWhitespace(); // remove trailing whitespace
			return true; // matched
		} else {
			pos = ptr; // reset position
			return false; // not matched
		}
	}

	bool TryMatch(char c) {
		if (pos < end && tolower(*pos) == tolower(c)) {
			pos++;
			SkipWhitespace(); // remove trailing whitespace
			return true; // matched
		}
		return false; // not matched
	}

	void Match(const char* str) {
		if (!TryMatch(str)) {
			throw InvalidInputException("Expected '%s' but got '%c' at position %zu", str,  *pos, pos - beg);
		}
	}

	void Match(char c) {
		if (!TryMatch(c)) {
			throw InvalidInputException("Expected '%c' but got '%c' at position %zu", c, *pos, pos - beg);
		}
	}

	double MatchNumber() {
		const char* start = pos;
		while (pos < end && (isdigit(*pos) || *pos == '.' || *pos == '-' || *pos == '+' || *pos == 'e' || *pos == 'E')) {
			pos++;
		}
		if (start == pos) {
			throw InvalidInputException("Expected a number but got '%c' at position %zu", *pos, pos - beg);
		}
		// Now use fast_float to parse the number
		double result;
		auto parse_result = duckdb_fast_float::from_chars(start, pos, result);
		if (parse_result.ec != std::errc()) {
			throw InvalidInputException("Failed to parse number at position %zu", pos - beg);
		}

		pos = parse_result.ptr; // update position to the end of the parsed number

		SkipWhitespace(); // remove trailing whitespace
		return result; // return the parsed number
	}

	idx_t GetPosition() const {
		return static_cast<idx_t>(pos - beg);
	}

	void Reset() {
		pos = beg;
	}

private:
	void SkipWhitespace() {
		while (pos < end && isspace(*pos)) {
			pos++;
		}
	}

	const char* beg;
	const char* pos;
	const char* end;
};



class BinaryReader {
public:
	BinaryReader(const char* data, size_t size) : beg(data), pos(data), end(data + size) { }

	template<class T, bool LE = true>
	T Read() {
		if (pos + sizeof(T) > end) {
			throw InvalidInputException("Unexpected end of binary data at position %zu", pos - beg);
		}
		T value;
		if (LE) {
			memcpy(&value, pos, sizeof(T));
			pos += sizeof(T);
		} else {
			char temp[sizeof(T)];
			for (size_t i = 0; i < sizeof(T); ++i) {
				temp[i] = pos[sizeof(T) - 1 - i];
			}
			memcpy(&value, temp, sizeof(T));
			pos += sizeof(T);
		}
		return value;
	}

	void Skip(size_t size) {
		if (pos + size > end) {
			throw InvalidInputException("Skipping beyond end of binary data at position %zu", pos - beg);
		}
		pos += size;
	}

	size_t GetPosition() const {
		return static_cast<idx_t>(pos - beg);
	}

private:
	const char* beg;
	const char* pos;
	const char* end;
};


class TextWriter {
public:
	void Write(const char* str) {
		buffer.insert(buffer.end(), str, str + strlen(str));
	}
	void Write(char c) {
		buffer.push_back(c);
	}
	void Write(double value) {
		duckdb_fmt::format_to(std::back_inserter(buffer), "{:G}", value);
	}
	const vector<char>& GetBuffer() const {
		return buffer;
	}
private:
	vector<char> buffer;
};

//----------------------------------------------------------------------------------------------------------------------
// FromString
//----------------------------------------------------------------------------------------------------------------------
// https://libgeos.org/specifications/wkt/

static void FromStringRecursive(TextReader &reader, BinaryWriter &writer, idx_t depth, bool parent_has_z, bool parent_has_m) {
	if (depth == Geometry::MAX_RECURSION_DEPTH) {
		throw InvalidInputException("Geometry string exceeds maximum recursion depth of %d", Geometry::MAX_RECURSION_DEPTH);
	}

	GeometryType type;

	if (reader.TryMatch("point")) {
		type = GeometryType::POINT;
	} else if (reader.TryMatch("linestring")) {
		type = GeometryType::LINESTRING;
	} else if (reader.TryMatch("polygon")) {
		type = GeometryType::POLYGON;
	} else if (reader.TryMatch("multipoint")) {
		type = GeometryType::MULTIPOINT;
	} else if (reader.TryMatch("multilinestring")) {
		type = GeometryType::MULTILINESTRING;
	} else if (reader.TryMatch("multipolygon")) {
		type = GeometryType::MULTIPOLYGON;
	} else if (reader.TryMatch("geometrycollection")) {
		type = GeometryType::GEOMETRYCOLLECTION;
	} else {
		throw InvalidInputException("Unknown geometry type at position %zu", reader.GetPosition());
	}

	const auto has_z = reader.TryMatch("z");
	const auto has_m = reader.TryMatch("m");

	const auto is_empty = reader.TryMatch("empty");

	if ((depth != 0) && ((parent_has_z != has_z) || (parent_has_m != has_m))) {
		throw InvalidInputException("Geometry has inconsistent Z/M dimensions, starting at position %zu", reader.GetPosition());
	}

	// How many dimensions does this geometry have?
	const uint32_t dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);

	// WKB type
	const auto meta = static_cast<uint32_t>(type) +  (has_z ? 1000 : 0) + (has_m ? 2000 : 0);
	// Write the geometry type and vertex type
	writer.Write<uint8_t>(1); // LE Byte Order
	writer.Write<uint32_t>(meta);

	switch (type) {
		case GeometryType::POINT: {
			if (is_empty) {
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					// Write NaN for each dimension, if point is empty
					writer.Write<double>(std::numeric_limits<double>::quiet_NaN());
				}
			} else {
				reader.Match('(');
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					auto value = reader.MatchNumber();
					writer.Write<double>(value);
				}
				reader.Match(')');
			}
		} break;
		case GeometryType::LINESTRING: {
			if (is_empty) {
				writer.Write<uint32_t>(0); // No vertices in empty linestring
				break;
			}
			auto vert_count = writer.Reserve<uint32_t>();
			reader.Match('(');
			do {
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					auto value = reader.MatchNumber();
					writer.Write<double>(value);
				}
				vert_count.value++;
			} while (reader.TryMatch(','));
			reader.Match(')');
			writer.Write(vert_count);
		} break;
		case GeometryType::POLYGON: {
			if (is_empty) {
				writer.Write<uint32_t>(0);
				break; // No rings in empty polygon
			}
			auto ring_count = writer.Reserve<uint32_t>();
			reader.Match('(');
			do {
				auto vert_count = writer.Reserve<uint32_t>();
				reader.Match('(');
				do {
					for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
						auto value = reader.MatchNumber();
						writer.Write<double>(value);
					}
					vert_count.value++;
				} while (reader.TryMatch(','));
				reader.Match(')');
				writer.Write(vert_count);
				ring_count.value++;
			} while (reader.TryMatch(','));
			reader.Match(')');
			writer.Write(ring_count);
		} break;
		case GeometryType::MULTIPOINT: {
			if (is_empty) {
				writer.Write<uint32_t>(0); // No points in empty multipoint
				break;
			}
			auto part_count = writer.Reserve<uint32_t>();
			reader.Match('(');
			do {
				bool has_paren = reader.TryMatch('(');

				const auto part_meta = static_cast<uint32_t>(GeometryType::POINT) + (has_z ? 1000 : 0) + (has_m ? 2000 : 0);
				writer.Write<uint8_t>(1);
				writer.Write<uint32_t>(part_meta);

				if (reader.TryMatch("EMPTY")) {
					for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
						// Write NaN for each dimension, if point is empty
						writer.Write<double>(std::numeric_limits<double>::quiet_NaN());
					}
				} else {
					for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
						auto value = reader.MatchNumber();
						writer.Write<double>(value);
					}
				}
				if (has_paren) {
					reader.Match(')'); // Match the closing parenthesis if it was opened
				}
			} while (reader.TryMatch(','));
			writer.Write(part_count);
		} break;
		case GeometryType::MULTILINESTRING: {
			if (is_empty) {
				return ; // No linestrings in empty multilinestring
			}
			auto part_count = writer.Reserve<uint32_t>();
			reader.Match('(');
			do {

				const auto part_meta = static_cast<uint32_t>(GeometryType::LINESTRING) + (has_z ? 1000 : 0) + (has_m ? 2000 : 0);
				writer.Write<uint8_t>(1);
				writer.Write<uint32_t>(part_meta);

				auto vert_count = writer.Reserve<uint32_t>();
				reader.Match('(');
				do {
					for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
						auto value = reader.MatchNumber();
						writer.Write<double>(value);
					}
					vert_count.value++;
				} while (reader.TryMatch(','));
				reader.Match(')');
				writer.Write(vert_count);
				part_count.value++;
			} while (reader.TryMatch(','));
			reader.Match(')');
			writer.Write(part_count);
		} break;
		case GeometryType::MULTIPOLYGON: {
			if (is_empty) {
				writer.Write<uint32_t>(0); // No polygons in empty multipolygon
				break;
			}
			auto part_count = writer.Reserve<uint32_t>();
			reader.Match('(');
			do {

				const auto part_meta = static_cast<uint32_t>(GeometryType::POLYGON) + (has_z ? 1000 : 0) + (has_m ? 2000 : 0);
				writer.Write<uint8_t>(1);
				writer.Write<uint32_t>(part_meta);

				auto ring_count = writer.Reserve<uint32_t>();
				reader.Match('(');
				do {
					auto vert_count = writer.Reserve<uint32_t>();
					reader.Match('(');
					do {
						for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
							auto value = reader.MatchNumber();
							writer.Write<double>(value);
						}
						vert_count.value++;
					} while (reader.TryMatch(','));
					reader.Match(')');
					writer.Write(vert_count);
					ring_count.value++;
				} while (reader.TryMatch(','));
				reader.Match(')');
				writer.Write(ring_count);
				part_count.value++;
			} while (reader.TryMatch(','));
			reader.Match(')');
			writer.Write(part_count);
		} break;
		case GeometryType::GEOMETRYCOLLECTION: {
			if (is_empty) {
				writer.Write<uint32_t>(0); // No geometries in empty geometry collection
				break;
			}
			auto part_count = writer.Reserve<uint32_t>();
			reader.Match('(');
			do {
				// Recursively parse the geometry inside the collection
				FromStringRecursive(reader, writer, depth + 1, has_z, has_m);
				part_count.value++;
			} while (reader.TryMatch(','));
			reader.Match(')');
			writer.Write(part_count);
		} break;
		default:
			throw InvalidInputException("Unknown geometry type %d at position %zu", static_cast<int>(type), reader.GetPosition());
	}
}

bool Geometry::FromString(const string &str, string_t &result, Vector &result_vector, bool strict) {
	TextReader reader(str.c_str(), str.size());
	BinaryWriter writer;

	FromStringRecursive(reader, writer, 0, false, false);

	const  auto &buffer = writer.GetBuffer();
	result = StringVector::AddStringOrBlob(result_vector, buffer.data(), buffer.size());
	return true;
}

static void ToStringRecursive(BinaryReader &reader, TextWriter &writer, idx_t depth, bool parent_has_z, bool parent_has_m) {
	if (depth == Geometry::MAX_RECURSION_DEPTH) {
		throw InvalidInputException("Geometry exceeds maximum recursion depth of %d", Geometry::MAX_RECURSION_DEPTH);
	}

	// Read the byte order (should always be 1 for little-endian)
	auto byte_order = reader.Read<uint8_t>();
	if (byte_order != 1) {
		throw InvalidInputException("Unsupported byte order %d in WKB", byte_order);
	}

	const auto meta = reader.Read<uint32_t>();
	const auto type = static_cast<GeometryType>(meta % 1000);
	const auto flag = meta / 1000;
	const auto has_z = (flag & 0x01) != 0;
	const auto has_m = (flag & 0x02) != 0;

	if ((depth != 0) && ((parent_has_z != has_z) || (parent_has_m != has_m))) {
		throw InvalidInputException("Geometry has inconsistent Z/M dimensions, starting at position %zu", reader.GetPosition());
	}

	const uint32_t dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);
	const auto flag_str = has_z ? (has_m ? " ZM " : " Z ") : (has_m ? " M " : " ");

	switch (type) {
		case GeometryType::POINT: {
			writer.Write("POINT");
			writer.Write(flag_str);

			double vert[4] = {0, 0, 0, 0};
			auto all_nan = true;
			for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
				vert[d_idx] = reader.Read<double>();
				all_nan &= std::isnan(vert[d_idx]);
			}
			if (all_nan) {
				writer.Write("EMPTY");
				return;
			}
			writer.Write('(');
			for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
				if (d_idx > 0) {
					writer.Write(' ');
				}
				writer.Write(vert[d_idx]);
			}
			writer.Write(')');
		} break;
		case GeometryType::LINESTRING: {
			writer.Write("LINESTRING");;
			writer.Write(flag_str);
			const auto vert_count = reader.Read<uint32_t>();
			if (vert_count == 0) {
				writer.Write("EMPTY");
				return;
			}
			writer.Write('(');
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				if (vert_idx > 0) {
					writer.Write(',');
				}
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					if (d_idx > 0) {
						writer.Write(' ');
					}
					auto value = reader.Read<double>();
					writer.Write(value);
				}
			}
			writer.Write(')');
		} break;
		case GeometryType::POLYGON: {
			writer.Write("POLYGON");
			writer.Write(flag_str);
			const auto ring_count = reader.Read<uint32_t>();
			if (ring_count == 0) {
				writer.Write("EMPTY");
				return;
			}
			writer.Write('(');
			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				if (ring_idx > 0) {
					writer.Write(',');
				}
				const auto vert_count = reader.Read<uint32_t>();
				if (vert_count == 0) {
					writer.Write("EMPTY");
					continue;
				}
				writer.Write('(');
				for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
					if (vert_idx > 0) {
						writer.Write(',');
					}
					for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
						if (d_idx > 0) {
							writer.Write(' ');
						}
						auto value = reader.Read<double>();
						writer.Write(value);
					}
				}
				writer.Write(')');
			}
			writer.Write(')');
		} break;
		case GeometryType::MULTIPOINT: {
			writer.Write("MULTIPOINT");
			writer.Write(flag_str);
			const auto part_count = reader.Read<uint32_t>();
			if (part_count == 0) {
				writer.Write("EMPTY");
				return;
			}
			writer.Write('(');
			for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
				const auto part_byte_order = reader.Read<uint8_t>();
				if (part_byte_order != 1) {
					throw InvalidInputException("Unsupported byte order %d in WKB", part_byte_order);
				}
				const auto part_meta = reader.Read<uint32_t>();
				const auto part_type = static_cast<GeometryType>(part_meta % 1000);
				const auto part_flag = part_meta / 1000;
				const auto part_has_z = (part_flag & 0x01) != 0;
				const auto part_has_m = (part_flag & 0x02) != 0;

				if (part_type != GeometryType::POINT) {
					throw InvalidInputException("Expected POINT in MULTIPOINT but got %d", static_cast<int>(part_type));
				}

				if ((has_z != part_has_z) || (has_m != part_has_m)) {
					throw InvalidInputException("Geometry has inconsistent Z/M dimensions in MULTIPOINT, starting at position %zu", reader.GetPosition());
				}
				if (part_idx > 0) {
					writer.Write(',');
				}
				double vert[4] = {0, 0, 0, 0};
				auto all_nan = true;
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					vert[d_idx] = reader.Read<double>();
					all_nan &= std::isnan(vert[d_idx]);
				}
				if (all_nan) {
					writer.Write("EMPTY");
					continue;
				}
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					if (d_idx > 0) {
						writer.Write(' ');
					}
					writer.Write(vert[d_idx]);
				}
			}
			writer.Write(')');

		} break;
		case GeometryType::MULTILINESTRING: {
			writer.Write("MULTILINESTRING");
			writer.Write(flag_str);
			const auto part_count = reader.Read<uint32_t>();
			if (part_count == 0) {
				writer.Write("EMPTY");
				return;
			}
			writer.Write('(');
			for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
				const auto part_byte_order = reader.Read<uint8_t>();
				if (part_byte_order != 1) {
					throw InvalidInputException("Unsupported byte order %d in WKB", part_byte_order);
				}
				const auto part_meta = reader.Read<uint32_t>();
				const auto part_type = static_cast<GeometryType>(part_meta % 1000);
				const auto part_flag = part_meta / 1000;
				const auto part_has_z = (part_flag & 0x01) != 0;
				const auto part_has_m = (part_flag & 0x02) != 0;

				if (part_type != GeometryType::LINESTRING) {
					throw InvalidInputException("Expected LINESTRING in MULTILINESTRING but got %d", static_cast<int>(part_type));
				}
				if ((has_z != part_has_z) || (has_m != part_has_m)) {
					throw InvalidInputException("Geometry has inconsistent Z/M dimensions in MULTILINESTRING, starting at position %zu", reader.GetPosition());
				}
				if (part_idx > 0) {
					writer.Write(',');
				}
				const auto vert_count = reader.Read<uint32_t>();
				if (vert_count == 0) {
					writer.Write("EMPTY");
					continue;
				}
				writer.Write('(');
				for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
					if (vert_idx > 0) {
						writer.Write(',');
					}
					for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
						if (d_idx > 0) {
							writer.Write(' ');
						}
						auto value = reader.Read<double>();
						writer.Write(value);
					}
				}
				writer.Write(')');
			}
			writer.Write(')');
		}
		break;
		case GeometryType::MULTIPOLYGON: {
			writer.Write("MULTIPOLYGON");
			writer.Write(flag_str);
			const auto part_count = reader.Read<uint32_t>();
			if (part_count == 0) {
				writer.Write("EMPTY");
				return;
			}
			writer.Write('(');
			for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
				if (part_idx > 0) {
					writer.Write(',');
				}

				const auto part_byte_order = reader.Read<uint8_t>();
				if (part_byte_order != 1) {
					throw InvalidInputException("Unsupported byte order %d in WKB", part_byte_order);
				}
				const auto part_meta = reader.Read<uint32_t>();
				const auto part_type = static_cast<GeometryType>(part_meta % 1000);
				const auto part_flag = part_meta / 1000;
				const auto part_has_z = (part_flag & 0x01) != 0;
				const auto part_has_m = (part_flag & 0x02) != 0;
				if (part_type != GeometryType::POLYGON) {
					throw InvalidInputException("Expected POLYGON in MULTIPOLYGON but got %d", static_cast<int>(part_type));
				}
				if ((has_z != part_has_z) || (has_m != part_has_m)) {
					throw InvalidInputException("Geometry has inconsistent Z/M dimensions in MULTIPOLYGON, starting at position %zu", reader.GetPosition());
				}

				const auto ring_count = reader.Read<uint32_t>();
				if (ring_count == 0) {
					writer.Write("EMPTY");
					continue;
				}
				writer.Write('(');
				for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
					if (ring_idx > 0) {
						writer.Write(',');
					}
					const auto vert_count = reader.Read<uint32_t>();
					if (vert_count == 0) {
						writer.Write("EMPTY");
						continue;
					}
					writer.Write('(');
					for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
						if (vert_idx > 0) {
							writer.Write(',');
						}
						for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
							if (d_idx > 0) {
								writer.Write(' ');
							}
							auto value = reader.Read<double>();
							writer.Write(value);
						}
					}
					writer.Write(')');
				}
				writer.Write(')');
			}
			writer.Write(')');
		} break;
		case GeometryType::GEOMETRYCOLLECTION: {
			writer.Write("GEOMETRYCOLLECTION");
			writer.Write(flag_str);
			const auto part_count = reader.Read<uint32_t>();
			if (part_count == 0) {
				writer.Write("EMPTY");
				return;
			}
			writer.Write('(');
			for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
				if (part_idx > 0) {
					writer.Write(',');
				}
				// Recursively parse the geometry inside the collection
				ToStringRecursive(reader, writer, depth + 1, has_z, has_m);
			}
			writer.Write(')');
		} break;
		default:
			throw InvalidInputException("Unsupported geometry type %d in WKB", static_cast<int>(type));
	}
}

string_t Geometry::ToString(Vector &result, const char *buf, idx_t len) {
	TextWriter writer;
	BinaryReader reader(buf, len);

	ToStringRecursive(reader, writer, 0, false, false);

	// Convert the buffer to string_t
	const auto &buffer = writer.GetBuffer();
	return StringVector::AddString(result, buffer.data(), buffer.size());
}

bool Geometry::FromWKB(const string_t &wkb_blob, string_t &result, Vector &result_vector, bool strict) {
	// TODO: Implement this/Verify/Rewrite to little endian
	result = StringVector::AddStringOrBlob(result_vector, wkb_blob);
	return true;
}

string_t Geometry::ToWKB(const string_t &geom, Vector &result) {
	// TODO: Implement this/Verify/Rewrite to little endian
	return StringVector::AddStringOrBlob(result, geom);
}


static idx_t GetBoundsRecursive(BinaryReader &reader, Box2D &result, uint32_t depth) {
	if (depth == Geometry::MAX_RECURSION_DEPTH) {
		throw InvalidInputException("Geometry exceeds maximum recursion depth of %d", Geometry::MAX_RECURSION_DEPTH);
	}

	const auto byte_order = reader.Read<uint8_t>();
	if (byte_order != 1) {
		throw InvalidInputException("Unsupported byte order %d in WKB", byte_order);
	}
	const auto meta = reader.Read<uint32_t>();
	const auto type = static_cast<GeometryType>(meta % 1000);
	const auto flag = meta / 1000;
	const auto has_z = (flag & 0x01) != 0;
	const auto has_m = (flag & 0x02) != 0;

	const uint32_t dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);

	switch (type) {
		case GeometryType::POINT: {
			double vert[4] = {0, 0, 0, 0};
			auto all_nan = true;
			for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
				vert[d_idx] = reader.Read<double>();
				all_nan &= std::isnan(vert[d_idx]);
			}
			if (all_nan) { // Empty point
				return 0;
			}
			result.Extend({vert[0], vert[1], vert[0], vert[1]});
			return 1; // One vertex
		} break;
		case GeometryType::LINESTRING: {
			const auto vert_count = reader.Read<uint32_t>();
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				double vert[4] = {0, 0, 0, 0};
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					vert[d_idx] = reader.Read<double>();
				}
				result.Extend({vert[0], vert[1], vert[0], vert[1]});
			}
			return vert_count;
		} break;
		case GeometryType::POLYGON: {
			uint32_t total_vert_count = 0;
			const auto ring_count = reader.Read<uint32_t>();
			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				const auto vert_count = reader.Read<uint32_t>();
				if (vert_count == 0) { // Empty ring
					continue;
				}
				for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
					double vert[4] = {0, 0, 0, 0};
					for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
						vert[d_idx] = reader.Read<double>();
					}
					result.Extend({vert[0], vert[1], vert[0], vert[1]});
				}
				total_vert_count += vert_count;
			}
			return total_vert_count; // Return total vertex count from all rings
		} break;
		case GeometryType::MULTIPOINT:
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON:
		case GeometryType::GEOMETRYCOLLECTION: {
			uint32_t total_vert_count = 0;
			const auto part_count = reader.Read<uint32_t>();
			for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
				total_vert_count += GetBoundsRecursive(reader, result, depth + 1);
			}
			return total_vert_count; // Return total vertex count from all parts
		}
		default:
			throw InvalidInputException("Unsupported geometry type %d in WKB", static_cast<int>(type));
		}
}

idx_t Geometry::GetBounds(const string_t &geom, Box2D &result) {
	BinaryReader reader(geom.GetData(), geom.GetSize());
	return GetBoundsRecursive(reader, result, 0);
}

void Geometry::Verify(const string_t &blob) {

}

/*

enum class VertexType : uint8_t { XY = 0, XYZ = 1, XYM = 2, XYZM = 3 };

struct VertexXY {
	static constexpr VertexType TYPE = VertexType::XY;
	static constexpr bool HAS_Z = false;
	static constexpr bool HAS_M = false;

	double x;
	double y;

	bool AllNan() const {
		return std::isnan(x) && std::isnan(y);
	}
};

struct VertexXYZ {
	static constexpr VertexType TYPE = VertexType::XYZ;
	static constexpr bool HAS_Z = true;
	static constexpr bool HAS_M = false;

	double x;
	double y;
	double z;

	bool AllNan() const {
		return std::isnan(x) && std::isnan(y) && std::isnan(z);
	}
};

struct VertexXYM {
	static constexpr VertexType TYPE = VertexType::XYM;
	static constexpr bool HAS_Z = false;
	static constexpr bool HAS_M = true;

	double x;
	double y;
	double m;

	bool AllNan() const {
		return std::isnan(x) && std::isnan(y) && std::isnan(m);
	}
};

struct VertexXYZM {
	static constexpr VertexType TYPE = VertexType::XYZM;
	static constexpr bool HAS_Z = true;
	static constexpr bool HAS_M = true;

	double x;
	double y;
	double z;
	double m;

	bool AllNan() const {
		return std::isnan(x) && std::isnan(y) && std::isnan(z) && std::isnan(m);
	}
};

class Visitor {
public:
	virtual ~Visitor() = default;

	virtual void Visit(const string_t &blob) {
		BinaryReader reader(blob.GetData(), blob.GetSize());
		OnStart();
		Visit(reader, 0, 0);
		OnDone();
	}

	virtual void OnStart() {};
	virtual void OnDone() {};

	virtual void OnVertex(const VertexXY &vertex, uint32_t index) = 0;
	virtual void OnVertex(const VertexXYZ &vertex, uint32_t index) = 0;
	virtual void OnVertex(const VertexXYM &vertex, uint32_t index) = 0;
	virtual void OnVertex(const VertexXYZM &vertex, uint32_t index) = 0;

	virtual void OnPointEnter(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnPointLeave(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnLineStringEnter(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnLineStringLeave(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnRingEnter(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnRingLeave(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnPolygonEnter(uint32_t ring_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnPolygonLeave(uint32_t ring_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnMultiPointEnter(uint32_t point_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnMultiPointLeave(uint32_t point_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnMultiLineStringEnter(uint32_t linestring_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnMultiLineStringLeave(uint32_t linestring_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnMultiPolygonEnter(uint32_t polygon_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnMultiPolygonLeave(uint32_t polygon_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnGeometryCollectionEnter(uint32_t geometry_count, uint32_t index, uint32_t depth, VertexType type) {}
	virtual void OnGeometryCollectionLeave(uint32_t geometry_count, uint32_t index, uint32_t depth, VertexType type) {}

private:
	template<bool LE, class V>
	void VisitInternal(BinaryReader &reader, GeometryType type, uint32_t index, uint32_t depth) {
		switch (type) {
		case GeometryType::POINT: {
			V vertex = reader.Read<V, LE>();
			if (vertex.AllNan()) {
				OnPointEnter(0, index, depth, V::TYPE);
				OnPointLeave(0, index, depth, V::TYPE);
			} else {
				OnPointEnter(1, index, depth, V::TYPE);
				OnVertex(vertex, 0);
				OnPointLeave(1, index, depth, V::TYPE);
			}
		} break;
		case GeometryType::LINESTRING: {
			const auto vert_count = reader.Read<uint32_t, LE>();
			OnLineStringEnter(vert_count, index, depth, V::TYPE);
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				V vertex = reader.Read<V, LE>();
				OnVertex(vertex, vert_idx);
			}
			OnLineStringLeave(vert_count, index, depth, V::TYPE);
		} break;
		case GeometryType::POLYGON: {
			const auto ring_count = reader.Read<uint32_t, LE>();
			OnPolygonEnter(ring_count, index, depth, V::TYPE);
			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				const auto vert_count = reader.Read<uint32_t, LE>();
				OnRingEnter(vert_count, ring_idx, depth, V::TYPE);
				for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
					V vertex = reader.Read<V, LE>();
					OnVertex(vertex, vert_idx);
				}
				OnRingLeave(vert_count, ring_idx, depth, V::TYPE);
			}
			OnPolygonLeave(ring_count, index, depth, V::TYPE);
		} break;
		case GeometryType::MULTIPOINT: {
			const auto part_count = reader.Read<uint32_t, LE>();
			OnMultiPointEnter(part_count, index, depth, V::TYPE);
			for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
				Visit(reader, part_idx, depth + 1);
			}
			OnMultiPointLeave(part_count, index, depth, V::TYPE);
		} break;
		case GeometryType::MULTILINESTRING: {
			const auto part_count = reader.Read<uint32_t, LE>();
			OnMultiLineStringEnter(part_count, index, depth, V::TYPE);
			for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
				Visit(reader, part_idx, depth + 1);
			}
			OnMultiLineStringLeave(part_count, index, depth, V::TYPE);
		} break;
		case GeometryType::MULTIPOLYGON: {
			const auto part_count = reader.Read<uint32_t, LE>();
			OnMultiPolygonEnter(part_count, index, depth, V::TYPE);
			for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
				Visit(reader, part_idx, depth + 1);
			}
			OnMultiPolygonLeave(part_count, index, depth, V::TYPE);
		} break;
		case GeometryType::GEOMETRYCOLLECTION: {
			const auto part_count = reader.Read<uint32_t, LE>();
			OnGeometryCollectionEnter(part_count, index, depth, V::TYPE);
			for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
				Visit(reader, part_idx, depth + 1);
			}
			OnGeometryCollectionLeave(part_count, index, depth, V::TYPE);
		} break;
		default:
			throw InvalidInputException("Unsupported geometry type %d in WKB", static_cast<int>(type));
		}
	}

	template<bool LE>
	void VisitRecursive(BinaryReader &reader, uint32_t index, uint32_t depth) {
		const auto meta = reader.Read<uint32_t, LE>();
		const auto type = static_cast<GeometryType>(meta % 1000);
		const auto flag = meta / 100;
		const auto has_z = (flag & 0x01) != 0;
		const auto has_m = (flag & 0x02) != 0;

		if (has_z && has_m) {
			VisitInternal<LE, VertexXYZM>(reader, type, index, depth);
		} else if (has_z) {
			VisitInternal<LE, VertexXYZ>(reader, type, index, depth);
		} else if (has_m) {
			VisitInternal<LE, VertexXYM>(reader, type, index, depth);
		} else {
			VisitInternal<LE, VertexXY>(reader, type, index, depth);
		}
	}

	void Visit(BinaryReader &reader, uint32_t index, uint32_t depth) {
		const auto le = reader.Read<uint8_t>();
		if (le) {
			VisitRecursive<true>(reader, index, depth);
		} else {
			VisitRecursive<false>(reader, index, depth);
		}
	}
};


class BoundVisitor final : public Visitor {
private:
	Box2D bounds;
public:
	const Box2D GetBounds() const {
		return bounds;
	}
protected:
	void OnStart() override {
		bounds = Box2D::Empty();
	}
	void OnVertex(const VertexXY &vertex, uint32_t index) override {
		bounds.Extend({vertex.x, vertex.y, vertex.x, vertex.y});
	}
	void OnVertex(const VertexXYZ &vertex, uint32_t index) override {
		bounds.Extend({vertex.x, vertex.y, vertex.x, vertex.y});
	}
	void OnVertex(const VertexXYM &vertex, uint32_t index) override {
		bounds.Extend({vertex.x, vertex.y, vertex.x, vertex.y});
	}
	void OnVertex(const VertexXYZM &vertex, uint32_t index) override {
		bounds.Extend({vertex.x, vertex.y, vertex.x, vertex.y});
	}
};

class PrintVisitor final : public Visitor {
public:
	void OnVertex(const VertexXY &vertex, uint32_t index) override {
		if (index > 0) { Write(", "); }
		Write("{:G} {:G}", vertex.x, vertex.y);
	}
	void OnVertex(const VertexXYZ &vertex, uint32_t index) override {
		if (index > 0) { Write(", "); }
		Write("{:G} {:G} {:G}", vertex.x, vertex.y, vertex.z);
	}
	void OnVertex(const VertexXYM &vertex, uint32_t index) override {
		if (index > 0) { Write(", "); }
		Write("{:G} {:G} {:G}", vertex.x, vertex.y, vertex.m);
	}
	void OnVertex(const VertexXYZM &vertex, uint32_t index) override {
		if (index > 0) { Write(", "); }
		Write("{:G} {:G} {:G} {:G}", vertex.x, vertex.y, vertex.z, vertex.m);
	}
	void OnPointEnter(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) override {
		Write("POINT");
		Write(type);
		if (vertex_count == 0) {
			Write("EMPTY");
		} else {
			Write('(');
		}
	}
	void OnPointLeave(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) override {
		if (vertex_count > 0) {
			Write(')');
		}
	}
	void OnLineStringEnter(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) override {
		Write("LINESTRING");
		Write(type);
		if (vertex_count == 0) {
			Write("EMPTY");
		} else {
			Write('(');
		}
	}
	void OnLineStringLeave(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) override {
		if (vertex_count > 0) {
			Write(')');
		}
	}
	void OnRingEnter(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) override {
		Write('(');
		if (vertex_count == 0) {
			Write("EMPTY");
		} else {
			Write('(');
		}
	}
	void OnRingLeave(uint32_t vertex_count, uint32_t index, uint32_t depth, VertexType type) override {
		if (vertex_count > 0) {
			Write(')');
		}
		Write(')');
	}
	void OnPolygonEnter(uint32_t ring_count, uint32_t index, uint32_t depth, VertexType type) override {
		Write("POLYGON");
		Write(type);
		if (ring_count == 0) {
			Write("EMPTY");
		} else {
			Write('(');
		}
	}
	void OnPolygonLeave(uint32_t ring_count, uint32_t index, uint32_t depth, VertexType type) override {
		if (ring_count > 0) {
			Write(')');
		}
	}
	void OnMultiPointEnter(uint32_t point_count, uint32_t index, uint32_t depth, VertexType type) override {
		Write("MULTIPOINT");
		Write(type);
		if (point_count == 0) {
			Write("EMPTY");
		} else {
			Write('(');
		}
	}
	void OnMultiPointLeave(uint32_t point_count, uint32_t index, uint32_t depth, VertexType type) override {
		if (point_count > 0) {
			Write(')');
		}
	}
	void OnMultiLineStringEnter(uint32_t linestring_count, uint32_t index, uint32_t depth, VertexType type) override {
		Write("MULTILINESTRING");
		Write(type);
		if (linestring_count == 0) {
			Write("EMPTY");
		} else {
			Write('(');
		}
	}
	void OnMultiLineStringLeave(uint32_t linestring_count, uint32_t index, uint32_t depth, VertexType type) override {
		if (linestring_count > 0) {
			Write(')');
		}
	}
	void OnMultiPolygonEnter(uint32_t polygon_count, uint32_t index, uint32_t depth, VertexType type) override {
		Write("MULTIPOLYGON");
		Write(type);
		if (polygon_count == 0) {
			Write("EMPTY");
		} else {
			Write('(');
		}
	}
	void OnMultiPolygonLeave(uint32_t polygon_count, uint32_t index, uint32_t depth, VertexType type) override {
		if (polygon_count > 0) {
			Write(')');
		}
	}
	void OnGeometryCollectionEnter(uint32_t geometry_count, uint32_t index, uint32_t depth, VertexType type) override {
		Write("GEOMETRYCOLLECTION");
		Write(type);
		if (geometry_count == 0) {
			Write("EMPTY");
		} else {
			Write('(');
		}
	}
	void OnGeometryCollectionLeave(uint32_t geometry_count, uint32_t index, uint32_t depth, VertexType type) override {
		if (geometry_count > 0) {
			Write(')');
		}
	}
private:
	void Write(VertexType type) {
		switch (type) {
			case VertexType::XYZ: Write(" Z "); break;
			case VertexType::XYM: Write(" M "); break;
			case VertexType::XYZM: Write("ZM"); break;
			default: Write(" "); break;
		}
	}
	void Write(const char* str) {
		buffer.insert(buffer.end(), str, str + strlen(str));
	}
	void Write(char c) {
		buffer.push_back(c);
	}
	template<class ARG, class ...ARGS>
	void Write(const char *str, ARG arg, ARGS... args) {
		duckdb_fmt::format_to(std::back_inserter(buffer), str, arg, args...);
	}

	vector<char> buffer;
};

static void foo(const string_t &str) {
	BoundVisitor visitor;
	visitor.Visit(str);
	auto &bounds = visitor.GetBounds();
}

struct GeomTags {
	struct Any {};

	struct Simple : Any { } ;
	struct Complex : Any { };

	struct Point : Simple { };
	struct LineString : Simple { };
	struct Ring : Simple { };

	struct Polygon : Complex { };
	struct Multi : Complex { };

	struct MultiPoint : Multi { };
	struct MultiLineString : Multi { };
	struct MultiPolygon : Multi { };
	struct GeometryCollection : Multi { };
};

template<class IMPL>
class GeomVisitor {
public:
	void Run() {
		Enter<GeomTags::Point>();
		Enter<GeomTags::LineString>();
		Leave<GeomTags::Polygon>();
	}
protected:

private:
	template<class T>
	void Enter() {
		static_cast<IMPL*>(this)->OnEnter(T{});
	}
	template<class T>
	void Leave() {
		static_cast<IMPL*>(this)->OnLeave(T{});
	}
	template<class V>
	void Vertex(const V &v) {
		static_cast<IMPL*>(this)->OnVertex(v);
	}
};

class BoundsVisitor : public GeomVisitor<BoundsVisitor> {
	friend class GeomVisitor<BoundsVisitor>;

	Box2D box;

	void OnEnter(GeomTags::Any) { }
	void OnLeave(GeomTags::Any) { }

	template<class V>
	void OnVertex(const V &v) {
		box.Extend({v.x, v.y, v.x, v.y});
	}
};

class TextVisitor : public GeomVisitor<TextVisitor> {
	friend class GeomVisitor<TextVisitor>;

	void OnVertex(const VertexXY &v) {
		Write("{:G} {:G}", v.x, v.y);
	}
	void OnVertex(const VertexXYZ &v) {
		Write("{:G} {:G} {:G}", v.x, v.y, v.z);
	}
	void OnVertex(const VertexXYM &v) {
		Write("{:G} {:G} {:G}", v.x, v.y, v.m);
	}
	void OnVertex(const VertexXYZM &v) {
		Write("{:G} {:G} {:G} {:G}", v.x, v.y, v.z, v.m);
	}
	void OnEnter(GeomTags::Point) {
		Write("POINT");
	}
	void OnEnter(GeomTags::LineString) {
		Write("LINESTRING");
	}
	void OnEnter(GeomTags::Ring) {
		Write('(');
	}
	void OnEnter(GeomTags::Polygon) {
		Write("POLYGON");
	}
	void OnEnter(GeomTags::MultiPoint) {
		Write("MULTIPOINT");
	}
	void OnEnter(GeomTags::MultiLineString) {
		Write("MULTILINESTRING");
	}
	void OnEnter(GeomTags::MultiPolygon) {
		Write("MULTIPOLYGON");
	}
	void OnEnter(GeomTags::GeometryCollection) {
		Write("GEOMETRYCOLLECTION");
	}
	void OnLeave(GeomTags::Any) {
		Write(')');
	}
};

static void bar() {
	BoundsVisitor visitor;
	visitor.Run();
}
*/
//
//
// static void foo(GeometryWriter &writer) {
//
// 	auto poly = writer.AddPolygon();
// 	{
// 		auto ring = poly.AddRing();
// 		{
// 			ring.AddVertex(1.0, 2.0);
// 			ring.Finish();
// 		}
// 	}
//
// 	writer.AddPolygon([&](PolygonWriter &writer) {
// 		writer.AddRing([&](RingWriter &writer) {
//
// 		});
// 	});
//
// 	writer.AddMultiPoint([&](MultiPointWriter &writer) {
// 		writer.AddPoint([&](PointWriter &writer) {
// 			writer.Flush();
// 		});
// 	});
//
// 	writer.AddGeometryCollection([&](CollectionWriter &writer) {
// 		writer.AddMultiPolygon([&](CollectionWriter &writer) {
// 			writer.AddPolygon([&](PolygonWriter &writer) {
// 				writer.AddRing([&](RingWriter &writer) {
//
// 				});
// 			});
// 		});
// 	});
// }
//
//
// class WKBWriter {
// public:
// 	void Begin(VertexType vertex_type_p) {
// 		vertex_type = vertex_type_p;
// 		buffer.clear();
//
// 	}
// 	void Begin(bool has_z, bool has_m) {
// 		if (has_z && has_m) {
// 			vertex_type = VertexType::XYZM;
// 		} else if (has_z) {
// 			vertex_type = VertexType::XYZ;
// 		} else if (has_m) {
// 			vertex_type = VertexType::XYM;
// 		} else {
// 			vertex_type = VertexType::XY;
// 		}
// 		buffer.clear();
// 	}
//
// 	bool HasZ() const {
// 		return vertex_type == VertexType::XYZ || vertex_type == VertexType::XYZM;
// 	}
// 	bool HasM() const {
// 		return vertex_type == VertexType::XYM || vertex_type == VertexType::XYZM;
// 	}
//
// 	void AddPoint() {
// 		WriteMeta(GeometryType::POINT, vertex_type);
// 	}
//
// 	void AddLine(uint32_t vertex_count) {
// 		WriteMeta(GeometryType::LINESTRING, vertex_type);
// 		Write<uint32_t>(vertex_count);
// 	}
//
// 	void AddRing(uint32_t vertex_count) {
// 		Write<uint32_t>(vertex_count);
// 	}
//
// 	void AddPolygon(uint32_t part_count) {
// 		WriteMeta(GeometryType::POLYGON, vertex_type);
// 		Write<uint32_t>(part_count);
// 	}
//
// 	void AddMultiPoint(uint32_t part_count) {
// 		WriteMeta(GeometryType::MULTIPOINT, vertex_type);
// 		Write<uint32_t>(part_count);
// 	}
//
// 	void AddMultiLineString(uint32_t part_count) {
// 		WriteMeta(GeometryType::MULTILINESTRING, vertex_type);
// 		Write<uint32_t>(part_count);
// 	}
//
// 	void AddMultiPolygon(uint32_t part_count) {
// 		WriteMeta(GeometryType::MULTIPOLYGON, vertex_type);
// 		Write<uint32_t>(part_count);
// 	}
//
// 	void AddGeometryCollection(uint32_t part_count) {
// 		WriteMeta(GeometryType::GEOMETRYCOLLECTION, vertex_type);
// 		Write<uint32_t>(part_count);
// 	}
//
// 	string_t Finalize(Vector &result) {
// 		return StringVector::AddStringOrBlob(result, buffer.data(), buffer.size());
// 	}
//
// private:
// 	template<class T>
// 	void Write(const T &value) {
// 		char data[sizeof(T)];
// 		memcpy(data, &value, sizeof(T));
// 		buffer.insert(buffer.end(), data, data + sizeof(T));
// 	}
//
// 	void WriteMeta(GeometryType part_type, VertexType vert_type) {
// 		Write<uint8_t>(1); // LE Byte Order
// 		uint32_t type = static_cast<uint32_t>(part_type);
// 		switch (vert_type) {
// 			case VertexType::XYZ: type += 1000; break; // XYZ
// 			case VertexType::XYM: type += 2000; break; // XYM
// 			case VertexType::XYZM: type += 3000; break; // XYZM
// 			default: break; // XY
// 		}
// 		Write<uint32_t>(type);
// 	}
//
// 	VertexType vertex_type;
// 	vector<char> buffer;
// };
//
//
// class WKBVisitor {
//
// };
//
// class TextReader {
// public:
// 	TextReader(const char *data, size_t size)
// 	    : beg(data), pos(data), end(data + size) {
// 		D_ASSERT(beg != nullptr);
// 	}
//
// 	bool TryMatch(const char *str) {
// 		auto cur = pos;
// 		while (*str != '\0') {
// 			if (cur >= end || *cur != *str) {
// 				return false;
// 			}
// 			cur++;
// 			str++;
// 		}
// 		pos = cur;
//
// 		SkipWhitespace();
//
// 		return true;
// 	}
//
// 	bool TryMatch(char c) {
// 		if (pos < end && *pos == c) {
// 			pos++;
//
// 			SkipWhitespace();
//
// 			return true;
// 		}
// 		return false;
// 	}
//
// 	bool Match(char c) {
// 		if (!TryMatch(c)) {
// 			throw InvalidInputException("Expected character '%c' not found in WKT string!", c);
// 		}
// 	}
//
// 	bool Match(const char *str) {
// 		if (!TryMatch(str)) {
// 			throw InvalidInputException("Expected string '%s' not found in WKT string!", str);
// 		}
// 		return true;
// 	}
//
// 	double ParseDouble() {
// 		if (pos >= end) {
// 			throw InvalidInputException("Unexpected end of WKT string while parsing double value!");
// 		}
// 		const char *start = pos;
// 		while (pos < end && (*pos == '.' || (*pos >= '0' && *pos <= '9') || *pos == '-' || *pos == '+')) {
// 			pos++;
// 		}
// 		if (start == pos) {
// 			throw InvalidInputException("Failed to parse double value in WKT string!");
// 		}
//
// 		SkipWhitespace();
//
// 		return strtod(start, nullptr);
// 	}
//
// 	void SkipWhitespace() {
// 		while (pos < end && (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r')) {
// 			pos++;
// 		}
// 	}
//
// private:
// 	const char *beg;
// 	const char *pos;
// 	const char *end;
// };
//
//
// //----------------------------------------------------------------------------------------------------------------------
// // FromString
// //----------------------------------------------------------------------------------------------------------------------
//
// static void FromStringInternal(TextReader &reader, WKBWriter &writer, uint32_t depth) {
//
// 	reader.SkipWhitespace();
//
// 	GeometryType type = GeometryType::INVALID;
// 	if (reader.TryMatch("POINT")) {
// 		type = GeometryType::POINT;
// 	} else if (reader.TryMatch("LINESTRING")) {
// 		type = GeometryType::LINESTRING;
// 	} else if (reader.TryMatch("POLYGON")) {
// 		type = GeometryType::POLYGON;
// 	} else if (reader.TryMatch("MULTIPOINT")) {
// 		type = GeometryType::MULTIPOINT;
// 	} else if (reader.TryMatch("MULTILINESTRING")) {
// 		type = GeometryType::MULTILINESTRING;
// 	} else if (reader.TryMatch("MULTIPOLYGON")) {
// 		type = GeometryType::MULTIPOLYGON;
// 	} else if (reader.TryMatch("GEOMETRYCOLLECTION")) {
// 		type = GeometryType::GEOMETRYCOLLECTION;
// 	} else {
// 		throw InvalidInputException("Invalid geometry type in WKT string!");
// 	}
//
// 	const auto has_z = reader.TryMatch('Z');
// 	const auto has_m = reader.TryMatch('M');
// 	const auto n_dim = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);
//
// 	auto is_empty = false;
// 	if (reader.TryMatch("EMPTY")) {
// 		is_empty = true;
// 		reader.SkipWhitespace();
// 	}
//
// 	if (depth == 0) {
// 		writer.Begin(has_z, has_m);
// 	} else {
// 		if (has_z != writer.HasZ() || has_m != writer.HasM()) {
// 			throw InvalidInputException("Inconsistent Z/M flags in WKT string at depth %u!", depth);
// 		}
// 	}
//
// 	switch (type) {
// 	case GeometryType::POINT: {
// 		writer.AddPoint([&] {
// 			if (!is_empty) {
// 				for (uint32_t i = 0; i < n_dim; i++) {
// 					writer.AddVertex();
// 				}
// 			}
// 		});
// 		writer.AddPoint();
// 		if (is_empty) {
// 			for (int i = 0; i < n_dim; i++) {
// 				writer.AddVertexData(std::numeric_limits<double>::quiet_NaN());
// 			}
// 		}
// 		else {
// 			reader.Match('(');
// 			for (int i = 0; i < n_dim; i++) {
// 				auto value = reader.ParseDouble();
// 				writer.AddVertexData(value);
// 			}
// 			reader.Match(')');
// 		}
// 	} break;
// 	case GeometryType::LINESTRING: {
// 		writer.AddLine([&] {
// 			writer.AddVertex();
// 			writer.AddVertex();
// 		});
//
// 	} break;
// 	}
// }
//
// bool Geometry::FromString(const string &str, string_t &result, Vector &result_vector, bool strict) {
//
// 	TextReader reader(str.data(), str.size());
// 	WKBWriter writer;
//
// }
//







}


//----------------------------------------------------------------------------------------------------------------------
// WKB Writer
//----------------------------------------------------------------------------------------------------------------------

/*

void WKBWriter::AddPoint() {
	Write<uint8_t>(0x01);
	Write<uint32_t>(1); // type 1;
}

static void Handle() {
	WKBWriter writer;

	writer.Begin(VertexType::XY);

	uint32_t count = 5;

	writer.AddLine(count);
	for (uint32_t i = 0; i < count; i++) {
		writer.AddVertex(i * 1.0, i * 2.0);
	}
}



//----------------------------------------------------------------------------------------------------------------------
// Binary Reader
//----------------------------------------------------------------------------------------------------------------------
class BinaryReader {
public:
	BinaryReader(const string_t &blob) : beg(blob.GetData()), pos(beg), end(beg + blob.GetSize()) {
	}

	template <typename T, bool LE = true>
	T Read() {
		CheckSize(sizeof(T));

		T value;
		if (LE) {	// Little endian
			memcpy(&value, pos, sizeof(T));
		} else {
			char temp[sizeof(T)];
			for (size_t i = 0; i < sizeof(T); i++) {
				temp[i] = pos[sizeof(T) - 1 - i];
			}
			memcpy(&value, temp, sizeof(T));
		}
		pos += sizeof(T);
		return value;
	}

	template<class T>
	void Copy(T* target, size_t count) {
		CheckSize(sizeof(T) * count);
		memcpy(target, pos, sizeof(T) * count);
		pos += sizeof(T) * count;
	}

	void Skip(size_t size) {
		CheckSize(size);
		pos += size;
	}

	const char* Reserve(size_t size) {
		CheckSize(size);
		const char* reserved = pos;
		pos += size;
		return reserved;
	}

	void Reset() {
		pos = beg;
	}

private:
	void CheckSize(size_t size) {
		if (pos + size > end) {
			throw InvalidInputException("Invalid WKB blob: not enough data to read!");
		}
	}
	const char* beg;
	const char* pos;
	const char* end;
};

class BinaryWriter {
public:
	BinaryWriter(char *data, size_t size) : beg(data), pos(data), end(data + size) {
		if (size == 0) {
			throw InvalidInputException("Cannot create BinaryWriter with zero size buffer!");
		}
	}

	template<class T>
	void Write(const T &value) {
		CheckSize(sizeof(T));
		memcpy(pos, &value, sizeof(T));
		pos += sizeof(T);
	}

	void Copy(const char* data, size_t size) {
		CheckSize(size);
		memcpy(pos, data, size);
		pos += size;
	}

private:
	void CheckSize(size_t size) {
		if (pos + size > end) {
			throw InvalidInputException("Invalid WKB blob: not enough data to read!");
		}
	}

	char* beg;
	char* pos;
	char* end;
};


//----------------------------------------------------------------------------------------------------------------------
// BKB Geometry Part Structure
//----------------------------------------------------------------------------------------------------------------------

struct GeometryPart {
	uint8_t kind; // 0 = WKB-BE, 1 = WKB-LE, 2 = BKB
	uint8_t type;
	uint8_t flag;
	uint8_t padd;
	uint32_t size;

	bool HasZ() const { return (flag & 0x01) != 0; }
	bool HasM() const { return (flag & 0x02) != 0; }
};

//----------------------------------------------------------------------------------------------------------------------
// Geometry Class Implementation
//----------------------------------------------------------------------------------------------------------------------
bool Geometry::FromString(const string &str, string_t &result, Vector &result_vector, bool strict) {
	return false;
}

class TextWriter {
public:
	void Write(char c) {
		buffer.push_back(c);
	}

	void Write(const char *str) {
		const auto len = strlen(str);
		buffer.insert(buffer.end(), str, str + len);
	}

	void Write(double value) {
		auto str = duckdb_fmt::format("{}", value);
		buffer.insert(buffer.end(), str.begin(), str.end());
	}

private:
	vector<char> buffer;
};

static void ToStringRecursive(BinaryReader &reader, TextWriter &writer, uint32_t depth, GeometryType parent) {

	const auto part = reader.Read<GeometryPart>();

	if (parent == GeometryType::INVALID || parent == GeometryType::GEOMETRYCOLLECTION) {
		switch (part.type) {
			case GeometryType::POINT: writer.Write("POINT"); break;
			case GeometryType::LINESTRING: writer.Write("LINESTRING"); break;
			case GeometryType::POLYGON: writer.Write("POLYGON"); break;
			case GeometryType::MULTIPOINT: writer.Write("MULTIPOINT"); break;
			case GeometryType::MULTILINESTRING: writer.Write("MULTILINESTRING"); break;
			case GeometryType::MULTIPOLYGON: writer.Write("MULTIPOLYGON"); break;
			case GeometryType::GEOMETRYCOLLECTION: writer.Write("GEOMETRYCOLLECTION"); break;
		}

		if (part.HasZ() && part.HasM()) {
			writer.Write(" ZM");
		}
		else if (part.HasZ()) {
			writer.Write(" Z");
		} else if (part.HasM()) {
			writer.Write(" M");
		}
	}

	if (part.size == 0) {
		writer.Write(" EMPTY");
		return;
	}

	const auto type = static_cast<GeometryType>(part.type);
	switch (type) {
		case GeometryType::POINT: {

		} break;
		case GeometryType::LINESTRING: {

		} break;
		case GeometryType::POLYGON: {
			writer.Write(" (");
			for (uint32_t i = 0; i < part.size; i++) {
				if (i > 0) {
					writer.Write(", ");
				}
				ToStringRecursive(reader, writer, depth + 1, type);
			}
			writer.Write(")");
		} break;
		case GeometryType::MULTIPOINT: {

		}
		case GeometryType::MULTILINESTRING: {
			writer.Write(" (");
			for (uint32_t i = 0; i < part.size; i++) {
				if (i > 0) {
					writer.Write(", ");
				}
				ToStringRecursive(reader, writer, depth + 1, type);
			}
			writer.Write(")");
		} break;
	}
}

string_t Geometry::ToString(Vector &result, const char *buf, idx_t len) {
	return string_t(buf, len);
}

//----------------------------------------------------------------------------------------------------------------------
// From WKB
//----------------------------------------------------------------------------------------------------------------------

static uint32_t GetRequiredSizeFromWKBRecursive(BinaryReader &reader);

template<bool LE>
static uint32_t GetRequiredSizeFromWKBInternal(BinaryReader &reader) {
	uint32_t result = 0;

	auto info = reader.Read<uint32_t, LE>();

	const auto type = static_cast<GeometryType>((info & 0xffff) % 1000);
	const auto flags = (info & 0xffff) / 1000;
	const auto has_z = (flags == 1) || (flags == 3) || ((info & 0x80000000) != 0);
	const auto has_m = (flags == 2) || (flags == 3) || ((info & 0x40000000) != 0);
	const auto has_srid = (info & 0x20000000) != 0;

	if (has_srid) {
		reader.Skip(sizeof(uint32_t)); // Skip SRID
	}

	const auto width = sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0));
	switch (type) {
		case GeometryType::POINT: {
			result += sizeof(uint32_t) + sizeof(uint32_t);
			double vertex[4] = {0, 0, 0, 0};
			bool all_nan = true;
			for (size_t i = 0; i < width / sizeof(double); i++) {
				vertex[i] = reader.Read<double, LE>();
				all_nan &= std::isnan(vertex[i]);
			}
			if (!all_nan) {
				result += width;
			}
		} break;
		case GeometryType::LINESTRING: {
			// type + count
			result += sizeof(uint32_t) + sizeof(uint32_t);

			const auto vertex_count = reader.Read<uint32_t, LE>();
			result += width * vertex_count;
		} break;
		case GeometryType::POLYGON: {
			// type + count
			result += sizeof(uint32_t) + sizeof(uint32_t);

			const auto count = reader.Read<uint32_t, LE>();
			for (uint32_t ring_idx = 0; ring_idx < count; ring_idx++) {
				result += sizeof(uint32_t) + sizeof(uint32_t);
				const auto vertex_count = reader.Read<uint32_t, LE>();
				result += width * vertex_count;
				reader.Skip(vertex_count * width);
			}
		} break;
		case GeometryType::MULTIPOINT:
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON:
		case GeometryType::GEOMETRYCOLLECTION: {
			// type + count
			result += sizeof(uint32_t) + sizeof(uint32_t);

			const auto count = reader.Read<uint32_t, LE>();
			for (uint32_t part_idx = 0; part_idx < count; part_idx++) {
				result += GetRequiredSizeFromWKBRecursive(reader);
			}
		} break;
		default:
			throw InvalidInputException("Unknown geometry type encountered in WKB blob!");
	}
}

static uint32_t GetRequiredSizeFromWKBRecursive(BinaryReader &reader) {
	switch (reader.Read<uint8_t>()) {
		case 0: return GetRequiredSizeFromWKBInternal<false>(reader);
		case 1: return GetRequiredSizeFromWKBInternal<true>(reader);
		default:
			throw InvalidInputException("Invalid WKB byte order encountered in WKB blob!");
	}
}

static void FromWKBRecursive(BinaryReader &reader, BinaryWriter &writer);

template<bool LE>
static void FromWKBInternal(BinaryReader &reader, BinaryWriter &writer) {

	const auto info = reader.Read<uint32_t, LE>();
	const auto type = static_cast<GeometryType>((info & 0xffff) % 1000);
	const auto flags = (info & 0xffff) / 1000;
	const auto has_z = (flags == 1) || (flags == 3) || ((info & 0x80000000) != 0);
	const auto has_m = (flags == 2) || (flags == 3) || ((info & 0x40000000) != 0);
	const auto has_srid = (info & 0x20000000) != 0;

	if (has_srid) {
		reader.Skip(sizeof(uint32_t)); // Skip SRID
	}

	const auto width = sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0));

	GeometryPart part;
	part.kind = 2;
	part.type = static_cast<uint8_t>(type);
	part.flag = (has_m ? 0x02 : 0) | (has_z ? 0x01 : 0);
	part.padd = 0;

	switch (type) {
		case GeometryType::POINT: {
			double vertex[4] = {0, 0, 0, 0};
			bool all_nan = true;
			for (size_t i = 0; i < width / sizeof(double); i++) {
				vertex[i] = reader.Read<double, LE>();
				all_nan &= std::isnan(vertex[i]);
			}
			if (all_nan) {
				part.size = 0;
				writer.Write(part);
				return;
			}
			part.size = 1;
			writer.Write(part);
			writer.Write(vertex);

		} break;
		case GeometryType::LINESTRING: {
			const auto vert_count = reader.Read<uint32_t, LE>();
			part.size = vert_count;
			writer.Write(part);
			if (vert_count == 0) {
				// Empty linestring, no vertices
				return;
			}
			auto vert_data = reader.Reserve(width * vert_count);
			writer.Copy(vert_data, width * vert_count);

		} break;
		case GeometryType::POLYGON: {
			const auto part_count = reader.Read<uint32_t, LE>();
			part.size = part_count;

			writer.Write(part);

			for (uint32_t ring_idx = 0; ring_idx < part_count; ring_idx++) {
				GeometryPart ring_part;
				ring_part.kind = 2;
				ring_part.type = static_cast<uint8_t>(GeometryType::LINESTRING);
				ring_part.flag = part.flag;
				ring_part.padd = 0;

				const auto vert_count = reader.Read<uint32_t, LE>();
				ring_part.size = vert_count;

				writer.Write(ring_part);
				if (vert_count == 0) {
					// Empty ring, no vertices
					continue;
				}
				auto vert_data = reader.Reserve(width * vert_count);
				writer.Copy(vert_data, width * vert_count);
			}
		} break;
		case GeometryType::MULTIPOINT:
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON:
		case GeometryType::GEOMETRYCOLLECTION: {
			const auto part_count = reader.Read<uint32_t, LE>();

			part.size = part_count;
			writer.Write(part);
			if (part_count == 0) {
				// Empty collection, no parts
				return;
			}

			for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
				FromWKBRecursive(reader, writer);
			}
		} break;
		default:
			throw InvalidInputException("Unknown geometry type encountered in WKB blob!");
		break;
	}
}

static void FromWKBRecursive(BinaryReader &reader, BinaryWriter &writer) {
	switch (reader.Read<uint8_t>()) {
		case 0: FromWKBInternal<false>(reader, writer); break;
		case 1: FromWKBInternal<true>(reader, writer); break;
		default:
			throw InvalidInputException("Invalid WKB byte order encountered in WKB blob!");
	}
}

bool Geometry::FromWKB(const string_t &wkb_blob, string_t &result, Vector &result_vector, bool strict) {

	BinaryReader reader(wkb_blob);
	auto required_size = GetRequiredSizeFromWKBRecursive(reader);

	auto geom = StringVector::EmptyString(result_vector, required_size);

	reader.Reset();
	BinaryWriter writer(geom.GetDataWriteable(), required_size);

	FromWKBRecursive(reader, writer);

	return true;
}

//----------------------------------------------------------------------------------------------------------------------
// To WKB
//----------------------------------------------------------------------------------------------------------------------
static uint32_t GetRequiredSizeToWKB(BinaryReader &reader) {
	GeometryPart stack[Geometry::MAX_RECURSION_DEPTH];
	idx_t depth = 0;

	uint32_t required_size = 0;

	while (true) {
		stack[depth] = reader.Read<GeometryPart>();
		auto &part = stack[depth];

		const auto width = sizeof(double) * (2 + ((part.flag & 0x01) ? 1 : 0) + ((part.flag & 0x02) ? 1 : 0));
		const auto count = part.size;

		switch (static_cast<GeometryType>(part.type)) {
		case GeometryType::POINT:
			// byte order + type + vertex
			required_size += sizeof(uint8_t) + sizeof(uint32_t) + width;
			reader.Skip(width * count);
			break;
		case GeometryType::LINESTRING:
			// byte order + type + size + vertices
			required_size += sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t) + (width * count);
			reader.Skip(width * count);
			break;
		case GeometryType::POLYGON:
			// byte order + type + count
			required_size += sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
			for (uint32_t part_idx = 0; part_idx < count; part_idx++) {
				const auto ring = reader.Read<GeometryPart>();
				// count + vertices
				required_size += sizeof(uint32_t) + (width * ring.size);
				reader.Skip(width * ring.size);
			}
			break;
		case GeometryType::MULTIPOINT:
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON:
		case GeometryType::GEOMETRYCOLLECTION: {
			// byte order + type + size
			required_size += sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
			if (part.size == 0) {
				// Empty collection, no parts
				break;
			}
			if (depth >= Geometry::MAX_RECURSION_DEPTH - 1) {
				throw InvalidInputException("WKB blob is too deep, maximum recursion depth exceeded!");
			}
			depth++;
		} continue;
		default:
			throw InvalidInputException("Unknown geometry type encountered!");
		}

		while (true) {
			if (depth == 0) {
				return required_size;
			}
			stack[depth - 1].size--;
			if (stack[depth - 1].size > 0) {
				// More parts remaining
				break;
			}
			depth--;
		}
	}
}

static void WriteWKB(BinaryReader &reader, BinaryWriter &writer) {

	GeometryPart stack[Geometry::MAX_RECURSION_DEPTH];
	idx_t depth = 0;

	while (true) {
		stack[depth] = reader.Read<GeometryPart>();
		auto &part = stack[depth];

		const auto width = sizeof(double) * (2 + ((part.flag & 0x01) ? 1 : 0) + ((part.flag & 0x02) ? 1 : 0));
		const auto count = part.size;
		const auto type = static_cast<GeometryType>(part.type);

		writer.Write<uint8_t>(1);
		writer.Write<uint32_t>(static_cast<uint32_t>(type));

		switch (type) {
			case GeometryType::POINT: {
				double data[4] = { std::numeric_limits<double>::quiet_NaN(),
				                   std::numeric_limits<double>::quiet_NaN(),
				                   std::numeric_limits<double>::quiet_NaN(),
				                   std::numeric_limits<double>::quiet_NaN() }; // x, y, z, m}
				if (count != 0) {
					reader.Copy(&data[0], width);
				}
				writer.Copy(reinterpret_cast<char*>(&data[0]), width);
			} break;
			case GeometryType::LINESTRING: {
				writer.Write<uint32_t>(count);
				auto vertex_data = reader.Reserve(width * count);
				writer.Copy(vertex_data, width * count);
			} break;
			case GeometryType::POLYGON: {
				writer.Write<uint32_t>(count);
				for (idx_t i = 0; i < count; i++) {
					auto ring_part = reader.Read<GeometryPart>();
					writer.Write<uint32_t>(ring_part.size);
					if (ring_part.size == 0) {
						continue; // Empty ring, no vertices
					}
					auto vertex_data = reader.Reserve(width * ring_part.size);
					writer.Copy(vertex_data, ring_part.size);
				}
			} break;
			case GeometryType::MULTIPOINT:
			case GeometryType::MULTILINESTRING:
			case GeometryType::MULTIPOLYGON:
			case GeometryType::GEOMETRYCOLLECTION: {
				writer.Write<uint32_t>(count);
				if (count == 0) {
					break;
				}
				if (depth >= Geometry::MAX_RECURSION_DEPTH - 1) {
					throw InvalidInputException("WKB blob is too deep, maximum recursion depth exceeded!");
				}
				depth++;
			} continue;
			default:
				throw InvalidInputException("Unknown geometry type encountered!");
		}

		while (true) {
			if (depth == 0) {
				return; // Finished writing the WKB blob
			}
			stack[depth - 1].size--;
			if (stack[depth - 1].size > 0) {
				// More parts remaining
				break;
			}
			depth--;
		}
	}
}

string_t Geometry::ToWKB(const string_t &geom, Vector &result) {
	BinaryReader reader(geom);
	uint32_t required_size = GetRequiredSizeToWKB(reader);

	auto blob = StringVector::EmptyString(result, required_size);

	reader.Reset();
	BinaryWriter writer(blob.GetDataWriteable(), blob.GetSize());
	WriteWKB(reader, writer);

	blob.Finalize();
	return blob;
}

//----------------------------------------------------------------------------------------------------------------------
// Bounds
//----------------------------------------------------------------------------------------------------------------------
idx_t Geometry::GetBounds(const string_t &blob, Box2D &result) {
	BinaryReader reader(blob);

	GeometryPart stack[Geometry::MAX_RECURSION_DEPTH];
	idx_t depth = 0;
	idx_t total_vertex_count = 0;

	while (true) {
		stack[depth] = reader.Read<GeometryPart>();
		auto &part = stack[depth];

		switch (static_cast<GeometryType>(part.type)) {
		case GeometryType::POINT:
		case GeometryType::LINESTRING: {
			const auto width = 2 + ((part.flag & 0x01 ? 1 : 0)) + (part.flag & 0x02 ? 1 : 0);
			const auto count = part.size;
			for (idx_t vert_idx = 0; vert_idx < count; vert_idx++) {
				double data[4] = {0, 0, 0, 0}; // x, y, z, m
				reader.Copy(data, width);
				result.min_x = MinValue(result.min_x, data[0]);
				result.min_y = MinValue(result.min_y, data[1]);
				result.max_x = MaxValue(result.max_x, data[0]);
				result.max_y = MaxValue(result.max_y, data[1]);
			}
			total_vertex_count += count;
		} break;
		case GeometryType::POLYGON:
		case GeometryType::MULTIPOINT:
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON:
		case GeometryType::GEOMETRYCOLLECTION: {
			if (part.size == 0) {
				break;
			}
			if (depth >= Geometry::MAX_RECURSION_DEPTH - 1) {
				throw InvalidInputException("WKB blob is too deep, maximum recursion depth exceeded!");
			}
			depth++;
		} continue;
		default:
			throw InvalidInputException("Unknown geometry type encountered!");
		}

		while (true) {
			if (depth == 0) {
				return total_vertex_count;
			}
			stack[depth - 1].size--;
			if (stack[depth - 1].size > 0) {
				// More parts remaining
				break;
			}
			depth--;
		}
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Verify
//----------------------------------------------------------------------------------------------------------------------

void Geometry::Verify(const string_t &blob) {

}

} // namespace duckdb








// Verify that this is a valid little-endian WKB blob
/*
enum class GeometryVerificationResult {
	OK = 0,
	IS_BIG_ENDIAN = 1,
	IS_EWKB = 2,
	IS_NOT_ISO = 3,
	IS_MALFORMED = 4,
	IS_MIXED_ZM = 5,
	IS_TOO_DEEP = 6,
};

static GeometryVerificationResult VerfiyRecursive(BinaryReader &reader, uint32_t depth, uint32_t parent_type, uint32_t parent_flag) {
	if (depth > Geometry::MAX_RECURSION_DEPTH) {
		// Too deep recursion, probably a malformed WKB
		return GeometryVerificationResult::IS_TOO_DEEP;
	}

	const auto byte_order = reader.Read<uint8_t>();
	if (byte_order != 1) {
		// WKB is not little-endian!
		return GeometryVerificationResult::IS_BIG_ENDIAN;
	}

	const auto header = reader.Read<uint32_t>();
	const auto is_ewkb = ((header & 0x80000000) != 0) || ((header & 0x40000000) != 0) || ((header & 0x20000000) == 0);
	if (is_ewkb) {
		// WKB is "Extended Well-Known Binary" (EWKB)
		return GeometryVerificationResult::IS_EWKB;
	}

	// Extract flags and type from the header
	const auto wkb_type = ((header & 0xffff) % 1000);
	const auto wkb_flag = (header & 0xffff) / 1000;

	if (wkb_flag > 4) {
		// WKB flag is not valid, not ISO WKB!
		return GeometryVerificationResult::IS_NOT_ISO;
	}

	if (parent_type == 0) {
		// This is the root geometry
		parent_flag = wkb_flag;
	} else {
		if (wkb_flag != parent_flag) {
			// This geometry has different flags than its parent!
			return GeometryVerificationResult::IS_MIXED_ZM;
		}
	}

	if (parent_type == 4 && wkb_type != 1) {
		// In a MULTIPOINT, the only valid type is POINT!
		return GeometryVerificationResult::IS_MALFORMED;
	}
	if (parent_type == 5 && wkb_type != 2) {
		// In a MULTILINESTRING, the only valid type is LINESTRING!
		return GeometryVerificationResult::IS_MALFORMED;
	}
	if (parent_type == 6 && wkb_type != 3) {
		// In a MULTIPOLYGON, the only valid type is POLYGON!
		return GeometryVerificationResult::IS_MALFORMED;
	}

	const auto has_z = (wkb_flag & 0x01) != 0;
	const auto has_m = (wkb_flag & 0x02) != 0;
	const auto width = sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0));

	switch (wkb_type) {
		case 1: {	// POINT
			reader.Skip(width);
		} break;
		case 2: {	// LINESTRING
			const auto count = reader.Read<uint32_t>();
			reader.Skip(count * width);
		} break;
		case 3: {	// POLYGON
			const auto ring_count = reader.Read<uint32_t>();
			for (uint32_t i = 0; i < ring_count; i++) {
				const auto vert_count = reader.Read<uint32_t>();
				reader.Skip(vert_count * width);
			}
		} break;
		case 4:		// MULTIPOINT
		case 5:		// MULTILINESTRING
		case 6:		// MULTIPOLYGON
		case 7: {	// GEOMETRYCOLLECTION
			auto count = reader.Read<uint32_t>();
			for (uint32_t i = 0; i < count; i++) {
				auto res = VerfiyRecursive(reader, depth + 1, wkb_type, wkb_flag);
				if (res != GeometryVerificationResult::OK) {
					return res;
				}
			}
			return GeometryVerificationResult::OK;
		} break;
		default:
			// Unsupported geometry type, not ISO WKB!
			return GeometryVerificationResult::IS_MALFORMED;
	}
}

static GeometryVerificationResult VerifyWKB(const string_t &blob) {
	BinaryReader reader(blob);
	return VerfiyRecursive(reader, 0, 0, 0);
}

void Geometry::Verify(const string_t &blob) {
	switch (VerifyWKB(blob)) {
		case GeometryVerificationResult::OK:
			return; // Valid WKB
		case GeometryVerificationResult::IS_BIG_ENDIAN:
			throw ConversionException("Invalid WKB: expected little-endian byte order");
		case GeometryVerificationResult::IS_EWKB:
			throw ConversionException("Invalid WKB: Extended Well-Known Binary (EWKB) is not supported");
		case GeometryVerificationResult::IS_NOT_ISO:
			throw ConversionException("Invalid WKB: not ISO WKB");
		case GeometryVerificationResult::IS_MALFORMED:
			throw ConversionException("Invalid WKB: malformed geometry data");
		case GeometryVerificationResult::IS_MIXED_ZM:
			throw ConversionException("Invalid WKB: mixed Z and M flags in geometry data");
		case GeometryVerificationResult::IS_TOO_DEEP:
			throw ConversionException("Invalid WKB: too deep recursion in geometry data");
		default:
			throw InternalException("Unknown verification result for WKB");
	}
}

//----------------------------------------------------------------------------------------------------------------------
// FromString
//----------------------------------------------------------------------------------------------------------------------
bool Geometry::FromString(const string &str, string_t &result, Vector &result_vector, bool strict) {
	double x;
	double y;

	if (sscanf(str.c_str(), "POINT (%lf %lf)", &x, &y) != 2) {
		if (strict) {
			throw ConversionException("Invalid POINT format: " + str);
		}
		return false;
	}

	result = StringVector::EmptyString(result_vector, sizeof(double) * 2);

	auto data = result.GetDataWriteable();
	memcpy(data, &x, sizeof(double));
	memcpy(data + sizeof(double), &y, sizeof(double));
	result.Finalize();

	return true;
}

//----------------------------------------------------------------------------------------------------------------------
// ToString
//----------------------------------------------------------------------------------------------------------------------
struct GeometryStack {
	struct Entry {
		GeometryType type;
		bool has_z;
		bool has_m;
		uint32_t count;
	} stack[Geometry::MAX_RECURSION_DEPTH + 1];
	uint32_t depth = 0;

	bool IsEmpty();
	Entry &Peek();
	void Pop();
	void Push(GeometryType type, bool has_z, bool has_m, uint32_t count);

};

static string_t ToString(Vector &result, const string_t &geom) {
	vector<char> buffer;
	BinaryReader reader(geom);

	GeometryStack stack;

	const auto byte_le = reader.Read<uint8_t>();
	if (byte_le != 1) {
		throw ConversionException("Invalid WKB: expected little-endian byte order");
	}
	const auto meta = reader.Read<uint32_t>();
	const auto type = static_cast<GeometryType>((meta & 0xffff) % 1000);
	const auto flag = (meta & 0xffff) / 1000;
	const auto hasz = flag & 0x1;
	const auto hasm = flag & 0x2;

	const auto width = (2 + (hasz ? 1 : 0) + (hasm ? 1 : 0));

	switch (type) {
		case GeometryType::POINT: {
			double data[4];
			bool all_nan = true;
			for (int dim = 0; dim < width; dim++) {
				data[dim] = reader.Read<double>();
				all_nan &= std::isnan(data[dim]);
			}
			if (all_nan) {

			}
		} break;
		case GeometryType::LINESTRING: {
			const auto vert_count = reader.Read<uint32_t>();
			if (vert_count == 0) {

			}
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				for (uint32_t dim = 0; dim < width; dim++) {
					auto v = reader.Read<double>();
				}
			}
		} break;
		case GeometryType::POLYGON: {
			const auto ring_count = reader.Read<uint32_t>();
			if (ring_count == 0) {

			}
			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				const auto vert_count = reader.Read<uint32_t>();
				for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
					auto v = reader.Read<double>();
				}
			}
		} break;
		case GeometryType::MULTIPOINT: {
			const auto part_count = reader.Read<uint32_t>();
			if (part_count == 0) {

			}
			for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
				double data[4];
				bool all_nan = true;
				for (uint32_t dim = 0; dim < width; dim++) {
					data[dim] = reader.Read<double>();
					all_nan &= std::isnan(data[dim]);
				}
				if (all_nan) {
					// Handle NaN case
				}
			}
		} break;
		case GeometryType::MULTILINESTRING: {
			const auto part_count = reader.Read<uint32_t>();
			if (part_count == 0) {
				const auto part_meta = reader.Read<uint32_t>();
				const auto part_type = static_cast<GeometryType>((part_meta & 0xffff) % 1000);
				const auto part_flag = (part_meta & 0xffff) / 1000;
				const auto part_has_z = part_flag & 0x1;
				const auto part_has_m = part_flag & 0x2;

				if (part_has_z != hasz || part_has_m != hasm) {
					throw ConversionException("Mismatched Z/M flags in MULTILINESTRING parts");
				}
				if (part_type != GeometryType::LINESTRING) {
					throw ConversionException("Invalid part type in MULTILINESTRING: expected LINESTRING");
				}

				const auto vert_count = reader.Read<uint32_t>();
				if (vert_count == 0) {
					// Handle empty part
				}
				for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
					for (uint32_t dim = 0; dim < width; dim++) {
						auto v = reader.Read<double>();
					}
				}
			}
		} break;
		case GeometryType::MULTIPOLYGON: {
			const auto part_count = reader.Read<uint32_t>();
			if (part_count == 0) {

			}
		} break;
		case GeometryType::GEOMETRYCOLLECTION: {
			const auto part_count = reader.Read<uint32_t>();
			if (part_count == 0) {

			}
		} break;
		default:
			throw ConversionException("Unsupported geometry type in WKB: " + to_string(static_cast<uint32_t>(type)));
	}

	// Return the string
	return StringVector::AddString(result, buffer.data(), buffer.size());
}




string_t Geometry::ToString(Vector &result, const char *buf, idx_t len) {
	double x;
	double y;
	if (len != sizeof(double) * 2) {
		throw ConversionException("Invalid geometry data length: " + to_string(len));
	}
	memcpy(&x, buf, sizeof(double));
	memcpy(&y, buf + sizeof(double), sizeof(double));

	return StringVector::AddString(result, StringUtil::Format("POINT (%g %g)", x, y));
}

//----------------------------------------------------------------------------------------------------------------------
// Bounds
//----------------------------------------------------------------------------------------------------------------------
idx_t Geometry::GetBounds(const string_t &blob, Box2D &result) {
	auto len = blob.GetSize();
	auto buf = blob.GetData();

	double x;
	double y;
	if (len != sizeof(double) * 2) {
		throw ConversionException("Invalid geometry data length: " + to_string(len));
	}
	memcpy(&x, buf, sizeof(double));
	memcpy(&y, buf + sizeof(double), sizeof(double));

	result.min_x = x;
	result.min_y = y;
	result.max_x = x;
	result.max_y = y;

	return 1;
}*/
