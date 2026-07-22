#include "capi_v2_internal.hpp"

#include "duckdb/parser/qualified_name.hpp"

#include <cstdlib>
#include <cstring>

namespace duckdb {

namespace {

QualifiedName *ToQualifiedName(duckdb_v2_qname_handle qname) {
	return reinterpret_cast<QualifiedName *>(qname);
}

// The handle invariant: one to three parts, none empty.
void CheckQualifiedNameParts(const QualifiedName &qname, const char *where) {
	auto &path = qname.Path();
	if (path.empty() || path.size() > 3) {
		throw InvalidInputException("a qualified name must have between one and three parts in %s", where);
	}
	for (auto &part : path) {
		if (part.empty()) {
			throw InvalidInputException("a qualified name part must be non-empty in %s", where);
		}
	}
}

} // namespace

} // namespace duckdb

DUCKDB_V2_ERROR duckdb_v2_qname_parse(duckdb_v2_str text, duckdb_v2_qname_handle *out_qname,
                                      duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if ((!text.ptr && text.len > 0) || !out_qname) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_qname_parse");
		}
		*out_qname = nullptr;
		auto parsed = duckdb::QualifiedName::Parse(duckdb::ToString(text));
		duckdb::CheckQualifiedNameParts(parsed, "duckdb_v2_qname_parse");
		*out_qname = reinterpret_cast<duckdb_v2_qname_handle>(new duckdb::QualifiedName(std::move(parsed)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_qname_create(const duckdb_v2_identifier_t *parts, idx_t part_count,
                                       duckdb_v2_qname_handle *out_qname, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if ((!parts && part_count > 0) || !out_qname) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_qname_create");
		}
		*out_qname = nullptr;
		if (part_count == 0 || part_count > 3) {
			throw duckdb::InvalidInputException("part_count must be between one and three in duckdb_v2_qname_create");
		}
		duckdb::vector<duckdb::Identifier> schema_path;
		for (idx_t i = 0; i < part_count; i++) {
			if (!parts[i].ptr || parts[i].len == 0) {
				throw duckdb::InvalidInputException(
				    "a qualified name part must be a non-empty string view in duckdb_v2_qname_create");
			}
			if (i + 1 < part_count) {
				schema_path.push_back(duckdb::ToIdentifier(parts[i]));
			}
		}
		auto name = duckdb::ToIdentifier(parts[part_count - 1]);
		*out_qname = reinterpret_cast<duckdb_v2_qname_handle>(
		    new duckdb::QualifiedName(std::move(schema_path), std::move(name)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_qname_get_part_count(duckdb_v2_qname_handle qname, idx_t *out_count,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!qname || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_qname_get_part_count");
		}
		*out_count = duckdb::ToQualifiedName(qname)->Path().size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_qname_get_part(duckdb_v2_qname_handle qname, idx_t index, duckdb_v2_identifier_t *out_part,
                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!qname || !out_part) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_qname_get_part");
		}
		auto &path = duckdb::ToQualifiedName(qname)->Path();
		if (index >= path.size()) {
			throw duckdb::InvalidInputException("part index %llu out of range for a qualified name with %llu parts",
			                                    index, path.size());
		}
		*out_part = duckdb::ToStr(path[index]);
	});
}

DUCKDB_V2_ERROR duckdb_v2_qname_render(duckdb_v2_qname_handle qname, char **out_text,
                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!qname || !out_text) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_qname_render");
		}
		*out_text = nullptr;
		auto rendered = duckdb::ToQualifiedName(qname)->ToString();
		auto *buf = static_cast<char *>(std::malloc(rendered.size() + 1));
		if (!buf) {
			throw duckdb::OutOfMemoryException("malloc failed in duckdb_v2_qname_render");
		}
		std::memcpy(buf, rendered.data(), rendered.size());
		buf[rendered.size()] = '\0';
		*out_text = buf;
	});
}

DUCKDB_V2_ERROR duckdb_v2_qname_equals(duckdb_v2_qname_handle left, duckdb_v2_qname_handle right, bool *result,
                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!left || !right || !result) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_qname_equals");
		}
		*result = (*duckdb::ToQualifiedName(left) == *duckdb::ToQualifiedName(right));
	});
}

DUCKDB_V2_ERROR duckdb_v2_qname_hash(duckdb_v2_qname_handle qname, uint64_t *out_hash,
                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!qname || !out_hash) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_qname_hash");
		}
		*out_hash = duckdb::ToQualifiedName(qname)->Hash();
	});
}

DUCKDB_V2_ERROR duckdb_v2_qname_destroy(duckdb_v2_qname_handle *qname) {
	if (!qname || !*qname) {
		return static_cast<DUCKDB_V2_ERROR>(DUCKDB_V2_ERROR_NONE);
	}
	delete duckdb::ToQualifiedName(*qname);
	*qname = nullptr;
	return static_cast<DUCKDB_V2_ERROR>(DUCKDB_V2_ERROR_NONE);
}
