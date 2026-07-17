#pragma once

#include "ext.h"

//===--------------------------------------------------------------------===//
// Function pointer struct
//===--------------------------------------------------------------------===//
typedef struct {
	// v1.0.0
	int32_t (*ext_open)(const char *path, ext_db *out_db);
	void (*ext_close)(ext_db db);
	const char *(*ext_version)(void);
	// Flush support

	int32_t (*ext_flush)(ext_db db);
	// Kind support

	ext_kind (*ext_get_kind)(ext_db db);
	void (*ext_extra_one)(ext_db db);
	int32_t (*ext_extra_two)(void);
} ext_api;

//===--------------------------------------------------------------------===//
// Struct Create Method
//===--------------------------------------------------------------------===//
inline ext_api CreateExtAPI(void) {
	ext_api result;
	result.ext_open = ext_open;
	result.ext_close = ext_close;
	result.ext_version = ext_version;
	result.ext_flush = ext_flush;
	result.ext_get_kind = ext_get_kind;
	result.ext_extra_one = ext_extra_one;
	result.ext_extra_two = ext_extra_two;
	return result;
}

#define EXT_API_VERSION_MAJOR 1
#define EXT_API_VERSION_MINOR 0
#define EXT_API_VERSION_PATCH 0
#define EXT_API_VERSION_STRING "v1.0.0"
