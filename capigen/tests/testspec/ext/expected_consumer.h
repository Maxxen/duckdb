#pragma once

#include "ext.h"

typedef struct {
#if EXT_API_VERSION_MINOR > 0 || (EXT_API_VERSION_MINOR == 0 && EXT_API_VERSION_PATCH >= 0) // v1.0.0
	int32_t (*ext_open)(const char *path, ext_db *out_db);
	void (*ext_close)(ext_db db);
	const char *(*ext_version)(void);
#endif

// Flush support
#ifdef EXT_API_UNSTABLE
	int32_t (*ext_flush)(ext_db db);
#endif

// Kind support
#ifdef EXT_API_UNSTABLE
	ext_kind (*ext_get_kind)(ext_db db);
#endif

	// capigen:begin appended
#ifdef EXT_API_UNSTABLE
	void (*ext_extra_one)(ext_db db);
	int32_t (*ext_extra_two)(void);
#endif
	// capigen:end appended
} ext_api;

#ifndef EXT_BUILD_STATIC
#define ext_open     ext_api.ext_open
#define ext_close    ext_api.ext_close
#define ext_version  ext_api.ext_version
#define ext_flush    ext_api.ext_flush
#define ext_get_kind ext_api.ext_get_kind

// capigen:begin appended
#define ext_extra_one ext_api.ext_extra_one
#define ext_extra_two ext_api.ext_extra_two
// capigen:end appended
#endif // EXT_BUILD_STATIC
