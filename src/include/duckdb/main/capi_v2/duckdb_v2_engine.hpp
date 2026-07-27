#pragma once

// Engine-side include of the V2 C header. duckdb_v2.h defines the plain Arrow
// C data interface structs unless ARROW_C_DATA_INTERFACE is already set;
// include the engine's method-carrying definitions first so engine TUs keep
// them.
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb_v2.h"
