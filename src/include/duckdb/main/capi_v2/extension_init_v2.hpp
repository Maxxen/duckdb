//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi_v2/extension_init_v2.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/main/capi_v2/duckdb_v2_engine.hpp"

namespace duckdb {
class DatabaseInstance;

//! Invokes a V2 C API extension entrypoint. Opens the context the entrypoint reads and runs queries through, hands it
//! a stack-allocated error slot, and tears both down on return. Returns false and fills error_message when the
//! entrypoint left an error on the slot.
bool InvokeCExtensionInitV2(duckdb_v2_extension_init_fn init_fun, duckdb_v2_extension_handle extension,
                            duckdb_v2_extension_get_api_fn get_api, DatabaseInstance &db, string &error_message);

} // namespace duckdb
