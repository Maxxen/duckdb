#include "duckdb_extension.h"

// V2 DuckDB extension API
#include "duckdb_cpp.hpp"

using namespace duckdb_api;

static void RegisterFunction(const Context &context) {
	ScalarFunction function(context);
	function.SetName("add_two")
	    .SetSignature(FunctionSignature::Create()
	                      .AddParameter("input", LogicalType::INTEGER())
	                      .SetReturnType(LogicalType::INTEGER()))
	    .SetExecCallback([](ScalarFunction::ExecInput &input) {
		    auto in_vec = input.GetInputChunk().GetVector(0);
		    auto out_vec = input.GetResultVector();

		    in_vec.Flatten();

		    const auto in = in_vec.GetDataMutable<const int32_t>();
		    auto out = out_vec.GetDataMutable<int32_t>();

		    for (idx_t i = 0; i < input.GetInputChunk().GetRowCount(); i++) {
			    out[i] = in[i] + 2;
		    }
	    })
	    .Register(context);
}

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, duckdb_extension_access *access) {
	// Register a demo function

	// TODO: Add proper V1ToV2 conversion function
	auto conn = Connection::FromOpaque(connection);

	try {
		conn.WithTransaction([](const Context &ctx) { RegisterFunction(ctx); });
	} catch (const std::exception &ex) {
		access->set_error(info, ex.what());
		return false;
	}

	// Return true to indicate successful initialization
	return true;
}
