// V2 DuckDB extension API
#include "duckdb_cpp.hpp"

using namespace duckdb_api;

DUCKDB_CPP_EXTENSION_ENTRYPOINT(extension, context) {
	// Resolve the signature types through the load's own context, the way an extension that needs the catalog to name
	// a type would. Equivalent to LogicalType::INTEGER() here; the point is that the context is live and transacted.
	auto int_type = context.ParseType("INTEGER");

	// Register a demo function under the extension being loaded
	ScalarFunction function;
	function.SetName("add_two")
	    .SetSignature(FunctionSignature::Create().AddParameter("input", int_type).SetReturnType(int_type))
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
	    .Register(extension);
}
