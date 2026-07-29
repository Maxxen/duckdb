#include "capi_v2_internal.hpp"

#include "duckdb/main/capi_v2/extension_init_v2.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

bool InvokeCExtensionInitV2(duckdb_v2_extension_init_fn init_fun, duckdb_v2_extension_handle extension,
                            duckdb_v2_extension_get_api_fn get_api, DatabaseInstance &db, string &error_message) {
	// A context of the load's own, opened on the database being loaded into. Deliberately not a Connection: this is
	// not a user connection and has no business showing up in duckdb_connections().
	auto context = make_shared_ptr<ClientContext>(db.shared_from_this());

	ErrorInfoV2 err {};
	auto err_ptr = reinterpret_cast<duckdb_v2_error_info_handle>(&err);

	duckdb_v2_extension_input input;
	input.extension = extension;
	input.context = reinterpret_cast<duckdb_v2_context_handle>(context.get());
	input.err = &err_ptr;
	input.get_api = get_api;

	try {
		// Run the entrypoint under a transaction, as connection_create_extension does for the in-memory path: the
		// context-scoped calls an extension reaches for at load time (logical_type_create_from_*,
		// value_cast_with_context) assume one is open. Throwing back out on a reported error rolls it back instead of
		// committing a partial load.
		context->RunFunctionInTransaction([&]() {
			(*init_fun)(&input);
			if (err.HasError()) {
				// The class is not carried across: the caller re-wraps this with the extension name either way
				throw InvalidInputException(err.message);
			}
		});
	} catch (std::exception &) {
		error_message = err.message.empty() ? "the extension reported a failure without describing it" : err.message;
		return false;
	}

	// The slot is the entrypoint's error channel. A failed get_api handshake leaves it untouched and is instead
	// recorded on the load state, which the caller inspects.
	return true;
}

} // namespace duckdb
