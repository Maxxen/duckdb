#include "capi_v2_internal.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"

namespace duckdb {
namespace {

class CCopyFunctionInfoV2 final : public CopyFunctionInfo {
public:
	string name;
	duckdb_v2_copy_function_bind_callback_fn bind_cb = nullptr;
	duckdb_v2_copy_function_init_callback_fn init_cb = nullptr;
	duckdb_v2_copy_function_batch_callback_fn batch_cb = nullptr;
	duckdb_v2_copy_function_flush_callback_fn flush_cb = nullptr;
	duckdb_v2_copy_function_finalize_callback_fn finalize_cb = nullptr;
	shared_ptr<OpaqueDataHandle> user_data = nullptr;
};

class CCopyFunctionBindDataV2 final : public FunctionData {
public:
	shared_ptr<OpaqueDataHandle> bind_data = nullptr;
	CCopyFunctionInfoV2 &info;

	explicit CCopyFunctionBindDataV2(CCopyFunctionInfoV2 &info) : info(info) {
	}

	auto Copy() const -> unique_ptr<FunctionData> override {
		auto copy = make_uniq<CCopyFunctionBindDataV2>(info);
		copy->bind_data = bind_data;
		return std::move(copy);
	}

	auto Equals(const FunctionData &other) const -> bool override {
		auto &other_bind_data = other.Cast<CCopyFunctionBindDataV2>();
		return bind_data->Equals(*other_bind_data.bind_data);
	}
};

class CCopyFunctionStateV2 final : public GlobalFunctionData {
public:
	OpaqueDataHandle init_data;
};

class CCopyFunctionBatchV2 final : public PreparedBatchData {
public:
	OpaqueDataHandle batch_data;
};

// --- Callback info structs (passed to user callbacks as opaque handles) ------

struct CopyFunctionBindInfoV2 {
	void *user_data = nullptr;
	// Borrowed arrays of column_count entries, valid only for the bind callback.
	const duckdb_v2_logical_type_handle *column_types = nullptr;
	const duckdb_v2_identifier_t *column_names = nullptr;
	idx_t column_count = 0;
	duckdb_v2_opaque out_bind_data = {};
};

struct CopyFunctionInitInfoV2 {
	void *user_data = nullptr;
	void *bind_data = nullptr;
	duckdb_v2_str file_path = {};
	duckdb_v2_opaque out_init_data = {};
};

struct CopyFunctionBatchInfoV2 {
	void *user_data = nullptr;
	void *bind_data = nullptr;
	void *init_data = nullptr;
	// The input collection, released from its unique_ptr; ownership passes to the callback.
	ColumnDataCollection *in_batch = nullptr;
	duckdb_v2_opaque out_batch = {};
};

struct CopyFunctionFlushInfoV2 {
	void *user_data = nullptr;
	void *bind_data = nullptr;
	void *init_data = nullptr;
	// The prepared batch data set by the batch callback.
	void *batch_data = nullptr;
};

struct CopyFunctionFinalizeInfoV2 {
	void *user_data = nullptr;
	void *bind_data = nullptr;
	void *init_data = nullptr;
};

struct CopyFunctionBuilderV2 {
	Identifier name;
	duckdb_v2_copy_function_bind_callback_fn bind_cb = nullptr;
	duckdb_v2_copy_function_init_callback_fn init_cb = nullptr;
	duckdb_v2_copy_function_batch_callback_fn batch_cb = nullptr;
	duckdb_v2_copy_function_flush_callback_fn flush_cb = nullptr;
	duckdb_v2_copy_function_finalize_callback_fn finalize_cb = nullptr;

	shared_ptr<OpaqueDataHandle> user_data = nullptr;

	static auto CopyToBind(ClientContext &context, CopyFunctionBindInput &input, const vector<Identifier> &names,
	                       const vector<LogicalType> &sql_types) -> unique_ptr<FunctionData> {
		auto &info = input.function_info->Cast<CCopyFunctionInfoV2>();

		// Setup arrays
		vector<duckdb_v2_identifier_t> names_array;
		names_array.reserve(names.size());
		for (const auto &name : names) {
			names_array.push_back(ToStr(name));
		}

		// Copy the (const) engine-owned types into a local mutable buffer so we can hand out non-const handles
		// without casting away constness. The handles point into this buffer and are only valid for the duration
		// of the bind callback.
		vector<LogicalType> types_copy(sql_types.begin(), sql_types.end());
		vector<duckdb_v2_logical_type_handle> types_array;
		types_array.reserve(types_copy.size());
		for (auto &type : types_copy) {
			types_array.push_back(reinterpret_cast<duckdb_v2_logical_type_handle>(&type));
		}

		CopyFunctionBindInfoV2 cb_info;
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.column_count = names.size();
		cb_info.column_names = names_array.data();
		cb_info.column_types = types_array.data();

		// The bind callback is optional: a copy function may not need any bind-time setup.
		if (info.bind_cb) {
			auto info_handle = reinterpret_cast<duckdb_v2_copy_function_bind_info_handle>(&cb_info);
			auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context);
			duckdb::InvokeWithErrorSlot<BinderException>(
			    [&](duckdb_v2_error_info_handle *err) { info.bind_cb(info_handle, ctx_ptr, err); });
		}

		auto result = make_uniq<CCopyFunctionBindDataV2>(info);

		// If the user set the bind data, move it out here
		if (cb_info.out_bind_data.ptr) {
			result->bind_data = make_shared_ptr<OpaqueDataHandle>(
			    cb_info.out_bind_data.ptr, cb_info.out_bind_data.destroy, cb_info.out_bind_data.equals);
		}

		return std::move(result);
	}

	static auto CopyToInit(ClientContext &context, FunctionData &bind_data, const string &file_path)
	    -> unique_ptr<GlobalFunctionData> {
		auto &data = bind_data.Cast<CCopyFunctionBindDataV2>();
		auto &info = data.info;

		CopyFunctionInitInfoV2 cb_info;
		cb_info.file_path = ToStr(file_path);
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.bind_data = data.bind_data ? data.bind_data->GetData() : nullptr;

		// The init callback is optional: when absent, the global state simply carries no init data.
		if (info.init_cb) {
			auto info_handle = reinterpret_cast<duckdb_v2_copy_function_init_info_handle>(&cb_info);
			auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context);
			duckdb::InvokeWithErrorSlot<InvalidInputException>(
			    [&](duckdb_v2_error_info_handle *err) { info.init_cb(info_handle, ctx_ptr, err); });
		}

		auto result = make_uniq<CCopyFunctionStateV2>();

		if (cb_info.out_init_data.ptr) {
			result->init_data = OpaqueDataHandle(cb_info.out_init_data.ptr, cb_info.out_init_data.destroy);
		}

		return std::move(result);
	}

	static auto CopyToBatch(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
	                        unique_ptr<ColumnDataCollection> collection) -> unique_ptr<PreparedBatchData> {
		auto &data = bind_data.Cast<CCopyFunctionBindDataV2>();
		auto &state = gstate.Cast<CCopyFunctionStateV2>();
		auto &info = data.info;

		CopyFunctionBatchInfoV2 cb_info;
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.bind_data = data.bind_data ? data.bind_data->GetData() : nullptr;
		cb_info.init_data = state.init_data.GetData();
		// Ownership of the collection is transferred to the callback: the callback (or the C++ wrapper around
		// it) is responsible for destroying it via duckdb_v2_column_data_collection_destroy. We release it from
		// the unique_ptr so it outlives this scope.
		cb_info.in_batch = collection.release();

		auto info_handle = reinterpret_cast<duckdb_v2_copy_function_batch_info_handle>(&cb_info);
		auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context);
		duckdb::InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.batch_cb(info_handle, ctx_ptr, err); });

		auto result = make_uniq<CCopyFunctionBatchV2>();

		if (cb_info.out_batch.ptr) {
			result->batch_data = OpaqueDataHandle(cb_info.out_batch.ptr, cb_info.out_batch.destroy);
		}

		return std::move(result);
	}

	static auto CopyToFlush(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
	                        PreparedBatchData &batch) -> void {
		const auto &data = bind_data.Cast<CCopyFunctionBindDataV2>();
		const auto &info = data.info;
		const auto &state = gstate.Cast<CCopyFunctionStateV2>();

		CopyFunctionFlushInfoV2 cb_info;
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.bind_data = data.bind_data ? data.bind_data->GetData() : nullptr;
		cb_info.init_data = state.init_data.GetData();
		cb_info.batch_data = batch.Cast<CCopyFunctionBatchV2>().batch_data.GetData();

		auto info_handle = reinterpret_cast<duckdb_v2_copy_function_flush_info_handle>(&cb_info);
		auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context);
		duckdb::InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.flush_cb(info_handle, ctx_ptr, err); });
	}

	static auto CopyToFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) -> void {
		const auto &data = bind_data.Cast<CCopyFunctionBindDataV2>();
		const auto &info = data.info;
		const auto &state = gstate.Cast<CCopyFunctionStateV2>();

		// Finalize is always wired on the underlying CopyFunction because the engine invokes it unconditionally
		// when finalizing a file state. The user-facing callback, however, is optional.
		if (!info.finalize_cb) {
			return;
		}

		CopyFunctionFinalizeInfoV2 cb_info;
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.bind_data = data.bind_data ? data.bind_data->GetData() : nullptr;
		cb_info.init_data = state.init_data.GetData();

		auto info_handle = reinterpret_cast<duckdb_v2_copy_function_finalize_info_handle>(&cb_info);
		auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context);
		duckdb::InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.finalize_cb(info_handle, ctx_ptr, err); });
	}
};

} // namespace
} // namespace duckdb

DUCKDB_V2_ERROR duckdb_v2_copy_function_builder_create(duckdb_v2_copy_function_builder_handle *out,
                                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out = reinterpret_cast<duckdb_v2_copy_function_builder_handle>(new duckdb::CopyFunctionBuilderV2());
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_builder_set_name(duckdb_v2_copy_function_builder_handle builder,
                                                         duckdb_v2_identifier_t name,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (!name.ptr && name.len > 0) {
			throw duckdb::InvalidInputException("Function name cannot be null.");
		}
		if (name.len == 0) {
			throw duckdb::InvalidInputException("Function name cannot be empty.");
		}

		reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(builder)->name = duckdb::ToIdentifier(name);
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_builder_set_bind_callback(duckdb_v2_copy_function_builder_handle builder,
                                                                  duckdb_v2_copy_function_bind_callback_fn callback,
                                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (!callback) {
			throw duckdb::InvalidInputException("Bind callback function pointer cannot be null.");
		}

		reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(builder)->bind_cb = callback;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_builder_set_init_callback(duckdb_v2_copy_function_builder_handle builder,
                                                                  duckdb_v2_copy_function_init_callback_fn callback,
                                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (!callback) {
			throw duckdb::InvalidInputException("Init callback function pointer cannot be null.");
		}
		reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(builder)->init_cb = callback;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_builder_set_batch_callback(duckdb_v2_copy_function_builder_handle builder,
                                                                   duckdb_v2_copy_function_batch_callback_fn callback,
                                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (!callback) {
			throw duckdb::InvalidInputException("Batch callback function pointer cannot be null.");
		}

		reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(builder)->batch_cb = callback;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_builder_set_flush_callback(duckdb_v2_copy_function_builder_handle builder,
                                                                   duckdb_v2_copy_function_flush_callback_fn callback,
                                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (!callback) {
			throw duckdb::InvalidInputException("Flush callback function pointer cannot be null.");
		}

		reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(builder)->flush_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_copy_function_builder_set_finalize_callback(duckdb_v2_copy_function_builder_handle builder,
                                                      duckdb_v2_copy_function_finalize_callback_fn callback,
                                                      duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (!callback) {
			throw duckdb::InvalidInputException("Finalize callback function pointer cannot be null.");
		}

		reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(builder)->finalize_cb = callback;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_builder_set_user_data(duckdb_v2_copy_function_builder_handle builder,
                                                              duckdb_v2_opaque data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}

		auto &builder_ref = *reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(builder);
		builder_ref.user_data = duckdb::make_shared_ptr<duckdb::OpaqueDataHandle>(data.ptr, data.destroy, data.equals);
	});
}

static duckdb::CopyFunction BuildCopyFunctionV2(duckdb::CopyFunctionBuilderV2 *builder) {
	auto &builder_ref = *builder;

	if (builder_ref.name.empty()) {
		throw duckdb::InvalidInputException("Function name cannot be empty.");
	}
	if (!builder_ref.bind_cb) {
		throw duckdb::InvalidInputException("Bind callback function cannot be null.");
	}
	if (!builder_ref.init_cb) {
		throw duckdb::InvalidInputException("Init callback function cannot be null.");
	}
	if (!builder_ref.batch_cb) {
		throw duckdb::InvalidInputException("Batch callback function cannot be null.");
	}
	if (!builder_ref.flush_cb) {
		throw duckdb::InvalidInputException("Flush callback must be set before registration.");
	}
	if (!builder_ref.finalize_cb) {
		throw duckdb::InvalidInputException("Finalize callback function cannot be null.");
	}

	duckdb::CopyFunction function(builder_ref.name);
	function.copy_to_bind = duckdb::CopyFunctionBuilderV2::CopyToBind;
	function.copy_to_initialize_global = duckdb::CopyFunctionBuilderV2::CopyToInit;
	function.prepare_batch = duckdb::CopyFunctionBuilderV2::CopyToBatch;
	function.flush_batch = duckdb::CopyFunctionBuilderV2::CopyToFlush;
	function.copy_to_finalize = duckdb::CopyFunctionBuilderV2::CopyToFinalize;

	// Setup the persistent info
	auto info = duckdb::make_shared_ptr<duckdb::CCopyFunctionInfoV2>();
	info->bind_cb = builder_ref.bind_cb;
	info->init_cb = builder_ref.init_cb;
	info->batch_cb = builder_ref.batch_cb;
	info->flush_cb = builder_ref.flush_cb;
	info->finalize_cb = builder_ref.finalize_cb;
	info->user_data = builder_ref.user_data;

	function.function_info = std::move(info);

	return function;
}

static void RegisterCopyFunctionV2(duckdb::ClientContext &context_ref, duckdb::CopyFunctionBuilderV2 *builder) {
	auto &catalog = duckdb::Catalog::GetSystemCatalog(context_ref);
	duckdb::CreateCopyFunctionInfo cf_info(BuildCopyFunctionV2(builder));
	cf_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateCopyFunction(context_ref, cf_info);
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_builder_register_with_connection(duckdb_v2_connection_handle conn,
                                                                         duckdb_v2_copy_function_builder_handle builder,
                                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn) {
			throw duckdb::InvalidInputException("Connection pointer cannot be null.");
		}
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}

		auto builder_ref = reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(builder);
		auto &context_ref = *duckdb::ToConn(conn)->context;
		context_ref.RunFunctionInTransaction([&]() { RegisterCopyFunctionV2(context_ref, builder_ref); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_builder_register_with_extension(duckdb_v2_extension_handle extension,
                                                                        duckdb_v2_copy_function_builder_handle builder,
                                                                        duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!extension) {
			throw duckdb::InvalidInputException("Extension pointer cannot be null.");
		}
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}

		auto builder_ref = reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(builder);
		auto &loader = *duckdb::ToExtension(extension);
		loader.RegisterFunction(BuildCopyFunctionV2(builder_ref));
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_builder_destroy(duckdb_v2_copy_function_builder_handle *builder) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!builder) {
			return;
		}
		if (!*builder) {
			return;
		}
		delete reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(*builder);
		*builder = nullptr;
	});
}

// --- Bind callback accessors -------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_copy_function_bind_get_user_data(duckdb_v2_copy_function_bind_info_handle info,
                                                           void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionBindInfoV2 *>(info)->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_bind_get_column_count(duckdb_v2_copy_function_bind_info_handle info,
                                                              idx_t *out_count, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_count = reinterpret_cast<duckdb::CopyFunctionBindInfoV2 *>(info)->column_count;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_bind_get_column_type(duckdb_v2_copy_function_bind_info_handle info, idx_t index,
                                                             duckdb_v2_logical_type_handle *out_type,
                                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_type) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<duckdb::CopyFunctionBindInfoV2 *>(info);
		if (index >= cb_info.column_count) {
			throw duckdb::InvalidInputException("Column index %llu out of range (have %llu).", index,
			                                    cb_info.column_count);
		}
		*out_type = cb_info.column_types[index];
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_bind_get_column_name(duckdb_v2_copy_function_bind_info_handle info, idx_t index,
                                                             duckdb_v2_identifier_t *out_name,
                                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_name) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<duckdb::CopyFunctionBindInfoV2 *>(info);
		if (index >= cb_info.column_count) {
			throw duckdb::InvalidInputException("Column index %llu out of range (have %llu).", index,
			                                    cb_info.column_count);
		}
		*out_name = cb_info.column_names[index];
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_bind_set_bind_data(duckdb_v2_copy_function_bind_info_handle info,
                                                           duckdb_v2_opaque data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		reinterpret_cast<duckdb::CopyFunctionBindInfoV2 *>(info)->out_bind_data = data;
	});
}

// --- Init callback accessors -------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_copy_function_init_get_user_data(duckdb_v2_copy_function_init_info_handle info,
                                                           void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionInitInfoV2 *>(info)->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_init_get_bind_data(duckdb_v2_copy_function_init_info_handle info,
                                                           void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionInitInfoV2 *>(info)->bind_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_init_get_file_path(duckdb_v2_copy_function_init_info_handle info,
                                                           duckdb_v2_str *out_path, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_path) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_path = reinterpret_cast<duckdb::CopyFunctionInitInfoV2 *>(info)->file_path;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_init_set_init_data(duckdb_v2_copy_function_init_info_handle info,
                                                           duckdb_v2_opaque data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		reinterpret_cast<duckdb::CopyFunctionInitInfoV2 *>(info)->out_init_data = data;
	});
}

// --- Batch callback accessors ------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_get_user_data(duckdb_v2_copy_function_batch_info_handle info,
                                                            void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionBatchInfoV2 *>(info)->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_get_bind_data(duckdb_v2_copy_function_batch_info_handle info,
                                                            void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionBatchInfoV2 *>(info)->bind_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_get_init_data(duckdb_v2_copy_function_batch_info_handle info,
                                                            void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionBatchInfoV2 *>(info)->init_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_get_input(duckdb_v2_copy_function_batch_info_handle info,
                                                        duckdb_v2_column_data_collection_handle *out_input,
                                                        duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_input) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<duckdb::CopyFunctionBatchInfoV2 *>(info);
		*out_input = reinterpret_cast<duckdb_v2_column_data_collection_handle>(cb_info.in_batch);
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_batch_set_batch_data(duckdb_v2_copy_function_batch_info_handle info,
                                                             duckdb_v2_opaque data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		reinterpret_cast<duckdb::CopyFunctionBatchInfoV2 *>(info)->out_batch = data;
	});
}

// --- Flush callback accessors ------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_copy_function_flush_get_user_data(duckdb_v2_copy_function_flush_info_handle info,
                                                            void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionFlushInfoV2 *>(info)->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_flush_get_bind_data(duckdb_v2_copy_function_flush_info_handle info,
                                                            void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionFlushInfoV2 *>(info)->bind_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_flush_get_init_data(duckdb_v2_copy_function_flush_info_handle info,
                                                            void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionFlushInfoV2 *>(info)->init_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_flush_get_batch_data(duckdb_v2_copy_function_flush_info_handle info,
                                                             void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionFlushInfoV2 *>(info)->batch_data;
	});
}

// --- Finalize callback accessors ---------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_copy_function_finalize_get_user_data(duckdb_v2_copy_function_finalize_info_handle info,
                                                               void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionFinalizeInfoV2 *>(info)->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_finalize_get_bind_data(duckdb_v2_copy_function_finalize_info_handle info,
                                                               void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionFinalizeInfoV2 *>(info)->bind_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_copy_function_finalize_get_init_data(duckdb_v2_copy_function_finalize_info_handle info,
                                                               void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CopyFunctionFinalizeInfoV2 *>(info)->init_data;
	});
}
