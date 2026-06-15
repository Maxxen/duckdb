#include "capi_v2_internal.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"

namespace duckdb {
namespace {

// Helper class to manage user data with custom copy, destroy, and equals callbacks
class UserDataHandle {
public:
	UserDataHandle() = default;

	explicit UserDataHandle(void *data, duckdb_v2_user_data_destroy_fn destroy_cb = nullptr,
	                        duckdb_v2_user_data_copy_fn copy_cb = nullptr,
	                        duckdb_v2_user_data_equals_fn equals_cb = nullptr)
	    : data(data), destroy_cb(destroy_cb), copy_cb(copy_cb), equals_cb(equals_cb) {
	}

	// Moveable
	UserDataHandle(UserDataHandle &&other) noexcept
	    : data(other.data), destroy_cb(other.destroy_cb), copy_cb(other.copy_cb), equals_cb(other.equals_cb) {
		other.data = nullptr;
		other.destroy_cb = nullptr;
		other.copy_cb = nullptr;
		other.equals_cb = nullptr;
	}

	UserDataHandle &operator=(UserDataHandle &&other) noexcept {
		if (this == &other) {
			return *this;
		}

		// Clean up existing data
		if (data && destroy_cb) {
			destroy_cb(data);
		}

		data = other.data;
		destroy_cb = other.destroy_cb;
		copy_cb = other.copy_cb;
		equals_cb = other.equals_cb;

		other.data = nullptr;
		other.destroy_cb = nullptr;
		other.copy_cb = nullptr;
		other.equals_cb = nullptr;

		return *this;
	}

	~UserDataHandle() {
		if (data && destroy_cb) {
			destroy_cb(data);
		}
	}

	bool operator==(const UserDataHandle &other) const {
		if (equals_cb) {
			return equals_cb(data, other.data);
		}
		return data == other.data;
	}

	bool operator!=(const UserDataHandle &other) const {
		return !(*this == other);
	}

	// explicit copy
	UserDataHandle Copy() const {
		if (copy_cb) {
			return UserDataHandle(copy_cb(data), destroy_cb, copy_cb, equals_cb);
		}
		return UserDataHandle(data, destroy_cb, copy_cb, equals_cb);
	}

	bool Equals(const UserDataHandle &other) const {
		return *this == other;
	}

	void *GetData() const {
		return data;
	}

private:
	void *data = nullptr;
	duckdb_v2_user_data_destroy_fn destroy_cb = nullptr;
	duckdb_v2_user_data_copy_fn copy_cb = nullptr;
	duckdb_v2_user_data_equals_fn equals_cb = nullptr;
};

class CCopyFunctionInfoV2 final : public CopyFunctionInfo {
public:
	string name;
	duckdb_v2_copy_function_bind_callback_fn bind_cb = nullptr;
	duckdb_v2_copy_function_init_callback_fn init_cb = nullptr;
	duckdb_v2_copy_function_batch_callback_fn batch_cb = nullptr;
	duckdb_v2_copy_function_flush_callback_fn flush_cb = nullptr;
	duckdb_v2_copy_function_finalize_callback_fn finalize_cb = nullptr;
	UserDataHandle user_data;
};

class CCopyFunctionBindDataV2 final : public FunctionData {
public:
	UserDataHandle bind_data;
	CCopyFunctionInfoV2 &info;

	explicit CCopyFunctionBindDataV2(CCopyFunctionInfoV2 &info) : info(info) {
	}

	auto Copy() const -> unique_ptr<FunctionData> override {
		auto copy = make_uniq<CCopyFunctionBindDataV2>(info);
		copy->bind_data = bind_data.Copy();
		return std::move(copy);
	}

	auto Equals(const FunctionData &other) const -> bool override {
		auto &other_bind_data = other.Cast<CCopyFunctionBindDataV2>();
		return bind_data.Equals(other_bind_data.bind_data);
	}
};

class CCopyFunctionStateV2 final : public GlobalFunctionData {
public:
	UserDataHandle init_data;
};

class CCopyFunctionBatchV2 final : public PreparedBatchData {
public:
	UserDataHandle batch_data;
};

struct CopyFunctionBuilderV2 {
	Identifier name;
	duckdb_v2_copy_function_bind_callback_fn bind_cb = nullptr;
	duckdb_v2_copy_function_init_callback_fn init_cb = nullptr;
	duckdb_v2_copy_function_batch_callback_fn batch_cb = nullptr;
	duckdb_v2_copy_function_flush_callback_fn flush_cb = nullptr;
	duckdb_v2_copy_function_finalize_callback_fn finalize_cb = nullptr;
	UserDataHandle user_data;

	static auto CopyToBind(ClientContext &context, CopyFunctionBindInput &input, const vector<Identifier> &names,
	                       const vector<LogicalType> &sql_types) -> unique_ptr<FunctionData> {
		auto &info = input.function_info->Cast<CCopyFunctionInfoV2>();

		// Setup arrays
		vector<duckdb_v2_str> names_array;
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

		duckdb_v2_copy_function_bind_args args = {};
		args.struct_size = sizeof(args);
		args.context = reinterpret_cast<duckdb_v2_context_handle>(&context);
		args.user_data = info.user_data.GetData();
		args.column_count = names.size();
		args.column_names = names_array.data();
		args.column_types = types_array.data();

		// The bind callback is optional: a copy function may not need any bind-time setup.
		if (info.bind_cb) {
			duckdb::InvokeWithErrorSlot<BinderException>(
			    [&](duckdb_v2_error_info_handle *err) { info.bind_cb(&args, err); });
		}

		auto result = make_uniq<CCopyFunctionBindDataV2>(info);

		// If the user set the bind data, move it out here
		if (args.out_bind_data) {
			result->bind_data = UserDataHandle(args.out_bind_data, args.out_bind_data_destructor,
			                                   args.out_bind_data_copy, args.out_bind_data_equality);
		}

		return std::move(result);
	}

	static auto CopyToInit(ClientContext &context, FunctionData &bind_data, const string &file_path)
	    -> unique_ptr<GlobalFunctionData> {
		auto &data = bind_data.Cast<CCopyFunctionBindDataV2>();
		auto &info = data.info;

		duckdb_v2_copy_function_init_args args = {};
		args.struct_size = sizeof(args);
		args.context = reinterpret_cast<duckdb_v2_context_handle>(&context);
		args.file_path = ToStr(file_path);
		args.user_data = info.user_data.GetData();
		args.bind_data = data.bind_data.GetData();
		args.out_init_data = nullptr;
		args.out_init_data_destructor = nullptr;

		// The init callback is optional: when absent, the global state simply carries no init data.
		if (info.init_cb) {
			duckdb::InvokeWithErrorSlot<InvalidInputException>(
			    [&](duckdb_v2_error_info_handle *err) { info.init_cb(&args, err); });
		}

		auto result = make_uniq<CCopyFunctionStateV2>();

		if (args.out_init_data) {
			result->init_data = UserDataHandle(args.out_init_data, args.out_init_data_destructor);
		}

		return std::move(result);
	}

	static auto CopyToBatch(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
	                        unique_ptr<ColumnDataCollection> collection) -> unique_ptr<PreparedBatchData> {
		auto &data = bind_data.Cast<CCopyFunctionBindDataV2>();
		auto &state = gstate.Cast<CCopyFunctionStateV2>();
		auto &info = data.info;

		duckdb_v2_copy_function_batch_args args = {};
		args.struct_size = sizeof(args);
		args.context = reinterpret_cast<duckdb_v2_context_handle>(&context);
		args.user_data = info.user_data.GetData();
		args.bind_data = data.bind_data.GetData();
		args.init_data = state.init_data.GetData();
		// Ownership of the collection is transferred to the callback: the callback (or the C++ wrapper around
		// it) is responsible for destroying it via duckdb_v2_column_data_collection_destroy. We release it from
		// the unique_ptr so it outlives this scope.
		args.in_batch = reinterpret_cast<duckdb_v2_column_data_collection_handle>(collection.release());
		args.out_batch = nullptr;
		args.out_batch_destructor = nullptr;

		duckdb::InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.batch_cb(&args, err); });

		auto result = make_uniq<CCopyFunctionBatchV2>();

		if (args.out_batch) {
			result->batch_data = UserDataHandle(args.out_batch, args.out_batch_destructor);
		}

		return std::move(result);
	}

	static auto CopyToFlush(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
	                        PreparedBatchData &batch) -> void {
		const auto &data = bind_data.Cast<CCopyFunctionBindDataV2>();
		const auto &info = data.info;
		const auto &state = gstate.Cast<CCopyFunctionStateV2>();

		duckdb_v2_copy_function_flush_args args = {};
		args.struct_size = sizeof(args);
		args.context = reinterpret_cast<duckdb_v2_context_handle>(&context);
		args.user_data = info.user_data.GetData();
		args.bind_data = data.bind_data.GetData();
		args.init_data = state.init_data.GetData();
		args.in_batch = batch.Cast<CCopyFunctionBatchV2>().batch_data.GetData();

		duckdb::InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.flush_cb(&args, err); });
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

		duckdb_v2_copy_function_finalize_args args = {};
		args.struct_size = sizeof(args);
		args.context = reinterpret_cast<duckdb_v2_context_handle>(&context);
		args.user_data = info.user_data.GetData();
		args.bind_data = data.bind_data.GetData();
		args.init_data = state.init_data.GetData();

		duckdb::InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.finalize_cb(&args, err); });
	}
};

} // namespace
} // namespace duckdb

DUCKDB_V2_API_CALL_t duckdb_v2_copy_function_builder_create(duckdb_v2_context_handle context,
                                                            duckdb_v2_copy_function_builder_handle *out,
                                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out = reinterpret_cast<duckdb_v2_copy_function_builder_handle>(new duckdb::CopyFunctionBuilderV2());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_copy_function_builder_set_name(duckdb_v2_copy_function_builder_handle builder,
                                                              duckdb_v2_str name, duckdb_v2_error_info_handle *err) {
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

DUCKDB_V2_API_CALL_t
duckdb_v2_copy_function_builder_set_bind_callback(duckdb_v2_copy_function_builder_handle builder,
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

DUCKDB_V2_API_CALL_t
duckdb_v2_copy_function_builder_set_init_callback(duckdb_v2_copy_function_builder_handle builder,
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

DUCKDB_V2_API_CALL_t
duckdb_v2_copy_function_builder_set_batch_callback(duckdb_v2_copy_function_builder_handle builder,
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

DUCKDB_V2_API_CALL_t
duckdb_v2_copy_function_builder_set_flush_callback(duckdb_v2_copy_function_builder_handle builder,
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

DUCKDB_V2_API_CALL_t
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

DUCKDB_V2_API_CALL_t duckdb_v2_copy_function_builder_set_user_data(duckdb_v2_copy_function_builder_handle builder,
                                                                   void *data, duckdb_v2_user_data_destroy_fn destroy,
                                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}

		auto &builder_ref = *reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(builder);
		builder_ref.user_data = duckdb::UserDataHandle(data, destroy);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_copy_function_builder_register(duckdb_v2_context_handle context,
                                                              duckdb_v2_copy_function_builder_handle builder,
                                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}

		auto &builder_ref = *reinterpret_cast<duckdb::CopyFunctionBuilderV2 *>(builder);

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

		auto &context_ref = *reinterpret_cast<duckdb::ClientContext *>(context);

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
		info->user_data = std::move(builder_ref.user_data);

		function.function_info = std::move(info);

		auto &catalog = duckdb::Catalog::GetSystemCatalog(context_ref);
		duckdb::CreateCopyFunctionInfo cf_info(function);
		cf_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
		catalog.CreateCopyFunction(context_ref, cf_info);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_copy_function_builder_destroy(duckdb_v2_copy_function_builder_handle *builder) {
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
