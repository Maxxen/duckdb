#include "capi_v2_internal.hpp"
#include "duckdb/common/enums/window_aggregation_mode.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {
namespace {

struct AggregateFunctionExtraDataV2 final : public AggregateFunctionInfo {
	duckdb_v2_aggregate_function_size_callback_fn size_cb = nullptr;
	duckdb_v2_aggregate_function_init_callback_fn init_cb = nullptr;
	duckdb_v2_aggregate_function_update_callback_fn update_cb = nullptr;
	duckdb_v2_aggregate_function_combine_callback_fn combine_cb = nullptr;
	duckdb_v2_aggregate_function_finalize_callback_fn finalize_cb = nullptr;
	duckdb_v2_aggregate_function_bind_callback_fn bind_cb = nullptr;
	duckdb_v2_aggregate_function_destroy_callback_fn destroy_cb = nullptr;

	shared_ptr<OpaqueDataHandle> user_data = nullptr;
};

class AggregateFunctionBindDataV2 final : public FunctionData {
public:
	explicit AggregateFunctionBindDataV2(shared_ptr<AggregateFunctionInfo> agg_info) : agg_info(std::move(agg_info)) {
	}

	auto Copy() const -> unique_ptr<FunctionData> override {
		auto result = make_uniq<AggregateFunctionBindDataV2>(agg_info);
		result->user_bind_data = user_bind_data;
		return std::move(result);
	}

	auto Equals(const FunctionData &other_p) const -> bool override {
		auto &other = other_p.Cast<AggregateFunctionBindDataV2>();
		auto &other_info = other.GetInfo();
		auto &info = GetInfo();

		if (!(info.size_cb == other_info.size_cb && info.init_cb == other_info.init_cb &&
		      info.update_cb == other_info.update_cb && info.combine_cb == other_info.combine_cb &&
		      info.finalize_cb == other_info.finalize_cb && info.destroy_cb == other_info.destroy_cb &&
		      info.bind_cb == other_info.bind_cb)) {
			return false;
		}
		// Compare user bind data: both unset is equal; otherwise defer to the opaque handle's equality.
		if (!user_bind_data || !other.user_bind_data) {
			return user_bind_data == other.user_bind_data;
		}
		return user_bind_data->Equals(*other.user_bind_data);
	}

	auto GetInfo() const -> const AggregateFunctionExtraDataV2 & {
		return agg_info->Cast<AggregateFunctionExtraDataV2>();
	}
	auto GetInfo() -> AggregateFunctionExtraDataV2 & {
		return agg_info->Cast<AggregateFunctionExtraDataV2>();
	}

	//! User bind data set by the bind callback, if any. Shared so copies alias the same resource.
	shared_ptr<OpaqueDataHandle> user_bind_data = nullptr;

private:
	shared_ptr<AggregateFunctionInfo> agg_info;
};

// --- Callback info structs (passed to user callbacks as opaque handles) ------

struct AggregateFunctionBindInfoV2 {
	duckdb_v2_identifier_t function_name = {};
	void *user_data = nullptr;
	BindArgumentsV2 *arguments = nullptr;
	duckdb_v2_opaque out_bind_data = {};
};

struct AggregateFunctionSizeInfoV2 {
	void *user_data = nullptr;
	idx_t out_size = 0;
};

struct AggregateFunctionInitInfoV2 {
	void *user_data = nullptr;
	void *state = nullptr;
};

struct AggregateFunctionUpdateInfoV2 {
	void *user_data = nullptr;
	void *bind_data = nullptr;
	idx_t count = 0;
	DataChunk *input = nullptr;
	void **states = nullptr;
};

struct AggregateFunctionCombineInfoV2 {
	void *user_data = nullptr;
	void *bind_data = nullptr;
	idx_t count = 0;
	void **sources = nullptr;
	void **targets = nullptr;
};

struct AggregateFunctionFinalizeInfoV2 {
	void *user_data = nullptr;
	void *bind_data = nullptr;
	idx_t count = 0;
	void **states = nullptr;
	Vector *result = nullptr;
	idx_t result_offset = 0;
};

struct AggregateFunctionDestroyInfoV2 {
	void *user_data = nullptr;
	void *bind_data = nullptr;
	idx_t count = 0;
	void **states = nullptr;
};

struct AggregateFunctionV2 {
	static auto BindCallback(BindAggregateFunctionInput &input) -> unique_ptr<FunctionData> {
		// Propagate the extra function info through the bind data
		auto result = make_uniq<AggregateFunctionBindDataV2>(input.GetBoundFunction().GetFunctionInfo());
		const auto &info = result->GetInfo();

		// Run the optional user bind callback and capture any bind data it sets.
		if (info.bind_cb) {
			BindArgumentsV2 bind_args;
			bind_args.arguments = &input.GetArguments();
			bind_args.argument_names = input.GetArgumentNames();

			AggregateFunctionBindInfoV2 cb_info;
			cb_info.function_name = ToStr(input.GetBoundFunction().GetName());
			cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
			cb_info.arguments = &bind_args;

			auto info_handle = reinterpret_cast<duckdb_v2_aggregate_function_bind_info_handle>(&cb_info);
			// Binding always runs under a client context.
			auto context = reinterpret_cast<duckdb_v2_context_handle>(&input.GetClientContext());
			InvokeWithErrorSlot<BinderException>(
			    [&](duckdb_v2_error_info_handle *err) { info.bind_cb(info_handle, context, err); });

			if (cb_info.out_bind_data.ptr) {
				result->user_bind_data = make_shared_ptr<OpaqueDataHandle>(
				    cb_info.out_bind_data.ptr, cb_info.out_bind_data.destroy, cb_info.out_bind_data.equals);
			}
		}

		return std::move(result);
	}

	static auto SizeCallback(AggregateStateInput &input) -> idx_t {
		const auto &info = input.function.GetExtraFunctionInfo().Cast<AggregateFunctionExtraDataV2>();

		D_ASSERT(info.size_cb);

		AggregateFunctionSizeInfoV2 cb_info;
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;

		auto info_handle = reinterpret_cast<duckdb_v2_aggregate_function_size_info_handle>(&cb_info);
		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.size_cb(info_handle, err); });

		return cb_info.out_size;
	}

	static auto InitCallback(AggregateStateInput &input, data_ptr_t *states, idx_t count) -> void {
		const auto &info = input.function.GetExtraFunctionInfo().Cast<AggregateFunctionExtraDataV2>();

		D_ASSERT(info.init_cb);

		// The C init callback is single-state; drive it once per requested state.
		for (idx_t i = 0; i < count; i++) {
			AggregateFunctionInitInfoV2 cb_info;
			cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
			cb_info.state = states[i];

			auto info_handle = reinterpret_cast<duckdb_v2_aggregate_function_init_info_handle>(&cb_info);
			InvokeWithErrorSlot<InvalidInputException>(
			    [&](duckdb_v2_error_info_handle *err) { info.init_cb(info_handle, err); });
		}
	}

	static auto UpdateCallback(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &state,
	                           idx_t count) -> void {
		auto &bind = aggr_input_data.bind_data->Cast<AggregateFunctionBindDataV2>();
		auto &info = bind.GetInfo();

		DataChunk chunk;
		for (idx_t i = 0; i < input_count; i++) {
			chunk.data.emplace_back(Vector::Ref(inputs[i]));
		}

		chunk.CheckCardinality(count);

		chunk.Flatten(); // TODO: Dont flatten here

		AggregateFunctionUpdateInfoV2 cb_info;
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.bind_data = bind.user_bind_data ? bind.user_bind_data->GetData() : nullptr;
		cb_info.input = &chunk;
		cb_info.states = FlatVector::GetDataMutableUnsafe<void *>(state);
		cb_info.count = count;

		auto info_handle = reinterpret_cast<duckdb_v2_aggregate_function_update_info_handle>(&cb_info);
		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.update_cb(info_handle, err); });
	}

	static auto CombineCallback(Vector &state, Vector &combined, AggregateInputData &aggr_input_data, idx_t count)
	    -> void {
		auto &bind = aggr_input_data.bind_data->Cast<AggregateFunctionBindDataV2>();
		auto &info = bind.GetInfo();

		state.Flatten(); // TODO: Dont flatten here

		AggregateFunctionCombineInfoV2 cb_info;
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.bind_data = bind.user_bind_data ? bind.user_bind_data->GetData() : nullptr;
		cb_info.count = count;
		cb_info.sources = FlatVector::GetDataMutableUnsafe<void *>(state);
		cb_info.targets = FlatVector::GetDataMutableUnsafe<void *>(combined);

		auto info_handle = reinterpret_cast<duckdb_v2_aggregate_function_combine_info_handle>(&cb_info);
		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.combine_cb(info_handle, err); });
	}

	static auto FinalizeCallback(Vector &state, AggregateFinalizeInputData &aggr_input_data, Vector &result,
	                             idx_t count, idx_t offset) -> void {
		auto &bind = aggr_input_data.bind_data->Cast<AggregateFunctionBindDataV2>();
		auto &info = bind.GetInfo();

		state.Flatten(); // TODO: Dont flatten here

		AggregateFunctionFinalizeInfoV2 cb_info;
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.bind_data = bind.user_bind_data ? bind.user_bind_data->GetData() : nullptr;
		cb_info.count = count;
		cb_info.states = FlatVector::GetDataMutableUnsafe<void *>(state);
		cb_info.result = &result;
		cb_info.result_offset = offset;

		auto info_handle = reinterpret_cast<duckdb_v2_aggregate_function_finalize_info_handle>(&cb_info);
		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.finalize_cb(info_handle, err); });
	}

	static auto DestroyCallback(Vector &state, AggregateInputData &aggr_input_data, idx_t count) -> void {
		auto &bind = aggr_input_data.bind_data->Cast<AggregateFunctionBindDataV2>();
		auto &info = bind.GetInfo();

		AggregateFunctionDestroyInfoV2 cb_info;
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.bind_data = bind.user_bind_data ? bind.user_bind_data->GetData() : nullptr;
		cb_info.count = count;
		cb_info.states = FlatVector::GetDataMutableUnsafe<void *>(state);

		auto info_handle = reinterpret_cast<duckdb_v2_aggregate_function_destroy_info_handle>(&cb_info);
		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.destroy_cb(info_handle, err); });
	}

	// See ThrowFunctionNotSerializable for why these throw.
	static auto SerializeCallback(Serializer &, const optional_ptr<FunctionData>,
	                              const BoundAggregateFunction &function) -> void {
		ThrowFunctionNotSerializable(function.GetName());
	}

	static auto DeserializeCallback(Deserializer &, BoundAggregateFunction &function) -> unique_ptr<FunctionData> {
		ThrowFunctionNotSerializable(function.GetName());
	}
};

struct AggregateFunctionBuilderV2 {
	Identifier name;
	//! Fixed parameters (names, types, defaults), varargs, and return type,
	//! copied in from a function_signature via set_signature.
	FunctionSignature signature;

	duckdb_v2_aggregate_function_bind_callback_fn bind_cb = nullptr;
	duckdb_v2_aggregate_function_size_callback_fn size_cb = nullptr;
	duckdb_v2_aggregate_function_init_callback_fn init_cb = nullptr;
	duckdb_v2_aggregate_function_update_callback_fn update_cb = nullptr;
	duckdb_v2_aggregate_function_combine_callback_fn combine_cb = nullptr;
	duckdb_v2_aggregate_function_finalize_callback_fn finalize_cb = nullptr;
	duckdb_v2_aggregate_function_destroy_callback_fn destroy_cb = nullptr;

	shared_ptr<OpaqueDataHandle> user_data = nullptr;
	AggregateFunctionProperties properties;
};

} // namespace
} // namespace duckdb

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_builder_create(duckdb_v2_aggregate_function_builder_handle *out,
                                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("output parameter cannot be null");
		}
		auto builder = new duckdb::AggregateFunctionBuilderV2();
		*out = reinterpret_cast<_duckdb_v2_aggregate_function_builder *>(builder);
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_builder_destroy(duckdb_v2_aggregate_function_builder_handle *builder) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!builder) {
			return;
		}
		if (!*builder) {
			return;
		}
		delete reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(*builder);
		*builder = nullptr;
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_builder_set_name(duckdb_v2_aggregate_function_builder_handle builder,
                                                              duckdb_v2_identifier_t name,
                                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		if (!name.ptr && name.len > 0) {
			throw duckdb::InvalidInputException("Function name cannot be null");
		}
		if (name.len == 0) {
			throw duckdb::InvalidInputException("Function name cannot be empty");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		agg_builder->name = duckdb::ToIdentifier(name);
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_builder_set_signature(duckdb_v2_aggregate_function_builder_handle func,
                                                   duckdb_v2_function_signature_handle sig,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null");
		}
		// Copy the borrowed signature in; the caller keeps ownership.
		reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(func)->signature = *duckdb::ToFunctionSignature(sig);
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_builder_set_bind_callback(duckdb_v2_aggregate_function_builder_handle builder,
                                                       duckdb_v2_aggregate_function_bind_callback_fn callback,
                                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		agg_builder->bind_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_builder_set_size_callback(duckdb_v2_aggregate_function_builder_handle builder,
                                                       duckdb_v2_aggregate_function_size_callback_fn callback,
                                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		agg_builder->size_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_builder_set_init_callback(duckdb_v2_aggregate_function_builder_handle builder,
                                                       duckdb_v2_aggregate_function_init_callback_fn callback,
                                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		agg_builder->init_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_builder_set_update_callback(duckdb_v2_aggregate_function_builder_handle builder,
                                                         duckdb_v2_aggregate_function_update_callback_fn callback,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		agg_builder->update_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_builder_set_combine_callback(duckdb_v2_aggregate_function_builder_handle builder,
                                                          duckdb_v2_aggregate_function_combine_callback_fn callback,
                                                          duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		agg_builder->combine_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_builder_set_finalize_callback(duckdb_v2_aggregate_function_builder_handle builder,
                                                           duckdb_v2_aggregate_function_finalize_callback_fn callback,
                                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		agg_builder->finalize_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_builder_set_destroy_callback(duckdb_v2_aggregate_function_builder_handle builder,
                                                          duckdb_v2_aggregate_function_destroy_callback_fn callback,
                                                          duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		agg_builder->destroy_cb = callback;
	});
}

static void RegisterAggregateFunctionV2(duckdb::ClientContext &ctx, duckdb::AggregateFunctionBuilderV2 *agg_builder) {
	if (agg_builder->name.empty()) {
		throw duckdb::InvalidInputException("Function name cannot be empty");
	}
	if (!agg_builder->size_cb) {
		throw duckdb::InvalidInputException("Size callback must be provided");
	}
	if (!agg_builder->init_cb) {
		throw duckdb::InvalidInputException("Init callback must be provided");
	}
	if (!agg_builder->update_cb) {
		throw duckdb::InvalidInputException("Update callback must be provided");
	}
	if (!agg_builder->combine_cb) {
		throw duckdb::InvalidInputException("Combine callback must be provided");
	}
	if (!agg_builder->finalize_cb) {
		throw duckdb::InvalidInputException("Finalize callback must be provided");
	}
	const auto &return_type = agg_builder->signature.GetReturnType();
	if (return_type.id() == duckdb::LogicalTypeId::INVALID) {
		throw duckdb::InvalidInputException("Return type must be set for the function.");
	}
	// ANY is a signature wildcard; a return type carries data, so keep ANY (and
	// any other incomplete type) out of execution (an ANY stored here throws
	// InternalException when hit).
	if (!return_type.IsComplete()) {
		throw duckdb::InvalidInputException("Return type must be a fully defined concrete type");
	}

	auto function_info = duckdb::make_shared_ptr<duckdb::AggregateFunctionExtraDataV2>();

	function_info->bind_cb = agg_builder->bind_cb;
	function_info->size_cb = agg_builder->size_cb;
	function_info->init_cb = agg_builder->init_cb;
	function_info->update_cb = agg_builder->update_cb;
	function_info->combine_cb = agg_builder->combine_cb;
	function_info->finalize_cb = agg_builder->finalize_cb;
	function_info->destroy_cb = agg_builder->destroy_cb;

	duckdb::AggregateFunction function(
	    agg_builder->name, {}, return_type, duckdb::AggregateFunctionV2::SizeCallback,
	    duckdb::AggregateFunctionV2::InitCallback, duckdb::AggregateFunctionV2::UpdateCallback,
	    duckdb::AggregateFunctionV2::CombineCallback, duckdb::AggregateFunctionV2::FinalizeCallback,
	    duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, duckdb::AggregateFunctionV2::BindCallback);

	if (agg_builder->destroy_cb) {
		function.GetCallbacks().SetStateDestructorCallback(duckdb::AggregateFunctionV2::DestroyCallback);
	}

	function.GetCallbacks().SetSerializeCallback(duckdb::AggregateFunctionV2::SerializeCallback);
	function.GetCallbacks().SetDeserializeCallback(duckdb::AggregateFunctionV2::DeserializeCallback);

	// Adopt the stored signature so parameter names, defaults, the variadic
	// tail, and the return type are all preserved on the registered function.
	function.GetSignature() = agg_builder->signature;

	function.SetExtraFunctionInfo(function_info);

	function.SetProperties(agg_builder->properties);

	// Verify signature
	function.GetSignature().Verify();

	auto &catalog = duckdb::Catalog::GetSystemCatalog(ctx);
	duckdb::CreateAggregateFunctionInfo create_info(function);
	create_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(ctx, create_info);

	function_info->user_data = agg_builder->user_data;
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_builder_register_with_connection(duckdb_v2_connection_handle conn,
                                                              duckdb_v2_aggregate_function_builder_handle builder,
                                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn) {
			throw duckdb::InvalidInputException("Connection pointer cannot be null.");
		}
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		auto &ctx = *duckdb::ToConn(conn)->context;
		ctx.RunFunctionInTransaction([&]() { RegisterAggregateFunctionV2(ctx, agg_builder); });
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_builder_register_with_context(duckdb_v2_context_handle context,
                                                           duckdb_v2_aggregate_function_builder_handle builder,
                                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("context cannot be null");
		}
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		auto &ctx = *duckdb::ToContext(context);
		RegisterAggregateFunctionV2(ctx, agg_builder);
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_builder_set_property(duckdb_v2_aggregate_function_builder_handle func,
                                                                  DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                                                                  DUCKDB_V2_FUNCTION_PROPERTY_VALUE value,
                                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(func);
		duckdb::SetAggregateFunctionProperty(agg_builder->properties, key, value);
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_builder_get_property(duckdb_v2_aggregate_function_builder_handle func,
                                                                  DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                                                                  DUCKDB_V2_FUNCTION_PROPERTY_VALUE *out_value,
                                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		if (!out_value) {
			throw duckdb::InvalidInputException("Output value pointer cannot be null");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(func);
		*out_value = duckdb::GetAggregateFunctionProperty(agg_builder->properties, key);
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_builder_set_user_data(duckdb_v2_aggregate_function_builder_handle builder,
                                                                   duckdb_v2_opaque data,
                                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}

		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		agg_builder->user_data = duckdb::make_shared_ptr<duckdb::OpaqueDataHandle>(data.ptr, data.destroy, data.equals);
	});
}

// --- Bind callback accessors ----------------------------------

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_bind_get_function_name(duckdb_v2_aggregate_function_bind_info_handle info,
                                                    duckdb_v2_identifier_t *out_name,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_name) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionBindInfoV2 *>(info);
		*out_name = c->function_name;
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_bind_get_user_data(duckdb_v2_aggregate_function_bind_info_handle info,
                                                                void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionBindInfoV2 *>(info);
		*out_data = c->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_bind_get_arguments(duckdb_v2_aggregate_function_bind_info_handle info,
                                                                duckdb_v2_bind_arguments_handle *out_arguments,
                                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_arguments) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionBindInfoV2 *>(info);
		*out_arguments = reinterpret_cast<duckdb_v2_bind_arguments_handle>(c->arguments);
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_bind_set_bind_data(duckdb_v2_aggregate_function_bind_info_handle info,
                                                                duckdb_v2_opaque data,
                                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionBindInfoV2 *>(info);
		c->out_bind_data = data;
	});
}

// --- Size callback accessors ----------------------------------

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_size_get_user_data(duckdb_v2_aggregate_function_size_info_handle info,
                                                                void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionSizeInfoV2 *>(info);
		*out_data = c->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_size_set_size(duckdb_v2_aggregate_function_size_info_handle info,
                                                           idx_t size, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionSizeInfoV2 *>(info);
		c->out_size = size;
	});
}

// --- Init callback accessors ----------------------------------

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_init_get_user_data(duckdb_v2_aggregate_function_init_info_handle info,
                                                                void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionInitInfoV2 *>(info);
		*out_data = c->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_init_get_state(duckdb_v2_aggregate_function_init_info_handle info,
                                                            void **out_state, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_state) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionInitInfoV2 *>(info);
		*out_state = c->state;
	});
}

// --- Update callback accessors --------------------------------

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_update_get_user_data(duckdb_v2_aggregate_function_update_info_handle info, void **out_data,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionUpdateInfoV2 *>(info);
		*out_data = c->user_data;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_update_get_bind_data(duckdb_v2_aggregate_function_update_info_handle info, void **out_data,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionUpdateInfoV2 *>(info);
		*out_data = c->bind_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_update_get_count(duckdb_v2_aggregate_function_update_info_handle info,
                                                              idx_t *out_count, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionUpdateInfoV2 *>(info);
		*out_count = c->count;
	});
}

DUCKDB_V2_ERROR duckdb_v2_aggregate_function_update_get_input(duckdb_v2_aggregate_function_update_info_handle info,
                                                              duckdb_v2_data_chunk_handle *out_input,
                                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_input) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionUpdateInfoV2 *>(info);
		*out_input = reinterpret_cast<duckdb_v2_data_chunk_handle>(c->input);
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_update_get_states(duckdb_v2_aggregate_function_update_info_handle info, void ***out_states,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_states) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionUpdateInfoV2 *>(info);
		*out_states = c->states;
	});
}

// --- Combine callback accessors -------------------------------

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_combine_get_user_data(duckdb_v2_aggregate_function_combine_info_handle info,
                                                   void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionCombineInfoV2 *>(info);
		*out_data = c->user_data;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_combine_get_bind_data(duckdb_v2_aggregate_function_combine_info_handle info,
                                                   void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionCombineInfoV2 *>(info);
		*out_data = c->bind_data;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_combine_get_count(duckdb_v2_aggregate_function_combine_info_handle info, idx_t *out_count,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionCombineInfoV2 *>(info);
		*out_count = c->count;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_combine_get_sources(duckdb_v2_aggregate_function_combine_info_handle info,
                                                 void ***out_sources, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_sources) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionCombineInfoV2 *>(info);
		*out_sources = c->sources;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_combine_get_targets(duckdb_v2_aggregate_function_combine_info_handle info,
                                                 void ***out_targets, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_targets) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionCombineInfoV2 *>(info);
		*out_targets = c->targets;
	});
}

// --- Finalize callback accessors ------------------------------

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_user_data(duckdb_v2_aggregate_function_finalize_info_handle info,
                                                    void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionFinalizeInfoV2 *>(info);
		*out_data = c->user_data;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_bind_data(duckdb_v2_aggregate_function_finalize_info_handle info,
                                                    void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionFinalizeInfoV2 *>(info);
		*out_data = c->bind_data;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_count(duckdb_v2_aggregate_function_finalize_info_handle info,
                                                idx_t *out_count, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionFinalizeInfoV2 *>(info);
		*out_count = c->count;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_states(duckdb_v2_aggregate_function_finalize_info_handle info,
                                                 void ***out_states, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_states) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionFinalizeInfoV2 *>(info);
		*out_states = c->states;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_result(duckdb_v2_aggregate_function_finalize_info_handle info,
                                                 duckdb_v2_vector_handle *out_result,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_result) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionFinalizeInfoV2 *>(info);
		*out_result = reinterpret_cast<duckdb_v2_vector_handle>(c->result);
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_finalize_get_result_offset(duckdb_v2_aggregate_function_finalize_info_handle info,
                                                        idx_t *out_offset, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_offset) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionFinalizeInfoV2 *>(info);
		*out_offset = c->result_offset;
	});
}

// --- Destroy callback accessors -------------------------------

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_destroy_get_user_data(duckdb_v2_aggregate_function_destroy_info_handle info,
                                                   void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionDestroyInfoV2 *>(info);
		*out_data = c->user_data;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_destroy_get_bind_data(duckdb_v2_aggregate_function_destroy_info_handle info,
                                                   void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionDestroyInfoV2 *>(info);
		*out_data = c->bind_data;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_destroy_get_count(duckdb_v2_aggregate_function_destroy_info_handle info, idx_t *out_count,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionDestroyInfoV2 *>(info);
		*out_count = c->count;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_aggregate_function_destroy_get_states(duckdb_v2_aggregate_function_destroy_info_handle info,
                                                void ***out_states, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_states) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto c = reinterpret_cast<duckdb::AggregateFunctionDestroyInfoV2 *>(info);
		*out_states = c->states;
	});
}
