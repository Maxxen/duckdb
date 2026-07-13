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

struct AggregateFunctionCallbackInfoV2 {
	void *user_data = nullptr;

	explicit AggregateFunctionCallbackInfoV2(void *user_data_p) : user_data(user_data_p) {
	}
};

struct AggregateFunctionV2 {
	static auto BindCallback(BindAggregateFunctionInput &input) -> unique_ptr<FunctionData> {
		// Propagate the extra function info through the bind data
		auto result = make_uniq<AggregateFunctionBindDataV2>(input.GetBoundFunction().GetFunctionInfo());
		const auto &info = result->GetInfo();

		// Run the optional user bind callback and capture any bind data it sets.
		if (info.bind_cb) {
			duckdb_v2_aggregate_function_bind_args args = {};
			args.struct_size = sizeof(args);
			args.function_name = ToStr(input.GetBoundFunction().GetName());
			args.user_data = info.user_data ? info.user_data->GetData() : nullptr;

			// Binding always runs under a client context.
			auto context = reinterpret_cast<duckdb_v2_context_handle>(&input.GetClientContext());
			InvokeWithErrorSlot<BinderException>(
			    [&](duckdb_v2_error_info_handle *err) { info.bind_cb(&args, context, err); });

			if (args.out_bind_data.ptr) {
				result->user_bind_data = make_shared_ptr<OpaqueDataHandle>(
				    args.out_bind_data.ptr, args.out_bind_data.destroy, args.out_bind_data.equals);
			}
		}

		return std::move(result);
	}

	static auto SizeCallback(const BoundAggregateFunction &function) -> idx_t {
		const auto &info = function.GetExtraFunctionInfo().Cast<AggregateFunctionExtraDataV2>();

		D_ASSERT(info.size_cb);

		ErrorInfoV2 err;
		auto err_ptr = reinterpret_cast<_duckdb_v2_error_info *>(&err);

		duckdb_v2_aggregate_function_size_args args = {};
		args.struct_size = sizeof(args);
		args.user_data = info.user_data ? info.user_data->GetData() : nullptr;

		info.size_cb(&args, &err_ptr);

		if (err.HasError()) {
			err.ThrowAsException();
		}

		return args.out_size;
	}

	static auto InitCallback(const BoundAggregateFunction &function, data_ptr_t state) -> void {
		const auto &info = function.GetExtraFunctionInfo().Cast<AggregateFunctionExtraDataV2>();

		D_ASSERT(info.init_cb);

		ErrorInfoV2 err;
		auto err_ptr = reinterpret_cast<_duckdb_v2_error_info *>(&err);

		duckdb_v2_aggregate_function_init_args args = {};
		args.struct_size = sizeof(args);
		args.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		args.state = state;

		info.init_cb(&args, &err_ptr);

		if (err.HasError()) {
			err.ThrowAsException();
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

		ErrorInfoV2 err;
		auto err_ptr = reinterpret_cast<_duckdb_v2_error_info *>(&err);

		duckdb_v2_aggregate_function_update_args args = {};
		args.struct_size = sizeof(args);
		args.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		args.bind_data = bind.user_bind_data ? bind.user_bind_data->GetData() : nullptr;
		args.input = reinterpret_cast<_duckdb_v2_data_chunk *>(&chunk);
		args.states = FlatVector::GetDataMutableUnsafe<void *>(state);
		args.count = count;

		info.update_cb(&args, &err_ptr);

		if (err.HasError()) {
			err.ThrowAsException();
		}
	}

	static auto CombineCallback(Vector &state, Vector &combined, AggregateInputData &aggr_input_data, idx_t count)
	    -> void {
		auto &bind = aggr_input_data.bind_data->Cast<AggregateFunctionBindDataV2>();
		auto &info = bind.GetInfo();

		state.Flatten(); // TODO: Dont flatten here

		ErrorInfoV2 err;
		auto err_ptr = reinterpret_cast<_duckdb_v2_error_info *>(&err);

		duckdb_v2_aggregate_function_combine_args args = {};
		args.struct_size = sizeof(args);
		args.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		args.bind_data = bind.user_bind_data ? bind.user_bind_data->GetData() : nullptr;
		args.count = count;
		args.sources = FlatVector::GetDataMutableUnsafe<void *>(state);
		args.targets = FlatVector::GetDataMutableUnsafe<void *>(combined);

		info.combine_cb(&args, &err_ptr);

		if (err.HasError()) {
			err.ThrowAsException();
		}
	}

	static auto FinalizeCallback(Vector &state, AggregateFinalizeInputData &aggr_input_data, Vector &result,
	                             idx_t count, idx_t offset) -> void {
		auto &bind = aggr_input_data.bind_data->Cast<AggregateFunctionBindDataV2>();
		auto &info = bind.GetInfo();

		state.Flatten(); // TODO: Dont flatten here

		ErrorInfoV2 err;
		auto err_ptr = reinterpret_cast<_duckdb_v2_error_info *>(&err);

		duckdb_v2_aggregate_function_finalize_args args = {};
		args.struct_size = sizeof(args);
		args.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		args.bind_data = bind.user_bind_data ? bind.user_bind_data->GetData() : nullptr;
		args.count = count;
		args.states = FlatVector::GetDataMutableUnsafe<void *>(state);
		args.result = reinterpret_cast<_duckdb_v2_vector *>(&result);
		args.result_offset = offset;

		info.finalize_cb(&args, &err_ptr);

		if (err.HasError()) {
			err.ThrowAsException();
		}
	}

	static auto DestroyCallback(Vector &state, AggregateInputData &aggr_input_data, idx_t count) -> void {
		auto &bind = aggr_input_data.bind_data->Cast<AggregateFunctionBindDataV2>();
		auto &info = bind.GetInfo();

		ErrorInfoV2 err;
		auto err_ptr = reinterpret_cast<_duckdb_v2_error_info *>(&err);

		duckdb_v2_aggregate_function_destroy_args args = {};
		args.struct_size = sizeof(args);
		args.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		args.bind_data = bind.user_bind_data ? bind.user_bind_data->GetData() : nullptr;
		args.count = count;
		args.states = FlatVector::GetDataMutableUnsafe<void *>(state);

		info.destroy_cb(&args, &err_ptr);

		if (err.HasError()) {
			err.ThrowAsException();
		}
	}
};

struct AggregateFunctionBuilderV2 {
	Identifier name;
	vector<pair<Identifier, LogicalType>> parameters;
	LogicalType return_type;

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

DUCKDB_V2_API_CALL_t duckdb_v2_aggregate_function_builder_create(duckdb_v2_context_handle context,
                                                                 duckdb_v2_aggregate_function_builder_handle *out,
                                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("context cannot be null");
		}
		if (!out) {
			throw duckdb::InvalidInputException("output parameter cannot be null");
		}
		auto builder = new duckdb::AggregateFunctionBuilderV2();
		*out = reinterpret_cast<_duckdb_v2_aggregate_function_builder *>(builder);
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_aggregate_function_builder_destroy(duckdb_v2_aggregate_function_builder_handle *builder) {
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

DUCKDB_V2_API_CALL_t duckdb_v2_aggregate_function_builder_set_name(duckdb_v2_aggregate_function_builder_handle builder,
                                                                   duckdb_v2_str name,
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

DUCKDB_V2_API_CALL_t
duckdb_v2_aggregate_function_builder_add_parameter(duckdb_v2_aggregate_function_builder_handle func, duckdb_v2_str name,
                                                   duckdb_v2_logical_type_handle type,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		if (!name.ptr && name.len > 0) {
			throw duckdb::InvalidInputException("Parameter name cannot be null");
		}
		if (name.len == 0) {
			throw duckdb::InvalidInputException("Parameter name cannot be empty");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Parameter type pointer cannot be null");
		}
		const auto &ltype = *reinterpret_cast<duckdb::LogicalType *>(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Parameter type cannot be invalid.");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(func);
		agg_builder->parameters.emplace_back(duckdb::ToIdentifier(name), ltype);
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_aggregate_function_builder_set_return_type(duckdb_v2_aggregate_function_builder_handle func,
                                                     duckdb_v2_logical_type_handle type,
                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Return type pointer cannot be null");
		}
		const auto &ltype = *reinterpret_cast<duckdb::LogicalType *>(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Return type cannot be invalid.");
		}
		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(func);
		agg_builder->return_type = ltype;
	});
}

DUCKDB_V2_API_CALL_t
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

DUCKDB_V2_API_CALL_t
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

DUCKDB_V2_API_CALL_t
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

DUCKDB_V2_API_CALL_t
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

DUCKDB_V2_API_CALL_t
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

DUCKDB_V2_API_CALL_t
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

DUCKDB_V2_API_CALL_t
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

DUCKDB_V2_API_CALL_t duckdb_v2_aggregate_function_builder_register(duckdb_v2_context_handle context,
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
		auto &ctx = *reinterpret_cast<duckdb::ClientContext *>(context);

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

		auto function_info = duckdb::make_shared_ptr<duckdb::AggregateFunctionExtraDataV2>();

		function_info->bind_cb = agg_builder->bind_cb;
		function_info->size_cb = agg_builder->size_cb;
		function_info->init_cb = agg_builder->init_cb;
		function_info->update_cb = agg_builder->update_cb;
		function_info->combine_cb = agg_builder->combine_cb;
		function_info->finalize_cb = agg_builder->finalize_cb;
		function_info->destroy_cb = agg_builder->destroy_cb;

		duckdb::vector<duckdb::LogicalType> argument_types;
		for (const auto &param : agg_builder->parameters) {
			argument_types.push_back(param.second);
		}

		duckdb::AggregateFunction function(
		    agg_builder->name, argument_types, agg_builder->return_type, duckdb::AggregateFunctionV2::SizeCallback,
		    duckdb::AggregateFunctionV2::InitCallback, duckdb::AggregateFunctionV2::UpdateCallback,
		    duckdb::AggregateFunctionV2::CombineCallback, duckdb::AggregateFunctionV2::FinalizeCallback,
		    duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, duckdb::AggregateFunctionV2::BindCallback);

		if (agg_builder->destroy_cb) {
			function.GetCallbacks().SetStateDestructorCallback(duckdb::AggregateFunctionV2::DestroyCallback);
		}

		function.SetExtraFunctionInfo(function_info);

		function.SetProperties(agg_builder->properties);

		// Verify signature
		function.GetSignature().Verify();

		auto &catalog = duckdb::Catalog::GetSystemCatalog(ctx);
		duckdb::CreateAggregateFunctionInfo create_info(function);
		create_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
		catalog.CreateFunction(ctx, create_info);

		function_info->user_data = agg_builder->user_data;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_aggregate_function_builder_set_property(duckdb_v2_aggregate_function_builder_handle func,
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

DUCKDB_V2_API_CALL_t duckdb_v2_aggregate_function_builder_get_property(duckdb_v2_aggregate_function_builder_handle func,
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

DUCKDB_V2_API_CALL_t
duckdb_v2_aggregate_function_builder_set_user_data(duckdb_v2_aggregate_function_builder_handle builder,
                                                   duckdb_v2_opaque data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Function builder cannot be null");
		}

		auto agg_builder = reinterpret_cast<duckdb::AggregateFunctionBuilderV2 *>(builder);
		agg_builder->user_data = duckdb::make_shared_ptr<duckdb::OpaqueDataHandle>(data.ptr, data.destroy, data.equals);
	});
}
