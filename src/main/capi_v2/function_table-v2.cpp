#include "capi_v2_internal.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {
namespace {

struct TableFunctionRuntimeInfoV2;

// --- User data wrappers (bind data, global state, local state) ---------------

struct TableFunctionBindDataV2 final : public FunctionData {
	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<TableFunctionBindDataV2>();
		copy->user_data = user_data;
		copy->runtime_info = runtime_info;
		copy->cardinality = cardinality;
		copy->cardinality_is_exact = cardinality_is_exact;
		copy->cardinality_set = cardinality_set;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other) const override {
		const auto &other_data = other.Cast<TableFunctionBindDataV2>().user_data;
		return user_data && other_data && user_data->Equals(*other_data);
	}

	shared_ptr<OpaqueDataHandle> user_data;

	// Borrowed pointer to the function's runtime info (callbacks + builder user
	// data). Set during bind; lets init/exec/cardinality/progress reach the
	// callbacks via the bind data DuckDB threads through every phase, rather
	// than reaching through the physical operator.
	optional_ptr<const TableFunctionRuntimeInfoV2> runtime_info;

	// Static cardinality set via table_function_bind_set_cardinality.
	idx_t cardinality = 0;
	bool cardinality_is_exact = false;
	bool cardinality_set = false;
};

struct TableFunctionGlobalStateV2 final : public GlobalTableFunctionState {
	idx_t MaxThreads() const override {
		return max_threads;
	}

	OpaqueDataHandle user_data;
	idx_t max_threads = 1;
};

struct TableFunctionLocalStateV2 final : public LocalTableFunctionState {
	OpaqueDataHandle user_data;
};

// --- Callback info structs (passed to user callbacks as opaque handles) ------

struct TableFunctionBindInfoV2 {
	vector<LogicalType> return_types;
	vector<string> return_names;
	unique_ptr<TableFunctionBindDataV2> bind_data;
	idx_t cardinality = 0;
	bool cardinality_is_exact = false;
	bool cardinality_set = false;
	void *user_data = nullptr;
	vector<Value> parameters;
	named_parameter_map_t named_parameters;
};

struct TableFunctionInitInfoV2 {
	optional_ptr<const TableFunctionBindDataV2> bind_data;
	optional_ptr<TableFunctionGlobalStateV2> global_state;
	vector<column_t> column_ids;
	vector<idx_t> projection_ids;
	unique_ptr<TableFunctionGlobalStateV2> set_global_state;
	unique_ptr<TableFunctionLocalStateV2> set_local_state;
	idx_t max_threads = 1;
	void *user_data = nullptr;
};

struct TableFunctionExecInfoV2 {
	optional_ptr<const TableFunctionBindDataV2> bind_data;
	optional_ptr<const TableFunctionGlobalStateV2> global_state;
	optional_ptr<TableFunctionLocalStateV2> local_state;
	DataChunk *output;
	void *user_data = nullptr;
};

// Passed to the pushdown_complex_filter callback. Borrows the engine's filter
// expression list; `handled[i]` records which filters the function claimed so
// the trampoline can drop them after the callback (the engine applies the rest).
struct TableFunctionFilterInfoV2 {
	vector<unique_ptr<Expression>> *expressions = nullptr;
	vector<bool> handled;
	void *user_data = nullptr;
	// The scan's column ids at pushdown time: what BoundColumnRef column_index
	// values in the candidate filters index. The engine may re-prune the
	// projection after handled filters drop, so this list is exposed here.
	const vector<ColumnIndex> *column_ids = nullptr;
};

// --- RuntimeInfo: stored on the TableFunction's function_info ----------------

struct TableFunctionRuntimeInfoV2 final : public TableFunctionInfo {
	duckdb_v2_table_function_bind_fn bind_cb = nullptr;
	duckdb_v2_table_function_init_global_fn init_global_cb = nullptr;
	duckdb_v2_table_function_init_local_fn init_local_cb = nullptr;
	duckdb_v2_table_function_exec_fn exec_cb = nullptr;
	duckdb_v2_table_function_cardinality_fn cardinality_cb = nullptr;
	duckdb_v2_table_function_progress_fn progress_cb = nullptr;
	duckdb_v2_table_function_pushdown_complex_filter_fn pushdown_complex_filter_cb = nullptr;

	shared_ptr<OpaqueDataHandle> user_data = nullptr;

	// Default values for the signature's optional (defaulted) parameters, which
	// route onto the engine's named-parameter map. The bind wrapper injects these
	// into the bind input for any name the call site omits, so get_named_parameter
	// always yields a value for a declared parameter.
	identifier_map_t<Value> named_parameter_defaults;
};

// --- Builder struct ----------------------------------------------------------

struct TableFunctionBuilderV2 {
	Identifier name;
	TableFunctionRuntimeInfoV2 info;
	//! Fixed parameters (names, types, defaults) and the variadic tail, copied in
	//! from a function_signature via set_signature. Parameters without a default
	//! route onto positional arguments; parameters with a default route onto named
	//! arguments (see set_signature). A return type is rejected.
	FunctionSignature signature;
	bool projection_pushdown = false;

	static unique_ptr<FunctionData> BindCallback(ClientContext &context, TableFunctionBindInput &input,
	                                             vector<LogicalType> &return_types, vector<string> &names) {
		auto &rt_info = input.info->Cast<TableFunctionRuntimeInfoV2>();
		D_ASSERT(rt_info.bind_cb);

		TableFunctionBindInfoV2 cb_info;
		cb_info.user_data = rt_info.user_data ? rt_info.user_data->GetData() : nullptr;
		for (auto &v : input.inputs) {
			cb_info.parameters.push_back(v);
		}
		cb_info.named_parameters = input.named_parameters;
		// Inject the declared default for every optional parameter the call site
		// omitted, so get_named_parameter yields a value for every declared
		// parameter (the scalar contract that bind observes a value for each).
		for (auto &entry : rt_info.named_parameter_defaults) {
			if (cb_info.named_parameters.find(entry.first) == cb_info.named_parameters.end()) {
				cb_info.named_parameters[entry.first] = entry.second;
			}
		}

		auto info_handle = reinterpret_cast<duckdb_v2_table_function_bind_info_handle>(&cb_info);
		auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context);

		InvokeWithErrorSlot<BinderException>(
		    [&](duckdb_v2_error_info_handle *err_ptr) { rt_info.bind_cb(info_handle, ctx_ptr, err_ptr); });

		return_types = std::move(cb_info.return_types);
		names = std::move(cb_info.return_names);

		auto result = cb_info.bind_data ? std::move(cb_info.bind_data) : make_uniq<TableFunctionBindDataV2>();
		result->runtime_info = &rt_info;
		result->cardinality = cb_info.cardinality;
		result->cardinality_is_exact = cb_info.cardinality_is_exact;
		result->cardinality_set = cb_info.cardinality_set;
		return std::move(result);
	}

	static unique_ptr<GlobalTableFunctionState> InitGlobalCallback(ClientContext &context,
	                                                               TableFunctionInitInput &input) {
		auto &bind = input.bind_data->Cast<TableFunctionBindDataV2>();
		auto &rt_info = *bind.runtime_info;

		auto result = make_uniq<TableFunctionGlobalStateV2>();

		if (!rt_info.init_global_cb) {
			return std::move(result);
		}

		TableFunctionInitInfoV2 cb_info;
		cb_info.bind_data = bind;
		cb_info.user_data = rt_info.user_data ? rt_info.user_data->GetData() : nullptr;
		cb_info.column_ids = input.column_ids;
		cb_info.projection_ids.assign(input.projection_ids.begin(), input.projection_ids.end());

		auto info_handle = reinterpret_cast<duckdb_v2_table_function_init_info_handle>(&cb_info);
		auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context);

		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err_ptr) { rt_info.init_global_cb(info_handle, ctx_ptr, err_ptr); });

		if (cb_info.set_global_state) {
			return std::move(cb_info.set_global_state);
		}

		result->max_threads = cb_info.max_threads;
		return std::move(result);
	}

	static unique_ptr<LocalTableFunctionState> InitLocalCallback(ExecutionContext &context,
	                                                             TableFunctionInitInput &input,
	                                                             GlobalTableFunctionState *global_state) {
		auto &bind = input.bind_data->Cast<TableFunctionBindDataV2>();
		auto &rt_info = *bind.runtime_info;

		if (!rt_info.init_local_cb) {
			return nullptr;
		}

		TableFunctionInitInfoV2 cb_info;
		cb_info.bind_data = bind;
		cb_info.global_state = global_state->Cast<TableFunctionGlobalStateV2>();
		cb_info.user_data = rt_info.user_data ? rt_info.user_data->GetData() : nullptr;
		cb_info.column_ids = input.column_ids;
		cb_info.projection_ids.assign(input.projection_ids.begin(), input.projection_ids.end());

		auto info_handle = reinterpret_cast<duckdb_v2_table_function_init_info_handle>(&cb_info);
		auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context.client);

		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err_ptr) { rt_info.init_local_cb(info_handle, ctx_ptr, err_ptr); });

		if (cb_info.set_local_state) {
			return std::move(cb_info.set_local_state);
		}
		return nullptr;
	}

	static void ExecCallback(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &bind = data_p.bind_data->Cast<TableFunctionBindDataV2>();
		auto &global_state = data_p.global_state->Cast<TableFunctionGlobalStateV2>();
		D_ASSERT(bind.runtime_info && bind.runtime_info->exec_cb);

		TableFunctionExecInfoV2 cb_info;
		cb_info.bind_data = bind;
		cb_info.global_state = global_state;
		cb_info.local_state = data_p.local_state ? &data_p.local_state->Cast<TableFunctionLocalStateV2>() : nullptr;
		cb_info.output = &output;
		cb_info.user_data = bind.runtime_info->user_data ? bind.runtime_info->user_data->GetData() : nullptr;

		auto info_handle = reinterpret_cast<duckdb_v2_table_function_exec_info_handle>(&cb_info);
		auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context);

		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err_ptr) { bind.runtime_info->exec_cb(info_handle, ctx_ptr, err_ptr); });

		// The V2 vector-write API sizes per vector (vector_set_size); the engine
		// reads the chunk's cardinality. A table-function scan always has at
		// least one output column, so derive the row count from the first
		// vector's size and propagate it to every output vector. Returning
		// cardinality 0 signals end of data. SetChildCardinality (not
		// CheckCardinality) is required here: callers typically size only the
		// first vector and write the others via direct buffer / assign_string
		// writes, so the remaining vectors still need their size set.
		output.SetChildCardinality(output.data[0].size());

		// After cardinality propagation so the verifier sees final sizes.
		VerifyUserChunk(output, "table function exec callback");
	}

	// Exact cardinality also pins the max; otherwise only the estimate is known.
	static unique_ptr<NodeStatistics> MakeNodeStatistics(idx_t cardinality, bool is_exact) {
		if (is_exact) {
			return make_uniq<NodeStatistics>(cardinality, cardinality);
		}
		return make_uniq<NodeStatistics>(cardinality);
	}

	static unique_ptr<NodeStatistics> CardinalityCallback(ClientContext &context, const FunctionData *bind_data) {
		auto &bind = bind_data->Cast<TableFunctionBindDataV2>();
		auto &rt_info = *bind.runtime_info;

		if (rt_info.cardinality_cb) {
			idx_t estimated = 0;
			bool is_exact = false;
			auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context);

			InvokeWithErrorSlot<InvalidInputException>([&](duckdb_v2_error_info_handle *err_ptr) {
				rt_info.cardinality_cb(bind.user_data ? bind.user_data->GetData() : nullptr, &estimated, &is_exact,
				                       ctx_ptr, err_ptr);
			});
			return MakeNodeStatistics(estimated, is_exact);
		}

		if (bind.cardinality_set) {
			return MakeNodeStatistics(bind.cardinality, bind.cardinality_is_exact);
		}

		return nullptr;
	}

	static double ProgressCallback(ClientContext &context, const FunctionData *bind_data,
	                               const GlobalTableFunctionState *global_state) {
		auto &bind = bind_data->Cast<TableFunctionBindDataV2>();
		auto &rt_info = *bind.runtime_info;
		D_ASSERT(rt_info.progress_cb);

		void *global_user_data = nullptr;
		if (global_state) {
			global_user_data = global_state->Cast<TableFunctionGlobalStateV2>().user_data.GetData();
		}

		double progress = 0.0;
		auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context);

		InvokeWithErrorSlot<InvalidInputException>([&](duckdb_v2_error_info_handle *err_ptr) {
			rt_info.progress_cb(bind.user_data ? bind.user_data->GetData() : nullptr, global_user_data, &progress,
			                    ctx_ptr, err_ptr);
		});
		// DuckDB's table_scan_progress reports a 0-100 percentage; the V2
		// contract is a 0.0-1.0 fraction.
		return progress * 100.0;
	}

	static void PushdownComplexFilterCallback(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
	                                          vector<unique_ptr<Expression>> &filters) {
		auto &bind = bind_data->Cast<TableFunctionBindDataV2>();
		auto &rt_info = *bind.runtime_info;
		D_ASSERT(rt_info.pushdown_complex_filter_cb);

		TableFunctionFilterInfoV2 filter_info;
		filter_info.expressions = &filters;
		filter_info.handled.resize(filters.size(), false);
		filter_info.user_data = rt_info.user_data ? rt_info.user_data->GetData() : nullptr;
		filter_info.column_ids = &get.GetColumnIds();

		auto info_handle = reinterpret_cast<duckdb_v2_table_function_filter_info_handle>(&filter_info);
		auto ctx_ptr = reinterpret_cast<duckdb_v2_context_handle>(&context);

		InvokeWithErrorSlot<InvalidInputException>([&](duckdb_v2_error_info_handle *err_ptr) {
			rt_info.pushdown_complex_filter_cb(bind.user_data ? bind.user_data->GetData() : nullptr, info_handle,
			                                   ctx_ptr, err_ptr);
		});

		// Drop the filters the callback claimed; the engine applies whatever
		// remains above the scan.
		vector<unique_ptr<Expression>> remaining;
		for (idx_t i = 0; i < filters.size(); i++) {
			if (!filter_info.handled[i]) {
				remaining.push_back(std::move(filters[i]));
			}
		}
		filters = std::move(remaining);
	}
};

} // namespace
} // namespace duckdb

// --- Public C API entry points -----------------------------------------------

using duckdb::TableFunctionBindDataV2;
using duckdb::TableFunctionBindInfoV2;
using duckdb::TableFunctionBuilderV2;
using duckdb::TableFunctionExecInfoV2;
using duckdb::TableFunctionFilterInfoV2;
using duckdb::TableFunctionGlobalStateV2;
using duckdb::TableFunctionInitInfoV2;
using duckdb::TableFunctionLocalStateV2;
using duckdb::WithErrorHandler;

DUCKDB_V2_ERROR duckdb_v2_table_function_builder_create(duckdb_v2_context_handle context,
                                                        duckdb_v2_table_function_builder_handle *out,
                                                        duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out = reinterpret_cast<duckdb_v2_table_function_builder_handle>(new TableFunctionBuilderV2());
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_builder_destroy(duckdb_v2_table_function_builder_handle *builder) {
	return WithErrorHandler(nullptr, [&]() {
		if (!builder || !*builder) {
			return;
		}
		delete reinterpret_cast<TableFunctionBuilderV2 *>(*builder);
		*builder = nullptr;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_builder_set_name(duckdb_v2_table_function_builder_handle builder,
                                                          duckdb_v2_identifier_t name,
                                                          duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (name.len == 0 || !name.ptr) {
			throw duckdb::InvalidInputException("Function name cannot be null or empty.");
		}
		reinterpret_cast<TableFunctionBuilderV2 *>(builder)->name = duckdb::ToIdentifier(name);
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_builder_set_signature(duckdb_v2_table_function_builder_handle builder,
                                                               duckdb_v2_function_signature_handle sig,
                                                               duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		// Copy the borrowed signature in; the caller keeps ownership. The routing
		// onto positional / named arguments happens at registration.
		reinterpret_cast<TableFunctionBuilderV2 *>(builder)->signature = *duckdb::ToFunctionSignature(sig);
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_builder_set_user_data(duckdb_v2_table_function_builder_handle builder,
                                                               duckdb_v2_opaque data,
                                                               duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		auto &b = *reinterpret_cast<TableFunctionBuilderV2 *>(builder);
		b.info.user_data = duckdb::make_shared_ptr<duckdb::OpaqueDataHandle>(data.ptr, data.destroy, data.equals);
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_builder_set_projection_pushdown(duckdb_v2_table_function_builder_handle builder, bool enable,
                                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		reinterpret_cast<TableFunctionBuilderV2 *>(builder)->projection_pushdown = enable;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_builder_set_bind_callback(duckdb_v2_table_function_builder_handle builder,
                                                                   duckdb_v2_table_function_bind_fn callback,
                                                                   duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		reinterpret_cast<TableFunctionBuilderV2 *>(builder)->info.bind_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_builder_set_init_global_callback(duckdb_v2_table_function_builder_handle builder,
                                                          duckdb_v2_table_function_init_global_fn callback,
                                                          duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		reinterpret_cast<TableFunctionBuilderV2 *>(builder)->info.init_global_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_builder_set_init_local_callback(duckdb_v2_table_function_builder_handle builder,
                                                         duckdb_v2_table_function_init_local_fn callback,
                                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		reinterpret_cast<TableFunctionBuilderV2 *>(builder)->info.init_local_cb = callback;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_builder_set_exec_callback(duckdb_v2_table_function_builder_handle builder,
                                                                   duckdb_v2_table_function_exec_fn callback,
                                                                   duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		reinterpret_cast<TableFunctionBuilderV2 *>(builder)->info.exec_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_builder_set_cardinality_callback(duckdb_v2_table_function_builder_handle builder,
                                                          duckdb_v2_table_function_cardinality_fn callback,
                                                          duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		reinterpret_cast<TableFunctionBuilderV2 *>(builder)->info.cardinality_cb = callback;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_builder_set_progress_callback(duckdb_v2_table_function_builder_handle builder,
                                                                       duckdb_v2_table_function_progress_fn callback,
                                                                       duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		reinterpret_cast<TableFunctionBuilderV2 *>(builder)->info.progress_cb = callback;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_builder_set_pushdown_complex_filter_callback(
    duckdb_v2_table_function_builder_handle builder, duckdb_v2_table_function_pushdown_complex_filter_fn callback,
    duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		reinterpret_cast<TableFunctionBuilderV2 *>(builder)->info.pushdown_complex_filter_cb = callback;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_builder_register(duckdb_v2_context_handle context,
                                                          duckdb_v2_table_function_builder_handle builder,
                                                          duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}

		auto &b = *reinterpret_cast<TableFunctionBuilderV2 *>(builder);
		auto &ctx = *reinterpret_cast<duckdb::ClientContext *>(context);

		if (b.name.empty()) {
			throw duckdb::InvalidInputException("Function name must be set before registration.");
		}
		if (!b.info.bind_cb) {
			throw duckdb::InvalidInputException("Bind callback must be set before registration.");
		}
		if (!b.info.exec_cb) {
			throw duckdb::InvalidInputException("Exec callback must be set before registration.");
		}
		// A table function declares its columns in bind; a return type on the
		// signature signals confusion.
		if (b.signature.GetReturnType().id() != duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("A table function signature cannot set a return type.");
		}
		// Structural validity: unique parameter names, defaults trailing.
		b.signature.Verify();

		// Route the signature onto the engine's two argument mechanisms: a
		// parameter without a default is a required positional argument; a
		// parameter with a default is an optional named argument.
		duckdb::vector<duckdb::LogicalType> positional;
		for (idx_t i = 0; i < b.signature.GetParameterCount(); i++) {
			const auto &param = b.signature.GetParameter(i);
			if (!param.HasDefaultValue()) {
				positional.push_back(param.GetType());
			}
		}

		duckdb::TableFunction func(b.name, positional, TableFunctionBuilderV2::ExecCallback,
		                           TableFunctionBuilderV2::BindCallback, TableFunctionBuilderV2::InitGlobalCallback,
		                           TableFunctionBuilderV2::InitLocalCallback);

		func.projection_pushdown = b.projection_pushdown;

		// Wire the varargs type (if any) so the engine expands trailing positional
		// arguments to it during binding.
		if (b.signature.HasVarArgs()) {
			func.SetVarArgs(b.signature.GetVarArgs());
		}

		// Always wire the cardinality hook: it serves both the cardinality
		// callback and the static cardinality set via bind_set_cardinality
		// (a bind-time decision not known at registration). It returns null
		// when neither is provided, which the optimizer treats as no estimate.
		func.cardinality = TableFunctionBuilderV2::CardinalityCallback;
		if (b.info.progress_cb) {
			func.table_scan_progress = TableFunctionBuilderV2::ProgressCallback;
		}
		if (b.info.pushdown_complex_filter_cb) {
			func.pushdown_complex_filter = TableFunctionBuilderV2::PushdownComplexFilterCallback;
		}

		auto *info_handle = new duckdb::TableFunctionRuntimeInfoV2();
		info_handle->bind_cb = b.info.bind_cb;
		info_handle->init_global_cb = b.info.init_global_cb;
		info_handle->init_local_cb = b.info.init_local_cb;
		info_handle->exec_cb = b.info.exec_cb;
		info_handle->cardinality_cb = b.info.cardinality_cb;
		info_handle->progress_cb = b.info.progress_cb;
		info_handle->pushdown_complex_filter_cb = b.info.pushdown_complex_filter_cb;
		info_handle->user_data = b.info.user_data;

		// Route each optional (defaulted) parameter onto the engine's named-parameter
		// declaration map, and remember its default for bind-time injection.
		for (idx_t i = 0; i < b.signature.GetParameterCount(); i++) {
			const auto &param = b.signature.GetParameter(i);
			if (param.HasDefaultValue()) {
				func.named_parameters[param.GetName()] = param.GetType();
				info_handle->named_parameter_defaults[param.GetName()] = *param.GetDefaultValue();
			}
		}

		func.function_info = duckdb::shared_ptr<duckdb::TableFunctionInfo>(info_handle);

		auto &catalog = duckdb::Catalog::GetSystemCatalog(ctx);
		duckdb::CreateTableFunctionInfo tf_info(func);
		tf_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
		catalog.CreateTableFunction(ctx, tf_info);
	});
}

// --- Bind callback methods ---------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_add_result_column(duckdb_v2_table_function_bind_info_handle info,
                                                                duckdb_v2_identifier_t name,
                                                                duckdb_v2_logical_type_handle type,
                                                                duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		if (!name.ptr && name.len > 0) {
			throw duckdb::InvalidInputException("Column name cannot be null.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Column type cannot be null.");
		}
		const auto &ltype = *reinterpret_cast<duckdb::LogicalType *>(type);
		// A result column carries data; a wildcard has no meaning, and an ANY
		// stored here throws InternalException when hit during execution.
		if (duckdb::TypeVisitor::Contains(ltype, duckdb::LogicalTypeId::ANY)) {
			throw duckdb::InvalidInputException("Result column type cannot be ANY.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionBindInfoV2 *>(info);
		cb_info.return_names.emplace_back(duckdb::ToString(name));
		cb_info.return_types.push_back(ltype);
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_set_cardinality(duckdb_v2_table_function_bind_info_handle info,
                                                              idx_t cardinality, bool is_exact,
                                                              duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionBindInfoV2 *>(info);
		cb_info.cardinality = cardinality;
		cb_info.cardinality_is_exact = is_exact;
		cb_info.cardinality_set = true;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_set_bind_data(duckdb_v2_table_function_bind_info_handle info,
                                                            duckdb_v2_opaque data, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionBindInfoV2 *>(info);
		cb_info.bind_data = duckdb::make_uniq<TableFunctionBindDataV2>();
		cb_info.bind_data->user_data =
		    duckdb::make_shared_ptr<duckdb::OpaqueDataHandle>(data.ptr, data.destroy, data.equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_get_user_data(duckdb_v2_table_function_bind_info_handle info,
                                                            void **out_data, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionBindInfoV2 *>(info);
		*out_data = cb_info.user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_get_parameter_count(duckdb_v2_table_function_bind_info_handle info,
                                                                  idx_t *out_count, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output count pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionBindInfoV2 *>(info);
		*out_count = cb_info.parameters.size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_get_parameter(duckdb_v2_table_function_bind_info_handle info, idx_t index,
                                                            duckdb_v2_value_handle *out_value,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		if (!out_value) {
			throw duckdb::InvalidInputException("Output value pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionBindInfoV2 *>(info);
		if (index >= cb_info.parameters.size()) {
			throw duckdb::InvalidInputException("Parameter index %llu out of range (have %llu).", index,
			                                    cb_info.parameters.size());
		}
		*out_value = reinterpret_cast<duckdb_v2_value_handle>(new duckdb::Value(cb_info.parameters[index]));
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_get_named_parameter(duckdb_v2_table_function_bind_info_handle info,
                                                                  duckdb_v2_identifier_t name,
                                                                  duckdb_v2_value_handle *out_value,
                                                                  duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		if (!name.ptr && name.len > 0) {
			throw duckdb::InvalidInputException("Parameter name cannot be null.");
		}
		if (!out_value) {
			throw duckdb::InvalidInputException("Output value pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionBindInfoV2 *>(info);
		auto name_str = duckdb::ToIdentifier(name);
		auto it = cb_info.named_parameters.find(name_str);
		if (it == cb_info.named_parameters.end()) {
			throw duckdb::InvalidInputException("Named parameter '%s' not found.", name_str);
		}
		*out_value = reinterpret_cast<duckdb_v2_value_handle>(new duckdb::Value(it->second));
	});
}

// --- Init callback methods ---------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_table_function_init_get_bind_data(duckdb_v2_table_function_init_info_handle info,
                                                            void **out_data, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionInitInfoV2 *>(info);
		if (cb_info.bind_data && cb_info.bind_data->user_data) {
			*out_data = cb_info.bind_data->user_data->GetData();
		} else {
			*out_data = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_init_get_global_state(duckdb_v2_table_function_init_info_handle info,
                                                               void **out_data, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionInitInfoV2 *>(info);
		*out_data = cb_info.global_state ? cb_info.global_state->user_data.GetData() : nullptr;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_init_set_global_state(duckdb_v2_table_function_init_info_handle info,
                                                               duckdb_v2_opaque data,
                                                               duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionInitInfoV2 *>(info);
		cb_info.set_global_state = duckdb::make_uniq<TableFunctionGlobalStateV2>();
		cb_info.set_global_state->user_data = duckdb::OpaqueDataHandle(data.ptr, data.destroy, data.equals);
		cb_info.set_global_state->max_threads = cb_info.max_threads;
		// Point the read view at what we just set so get_global_state observes it
		// within init_global too, not only after init_local wires global_state.
		cb_info.global_state = cb_info.set_global_state.get();
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_init_set_local_state(duckdb_v2_table_function_init_info_handle info,
                                                              duckdb_v2_opaque data, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionInitInfoV2 *>(info);
		cb_info.set_local_state = duckdb::make_uniq<TableFunctionLocalStateV2>();
		cb_info.set_local_state->user_data = duckdb::OpaqueDataHandle(data.ptr, data.destroy, data.equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_init_set_max_threads(duckdb_v2_table_function_init_info_handle info,
                                                              idx_t max_threads, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionInitInfoV2 *>(info);
		cb_info.max_threads = max_threads;
		if (cb_info.set_global_state) {
			cb_info.set_global_state->max_threads = max_threads;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_init_get_column_count(duckdb_v2_table_function_init_info_handle info,
                                                               idx_t *out_count, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output count pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionInitInfoV2 *>(info);
		*out_count = cb_info.projection_ids.empty() ? cb_info.column_ids.size() : cb_info.projection_ids.size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_init_get_column_index(duckdb_v2_table_function_init_info_handle info,
                                                               idx_t projected_index, idx_t *out_original_index,
                                                               duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		if (!out_original_index) {
			throw duckdb::InvalidInputException("Output index pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionInitInfoV2 *>(info);
		if (cb_info.projection_ids.empty()) {
			if (projected_index >= cb_info.column_ids.size()) {
				throw duckdb::InvalidInputException("Projected index %llu out of range (have %llu).", projected_index,
				                                    cb_info.column_ids.size());
			}
			*out_original_index = cb_info.column_ids[projected_index];
		} else {
			if (projected_index >= cb_info.projection_ids.size()) {
				throw duckdb::InvalidInputException("Projected index %llu out of range (have %llu).", projected_index,
				                                    cb_info.projection_ids.size());
			}
			*out_original_index = cb_info.column_ids[cb_info.projection_ids[projected_index]];
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_init_get_user_data(duckdb_v2_table_function_init_info_handle info,
                                                            void **out_data, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionInitInfoV2 *>(info);
		*out_data = cb_info.user_data;
	});
}

// --- Complex filter pushdown callback methods --------------------------------

DUCKDB_V2_ERROR duckdb_v2_table_function_filter_get_count(duckdb_v2_table_function_filter_info_handle info,
                                                          idx_t *out_count, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Filter info pointer cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output count pointer cannot be null.");
		}
		auto &fi = *reinterpret_cast<TableFunctionFilterInfoV2 *>(info);
		*out_count = fi.expressions->size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_filter_get_expression(duckdb_v2_table_function_filter_info_handle info,
                                                               idx_t index, duckdb_v2_expression_handle *out_expression,
                                                               duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Filter info pointer cannot be null.");
		}
		if (!out_expression) {
			throw duckdb::InvalidInputException("Output expression pointer cannot be null.");
		}
		auto &fi = *reinterpret_cast<TableFunctionFilterInfoV2 *>(info);
		if (index >= fi.expressions->size()) {
			throw duckdb::InvalidInputException("Filter index %llu out of range (have %llu).", index,
			                                    fi.expressions->size());
		}
		*out_expression = reinterpret_cast<duckdb_v2_expression_handle>((*fi.expressions)[index].get());
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_filter_mark_handled(duckdb_v2_table_function_filter_info_handle info,
                                                             idx_t index, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Filter info pointer cannot be null.");
		}
		auto &fi = *reinterpret_cast<TableFunctionFilterInfoV2 *>(info);
		if (index >= fi.handled.size()) {
			throw duckdb::InvalidInputException("Filter index %llu out of range (have %llu).", index,
			                                    fi.handled.size());
		}
		fi.handled[index] = true;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_filter_get_column_count(duckdb_v2_table_function_filter_info_handle info,
                                                                 idx_t *out_count, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Filter info pointer cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output count pointer cannot be null.");
		}
		auto &fi = *reinterpret_cast<TableFunctionFilterInfoV2 *>(info);
		*out_count = fi.column_ids ? fi.column_ids->size() : 0;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_filter_get_column_index(duckdb_v2_table_function_filter_info_handle info,
                                                                 idx_t index, idx_t *out_column_index,
                                                                 duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Filter info pointer cannot be null.");
		}
		if (!out_column_index) {
			throw duckdb::InvalidInputException("Output column index pointer cannot be null.");
		}
		auto &fi = *reinterpret_cast<TableFunctionFilterInfoV2 *>(info);
		if (!fi.column_ids || index >= fi.column_ids->size()) {
			throw duckdb::InvalidInputException("Column position %llu out of range (have %llu).", index,
			                                    fi.column_ids ? fi.column_ids->size() : 0);
		}
		*out_column_index = (*fi.column_ids)[index].GetPrimaryIndex();
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_filter_get_user_data(duckdb_v2_table_function_filter_info_handle info,
                                                              void **out_data, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Filter info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &fi = *reinterpret_cast<TableFunctionFilterInfoV2 *>(info);
		*out_data = fi.user_data;
	});
}

// --- Exec callback methods ---------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_bind_data(duckdb_v2_table_function_exec_info_handle info,
                                                            void **out_data, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Exec info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionExecInfoV2 *>(info);
		if (cb_info.bind_data && cb_info.bind_data->user_data) {
			*out_data = cb_info.bind_data->user_data->GetData();
		} else {
			*out_data = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_global_state(duckdb_v2_table_function_exec_info_handle info,
                                                               void **out_data, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Exec info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionExecInfoV2 *>(info);
		*out_data = cb_info.global_state ? cb_info.global_state->user_data.GetData() : nullptr;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_local_state(duckdb_v2_table_function_exec_info_handle info,
                                                              void **out_data, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Exec info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionExecInfoV2 *>(info);
		*out_data = cb_info.local_state ? cb_info.local_state->user_data.GetData() : nullptr;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_output_chunk(duckdb_v2_table_function_exec_info_handle info,
                                                               duckdb_v2_data_chunk_handle *out_chunk,
                                                               duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Exec info pointer cannot be null.");
		}
		if (!out_chunk) {
			throw duckdb::InvalidInputException("Output chunk pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionExecInfoV2 *>(info);
		if (!cb_info.output) {
			throw duckdb::InvalidInputException("Output chunk is not available in this context.");
		}
		*out_chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(cb_info.output);
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_exec_get_user_data(duckdb_v2_table_function_exec_info_handle info,
                                                            void **out_data, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Exec info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<TableFunctionExecInfoV2 *>(info);
		*out_data = cb_info.user_data;
	});
}
