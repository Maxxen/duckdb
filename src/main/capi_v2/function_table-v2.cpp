#include "capi_v2_internal.hpp"
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
		if (user_data_copy_cb) {
			copy->user_data = user_data_copy_cb(user_data);
			copy->user_data_destructor_cb = user_data_destructor_cb;
			copy->user_data_copy_cb = user_data_copy_cb;
		} else {
			copy->user_data = user_data;
		}
		copy->user_data_equals_cb = user_data_equals_cb;
		copy->runtime_info = runtime_info;
		copy->cardinality = cardinality;
		copy->cardinality_is_exact = cardinality_is_exact;
		copy->cardinality_set = cardinality_set;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other) const override {
		if (user_data_equals_cb) {
			return user_data_equals_cb(user_data, other.Cast<TableFunctionBindDataV2>().user_data);
		}
		return user_data == other.Cast<TableFunctionBindDataV2>().user_data;
	}

	~TableFunctionBindDataV2() override {
		if (user_data && user_data_destructor_cb) {
			user_data_destructor_cb(user_data);
		}
	}

	void *user_data = nullptr;
	duckdb_v2_user_data_destroy_cb user_data_destructor_cb = nullptr;
	duckdb_v2_user_data_copy_cb user_data_copy_cb = nullptr;
	duckdb_v2_user_data_equals_cb user_data_equals_cb = nullptr;

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
	~TableFunctionGlobalStateV2() override {
		if (user_data && user_data_destructor_cb) {
			user_data_destructor_cb(user_data);
		}
	}

	idx_t MaxThreads() const override {
		return max_threads;
	}

	void *user_data = nullptr;
	duckdb_v2_user_data_destroy_cb user_data_destructor_cb = nullptr;
	idx_t max_threads = 1;
};

struct TableFunctionLocalStateV2 final : public LocalTableFunctionState {
	~TableFunctionLocalStateV2() override {
		if (user_data && user_data_destructor_cb) {
			user_data_destructor_cb(user_data);
		}
	}

	void *user_data = nullptr;
	duckdb_v2_user_data_destroy_cb user_data_destructor_cb = nullptr;
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
	case_insensitive_map_t<Value> named_parameters;
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
};

// --- RuntimeInfo: stored on the TableFunction's function_info ----------------

struct TableFunctionRuntimeInfoV2 final : public TableFunctionInfo {
	duckdb_v2_table_function_bind_cb bind_cb = nullptr;
	duckdb_v2_table_function_init_global_cb init_global_cb = nullptr;
	duckdb_v2_table_function_init_local_cb init_local_cb = nullptr;
	duckdb_v2_table_function_exec_cb exec_cb = nullptr;
	duckdb_v2_table_function_cardinality_cb cardinality_cb = nullptr;
	duckdb_v2_table_function_progress_cb progress_cb = nullptr;
	duckdb_v2_table_function_pushdown_complex_filter_cb pushdown_complex_filter_cb = nullptr;

	void *user_data = nullptr;
	duckdb_v2_user_data_destroy_cb user_data_destructor_cb = nullptr;

	~TableFunctionRuntimeInfoV2() override {
		if (user_data && user_data_destructor_cb) {
			user_data_destructor_cb(user_data);
		}
	}
};

// --- Builder struct ----------------------------------------------------------

struct TableFunctionBuilderV2 {
	string name;
	TableFunctionRuntimeInfoV2 info;
	vector<LogicalType> parameters;
	case_insensitive_map_t<LogicalType> named_parameters;
	bool projection_pushdown = false;

	static unique_ptr<FunctionData> BindCallback(ClientContext &context, TableFunctionBindInput &input,
	                                             vector<LogicalType> &return_types, vector<string> &names) {
		auto &rt_info = input.info->Cast<TableFunctionRuntimeInfoV2>();
		D_ASSERT(rt_info.bind_cb);

		TableFunctionBindInfoV2 cb_info;
		cb_info.user_data = rt_info.user_data;
		for (auto &v : input.inputs) {
			cb_info.parameters.push_back(v);
		}
		cb_info.named_parameters = input.named_parameters;

		auto info_ptr = static_cast<duckdb_v2_table_function_bind_info_ptr>(&cb_info);
		auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&context);

		InvokeWithErrorSlot<BinderException>(
		    [&](duckdb_v2_error_info_ptr *err_ptr) { rt_info.bind_cb(info_ptr, ctx_ptr, err_ptr); });

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
		cb_info.user_data = rt_info.user_data;
		cb_info.column_ids = input.column_ids;
		cb_info.projection_ids.assign(input.projection_ids.begin(), input.projection_ids.end());

		auto info_ptr = static_cast<duckdb_v2_table_function_init_info_ptr>(&cb_info);
		auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&context);

		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_ptr *err_ptr) { rt_info.init_global_cb(info_ptr, ctx_ptr, err_ptr); });

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
		cb_info.user_data = rt_info.user_data;
		cb_info.column_ids = input.column_ids;
		cb_info.projection_ids.assign(input.projection_ids.begin(), input.projection_ids.end());

		auto info_ptr = static_cast<duckdb_v2_table_function_init_info_ptr>(&cb_info);
		auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&context.client);

		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_ptr *err_ptr) { rt_info.init_local_cb(info_ptr, ctx_ptr, err_ptr); });

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
		cb_info.user_data = bind.runtime_info->user_data;

		auto info_ptr = static_cast<duckdb_v2_table_function_exec_info_ptr>(&cb_info);
		auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&context);

		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_ptr *err_ptr) { bind.runtime_info->exec_cb(info_ptr, ctx_ptr, err_ptr); });

		// The V2 vector-write API sizes per vector (vector_set_size); the engine
		// reads the chunk's cardinality. A table-function scan always has at
		// least one output column, so derive the row count from the first
		// vector's size. Returning cardinality 0 signals end of data.
		output.SetCardinality(output.data[0].size());
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
			auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&context);

			InvokeWithErrorSlot<InvalidInputException>([&](duckdb_v2_error_info_ptr *err_ptr) {
				rt_info.cardinality_cb(bind.user_data, &estimated, &is_exact, ctx_ptr, err_ptr);
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
			global_user_data = global_state->Cast<TableFunctionGlobalStateV2>().user_data;
		}

		double progress = 0.0;
		auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&context);

		InvokeWithErrorSlot<InvalidInputException>([&](duckdb_v2_error_info_ptr *err_ptr) {
			rt_info.progress_cb(bind.user_data, global_user_data, &progress, ctx_ptr, err_ptr);
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

		auto info_ptr = static_cast<duckdb_v2_table_function_filter_info_ptr>(&filter_info);
		auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&context);

		InvokeWithErrorSlot<InvalidInputException>([&](duckdb_v2_error_info_ptr *err_ptr) {
			rt_info.pushdown_complex_filter_cb(bind.user_data, info_ptr, ctx_ptr, err_ptr);
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

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_create(duckdb_v2_context_ptr context,
                                                             duckdb_v2_table_function_builder_ptr *out,
                                                             duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out = static_cast<duckdb_v2_table_function_builder_ptr>(new TableFunctionBuilderV2());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_destroy(duckdb_v2_table_function_builder_ptr *builder) {
	return WithErrorHandler(nullptr, [&]() {
		if (!builder || !*builder) {
			return;
		}
		delete static_cast<TableFunctionBuilderV2 *>(*builder);
		*builder = nullptr;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_set_name(duckdb_v2_table_function_builder_ptr builder,
                                                               const char *name, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (!name || strlen(name) == 0) {
			throw duckdb::InvalidInputException("Function name cannot be null or empty.");
		}
		static_cast<TableFunctionBuilderV2 *>(builder)->name = name;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_add_parameter(duckdb_v2_table_function_builder_ptr builder,
                                                                    duckdb_v2_logical_type_ptr type,
                                                                    duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Parameter type cannot be null.");
		}
		auto &b = *static_cast<TableFunctionBuilderV2 *>(builder);
		b.parameters.push_back(*static_cast<duckdb::LogicalType *>(type));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_add_named_parameter(duckdb_v2_table_function_builder_ptr builder,
                                                                          const char *name,
                                                                          duckdb_v2_logical_type_ptr type,
                                                                          duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (!name || strlen(name) == 0) {
			throw duckdb::InvalidInputException("Parameter name cannot be null or empty.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Parameter type cannot be null.");
		}
		auto &b = *static_cast<TableFunctionBuilderV2 *>(builder);
		b.named_parameters[name] = *static_cast<duckdb::LogicalType *>(type);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_set_user_data(duckdb_v2_table_function_builder_ptr builder,
                                                                    void *data, duckdb_v2_user_data_destroy_cb destroy,
                                                                    duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		auto &b = *static_cast<TableFunctionBuilderV2 *>(builder);
		b.info.user_data = data;
		b.info.user_data_destructor_cb = destroy;
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_table_function_builder_set_projection_pushdown(duckdb_v2_table_function_builder_ptr builder, bool enable,
                                                         duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		static_cast<TableFunctionBuilderV2 *>(builder)->projection_pushdown = enable;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_set_bind_cb(duckdb_v2_table_function_builder_ptr builder,
                                                                  duckdb_v2_table_function_bind_cb callback,
                                                                  duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		static_cast<TableFunctionBuilderV2 *>(builder)->info.bind_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_table_function_builder_set_init_global_cb(duckdb_v2_table_function_builder_ptr builder,
                                                    duckdb_v2_table_function_init_global_cb callback,
                                                    duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		static_cast<TableFunctionBuilderV2 *>(builder)->info.init_global_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_set_init_local_cb(duckdb_v2_table_function_builder_ptr builder,
                                                                        duckdb_v2_table_function_init_local_cb callback,
                                                                        duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		static_cast<TableFunctionBuilderV2 *>(builder)->info.init_local_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_set_exec_cb(duckdb_v2_table_function_builder_ptr builder,
                                                                  duckdb_v2_table_function_exec_cb callback,
                                                                  duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		static_cast<TableFunctionBuilderV2 *>(builder)->info.exec_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_table_function_builder_set_cardinality_cb(duckdb_v2_table_function_builder_ptr builder,
                                                    duckdb_v2_table_function_cardinality_cb callback,
                                                    duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		static_cast<TableFunctionBuilderV2 *>(builder)->info.cardinality_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_set_progress_cb(duckdb_v2_table_function_builder_ptr builder,
                                                                      duckdb_v2_table_function_progress_cb callback,
                                                                      duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		static_cast<TableFunctionBuilderV2 *>(builder)->info.progress_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_set_pushdown_complex_filter_cb(
    duckdb_v2_table_function_builder_ptr builder, duckdb_v2_table_function_pushdown_complex_filter_cb callback,
    duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		static_cast<TableFunctionBuilderV2 *>(builder)->info.pushdown_complex_filter_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_builder_register(duckdb_v2_context_ptr context,
                                                               duckdb_v2_table_function_builder_ptr builder,
                                                               duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}

		auto &b = *static_cast<TableFunctionBuilderV2 *>(builder);
		auto &ctx = *static_cast<duckdb::ClientContext *>(context);

		if (b.name.empty()) {
			throw duckdb::InvalidInputException("Function name must be set before registration.");
		}
		if (!b.info.bind_cb) {
			throw duckdb::InvalidInputException("Bind callback must be set before registration.");
		}
		if (!b.info.exec_cb) {
			throw duckdb::InvalidInputException("Exec callback must be set before registration.");
		}

		duckdb::TableFunction func(b.name, b.parameters, TableFunctionBuilderV2::ExecCallback,
		                           TableFunctionBuilderV2::BindCallback, TableFunctionBuilderV2::InitGlobalCallback,
		                           TableFunctionBuilderV2::InitLocalCallback);

		func.projection_pushdown = b.projection_pushdown;

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

		auto *info_ptr = new duckdb::TableFunctionRuntimeInfoV2();
		info_ptr->bind_cb = b.info.bind_cb;
		info_ptr->init_global_cb = b.info.init_global_cb;
		info_ptr->init_local_cb = b.info.init_local_cb;
		info_ptr->exec_cb = b.info.exec_cb;
		info_ptr->cardinality_cb = b.info.cardinality_cb;
		info_ptr->progress_cb = b.info.progress_cb;
		info_ptr->pushdown_complex_filter_cb = b.info.pushdown_complex_filter_cb;
		info_ptr->user_data = b.info.user_data;
		info_ptr->user_data_destructor_cb = b.info.user_data_destructor_cb;
		b.info.user_data = nullptr;
		b.info.user_data_destructor_cb = nullptr;
		func.function_info = duckdb::shared_ptr<duckdb::TableFunctionInfo>(info_ptr);

		for (auto &[name, type] : b.named_parameters) {
			func.named_parameters[name] = type;
		}

		auto &catalog = duckdb::Catalog::GetSystemCatalog(ctx);
		duckdb::CreateTableFunctionInfo tf_info(func);
		tf_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
		catalog.CreateTableFunction(ctx, tf_info);
	});
}

// --- Bind callback methods ---------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_bind_add_result_column(duckdb_v2_table_function_bind_info_ptr info,
                                                                     const char *name, duckdb_v2_logical_type_ptr type,
                                                                     duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		if (!name) {
			throw duckdb::InvalidInputException("Column name cannot be null.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Column type cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionBindInfoV2 *>(info);
		cb_info.return_names.emplace_back(name);
		cb_info.return_types.push_back(*static_cast<duckdb::LogicalType *>(type));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_bind_set_cardinality(duckdb_v2_table_function_bind_info_ptr info,
                                                                   idx_t cardinality, bool is_exact,
                                                                   duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionBindInfoV2 *>(info);
		cb_info.cardinality = cardinality;
		cb_info.cardinality_is_exact = is_exact;
		cb_info.cardinality_set = true;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_bind_set_bind_data(duckdb_v2_table_function_bind_info_ptr info,
                                                                 void *data, duckdb_v2_user_data_copy_cb copy,
                                                                 duckdb_v2_user_data_equals_cb equals,
                                                                 duckdb_v2_user_data_destroy_cb destroy,
                                                                 duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionBindInfoV2 *>(info);
		cb_info.bind_data = duckdb::make_uniq<TableFunctionBindDataV2>();
		cb_info.bind_data->user_data = data;
		cb_info.bind_data->user_data_copy_cb = copy;
		cb_info.bind_data->user_data_equals_cb = equals;
		cb_info.bind_data->user_data_destructor_cb = destroy;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_bind_get_user_data(duckdb_v2_table_function_bind_info_ptr info,
                                                                 void **out_data, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionBindInfoV2 *>(info);
		*out_data = cb_info.user_data;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_bind_get_parameter(duckdb_v2_table_function_bind_info_ptr info,
                                                                 idx_t index, duckdb_v2_value_ptr *out_value,
                                                                 duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		if (!out_value) {
			throw duckdb::InvalidInputException("Output value pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionBindInfoV2 *>(info);
		if (index >= cb_info.parameters.size()) {
			throw duckdb::InvalidInputException("Parameter index %llu out of range (have %llu).", index,
			                                    cb_info.parameters.size());
		}
		*out_value = static_cast<duckdb_v2_value_ptr>(new duckdb::Value(cb_info.parameters[index]));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_bind_get_named_parameter(duckdb_v2_table_function_bind_info_ptr info,
                                                                       const char *name, duckdb_v2_value_ptr *out_value,
                                                                       duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}
		if (!name) {
			throw duckdb::InvalidInputException("Parameter name cannot be null.");
		}
		if (!out_value) {
			throw duckdb::InvalidInputException("Output value pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionBindInfoV2 *>(info);
		auto it = cb_info.named_parameters.find(name);
		if (it == cb_info.named_parameters.end()) {
			throw duckdb::InvalidInputException("Named parameter '%s' not found.", name);
		}
		*out_value = static_cast<duckdb_v2_value_ptr>(new duckdb::Value(it->second));
	});
}

// --- Init callback methods ---------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_init_get_bind_data(duckdb_v2_table_function_init_info_ptr info,
                                                                 void **out_data, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionInitInfoV2 *>(info);
		*out_data = cb_info.bind_data ? cb_info.bind_data->user_data : nullptr;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_init_get_global_state(duckdb_v2_table_function_init_info_ptr info,
                                                                    void **out_data, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionInitInfoV2 *>(info);
		*out_data = cb_info.global_state ? cb_info.global_state->user_data : nullptr;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_init_set_global_state(duckdb_v2_table_function_init_info_ptr info,
                                                                    void *data, duckdb_v2_user_data_destroy_cb destroy,
                                                                    duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionInitInfoV2 *>(info);
		cb_info.set_global_state = duckdb::make_uniq<TableFunctionGlobalStateV2>();
		cb_info.set_global_state->user_data = data;
		cb_info.set_global_state->user_data_destructor_cb = destroy;
		cb_info.set_global_state->max_threads = cb_info.max_threads;
		// Point the read view at what we just set so get_global_state observes it
		// within init_global too, not only after init_local wires global_state.
		cb_info.global_state = cb_info.set_global_state.get();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_init_set_local_state(duckdb_v2_table_function_init_info_ptr info,
                                                                   void *data, duckdb_v2_user_data_destroy_cb destroy,
                                                                   duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionInitInfoV2 *>(info);
		cb_info.set_local_state = duckdb::make_uniq<TableFunctionLocalStateV2>();
		cb_info.set_local_state->user_data = data;
		cb_info.set_local_state->user_data_destructor_cb = destroy;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_init_set_max_threads(duckdb_v2_table_function_init_info_ptr info,
                                                                   idx_t max_threads, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionInitInfoV2 *>(info);
		cb_info.max_threads = max_threads;
		if (cb_info.set_global_state) {
			cb_info.set_global_state->max_threads = max_threads;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_init_get_column_count(duckdb_v2_table_function_init_info_ptr info,
                                                                    idx_t *out_count, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output count pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionInitInfoV2 *>(info);
		*out_count = cb_info.projection_ids.empty() ? cb_info.column_ids.size() : cb_info.projection_ids.size();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_init_get_column_index(duckdb_v2_table_function_init_info_ptr info,
                                                                    idx_t projected_index, idx_t *out_original_index,
                                                                    duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		if (!out_original_index) {
			throw duckdb::InvalidInputException("Output index pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionInitInfoV2 *>(info);
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

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_init_get_user_data(duckdb_v2_table_function_init_info_ptr info,
                                                                 void **out_data, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Init info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionInitInfoV2 *>(info);
		*out_data = cb_info.user_data;
	});
}

// --- Complex filter pushdown callback methods --------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_filter_get_count(duckdb_v2_table_function_filter_info_ptr info,
                                                               idx_t *out_count, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Filter info pointer cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output count pointer cannot be null.");
		}
		auto &fi = *static_cast<TableFunctionFilterInfoV2 *>(info);
		*out_count = fi.expressions->size();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_filter_get_expression(duckdb_v2_table_function_filter_info_ptr info,
                                                                    idx_t index,
                                                                    duckdb_v2_expression_ptr *out_expression,
                                                                    duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Filter info pointer cannot be null.");
		}
		if (!out_expression) {
			throw duckdb::InvalidInputException("Output expression pointer cannot be null.");
		}
		auto &fi = *static_cast<TableFunctionFilterInfoV2 *>(info);
		if (index >= fi.expressions->size()) {
			throw duckdb::InvalidInputException("Filter index %llu out of range (have %llu).", index,
			                                    fi.expressions->size());
		}
		*out_expression = static_cast<duckdb_v2_expression_ptr>((*fi.expressions)[index].get());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_filter_mark_handled(duckdb_v2_table_function_filter_info_ptr info,
                                                                  idx_t index, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Filter info pointer cannot be null.");
		}
		auto &fi = *static_cast<TableFunctionFilterInfoV2 *>(info);
		if (index >= fi.handled.size()) {
			throw duckdb::InvalidInputException("Filter index %llu out of range (have %llu).", index,
			                                    fi.handled.size());
		}
		fi.handled[index] = true;
	});
}

// --- Exec callback methods ---------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_exec_get_bind_data(duckdb_v2_table_function_exec_info_ptr info,
                                                                 void **out_data, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Exec info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionExecInfoV2 *>(info);
		*out_data = cb_info.bind_data ? cb_info.bind_data->user_data : nullptr;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_exec_get_global_state(duckdb_v2_table_function_exec_info_ptr info,
                                                                    void **out_data, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Exec info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionExecInfoV2 *>(info);
		*out_data = cb_info.global_state ? cb_info.global_state->user_data : nullptr;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_exec_get_local_state(duckdb_v2_table_function_exec_info_ptr info,
                                                                   void **out_data, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Exec info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionExecInfoV2 *>(info);
		*out_data = cb_info.local_state ? cb_info.local_state->user_data : nullptr;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_exec_get_output_chunk(duckdb_v2_table_function_exec_info_ptr info,
                                                                    duckdb_v2_data_chunk_ptr *out_chunk,
                                                                    duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Exec info pointer cannot be null.");
		}
		if (!out_chunk) {
			throw duckdb::InvalidInputException("Output chunk pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionExecInfoV2 *>(info);
		if (!cb_info.output) {
			throw duckdb::InvalidInputException("Output chunk is not available in this context.");
		}
		*out_chunk = static_cast<duckdb_v2_data_chunk_ptr>(cb_info.output);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_table_function_exec_get_user_data(duckdb_v2_table_function_exec_info_ptr info,
                                                                 void **out_data, duckdb_v2_error_info_ptr *err) {
	return WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Exec info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}
		auto &cb_info = *static_cast<TableFunctionExecInfoV2 *>(info);
		*out_data = cb_info.user_data;
	});
}
