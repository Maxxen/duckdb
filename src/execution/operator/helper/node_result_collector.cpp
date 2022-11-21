#include "duckdb/execution/operator/helper/node_result_collector.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/main/node_query_result.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {


NodeResultCollector::NodeResultCollector(ClientContext &context, PreparedStatementData &data, const NodeQueryResult::NodeResultCallback &callback, bool parallel)
    : PhysicalResultCollector(data), parallel(parallel), callback(callback) {
}

unique_ptr<QueryResult> NodeResultCollector::GetResult(GlobalSinkState &state) {
    auto &gstate = (NodeCollectorGlobalState &)state;
	if (!gstate.collection) {
		gstate.collection = make_unique<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	}

    auto result = make_unique<NodeQueryResult>(statement_type, properties, names, move(gstate.collection),
	                                                   gstate.context->GetClientProperties(), callback);

    return move(result);
}


//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class NodeCollectorGlobalState : public GlobalSinkState {
public:
	mutex glock;
	unique_ptr<ColumnDataCollection> collection;
	shared_ptr<ClientContext> context;
};

class NodeCollectorLocalState : public LocalSinkState {
public:
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
};

SinkResultType NodeResultCollector::Sink(ExecutionContext &context, GlobalSinkState &gstate_p,
                                                   LocalSinkState &lstate_p, DataChunk &input) const {
	auto &lstate = (NodeCollectorLocalState &)lstate_p;
	lstate.collection->Append(lstate.append_state, input);
	return SinkResultType::NEED_MORE_INPUT;
}

void NodeResultCollector::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                            LocalSinkState &lstate_p) const {
	auto &gstate = (NodeCollectorGlobalState &)gstate_p;
	auto &lstate = (NodeCollectorLocalState &)lstate_p;
	if (lstate.collection->Count() == 0) {
		return;
	}

	lock_guard<mutex> l(gstate.glock);
	if (!gstate.collection) {
		gstate.collection = move(lstate.collection);
	} else {
		gstate.collection->Combine(*lstate.collection);
	}
}

unique_ptr<GlobalSinkState> NodeResultCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<NodeCollectorGlobalState>();
	state->context = context.shared_from_this();
	return move(state);
}

unique_ptr<LocalSinkState> NodeResultCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_unique<NodeCollectorLocalState>();
	state->collection = make_unique<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	state->collection->InitializeAppend(state->append_state);
	return move(state);
}

bool NodeResultCollector::ParallelSink() const {
	return parallel;
}

} // namespace duckdb


//std::function<unique_ptr<PhysicalResultCollector>(ClientContext &context, PreparedStatementData &data)>namespace node_duckdb {

        
        /*
        state.
        

        Napi::Array result_arr(Napi::Array::New(env, materialized_result->RowCount() + 1));

        auto deleter = [](Napi::Env, void *finalizeData, void *hint) {
            delete static_cast<std::shared_ptr<QueryResult> *>(hint);
        };

        std::shared_ptr<QueryResult> result_ptr = move(result);

        idx_t out_idx = 1;
        while (true) {
            auto chunk = result_ptr->Fetch();

            if (!chunk || chunk->size() == 0) {
                break;
            }

            D_ASSERT(chunk->ColumnCount() == 2);
            D_ASSERT(chunk->data[0].GetType() == LogicalType::BLOB);
            D_ASSERT(chunk->data[1].GetType() == LogicalType::BOOLEAN);

            for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
                string_t blob = ((string_t *)(chunk->data[0].GetData()))[row_idx];
                bool is_header = chunk->data[1].GetData()[row_idx];

                // Create shared pointer to give (shared) ownership to ArrayBuffer, not that for these materialized
                // query results, the string data is owned by the QueryResult
                auto result_ref_ptr = new std::shared_ptr<QueryResult>(result_ptr);

                auto array_buffer = Napi::ArrayBuffer::New(env, (void *)blob.GetDataUnsafe(), blob.GetSize(),
                                                            deleter, result_ref_ptr);

                auto typed_array = Napi::Uint8Array::New(env, blob.GetSize(), array_buffer, 0);

                // TODO we should handle this in duckdb probably
                if (is_header) {
                    result_arr.Set((uint32_t)0, typed_array);
                } else {
                    D_ASSERT(out_idx < materialized_result->RowCount());
                    result_arr.Set(out_idx++, typed_array);
                }
            }
        }

        // TODO we should handle this in duckdb probably
        auto null_arr = Napi::Uint8Array::New(env, 4);
        memset(null_arr.Data(), '\0', 4);
        result_arr.Set(out_idx++, null_arr);

        // Confirm all rows are set
        D_ASSERT(out_idx == materialized_result->RowCount() + 1);

        */
        