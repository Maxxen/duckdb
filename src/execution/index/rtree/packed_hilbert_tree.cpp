#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

// Static packed hilbert tree
class PackedHilbertTree {
private:
    static const idx_t node_size = 16;
    ArenaAllocator &allocator;

    idx_t num_items = 0;
    vector<idx_t> level_bounds;

    data_ptr_t box_data;
    idx_t box_data_size;

public:
    void Add(idx_t count, Vector box_vector, Vector rowid_vector);
    void Build();
    void Search();
};

void PackedHilbertTree::Add(idx_t count, Vector box_vector, Vector rowid_vector) {
    box_vector.Flatten(count);
    rowid_vector.Flatten(count);
    auto &entries = StructVector::GetEntries(box_vector);
    auto min_x_vec = FlatVector::GetData<double>(*entries[0]);
    auto min_y_vec = FlatVector::GetData<double>(*entries[1]);
    auto max_x_vec = FlatVector::GetData<double>(*entries[2]);
    auto max_y_vec = FlatVector::GetData<double>(*entries[3]);
    auto rowid_vec = FlatVector::GetData<row_t>(rowid_vector);
    
    auto additional_size = (sizeof(row_t) + 4 * sizeof(double)) * count;
    box_data = allocator.ReallocateAligned(box_data, box_data_size, box_data_size + additional_size);
    
    auto cursor = box_data + box_data_size;
    for (idx_t i = 0; i < count; i++) {
        auto row_id = rowid_vec[i];
        auto min_x = min_x_vec[i];
        auto min_y = min_y_vec[i];
        auto max_x = max_x_vec[i];
        auto max_y = max_y_vec[i];

        Store<row_t>(row_id, cursor);
        cursor += sizeof(row_t);
        Store<double>(min_x, cursor);
        cursor += sizeof(double);
        Store<double>(min_y, cursor);
        cursor += sizeof(double);
        Store<double>(max_x, cursor);
        cursor += sizeof(double);
        Store<double>(max_y, cursor);
        cursor += sizeof(double);
    }
    
    box_data_size += additional_size;
}

} // namespace duckdb
