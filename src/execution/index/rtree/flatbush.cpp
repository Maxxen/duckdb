#include "duckdb/common/radix.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/rtree/rtree.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

//------------------------------------------------------------------------------
// Flatbush algorithm
//------------------------------------------------------------------------------

struct Box {
	row_t index;
	double x1, y1, x2, y2;

	// Create a default box that is invalid
	static constexpr Box Default() {
		return Box {0, std::numeric_limits<double>::max(), std::numeric_limits<double>::max(),
		            std::numeric_limits<double>::min(), std::numeric_limits<double>::min()};
	}

	void Union(const Box &other) {
		x1 = std::min(x1, other.x1);
		y1 = std::min(y1, other.y1);
		x2 = std::max(x2, other.x2);
		y2 = std::max(y2, other.y2);
	}
};

struct StackEntry {
	idx_t node_idx;
	idx_t level_idx;
};

class FlatBush {
private:
	idx_t node_size = 16;
	idx_t num_items = 0;
	Box total_bounds = Box::Default();

	vector<Box> boxes;
	vector<uint32_t> hilbert_values;
	vector<idx_t> level_bounds;

public:
	void Reserve(idx_t num_entries);
	idx_t Add(double minx, double miny, double maxx, double maxy);
	void Sort(uint32_t *values, Box *boxes, idx_t left, idx_t right);
	void Finish();
	void Search(double minx, double miny, double maxx, double maxy, vector<idx_t> &result) const;
};

void FlatBush::Reserve(idx_t size) {
	auto n = size;
	auto num_nodes = n;
	do {
		n = (n + node_size - 1) / node_size;
		num_nodes += n;
	} while (n > 1);
	boxes.reserve(num_nodes);
}

idx_t FlatBush::Add(double minx, double miny, double maxx, double maxy) {
	idx_t index = boxes.size();
	boxes.push_back(Box {(row_t)index, minx, miny, maxx, maxy});
	// Update total bounds
	total_bounds.x1 = std::min(total_bounds.x1, minx);
	total_bounds.y1 = std::min(total_bounds.y1, miny);
	total_bounds.x2 = std::max(total_bounds.x2, maxx);
	total_bounds.y2 = std::max(total_bounds.y2, maxy);
	return index;
}

// Pairwise quicksort
void Sort(uint32_t *values, Box *boxes, idx_t left, idx_t right) {
	if (left >= right)
		return;

	uint32_t pivot = values[(left + right) >> 1];
	size_t i = left - 1;
	size_t j = right + 1;

	while (true) {
		do {
			i++;
		} while (values[i] < pivot);
		do {
			j--;
		} while (values[j] > pivot);
		if (i >= j) {
			break;
		}
		std::swap(values[i], values[j]);
		std::swap(boxes[i], boxes[j]);
	}

	Sort(values, boxes, left, j);
	Sort(values, boxes, j + 1, right);
}

void FlatBush::Sort(uint32_t *values, Box *boxes, idx_t left, idx_t right) {
    Sort(values, boxes, left, right);
}

void FlatBush::Finish() {

	num_items = boxes.size();

	auto n = num_items;
	auto num_nodes = n;
	level_bounds.push_back(num_nodes);
	do {
		n = (n + node_size - 1) / node_size;
		num_nodes += n;
		level_bounds.push_back(num_nodes);
	} while (n > 1);

	auto width = total_bounds.x2 - total_bounds.x1;
	auto height = total_bounds.y2 - total_bounds.y1;
	hilbert_values.resize(boxes.size());
	double hilbert_max = (double)(1 << 16) - 1;

	// Convert box centroids to hilbert values
	for (idx_t i = 0; i < boxes.size(); i++) {
		auto &box = boxes[i];
		auto x = (box.x1 - total_bounds.x1) / width;
		auto y = (box.y1 - total_bounds.y1) / height;
		auto h = HilbertXYToIndex(16, x * hilbert_max, y * hilbert_max);
		hilbert_values[i] = h;
	}

	// Sort the boxes by their hilbert values
	Sort(&hilbert_values[0], &boxes[0], 0, boxes.size() - 1);

	// Create the tree, bottom up
	idx_t box_idx = 0;
	for (idx_t i = 0; i < level_bounds.size() - 1; i++) {
		idx_t end = level_bounds[i];

		while (box_idx < end) {
			auto node_box = Box::Default();
			node_box.index = box_idx;

			// Calculate bounds for the node
			for (idx_t j = 0; j < node_size && box_idx < end; j++) {
				node_box.Union(boxes[box_idx++]);
			}
			// Add the node to the tree
			boxes.push_back(node_box);
		}
	}
}



void FlatBush::Search(double minx, double miny, double maxx, double maxy, vector<idx_t> &result) const {
	D_ASSERT(level_bounds.size() != 0); // Not constructed

	vector<StackEntry> stack;
	// Start from the topmost node and work down
	stack.push_back(StackEntry {boxes.size() - 1, level_bounds.size() - 1});

	while (!stack.empty()) {
		auto entry = stack.back();
		stack.pop_back();

		auto node_idx = entry.node_idx;
		auto level = entry.level_idx;

		// End index of the node
		auto end = std::min(node_idx + node_size, level_bounds[level]);

		// Search through the children of the node
		for (idx_t child_idx = node_idx; child_idx < end; child_idx++) {
			auto &child = boxes[child_idx];
			if (child.x1 > maxx || child.x2 < minx || child.y1 > maxy || child.y2 < miny) {
				// Child is outside the search area
				continue;
			}
			if (node_idx < num_items) {
				// Leaf
				result.push_back(boxes[child_idx].index);
			} else {
				// Not a leaf, add the child to the stack
				stack.push_back(StackEntry {child_idx, level - 1});
			}
		}
	}
}

// From https://github.com/rawrunprotected/hilbert_curves (public domain)
static uint32_t Interleave(uint32_t x) {
	x = (x | (x << 8)) & 0x00FF00FF;
	x = (x | (x << 4)) & 0x0F0F0F0F;
	x = (x | (x << 2)) & 0x33333333;
	x = (x | (x << 1)) & 0x55555555;
	return x;
}

static uint32_t HilbertXYToIndex(uint32_t n, uint32_t x, uint32_t y) {
	x = x << (16 - n);
	y = y << (16 - n);

	uint32_t A, B, C, D;

	// Initial prefix scan round, prime with x and y
	{
		uint32_t a = x ^ y;
		uint32_t b = 0xFFFF ^ a;
		uint32_t c = 0xFFFF ^ (x | y);
		uint32_t d = x & (y ^ 0xFFFF);

		A = a | (b >> 1);
		B = (a >> 1) ^ a;

		C = ((c >> 1) ^ (b & (d >> 1))) ^ c;
		D = ((a & (c >> 1)) ^ (d >> 1)) ^ d;
	}

	{
		uint32_t a = A;
		uint32_t b = B;
		uint32_t c = C;
		uint32_t d = D;

		A = ((a & (a >> 2)) ^ (b & (b >> 2)));
		B = ((a & (b >> 2)) ^ (b & ((a ^ b) >> 2)));

		C ^= ((a & (c >> 2)) ^ (b & (d >> 2)));
		D ^= ((b & (c >> 2)) ^ ((a ^ b) & (d >> 2)));
	}

	{
		uint32_t a = A;
		uint32_t b = B;
		uint32_t c = C;
		uint32_t d = D;

		A = ((a & (a >> 4)) ^ (b & (b >> 4)));
		B = ((a & (b >> 4)) ^ (b & ((a ^ b) >> 4)));

		C ^= ((a & (c >> 4)) ^ (b & (d >> 4)));
		D ^= ((b & (c >> 4)) ^ ((a ^ b) & (d >> 4)));
	}

	// Final round and projection
	{
		uint32_t a = A;
		uint32_t b = B;
		uint32_t c = C;
		uint32_t d = D;

		C ^= ((a & (c >> 8)) ^ (b & (d >> 8)));
		D ^= ((b & (c >> 8)) ^ ((a ^ b) & (d >> 8)));
	}

	// Undo transformation prefix scan
	uint32_t a = C ^ (C >> 1);
	uint32_t b = D ^ (D >> 1);

	// Recover index bits
	uint32_t i0 = x ^ y;
	uint32_t i1 = b | (0xFFFF ^ (i0 | a));

	return ((Interleave(i1) << 1) | Interleave(i0)) >> (32 - 2 * n);
}



//-------------------------------------------------------------------
// One-off algorithm
//-------------------------------------------------------------------
static void FlatBushAlgorithm(idx_t count, Vector row_ids, Vector box_vector) {

    // Setup
    box_vector.Flatten(count);
    row_ids.Flatten(count);
    auto &entries = StructVector::GetEntries(box_vector);
    auto min_x_vec = FlatVector::GetData<double>(*entries[0]);
    auto min_y_vec = FlatVector::GetData<double>(*entries[1]);
    auto max_x_vec = FlatVector::GetData<double>(*entries[2]);
    auto max_y_vec = FlatVector::GetData<double>(*entries[3]);
    auto row_id_data = FlatVector::GetData<row_t>(row_ids);

    // State
    const idx_t node_size = 16;
    vector<uint32_t> hilbert_values(count);
    vector<idx_t> level_bounds;
    auto total_bounds = Box::Default();

    auto n = count;
	auto num_nodes = n;
	level_bounds.push_back(num_nodes);
	do {
		n = (n + node_size - 1) / node_size;
		num_nodes += n;
		level_bounds.push_back(num_nodes);
	} while (n > 1);

    // Note: This also include the branch nodes. which is != count (entries)
    vector<Box> boxes(num_nodes);

    // First pass: Create boxes and calculate total bounds
    for (idx_t i = 0; i < count; i++) {
        // TODO: We should insert the row id instead of i when creating the box here?
        auto row_id = row_id_data[i];
        auto box = Box {row_id, min_x_vec[i], min_y_vec[i], max_x_vec[i], max_y_vec[i]};
        boxes[i] = box;
        total_bounds.Union(box);
    }

	auto width = total_bounds.x2 - total_bounds.x1;
	auto height = total_bounds.y2 - total_bounds.y1;
	double hilbert_max = (double)(1 << 16) - 1;

	// Convert box centroids to hilbert values
	for (idx_t i = 0; i < boxes.size(); i++) {
		auto &box = boxes[i];
		auto x = (box.x1 - total_bounds.x1) / width;
		auto y = (box.y1 - total_bounds.y1) / height;
		auto h = HilbertXYToIndex(16, x * hilbert_max, y * hilbert_max);
		hilbert_values[i] = h;
	}

    // Sort the boxes by their hilbert values
	Sort(&hilbert_values[0], &boxes[0], 0, boxes.size() - 1);

	// Create the tree, bottom up
	idx_t box_idx = 0;
	for (idx_t i = 0; i < level_bounds.size() - 1; i++) {
		idx_t end = level_bounds[i];

		while (box_idx < end) {
			auto node_box = Box::Default();
			node_box.index = box_idx;

			// Calculate bounds for the node
			for (idx_t j = 0; j < node_size && box_idx < end; j++) {
				node_box.Union(boxes[box_idx++]);
			}
			// Add the node to the tree
			boxes.push_back(node_box);
		}
	}
}





} // namespace duckdb