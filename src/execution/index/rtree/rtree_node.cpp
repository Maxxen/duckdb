#include "duckdb/execution/index/art/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/swizzleable_pointer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/table_io_manager.hpp"

#include "duckdb/storage/index.hpp"

namespace duckdb {

enum class NodeType : uint8_t {
    LeafEntry,
    IndexEntry,
    LeafNode,
    IndexNode
};

struct NodePtr : SwizzleablePointer {
    NodeType GetType() {
        return NodeType(type);
    }
};

struct Rectangle {
    double x1, y1, x2, y2;
};

struct LeafEntry {
    Rectangle mbr;
    row_t row_id;

    static LeafEntry Get(NodePtr ptr) {

    }

    BlockPointer Serialize() {}
};

struct IndexEntry {
    Rectangle mbr;
    NodePtr child;
};

struct Node {
    idx_t level;
    idx_t count;
    NodePtr entries[16];

    void Search(Rectangle &rect, vector<row_t> &results) {
        for(idx_t i = 0; i < count; i++) {
            auto &entry = entries[i];
            switch(entry.GetType()) {
                case NodeType::LeafEntry: {
                    // get the entry, check if it overlaps with the rectangle
                } break;
                case NodeType::IndexEntry: {
                    // get the entry, check if it overlaps with the rectangle
                    // if it does, recurse into the child
                } break;
                default:
                    throw InternalException("Corrupt node");
            }
        }
    }
};

struct RTree {
    NodePtr root;
};


}

namespace woo {
using namespace duckdb;

class RTree;

struct Rectangle {
    double x1, y1, x2, y2;
};

struct LeafNode {
    static constexpr idx_t CAPACITY = 16;
    uint32_t count;
    Rectangle mbrs[CAPACITY];
    row_t row_ids[CAPACITY];

    static LeafNode& New(RTree &tree, NodeHandle &node) {
        node.SetPtr(NodeHandle::GetAllocator(tree, NodeHandle::Type::Leaf).New());
        node.type = (uint8_t)NodeHandle::Type::Leaf;
        auto &leaf = LeafNode::Get(tree, node);
        
        leaf.count = 0;
        return leaf;
    }

    static void Free(RTree &tree, NodeHandle &node) {
        D_ASSERT(node.IsSet());
        D_ASSERT(!node.IsSwizzled());
        // No-op? We don't need to do anything here
    }

    BlockPointer Serialize(RTree &tree, MetaBlockWriter &writer) {
        auto ptr = writer.GetBlockPointer();
        writer.Write<uint32_t>(count);
        for(idx_t i = 0; i < count; i++) {
            writer.Write<double>(mbrs[i].x1);
            writer.Write<double>(mbrs[i].y1);
            writer.Write<double>(mbrs[i].x2);
            writer.Write<double>(mbrs[i].y2);
            writer.Write<row_t>(row_ids[i]);
        }
        return ptr;
    }

    void Deserialize(RTree &tree, MetaBlockReader &reader) {
        count = reader.Read<uint32_t>();
        for(idx_t i = 0; i < count; i++) {
            mbrs[i].x1 = reader.Read<double>();
            mbrs[i].y1 = reader.Read<double>();
            mbrs[i].x2 = reader.Read<double>();
            mbrs[i].y2 = reader.Read<double>();
            row_ids[i] = reader.Read<row_t>();
        }
    }

    static LeafNode &Get(RTree &tree, const NodeHandle &node) {
        return *NodeHandle::GetAllocator(tree, NodeHandle::Type::Leaf).Get<LeafNode>(node);
    }
};

struct BranchNode {
    static constexpr idx_t CAPACITY = 16;
    uint32_t count;
    Rectangle mbrs[CAPACITY];
    NodeHandle children[CAPACITY];

    static BranchNode& New(RTree &tree, NodeHandle &node) {
        node.SetPtr(NodeHandle::GetAllocator(tree, NodeHandle::Type::Branch).New());
        node.type = (uint8_t)NodeHandle::Type::Branch;
        auto &branch = BranchNode::Get(tree, node);
        branch.count = 0;
        return branch;
    }

    static void Free(RTree &tree, NodeHandle &node) {
        auto &branch = BranchNode::Get(tree, node);
        
        // Free all children
        for(idx_t i = 0; i < branch.count; i++) {
            NodeHandle::Free(tree, branch.children[i]);
        }
    }

    BlockPointer Serialize(RTree &tree, MetaBlockWriter &writer) {
        auto ptr = writer.GetBlockPointer();
        writer.Write<uint32_t>(count);
        for(idx_t i = 0; i < count; i++) {
            writer.Write<double>(mbrs[i].x1);
            writer.Write<double>(mbrs[i].y1);
            writer.Write<double>(mbrs[i].x2);
            writer.Write<double>(mbrs[i].y2);

            auto child_block_ptr = children[i].Serialize(tree, writer);
            writer.Write(child_block_ptr.block_id);
		    writer.Write(child_block_ptr.offset);
        }
        return ptr;
    }

    void Deserialize(RTree &tree, MetaBlockReader &reader) {
        count = reader.Read<uint32_t>();
        for(idx_t i = 0; i < count; i++) {
            mbrs[i].x1 = reader.Read<double>();
            mbrs[i].y1 = reader.Read<double>();
            mbrs[i].x2 = reader.Read<double>();
            mbrs[i].y2 = reader.Read<double>();

            auto block_id = reader.Read<block_id_t>();
            auto offset = reader.Read<uint32_t>();
            children[i] = NodeHandle(reader);
        }
    }

    static BranchNode &Get(RTree &tree, const NodeHandle &node) {
        return *NodeHandle::GetAllocator(tree, NodeHandle::Type::Branch).Get<BranchNode>(node);
    }
};

// A lazy blockpointer to a rtree node
struct NodeHandle : SwizzleablePointer {
    explicit NodeHandle(MetaBlockReader &reader) : SwizzleablePointer(reader) {}

    enum class Type : uint8_t {
        Leaf,
        Branch
    };

    Type GetType() {
        return Type(type);
    }

    static void New(RTree &tree, NodeHandle& result, Type type) {
        switch(type) {
            case Type::Leaf: {
                LeafNode::New(tree, result);
            } break;
            case Type::Branch: {
                BranchNode::New(tree, result);
            } break;
            default: {
                throw InternalException("Unknown node type");
            }
        }
    }

    static void Free(RTree &tree, NodeHandle& node) {
        auto type = node.GetType();
        switch(type) {
            case Type::Leaf: {
                // free the leaf node
            } break;
            case Type::Branch: {
                // free the branch node
            } break;
            default: {
                throw InternalException("Unknown node type");
            }
        }
    }

    BlockPointer Serialize(RTree &tree, MetaBlockWriter &writer) {
        if(!IsSet()) {
            return {(block_id_t)DConstants::INVALID_INDEX, 0};
        }

        if (IsSwizzled()) {
		    Deserialize(tree);
	    }

        auto type = GetType();
        switch(type) {
            case Type::Leaf: {
                return LeafNode::Get(tree, *this).Serialize(tree, writer);
            } break;
            case Type::Branch: {
                return BranchNode::Get(tree, *this).Serialize(tree, writer);
            } break;
            default: {
                throw InternalException("Unknown node type");
            }
        }
    }
    

    void Deserialize(RTree &tree) {
        MetaBlockReader reader(tree.table_io_manager.GetIndexBlockManager(), buffer_id);
        reader.offset = offset;
        type = reader.Read<uint8_t>();
        swizzle_flag = 0;

        auto type = GetType();
        SetPtr(NodeHandle::GetAllocator(tree, type).New());

        switch(type) {
            case Type::Leaf: {
                return LeafNode::Get(tree, *this).Deserialize(tree, reader);
            } break;
            case Type::Branch: {
                return BranchNode::Get(tree, *this).Deserialize(tree, reader);
            } break;
            default: {
                throw InternalException("Unknown node type");
            }
        }
    }

    static void Insert(RTree &tree, NodeHandle &node, const Rectangle &mbr, const row_t row_id) {
        switch (node.GetType()) {
            case Type::Leaf: {
                // check capacity, whatever.
            }
        }
    }

    inline void SetPtr(const SwizzleablePointer ptr) {
		offset = ptr.offset;
		buffer_id = ptr.buffer_id;
	}

    static FixedSizeAllocator& GetAllocator(RTree &tree, NodeHandle::Type type) {
        switch(type) {
            case Type::Leaf: {
                return tree.leaf_allocator;
            } break;
            case Type::Branch: {
                return tree.branch_allocator;
            } break;
            default: {
                throw InternalException("Unknown node type");
            }
        }
    }
};

class RTree : public Index {
    NodeHandle root;
public:
    FixedSizeAllocator branch_allocator;
    FixedSizeAllocator leaf_allocator; 
};

} // namespace woo
