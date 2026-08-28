#include "duckdb/planner/bound_expression_memo.hpp"

#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

void BoundExpressionMemo::Insert(const ParsedExpression &node, unique_ptr<Expression> expr) {
	D_ASSERT(!scope_stack.empty());
	D_ASSERT(expr);
	MemoEntry entry;
	entry.expression = std::move(expr);
	entry.scope_index = scope_stack.size() - 1;
#ifdef DEBUG
	entry.expr_class = node.GetExpressionClass();
	entry.expr_type = node.GetExpressionType();
#endif
	entries[node] = std::move(entry);
	scope_stack.back().push_back(node);
}

bool BoundExpressionMemo::IsBound(const ParsedExpression &node) const {
	auto entry = entries.find(node);
	if (entry == entries.end()) {
		return false;
	}
	VerifyEntry(node);
	return true;
}

const BoundExpressionMemo::MemoEntry &BoundExpressionMemo::GetEntry(const ParsedExpression &node) const {
	auto entry = entries.find(node);
	if (entry == entries.end()) {
		throw InternalException("BoundExpressionMemo does not contain a bound expression for this node");
	}
	VerifyEntry(node);
	return entry->second;
}

void BoundExpressionMemo::VerifyEntry(const ParsedExpression &node) const {
#ifdef DEBUG
	auto &entry = entries.find(node)->second;
	D_ASSERT(entry.expr_class == node.GetExpressionClass());
	D_ASSERT(entry.expr_type == node.GetExpressionType());
#endif
}

Expression &BoundExpressionMemo::Get(const ParsedExpression &node) const {
	return *GetEntry(node).expression;
}

unique_ptr<Expression> &BoundExpressionMemo::GetMutable(const ParsedExpression &node) {
	auto entry = entries.find(node);
	if (entry == entries.end()) {
		throw InternalException("BoundExpressionMemo does not contain a bound expression for this node");
	}
	VerifyEntry(node);
	return entry->second.expression;
}

unique_ptr<Expression> BoundExpressionMemo::Consume(const ParsedExpression &node) {
	auto entry = entries.find(node);
	if (entry == entries.end()) {
		throw InternalException("BoundExpressionMemo::Consume called on a node without a bound expression");
	}
	VerifyEntry(node);
	auto result = std::move(entry->second.expression);
	entries.erase(entry);
	return result;
}

bool BoundExpressionMemo::HasBoundDescendant(const ParsedExpression &node) const {
	if (entries.empty()) {
		return false;
	}
	if (IsBound(node)) {
		return true;
	}
	bool found = false;
	ParsedExpressionIterator::EnumerateChildren(node, [&](const ParsedExpression &child) {
		if (!found && HasBoundDescendant(child)) {
			found = true;
		}
	});
	return found;
}

void BoundExpressionMemo::EraseSubtree(const ParsedExpression &node) {
	if (entries.empty()) {
		return;
	}
	entries.erase(node);
	ParsedExpressionIterator::EnumerateChildren(node, [&](const ParsedExpression &child) { EraseSubtree(child); });
}

BoundExpressionScope::BoundExpressionScope(BoundExpressionMemo &memo) : memo(memo) {
	memo.scope_stack.emplace_back();
}

BoundExpressionScope::~BoundExpressionScope() {
	auto scope_index = memo.scope_stack.size() - 1;
	for (auto &node : memo.scope_stack.back()) {
		auto entry = memo.entries.find(node);
		if (entry != memo.entries.end() && entry->second.scope_index == scope_index) {
			memo.entries.erase(entry);
		}
	}
	memo.scope_stack.pop_back();
}

} // namespace duckdb
