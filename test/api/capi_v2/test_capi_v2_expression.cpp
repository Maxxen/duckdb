#include "catch.hpp"
#include "capi_v2_internal.hpp"

#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// V2 expression read-side tests.
//
// A duckdb_v2_expression is a borrowed pointer to a duckdb::Expression. These
// tests build bound-expression fixtures directly with the internal C++
// classes and cast the Expression* to the V2 handle — the same unwrapped-
// pointer identity the logical_type / value suites rely on. No engine,
// connection, or plan is required.
//
// Fixtures exercised:
//   ref          BoundReferenceExpression           class BOUND_REF
//   constant     BoundConstantExpression            class BOUND_CONSTANT
//   comparison   BoundComparisonExpression::Create  class BOUND_FUNCTION
//   operator     BoundOperatorExpression            class BOUND_OPERATOR
//   conjunction  BoundConjunctionExpression         class BOUND_CONJUNCTION
//   cast         BoundCastExpression                class BOUND_FUNCTION
// ---------------------------------------------------------------------------

namespace {

duckdb_v2_expression_handle AsExpr(duckdb::Expression &expr) {
	return reinterpret_cast<duckdb_v2_expression_handle>(&expr);
}

} // namespace

// ===========================================================================
// get_class
// ===========================================================================

TEST_CASE("V2 expression: get_class across all fixtures", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	auto con = duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10));
	auto cmp = duckdb::BoundComparisonExpression::Create(
	    duckdb::ExpressionType::COMPARE_EQUAL,
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10)));

	auto op = duckdb::make_uniq<duckdb::BoundOperatorExpression>(duckdb::ExpressionType::OPERATOR_IS_NULL,
	                                                             duckdb::LogicalType::BOOLEAN);
	op->GetChildrenMutable().push_back(
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0));

	auto conj = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
	    duckdb::ExpressionType::CONJUNCTION_AND,
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::BOOLEAN(true)),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::BOOLEAN(false)));

	auto cast = duckdb::BoundCastExpression::AddDefaultCastToType(
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::LogicalType::BIGINT);

	struct {
		duckdb_v2_expression_handle expr;
		DUCKDB_V2_EXPRESSION_CLASS expected;
	} cases[] = {
	    {AsExpr(*ref), DUCKDB_V2_EXPRESSION_CLASS_BOUND_REF},
	    {AsExpr(*con), DUCKDB_V2_EXPRESSION_CLASS_BOUND_CONSTANT},
	    {AsExpr(*cmp), DUCKDB_V2_EXPRESSION_CLASS_BOUND_FUNCTION},
	    {AsExpr(*op), DUCKDB_V2_EXPRESSION_CLASS_BOUND_OPERATOR},
	    {AsExpr(*conj), DUCKDB_V2_EXPRESSION_CLASS_BOUND_CONJUNCTION},
	    {AsExpr(*cast), DUCKDB_V2_EXPRESSION_CLASS_BOUND_FUNCTION},
	};

	for (auto &c : cases) {
		DUCKDB_V2_EXPRESSION_CLASS out = DUCKDB_V2_EXPRESSION_CLASS_INVALID;
		REQUIRE(duckdb_v2_expression_get_class(c.expr, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out == c.expected);
	}
}

TEST_CASE("V2 expression: get_class rejects null args", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	DUCKDB_V2_EXPRESSION_CLASS out = DUCKDB_V2_EXPRESSION_CLASS_INVALID;

	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_expression_get_class(nullptr, &out, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_destroy(&err);

	REQUIRE(duckdb_v2_expression_get_class(AsExpr(*ref), nullptr, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_destroy(&err);

	// err is optional.
	REQUIRE(duckdb_v2_expression_get_class(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

// ===========================================================================
// get_type
// ===========================================================================

TEST_CASE("V2 expression: get_type across all fixtures", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	auto con = duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10));
	auto cmp = duckdb::BoundComparisonExpression::Create(
	    duckdb::ExpressionType::COMPARE_EQUAL,
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10)));
	auto op = duckdb::make_uniq<duckdb::BoundOperatorExpression>(duckdb::ExpressionType::OPERATOR_IS_NULL,
	                                                             duckdb::LogicalType::BOOLEAN);
	op->GetChildrenMutable().push_back(
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0));
	auto conj = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
	    duckdb::ExpressionType::CONJUNCTION_AND,
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::BOOLEAN(true)),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::BOOLEAN(false)));
	auto cast = duckdb::BoundCastExpression::AddDefaultCastToType(
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::LogicalType::BIGINT);

	struct {
		duckdb_v2_expression_handle expr;
		DUCKDB_V2_EXPRESSION_TYPE expected;
	} cases[] = {
	    {AsExpr(*ref), DUCKDB_V2_EXPRESSION_TYPE_BOUND_REF},
	    {AsExpr(*con), DUCKDB_V2_EXPRESSION_TYPE_VALUE_CONSTANT},
	    {AsExpr(*cmp), DUCKDB_V2_EXPRESSION_TYPE_COMPARE_EQUAL},
	    {AsExpr(*op), DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NULL},
	    {AsExpr(*conj), DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_AND},
	    {AsExpr(*cast), DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_CAST},
	};

	for (auto &c : cases) {
		DUCKDB_V2_EXPRESSION_TYPE out = DUCKDB_V2_EXPRESSION_TYPE_INVALID;
		REQUIRE(duckdb_v2_expression_get_type(c.expr, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out == c.expected);
	}
}

TEST_CASE("V2 expression: get_type rejects null args", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	DUCKDB_V2_EXPRESSION_TYPE out = DUCKDB_V2_EXPRESSION_TYPE_INVALID;
	REQUIRE(duckdb_v2_expression_get_type(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_expression_get_type(AsExpr(*ref), nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

// ===========================================================================
// get_return_type
// ===========================================================================

TEST_CASE("V2 expression: get_return_type returns an owned logical type", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	auto cmp = duckdb::BoundComparisonExpression::Create(
	    duckdb::ExpressionType::COMPARE_EQUAL,
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10)));
	auto cast = duckdb::BoundCastExpression::AddDefaultCastToType(
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::LogicalType::BIGINT);

	struct {
		duckdb_v2_expression_handle expr;
		DUCKDB_V2_LOGICAL_TYPE_ID expected;
	} cases[] = {
	    {AsExpr(*ref), DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER},
	    {AsExpr(*cmp), DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN},
	    {AsExpr(*cast), DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT},
	};

	for (auto &c : cases) {
		duckdb_v2_logical_type_handle type = nullptr;
		REQUIRE(duckdb_v2_expression_get_return_type(c.expr, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(type != nullptr);
		DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		REQUIRE(duckdb_v2_logical_type_get_id(type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(id == c.expected);
		REQUIRE(duckdb_v2_logical_type_destroy(&type) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(type == nullptr);
	}
}

TEST_CASE("V2 expression: get_return_type nulls out-param and rejects null args", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	duckdb_v2_logical_type_handle type = reinterpret_cast<duckdb_v2_logical_type_handle>(0x1);
	REQUIRE(duckdb_v2_expression_get_return_type(nullptr, &type, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(type == nullptr); // pointer out-param zeroed on failure
	REQUIRE(duckdb_v2_expression_get_return_type(AsExpr(*ref), nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

// ===========================================================================
// get_child_count
// ===========================================================================

TEST_CASE("V2 expression: get_child_count", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	auto con = duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10));
	auto cmp = duckdb::BoundComparisonExpression::Create(
	    duckdb::ExpressionType::COMPARE_EQUAL,
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10)));
	auto op = duckdb::make_uniq<duckdb::BoundOperatorExpression>(duckdb::ExpressionType::OPERATOR_IS_NULL,
	                                                             duckdb::LogicalType::BOOLEAN);
	op->GetChildrenMutable().push_back(
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0));
	auto conj = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
	    duckdb::ExpressionType::CONJUNCTION_AND,
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::BOOLEAN(true)),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::BOOLEAN(false)));
	auto cast = duckdb::BoundCastExpression::AddDefaultCastToType(
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::LogicalType::BIGINT);

	struct {
		duckdb_v2_expression_handle expr;
		idx_t expected;
	} cases[] = {
	    {AsExpr(*ref), 0},  // leaf
	    {AsExpr(*con), 0},  // leaf
	    {AsExpr(*cmp), 2},  // lhs, rhs
	    {AsExpr(*op), 1},   // single operand
	    {AsExpr(*conj), 2}, // AND of two
	    {AsExpr(*cast), 1}, // cast input
	};

	for (auto &c : cases) {
		idx_t count = 999;
		REQUIRE(duckdb_v2_expression_get_child_count(c.expr, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(count == c.expected);
	}
}

TEST_CASE("V2 expression: get_child_count rejects null args", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	idx_t count = 0;
	REQUIRE(duckdb_v2_expression_get_child_count(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_expression_get_child_count(AsExpr(*ref), nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

// ===========================================================================
// get_child
// ===========================================================================

TEST_CASE("V2 expression: get_child recurses through borrowed handles", "[capi_v2][expression]") {
	// AND(col0 = 10, col5 = 20)
	//   conjunction
	//     ├─ comparison ├─ ref(0)
	//     │             └─ const(10)
	//     └─ comparison ├─ ref(5)
	//                   └─ const(20)
	// Every node below the root is reached only through a borrowed handle
	// returned by a previous get_child — exercising the borrow chain end to end.
	auto left_cmp = duckdb::BoundComparisonExpression::Create(
	    duckdb::ExpressionType::COMPARE_EQUAL,
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10)));
	auto right_cmp = duckdb::BoundComparisonExpression::Create(
	    duckdb::ExpressionType::COMPARE_EQUAL,
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 5),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(20)));
	auto conj = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_AND,
	                                                                  std::move(left_cmp), std::move(right_cmp));
	auto root = AsExpr(*conj);

	// Helper: assert a node is a comparison whose children are ref(index) and const(value).
	auto check_comparison = [](duckdb_v2_expression_handle cmp, idx_t expect_ref, int32_t expect_const) {
		DUCKDB_V2_EXPRESSION_CLASS cls = DUCKDB_V2_EXPRESSION_CLASS_INVALID;
		REQUIRE(duckdb_v2_expression_get_class(cmp, &cls, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(cls == DUCKDB_V2_EXPRESSION_CLASS_BOUND_FUNCTION);
		idx_t count = 0;
		REQUIRE(duckdb_v2_expression_get_child_count(cmp, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(count == 2);

		duckdb_v2_expression_handle ref = nullptr;
		duckdb_v2_expression_handle con = nullptr;
		REQUIRE(duckdb_v2_expression_get_child(cmp, 0, &ref, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_expression_get_child(cmp, 1, &con, nullptr) == DUCKDB_V2_ERROR_NONE);

		idx_t ref_index = 999;
		REQUIRE(duckdb_v2_expression_get_reference_index(ref, &ref_index, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(ref_index == expect_ref);

		duckdb_v2_value_handle value = nullptr;
		REQUIRE(duckdb_v2_expression_get_constant_value(con, &value, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb::ToValue(value)->GetValue<int32_t>() == expect_const);
		REQUIRE(duckdb_v2_value_destroy(&value) == DUCKDB_V2_ERROR_NONE);
	};

	// Level 0: the conjunction.
	DUCKDB_V2_EXPRESSION_CLASS root_class = DUCKDB_V2_EXPRESSION_CLASS_INVALID;
	REQUIRE(duckdb_v2_expression_get_class(root, &root_class, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(root_class == DUCKDB_V2_EXPRESSION_CLASS_BOUND_CONJUNCTION);
	idx_t root_count = 0;
	REQUIRE(duckdb_v2_expression_get_child_count(root, &root_count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(root_count == 2);

	// Level 1 + 2: descend into each comparison through its borrowed handle.
	duckdb_v2_expression_handle left = nullptr;
	duckdb_v2_expression_handle right = nullptr;
	REQUIRE(duckdb_v2_expression_get_child(root, 0, &left, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_expression_get_child(root, 1, &right, nullptr) == DUCKDB_V2_ERROR_NONE);
	check_comparison(left, 0, 10);
	check_comparison(right, 5, 20);
}

TEST_CASE("V2 expression: get_child descends into a comparison", "[capi_v2][expression]") {
	// col0 = 10  ->  BOUND_FUNCTION(BOUND_REF, BOUND_CONSTANT)
	auto cmp = duckdb::BoundComparisonExpression::Create(
	    duckdb::ExpressionType::COMPARE_EQUAL,
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 7),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10)));
	auto root = AsExpr(*cmp);

	duckdb_v2_expression_handle lhs = nullptr;
	duckdb_v2_expression_handle rhs = nullptr;
	REQUIRE(duckdb_v2_expression_get_child(root, 0, &lhs, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_expression_get_child(root, 1, &rhs, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(lhs != nullptr);
	REQUIRE(rhs != nullptr);

	DUCKDB_V2_EXPRESSION_CLASS lhs_class = DUCKDB_V2_EXPRESSION_CLASS_INVALID;
	DUCKDB_V2_EXPRESSION_CLASS rhs_class = DUCKDB_V2_EXPRESSION_CLASS_INVALID;
	REQUIRE(duckdb_v2_expression_get_class(lhs, &lhs_class, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_expression_get_class(rhs, &rhs_class, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(lhs_class == DUCKDB_V2_EXPRESSION_CLASS_BOUND_REF);
	REQUIRE(rhs_class == DUCKDB_V2_EXPRESSION_CLASS_BOUND_CONSTANT);

	// The borrowed lhs is itself usable: read its reference index.
	idx_t ref_index = 999;
	REQUIRE(duckdb_v2_expression_get_reference_index(lhs, &ref_index, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ref_index == 7);
}

TEST_CASE("V2 expression: get_child on the single cast input", "[capi_v2][expression]") {
	auto cast = duckdb::BoundCastExpression::AddDefaultCastToType(
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 3),
	    duckdb::LogicalType::BIGINT);
	duckdb_v2_expression_handle child = nullptr;
	REQUIRE(duckdb_v2_expression_get_child(AsExpr(*cast), 0, &child, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_EXPRESSION_CLASS cls = DUCKDB_V2_EXPRESSION_CLASS_INVALID;
	REQUIRE(duckdb_v2_expression_get_class(child, &cls, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(cls == DUCKDB_V2_EXPRESSION_CLASS_BOUND_REF);
}

TEST_CASE("V2 expression: get_child out-of-range is an error", "[capi_v2][expression]") {
	auto cmp = duckdb::BoundComparisonExpression::Create(
	    duckdb::ExpressionType::COMPARE_EQUAL,
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10)));

	duckdb_v2_expression_handle child = reinterpret_cast<duckdb_v2_expression_handle>(0x1);
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_expression_get_child(AsExpr(*cmp), 2, &child, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(child == nullptr); // pointer out-param zeroed on failure
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2 expression: get_child on a leaf is always out-of-range", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	auto con = duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10));
	duckdb_v2_expression_handle child = nullptr;
	REQUIRE(duckdb_v2_expression_get_child(AsExpr(*ref), 0, &child, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(child == nullptr);
	REQUIRE(duckdb_v2_expression_get_child(AsExpr(*con), 0, &child, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(child == nullptr);
}

TEST_CASE("V2 expression: child traversal works on a class this API doesn't model specially", "[capi_v2][expression]") {
	// BOUND_CASE has no class-specific accessor here, but children are traversed
	// via the engine's ExpressionIterator, so they are NOT silently dropped: the
	// iterator surfaces CASE's when/then/else as a flat child list. This is the
	// whole point of delegating to ExpressionIterator — a recursive walker can't
	// mistake an unmodeled node for a leaf.
	auto case_expr = duckdb::make_uniq<duckdb::BoundCaseExpression>(
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::BOOLEAN(true)), // when
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(1)),    // then
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(2)));   // else
	auto expr = AsExpr(*case_expr);

	DUCKDB_V2_EXPRESSION_CLASS cls = DUCKDB_V2_EXPRESSION_CLASS_INVALID;
	REQUIRE(duckdb_v2_expression_get_class(expr, &cls, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(cls == DUCKDB_V2_EXPRESSION_CLASS_BOUND_CASE);
	DUCKDB_V2_EXPRESSION_TYPE type = DUCKDB_V2_EXPRESSION_TYPE_INVALID;
	REQUIRE(duckdb_v2_expression_get_type(expr, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(type == DUCKDB_V2_EXPRESSION_TYPE_CASE_EXPR);

	// when + then + else = 3 children, all reachable.
	idx_t count = 0;
	REQUIRE(duckdb_v2_expression_get_child_count(expr, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 3);
	for (idx_t i = 0; i < count; i++) {
		duckdb_v2_expression_handle child = nullptr;
		REQUIRE(duckdb_v2_expression_get_child(expr, i, &child, nullptr) == DUCKDB_V2_ERROR_NONE);
		DUCKDB_V2_EXPRESSION_CLASS child_cls = DUCKDB_V2_EXPRESSION_CLASS_INVALID;
		REQUIRE(duckdb_v2_expression_get_class(child, &child_cls, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(child_cls == DUCKDB_V2_EXPRESSION_CLASS_BOUND_CONSTANT);
	}
	// Out of range past the real count still errors.
	duckdb_v2_expression_handle child = reinterpret_cast<duckdb_v2_expression_handle>(0x1);
	REQUIRE(duckdb_v2_expression_get_child(expr, 3, &child, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(child == nullptr);
}

TEST_CASE("V2 expression: get_child rejects null args", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	duckdb_v2_expression_handle child = nullptr;
	REQUIRE(duckdb_v2_expression_get_child(nullptr, 0, &child, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_expression_get_child(AsExpr(*ref), 0, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

// ===========================================================================
// get_function_name
// ===========================================================================

TEST_CASE("V2 expression: get_function_name on a BOUND_FUNCTION", "[capi_v2][expression]") {
	// A comparison is a BOUND_FUNCTION; its registered name is the operator
	// symbol of the underlying scalar function (e.g. "=" for COMPARE_EQUAL).
	auto cmp = duckdb::BoundComparisonExpression::Create(
	    duckdb::ExpressionType::COMPARE_EQUAL,
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10)));
	duckdb_v2_str name = {nullptr, 0};
	REQUIRE(duckdb_v2_expression_get_function_name(AsExpr(*cmp), &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(name == "=");

	// A cast is a BOUND_FUNCTION too (the __cast scalar function).
	auto cast = duckdb::BoundCastExpression::AddDefaultCastToType(
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::LogicalType::BIGINT);
	duckdb_v2_str cast_name = {nullptr, 0};
	REQUIRE(duckdb_v2_expression_get_function_name(AsExpr(*cast), &cast_name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(cast_name == "__cast");
}

TEST_CASE("V2 expression: get_function_name errors on non-function classes", "[capi_v2][expression]") {
	// Only BOUND_FUNCTION has a registered name. Operator / conjunction are
	// NOT functions here (their "name" would be a synthesized stringized
	// enum), so they error and push the caller to get_type. Leaves error too.
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	auto con = duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10));
	auto op = duckdb::make_uniq<duckdb::BoundOperatorExpression>(duckdb::ExpressionType::OPERATOR_IS_NULL,
	                                                             duckdb::LogicalType::BOOLEAN);
	op->GetChildrenMutable().push_back(
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0));
	auto conj = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
	    duckdb::ExpressionType::CONJUNCTION_AND,
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::BOOLEAN(true)),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::BOOLEAN(false)));

	duckdb_v2_expression_handle cases[] = {AsExpr(*ref), AsExpr(*con), AsExpr(*op), AsExpr(*conj)};
	for (auto expr : cases) {
		duckdb_v2_str name = {reinterpret_cast<const char *>(0x1), 99};
		REQUIRE(duckdb_v2_expression_get_function_name(expr, &name, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(name.ptr == nullptr); // out-param zeroed on failure
	}
}

TEST_CASE("V2 expression: get_function_name rejects null args", "[capi_v2][expression]") {
	auto cmp = duckdb::BoundComparisonExpression::Create(
	    duckdb::ExpressionType::COMPARE_EQUAL,
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10)));
	// Exercise the real duckdb_v2_str signature directly for the null checks.
	duckdb_v2_str name = {nullptr, 0};
	REQUIRE(duckdb_v2_expression_get_function_name(nullptr, &name, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_expression_get_function_name(AsExpr(*cmp), nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

// ===========================================================================
// get_constant_value
// ===========================================================================

TEST_CASE("V2 expression: get_constant_value on a BOUND_CONSTANT", "[capi_v2][expression]") {
	auto con = duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(4242));
	duckdb_v2_value_handle value = nullptr;
	REQUIRE(duckdb_v2_expression_get_constant_value(AsExpr(*con), &value, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(value != nullptr);
	REQUIRE(duckdb::ToValue(value)->GetValue<int32_t>() == 4242);
	REQUIRE(duckdb_v2_value_destroy(&value) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(value == nullptr);
}

TEST_CASE("V2 expression: get_constant_value errors on non-constant", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	auto cmp = duckdb::BoundComparisonExpression::Create(
	    duckdb::ExpressionType::COMPARE_EQUAL,
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0),
	    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10)));

	duckdb_v2_value_handle value = reinterpret_cast<duckdb_v2_value_handle>(0x1);
	REQUIRE(duckdb_v2_expression_get_constant_value(AsExpr(*ref), &value, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(value == nullptr);
	value = reinterpret_cast<duckdb_v2_value_handle>(0x1);
	REQUIRE(duckdb_v2_expression_get_constant_value(AsExpr(*cmp), &value, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(value == nullptr);
}

TEST_CASE("V2 expression: get_constant_value rejects null args", "[capi_v2][expression]") {
	auto con = duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10));
	duckdb_v2_value_handle value = nullptr;
	REQUIRE(duckdb_v2_expression_get_constant_value(nullptr, &value, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_expression_get_constant_value(AsExpr(*con), nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

// ===========================================================================
// get_reference_index
// ===========================================================================

TEST_CASE("V2 expression: get_reference_index on a BOUND_REF", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 13);
	idx_t index = 999;
	REQUIRE(duckdb_v2_expression_get_reference_index(AsExpr(*ref), &index, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(index == 13);
}

TEST_CASE("V2 expression: get_reference_index errors on non-reference", "[capi_v2][expression]") {
	auto con = duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10));
	idx_t index = 0;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_expression_get_reference_index(AsExpr(*con), &index, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_str text = {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(err, &text) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(text.ptr != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2 expression: get_reference_index rejects null args", "[capi_v2][expression]") {
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	idx_t index = 0;
	REQUIRE(duckdb_v2_expression_get_reference_index(nullptr, &index, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_expression_get_reference_index(AsExpr(*ref), nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

// ===========================================================================
// get_column_binding
// ===========================================================================

TEST_CASE("V2 expression: get_column_binding on a BOUND_COLUMN_REF", "[capi_v2][expression]") {
	// table_index 3, column_index 5.
	duckdb::ColumnBinding binding(duckdb::TableIndex(3), duckdb::ProjectionIndex(5));
	auto col = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::INTEGER, binding, 1);
	auto expr = AsExpr(*col);

	idx_t table_index = 999;
	idx_t column_index = 999;
	REQUIRE(duckdb_v2_expression_get_column_binding(expr, &table_index, &column_index, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(table_index == 3);
	REQUIRE(column_index == 5);

	// Each out-param is optional: take only column_index.
	idx_t only_column = 999;
	REQUIRE(duckdb_v2_expression_get_column_binding(expr, nullptr, &only_column, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(only_column == 5);

	// All out-params NULL is a valid (if pointless) call on a BOUND_COLUMN_REF —
	// it succeeds after the class check rather than requiring at least one.
	REQUIRE(duckdb_v2_expression_get_column_binding(expr, nullptr, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 expression: get_column_binding errors on non-column-ref", "[capi_v2][expression]") {
	auto con = duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::INTEGER(10));
	auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(duckdb::LogicalType::INTEGER, 0);
	idx_t table_index = 0;
	idx_t column_index = 0;
	// BOUND_REF is the physical-stage column representation — distinct from
	// BOUND_COLUMN_REF, so it must NOT satisfy get_column_binding.
	REQUIRE(duckdb_v2_expression_get_column_binding(AsExpr(*con), &table_index, &column_index, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_expression_get_column_binding(AsExpr(*ref), &table_index, &column_index, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
}

TEST_CASE("V2 expression: get_column_binding rejects null expression", "[capi_v2][expression]") {
	idx_t v = 0;
	REQUIRE(duckdb_v2_expression_get_column_binding(nullptr, &v, &v, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}
