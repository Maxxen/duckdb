#include "capi_v2_internal.hpp"

#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

// ---------------------------------------------------------------------------
// V2 expression read-side bridge.
//
// duckdb_v2_expression is a borrowed pointer to a duckdb::Expression. All
// accessors are read-only; there is no constructor or destructor on this
// surface. Bodies wrap in WithErrorHandler so any thrown DuckDB exception
// (e.g. an InvalidInputException for a class mismatch, or a
// NotImplementedException from EnumUtil on an unmapped enum value) becomes a
// V2 error code.
//
// Exception policy: every body is wrapped, so the bound-subclass Cast<T>()
// calls below are safe even though a mismatched class would assert/throw —
// the class is checked first on each class-specific path, and the universal
// paths only touch the base Expression.
// ---------------------------------------------------------------------------

// The whole module rests on the V2 enums being numerically identical to the
// internal ones, so the static_cast in get_class / get_type is a pure
// reinterpretation. These asserts cover the *entire* enum (not just the
// handful the runtime tests reach), turning any future internal renumbering
// into a compile error here rather than a silent wrong mapping at runtime.
// The macro derives the V2 macro name from the internal enumerator name, so a
// value present internally but missing from the spec also fails to compile.
#define DUCKDB_V2_ASSERT_EXPR_CLASS(NAME)                                                                              \
	static_assert(static_cast<int>(duckdb::ExpressionClass::NAME) == DUCKDB_V2_EXPRESSION_CLASS_##NAME,                \
	              "EXPRESSION_CLASS numeric drift: " #NAME)
#define DUCKDB_V2_ASSERT_EXPR_TYPE(NAME)                                                                               \
	static_assert(static_cast<int>(duckdb::ExpressionType::NAME) == DUCKDB_V2_EXPRESSION_TYPE_##NAME,                  \
	              "EXPRESSION_TYPE numeric drift: " #NAME)

DUCKDB_V2_ASSERT_EXPR_CLASS(INVALID);
DUCKDB_V2_ASSERT_EXPR_CLASS(AGGREGATE);
DUCKDB_V2_ASSERT_EXPR_CLASS(CASE);
DUCKDB_V2_ASSERT_EXPR_CLASS(CAST);
DUCKDB_V2_ASSERT_EXPR_CLASS(COLUMN_REF);
DUCKDB_V2_ASSERT_EXPR_CLASS(COMPARISON);
DUCKDB_V2_ASSERT_EXPR_CLASS(CONJUNCTION);
DUCKDB_V2_ASSERT_EXPR_CLASS(CONSTANT);
DUCKDB_V2_ASSERT_EXPR_CLASS(DEFAULT);
DUCKDB_V2_ASSERT_EXPR_CLASS(FUNCTION);
DUCKDB_V2_ASSERT_EXPR_CLASS(OPERATOR);
DUCKDB_V2_ASSERT_EXPR_CLASS(STAR);
DUCKDB_V2_ASSERT_EXPR_CLASS(SUBQUERY);
DUCKDB_V2_ASSERT_EXPR_CLASS(WINDOW);
DUCKDB_V2_ASSERT_EXPR_CLASS(PARAMETER);
DUCKDB_V2_ASSERT_EXPR_CLASS(COLLATE);
DUCKDB_V2_ASSERT_EXPR_CLASS(LAMBDA);
DUCKDB_V2_ASSERT_EXPR_CLASS(POSITIONAL_REFERENCE);
DUCKDB_V2_ASSERT_EXPR_CLASS(BETWEEN);
DUCKDB_V2_ASSERT_EXPR_CLASS(LAMBDA_REF);
DUCKDB_V2_ASSERT_EXPR_CLASS(TYPE);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_AGGREGATE);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_CASE);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_CAST);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_COLUMN_REF);
DUCKDB_V2_ASSERT_EXPR_CLASS(LEGACY_BOUND_COMPARISON);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_CONJUNCTION);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_CONSTANT);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_DEFAULT);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_FUNCTION);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_OPERATOR);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_PARAMETER);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_REF);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_SUBQUERY);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_WINDOW);
DUCKDB_V2_ASSERT_EXPR_CLASS(LEGACY_BOUND_BETWEEN);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_UNNEST);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_LAMBDA);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_LAMBDA_REF);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_EXPRESSION);
DUCKDB_V2_ASSERT_EXPR_CLASS(BOUND_EXPANDED);

DUCKDB_V2_ASSERT_EXPR_TYPE(INVALID);
DUCKDB_V2_ASSERT_EXPR_TYPE(OPERATOR_CAST);
DUCKDB_V2_ASSERT_EXPR_TYPE(OPERATOR_NOT);
DUCKDB_V2_ASSERT_EXPR_TYPE(OPERATOR_IS_NULL);
DUCKDB_V2_ASSERT_EXPR_TYPE(OPERATOR_IS_NOT_NULL);
DUCKDB_V2_ASSERT_EXPR_TYPE(OPERATOR_UNPACK);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_EQUAL);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_NOTEQUAL);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_LESSTHAN);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_GREATERTHAN);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_LESSTHANOREQUALTO);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_GREATERTHANOREQUALTO);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_IN);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_NOT_IN);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_DISTINCT_FROM);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_BETWEEN);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_NOT_BETWEEN);
DUCKDB_V2_ASSERT_EXPR_TYPE(COMPARE_NOT_DISTINCT_FROM);
DUCKDB_V2_ASSERT_EXPR_TYPE(CONJUNCTION_AND);
DUCKDB_V2_ASSERT_EXPR_TYPE(CONJUNCTION_OR);
DUCKDB_V2_ASSERT_EXPR_TYPE(VALUE_CONSTANT);
DUCKDB_V2_ASSERT_EXPR_TYPE(VALUE_PARAMETER);
DUCKDB_V2_ASSERT_EXPR_TYPE(VALUE_TUPLE);
DUCKDB_V2_ASSERT_EXPR_TYPE(VALUE_TUPLE_ADDRESS);
DUCKDB_V2_ASSERT_EXPR_TYPE(VALUE_NULL);
DUCKDB_V2_ASSERT_EXPR_TYPE(VALUE_VECTOR);
DUCKDB_V2_ASSERT_EXPR_TYPE(VALUE_SCALAR);
DUCKDB_V2_ASSERT_EXPR_TYPE(VALUE_DEFAULT);
DUCKDB_V2_ASSERT_EXPR_TYPE(AGGREGATE);
DUCKDB_V2_ASSERT_EXPR_TYPE(BOUND_AGGREGATE);
DUCKDB_V2_ASSERT_EXPR_TYPE(GROUPING_FUNCTION);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_AGGREGATE);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_FUNCTION);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_RANK);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_RANK_DENSE);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_NTILE);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_PERCENT_RANK);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_CUME_DIST);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_ROW_NUMBER);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_FIRST_VALUE);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_LAST_VALUE);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_LEAD);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_LAG);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_NTH_VALUE);
DUCKDB_V2_ASSERT_EXPR_TYPE(WINDOW_FILL);
DUCKDB_V2_ASSERT_EXPR_TYPE(FUNCTION);
DUCKDB_V2_ASSERT_EXPR_TYPE(BOUND_FUNCTION);
DUCKDB_V2_ASSERT_EXPR_TYPE(CASE_EXPR);
DUCKDB_V2_ASSERT_EXPR_TYPE(OPERATOR_NULLIF);
DUCKDB_V2_ASSERT_EXPR_TYPE(OPERATOR_COALESCE);
DUCKDB_V2_ASSERT_EXPR_TYPE(ARRAY_EXTRACT);
DUCKDB_V2_ASSERT_EXPR_TYPE(ARRAY_SLICE);
DUCKDB_V2_ASSERT_EXPR_TYPE(STRUCT_EXTRACT);
DUCKDB_V2_ASSERT_EXPR_TYPE(ARRAY_CONSTRUCTOR);
DUCKDB_V2_ASSERT_EXPR_TYPE(ARROW);
DUCKDB_V2_ASSERT_EXPR_TYPE(OPERATOR_TRY);
DUCKDB_V2_ASSERT_EXPR_TYPE(SUBQUERY);
DUCKDB_V2_ASSERT_EXPR_TYPE(STAR);
DUCKDB_V2_ASSERT_EXPR_TYPE(TABLE_STAR);
DUCKDB_V2_ASSERT_EXPR_TYPE(PLACEHOLDER);
DUCKDB_V2_ASSERT_EXPR_TYPE(COLUMN_REF);
DUCKDB_V2_ASSERT_EXPR_TYPE(FUNCTION_REF);
DUCKDB_V2_ASSERT_EXPR_TYPE(TABLE_REF);
DUCKDB_V2_ASSERT_EXPR_TYPE(LAMBDA_REF);
DUCKDB_V2_ASSERT_EXPR_TYPE(TYPE);
DUCKDB_V2_ASSERT_EXPR_TYPE(CAST);
DUCKDB_V2_ASSERT_EXPR_TYPE(BOUND_REF);
DUCKDB_V2_ASSERT_EXPR_TYPE(BOUND_COLUMN_REF);
DUCKDB_V2_ASSERT_EXPR_TYPE(BOUND_UNNEST);
DUCKDB_V2_ASSERT_EXPR_TYPE(COLLATE);
DUCKDB_V2_ASSERT_EXPR_TYPE(LAMBDA);
DUCKDB_V2_ASSERT_EXPR_TYPE(POSITIONAL_REFERENCE);
DUCKDB_V2_ASSERT_EXPR_TYPE(BOUND_LAMBDA_REF);
DUCKDB_V2_ASSERT_EXPR_TYPE(BOUND_EXPANDED);

#undef DUCKDB_V2_ASSERT_EXPR_CLASS
#undef DUCKDB_V2_ASSERT_EXPR_TYPE

namespace duckdb {
namespace {

// Children are read through ExpressionIterator::EnumerateChildren — the same
// generic traversal the engine itself uses — rather than a per-class switch.
// This keeps get_child_count / get_child total over every bound class (it even
// surfaces the "hidden" children of nodes this module otherwise doesn't model:
// case when/then/else, aggregate filter/order keys, window frame bounds), so a
// recursive walker can never mistake an unmodeled node for a leaf. Child order
// follows the iterator. The iterator throws on an unbound expression, which a
// duckdb_v2_expression handle never wraps. Access is O(n) per call (the
// iterator is enumerate-only — core never counts or indexes), which is
// irrelevant for the small fan-outs expressions have.
idx_t ExpressionChildCount(Expression &expr) {
	idx_t count = 0;
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &) { count++; });
	return count;
}

// Borrow child `index`. Caller must have validated index < ExpressionChildCount.
Expression &ExpressionChild(Expression &expr, idx_t index) {
	optional_ptr<Expression> result;
	idx_t i = 0;
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
		if (i++ == index) {
			result = &child;
		}
	});
	if (!result) {
		throw InternalException("expression_get_child: index past child count after range check");
	}
	return *result;
}

} // namespace
} // namespace duckdb

DUCKDB_V2_ERROR duckdb_v2_expression_get_class(duckdb_v2_expression_handle expression,
                                               DUCKDB_V2_EXPRESSION_CLASS *out_class,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!expression || !out_class) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_expression_get_class");
		}
		auto &expr = *duckdb::ToExpression(expression);
		*out_class = static_cast<DUCKDB_V2_EXPRESSION_CLASS>(expr.GetExpressionClass());
	});
}

DUCKDB_V2_ERROR duckdb_v2_expression_get_type(duckdb_v2_expression_handle expression,
                                              DUCKDB_V2_EXPRESSION_TYPE *out_type, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!expression || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_expression_get_type");
		}
		auto &expr = *duckdb::ToExpression(expression);
		*out_type = static_cast<DUCKDB_V2_EXPRESSION_TYPE>(expr.GetExpressionType());
	});
}

DUCKDB_V2_ERROR duckdb_v2_expression_get_return_type(duckdb_v2_expression_handle expression,
                                                     duckdb_v2_logical_type_handle *out_type,
                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (out_type) {
			*out_type = nullptr;
		}
		if (!expression || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_expression_get_return_type");
		}
		auto &expr = *duckdb::ToExpression(expression);
		*out_type = reinterpret_cast<_duckdb_v2_logical_type *>(new duckdb::LogicalType(expr.GetReturnType()));
	});
}

DUCKDB_V2_ERROR duckdb_v2_expression_get_child_count(duckdb_v2_expression_handle expression, idx_t *out_count,
                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!expression || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_expression_get_child_count");
		}
		auto &expr = *duckdb::ToExpression(expression);
		*out_count = duckdb::ExpressionChildCount(expr);
	});
}

DUCKDB_V2_ERROR duckdb_v2_expression_get_child(duckdb_v2_expression_handle expression, idx_t index,
                                               duckdb_v2_expression_handle *out_child,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (out_child) {
			*out_child = nullptr;
		}
		if (!expression || !out_child) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_expression_get_child");
		}
		auto &expr = *duckdb::ToExpression(expression);
		auto count = duckdb::ExpressionChildCount(expr);
		if (index >= count) {
			throw duckdb::InvalidInputException("expression child index out of range");
		}
		auto &child = duckdb::ExpressionChild(expr, index);
		*out_child = reinterpret_cast<_duckdb_v2_expression *>(&child);
	});
}

DUCKDB_V2_ERROR duckdb_v2_expression_get_function_name(duckdb_v2_expression_handle expression,
                                                       duckdb_v2_identifier_t *out_name,
                                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (out_name) {
			*out_name = duckdb_v2_identifier_t {nullptr, 0};
		}
		if (!expression || !out_name) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_expression_get_function_name");
		}
		auto &expr = *duckdb::ToExpression(expression);
		if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_expression_get_function_name: expression is not a BOUND_FUNCTION");
		}
		// Borrowed from the bound function's name member (GetName returns a const
		// reference into it) — valid for the expression handle's lifetime.
		*out_name = duckdb::ToStr(expr.Cast<duckdb::BoundFunctionExpression>().Function().GetName());
	});
}

DUCKDB_V2_ERROR duckdb_v2_expression_get_constant_value(duckdb_v2_expression_handle expression,
                                                        duckdb_v2_value_handle *out_value,
                                                        duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (out_value) {
			*out_value = nullptr;
		}
		if (!expression || !out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_expression_get_constant_value");
		}
		auto &expr = *duckdb::ToExpression(expression);
		if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_expression_get_constant_value: expression is not a BOUND_CONSTANT");
		}
		*out_value = reinterpret_cast<_duckdb_v2_value *>(
		    new duckdb::Value(expr.Cast<duckdb::BoundConstantExpression>().GetValue()));
	});
}

DUCKDB_V2_ERROR duckdb_v2_expression_get_reference_index(duckdb_v2_expression_handle expression, idx_t *out_index,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!expression || !out_index) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_expression_get_reference_index");
		}
		auto &expr = *duckdb::ToExpression(expression);
		if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_REF) {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_expression_get_reference_index: expression is not a BOUND_REF");
		}
		*out_index = expr.Cast<duckdb::BoundReferenceExpression>().Index();
	});
}

DUCKDB_V2_ERROR duckdb_v2_expression_get_column_binding(duckdb_v2_expression_handle expression, idx_t *out_table_index,
                                                        idx_t *out_column_index, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!expression) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_expression_get_column_binding");
		}
		auto &expr = *duckdb::ToExpression(expression);
		if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_expression_get_column_binding: expression is not a BOUND_COLUMN_REF");
		}
		auto &col = expr.Cast<duckdb::BoundColumnRefExpression>();
		if (out_table_index) {
			*out_table_index = col.Binding().table_index.index;
		}
		if (out_column_index) {
			*out_column_index = col.Binding().column_index.GetIndex();
		}
	});
}
