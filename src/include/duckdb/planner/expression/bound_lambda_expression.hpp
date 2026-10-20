//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_lambda_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BoundLambdaExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_LAMBDA;

public:
	BoundLambdaExpression(ExpressionType type_p, LogicalType return_type_p, unique_ptr<Expression> lambda_expr_p,
	                      idx_t parameter_count_p);
	//! A lambda with a known signature - its type is LAMBDA(parameter_types -> body type)
	BoundLambdaExpression(vector<LogicalType> parameter_types_p, unique_ptr<Expression> lambda_expr_p,
	                      vector<Identifier> parameter_names_p = {});

public:
	const unique_ptr<Expression> &LambdaExpr() const {
		return lambda_expr;
	}
	unique_ptr<Expression> &LambdaExprMutable() {
		return lambda_expr;
	}
	//! Replaces the body, keeping the lambda type in sync with the body's return type
	void SetLambdaExpr(unique_ptr<Expression> lambda_expr_p);
	//! Whether the lambda type carries parameter and return types (plans serialized before v2.1.0 do not)
	bool HasSignature() const {
		return return_type.HasParameters();
	}
	const vector<LogicalType> &ParameterTypes() const {
		return LambdaType::GetParameterTypes(return_type);
	}
	const vector<unique_ptr<Expression>> &Captures() const {
		return captures;
	}
	vector<unique_ptr<Expression>> &CapturesMutable() {
		return captures;
	}
	idx_t ParameterCount() const {
		return parameter_count;
	}
	idx_t &ParameterCountMutable() {
		return parameter_count;
	}
	const vector<Identifier> &ParameterNames() const {
		return parameter_names;
	}
	void SetParameterNames(vector<Identifier> names) {
		parameter_names = std::move(names);
	}

	string ToString() const override;
	bool Equals(const BaseExpression &other) const override;
	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

private:
	//! The lambda expression that we'll use in the expression executor during execution
	unique_ptr<Expression> lambda_expr;
	//! Non-lambda constants, column references, and outer lambda parameters that we need to pass
	//! into the execution chunk
	vector<unique_ptr<Expression>> captures;
	//! The number of lhs parameters of the lambda function
	idx_t parameter_count;
	//! The names of the lhs parameters, purely for display - they do not take part in Equals, because two
	//! lambdas that differ only in their parameter names are the same expression. Can be empty, in which case
	//! ToString falls back to printing only the body
	vector<Identifier> parameter_names;
};
} // namespace duckdb
