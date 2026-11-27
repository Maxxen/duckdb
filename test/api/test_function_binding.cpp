#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb;

static void NoopExec(DataChunk &args, ExpressionState &state, Vector &result) {
}

//! f(list T[], value T, *args VARCHAR, opt BIGINT := 0, **kwargs ANY) -> T
static ScalarFunctionSet GetTemplatedSet() {
	FunctionSignature sig;
	sig.AddVariable("T");
	sig.AddParameter("list", TypeName::LIST(TypeName::VARIABLE("T")));
	sig.AddParameter("value", TypeName::VARIABLE("T"));
	sig.AddArgs("args", LogicalType::VARCHAR);
	sig.AddKeywordOnly("opt", LogicalType::BIGINT, Value::BIGINT(0));
	sig.AddKwargs("kwargs", LogicalType::ANY);
	sig.SetReturnType(TypeName::VARIABLE("T"));
	ScalarFunctionSet set("f");
	set.AddFunction(ScalarFunction("f", std::move(sig), NoopExec));
	return set;
}

//! The parameter that receives each argument a function is bound with
static vector<idx_t> ParameterIndexes(const FunctionBinding &binding) {
	vector<idx_t> result;
	for (auto &argument : binding.layout.arguments) {
		result.push_back(argument.parameter_index);
	}
	return result;
}

//! The argument of the call each argument a function is bound with is
static vector<idx_t> ArgumentIndexes(const FunctionBinding &binding) {
	vector<idx_t> result;
	for (auto &argument : binding.layout.arguments) {
		result.push_back(argument.argument_index.GetIndex());
	}
	return result;
}

//! g(a INTEGER, b BIGINT), g(a BIGINT, b INTEGER), g(a VARCHAR)
static ScalarFunctionSet GetOverloadedSet() {
	ScalarFunctionSet set("g");
	for (auto &parameters : vector<vector<LogicalType>> {{LogicalType::INTEGER, LogicalType::BIGINT},
	                                                     {LogicalType::BIGINT, LogicalType::INTEGER},
	                                                     {LogicalType::VARCHAR}}) {
		FunctionSignature sig;
		sig.AddParameter("a", parameters[0]);
		if (parameters.size() > 1) {
			sig.AddParameter("b", parameters[1]);
		}
		sig.SetReturnType(LogicalType::INTEGER);
		set.AddFunction(ScalarFunction("g", std::move(sig), NoopExec));
	}
	return set;
}

//! The type the overload of a binding returns for the call
static LogicalType ReturnTypeOf(Connection &con, const FunctionBinding &binding) {
	TypeCache cache(con.context.get());
	return FunctionBinder::BindReturnType(cache, binding);
}

//! Why no overload matches - the selection must have failed to match
static const vector<FunctionMismatch> &GetMismatches(const FunctionBindResult &selection) {
	REQUIRE(!selection.IsSuccess());
	REQUIRE(selection.GetError().GetType() == FunctionBindError::Type::NO_MATCH);
	return selection.GetError().Cast<FunctionBindError::NoMatch>().mismatches;
}

static string GetErrorMessage(const FunctionBindResult &selection) {
	return selection.GetError().RawMessage();
}

TEST_CASE("Argument mapping preserves defaults, templates and keyword restrictions", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	FunctionSignature sig;
	sig.AddVariable("T");
	sig.AddParameter("a", TypeName::VARIABLE("T"));
	sig.AddParameter("b", TypeName::VARIABLE("T"), Value::BIGINT(0));
	sig.SetReturnType(TypeName::VARIABLE("T"));
	ScalarFunctionSet set("mapping");
	set.AddFunction(ScalarFunction("mapping", std::move(sig), NoopExec));

	// Both parameters are supplied: the default must not participate in template deduction.
	auto selection = FunctionBinder::SelectFunction(*con.context, set, {LogicalType::INTEGER, LogicalType::INTEGER});
	REQUIRE(selection.IsSuccess());
	REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::INTEGER);
	const vector<idx_t> identity {0, 1};
	REQUIRE(ParameterIndexes(selection.GetBinding()) == identity);
	REQUIRE(ArgumentIndexes(selection.GetBinding()) == identity);

	// An omitted parameter still contributes its default to template deduction.
	selection = FunctionBinder::SelectFunction(*con.context, set, {LogicalType::INTEGER});
	REQUIRE(selection.IsSuccess());
	REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::BIGINT);

	vector<LogicalType> no_positional;
	vector<pair<Identifier, LogicalType>> reversed {{"b", LogicalType::BIGINT}, {"a", LogicalType::INTEGER}};
	selection = FunctionBinder::SelectFunction(*con.context, set, {no_positional, reversed});
	REQUIRE(selection.IsSuccess());
	// the arguments are bound in the order of the parameters, whatever order the call passes them in
	const vector<idx_t> reordered {1, 0};
	REQUIRE(ParameterIndexes(selection.GetBinding()) == identity);
	REQUIRE(ArgumentIndexes(selection.GetBinding()) == reordered);
	REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::BIGINT);

	// Equal argument and parameter counts do not make a keyword-only parameter positional.
	FunctionSignature keyword_sig;
	keyword_sig.AddParameter("a", LogicalType::INTEGER);
	keyword_sig.AddKeywordOnly("b", LogicalType::INTEGER, Value::INTEGER(0));
	keyword_sig.SetReturnType(LogicalType::INTEGER);
	ScalarFunctionSet keyword_set("keyword");
	keyword_set.AddFunction(ScalarFunction("keyword", std::move(keyword_sig), NoopExec));
	selection = FunctionBinder::SelectFunction(*con.context, keyword_set, {LogicalType::INTEGER, LogicalType::INTEGER});
	REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::TOO_MANY_ARGUMENTS);
}

TEST_CASE("Selecting a function maps every argument to the parameter that receives it", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.context->RunFunctionInTransaction([&]() {
		auto set = GetTemplatedSet();

		vector<LogicalType> positional {LogicalType::LIST(LogicalType::INTEGER), LogicalType::INTEGER,
		                                LogicalType::VARCHAR, LogicalType::VARCHAR};
		vector<pair<Identifier, LogicalType>> named {{"opt", LogicalType::BIGINT}, {"extra", LogicalType::BOOLEAN}};
		auto selection = FunctionBinder::SelectFunction(*con.context, set, {positional, named});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 0);

		// list, value, the two arguments "*args" receives, the keyword-only parameter, and "**kwargs"
		auto &binding = selection.GetBinding();
		const vector<idx_t> expected_parameters {0, 1, 2, 2, 3, 4};
		REQUIRE(ParameterIndexes(binding) == expected_parameters);
		// "**kwargs" takes any type, so its argument is passed as the type it is
		const vector<LogicalType> expected_types {LogicalType::LIST(LogicalType::INTEGER),
		                                          LogicalType::INTEGER,
		                                          LogicalType::VARCHAR,
		                                          LogicalType::VARCHAR,
		                                          LogicalType::BIGINT,
		                                          LogicalType::BOOLEAN};
		REQUIRE(binding.layout.GetArgumentTypes() == expected_types);
		REQUIRE(ReturnTypeOf(con, binding) == LogicalType::INTEGER);
	});
}

TEST_CASE("Overload selection keeps cheaper later matches and equal-cost ambiguities", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	ScalarFunctionSet set("costs");
	for (auto &type : {LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::DOUBLE}) {
		FunctionSignature sig;
		sig.AddParameter("a", type);
		sig.SetReturnType(type);
		set.AddFunction(ScalarFunction("costs", std::move(sig), NoopExec));
	}
	auto selection = FunctionBinder::SelectFunction(*con.context, set, {LogicalType::INTEGER});
	REQUIRE(selection.IsSuccess());
	REQUIRE(selection.GetBinding().GetOverloadIndex() == 1);
	REQUIRE(selection.GetBinding().GetFunction().GetSignature().GetReturnType() == LogicalType::INTEGER);

	// A later equally good overload must still be retained.
	FunctionSignature sig;
	sig.AddParameter("a", LogicalType::INTEGER);
	sig.SetReturnType(LogicalType::BIGINT);
	set.AddFunction(ScalarFunction("costs", std::move(sig), NoopExec));
	selection = FunctionBinder::SelectFunction(*con.context, set, {LogicalType::INTEGER});
	REQUIRE(!selection.IsSuccess());
	REQUIRE(selection.GetError().GetType() == FunctionBindError::Type::AMBIGUOUS_MATCH);
}

TEST_CASE("Selecting a function deduces the templates of the overload", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.context->RunFunctionInTransaction([&]() {
		auto set = GetTemplatedSet();

		// the template unifies the element type of the list with the value
		auto selection = FunctionBinder::SelectFunction(*con.context, set,
		                                                {LogicalType::LIST(LogicalType::INTEGER), LogicalType::BIGINT});
		REQUIRE(selection.IsSuccess());
		auto argument_types = selection.GetBinding().layout.GetArgumentTypes();
		REQUIRE(argument_types[0] == LogicalType::LIST(LogicalType::BIGINT));
		REQUIRE(argument_types[1] == LogicalType::BIGINT);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::BIGINT);

		// arguments that determine conflicting types for the template do not match the overload
		vector<LogicalType> conflicting {LogicalType::LIST(LogicalType::DATE), LogicalType::BOOLEAN};
		selection = FunctionBinder::SelectFunction(*con.context, set, conflicting);
		auto &deduction = GetMismatches(selection)[0];
		REQUIRE(deduction.GetType() == FunctionMismatch::Type::TEMPLATE_DEDUCTION);
		REQUIRE(deduction.Cast<FunctionMismatch::TemplateDeduction>().template_name == "T");
		REQUIRE(deduction.Cast<FunctionMismatch::TemplateDeduction>().argument.GetIndex() == 1);
		REQUIRE(StringUtil::Contains(GetErrorMessage(selection), "Cannot deduce template type 'T'"));

		// without a client context, the template is unified with the built-in implicit casts
		selection = FunctionBinder::SelectFunction(nullptr, set,
		                                           {LogicalType::LIST(LogicalType::INTEGER), LogicalType::BIGINT});
		REQUIRE(selection.IsSuccess());
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::BIGINT);

		// a template that neither the arguments nor the defaults determine does not match either
		FunctionSignature sig;
		sig.AddVariable("T");
		sig.AddParameter("value", LogicalType::INTEGER);
		sig.SetReturnType(TypeName::VARIABLE("T"));
		ScalarFunctionSet undetermined("u");
		undetermined.AddFunction(ScalarFunction("u", std::move(sig), NoopExec));
		selection = FunctionBinder::SelectFunction(*con.context, undetermined, {LogicalType::INTEGER});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::TEMPLATE_UNRESOLVED);
		REQUIRE(StringUtil::Contains(GetErrorMessage(selection), "is not determined by the arguments"));
	});
}

TEST_CASE("A struct parameter of type variables takes the fields of a struct by name", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.context->RunFunctionInTransaction([&]() {
		// second<T, U>(s STRUCT(a T, b U)) -> U
		ScalarFunctionSet set("second");
		set.AddFunction(ScalarFunction(
		    "second",
		    FunctionSignature()
		        .AddVariable("T")
		        .AddVariable("U")
		        .AddParameter("s", TypeName::STRUCT({{"a", TypeName::VARIABLE("T")}, {"b", TypeName::VARIABLE("U")}}))
		        .SetReturnType(TypeName::VARIABLE("U")),
		    NoopExec));
		const auto declared = LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}});

		auto selection = FunctionBinder::SelectFunction(*con.context, set, {declared});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == declared);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::VARCHAR);

		// fields in another order are matched by name, the argument is cast to the order the parameter declares
		const auto reordered = LogicalType::STRUCT({{"b", LogicalType::VARCHAR}, {"a", LogicalType::INTEGER}});
		selection = FunctionBinder::SelectFunction(*con.context, set, {reordered});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.types.at("T") == LogicalType::INTEGER);
		REQUIRE(selection.GetBinding().variables.types.at("U") == LogicalType::VARCHAR);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == declared);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::VARCHAR);

		// a struct without one of the fields is not taken
		const auto other = LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"c", LogicalType::VARCHAR}});
		selection = FunctionBinder::SelectFunction(*con.context, set, {other});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
	});
}

TEST_CASE("Selecting a function determines the value variables of the overload", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.context->RunFunctionInTransaction([&]() {
		auto array_of = [](TypeName element, const string &size) {
			return TypeName(Identifier("ARRAY"), {std::move(element), TypeName(Identifier(size))});
		};
		const auto int_array = LogicalType::ARRAY(LogicalType::INTEGER, 3);
		const auto bigint_array = LogicalType::ARRAY(LogicalType::BIGINT, 3);

		// zeros<N: INTEGER>(n N) -> DOUBLE[N]: the value of a constant argument determines the variable
		ScalarFunctionSet zeros("zeros");
		zeros.AddFunction(ScalarFunction("zeros",
		                                 FunctionSignature()
		                                     .AddVariable("N", TypeVariable::Kind::INTEGER)
		                                     .AddParameter("n", TypeName(Identifier("N")))
		                                     .SetReturnType(array_of(TypeName(LogicalType::DOUBLE), "N")),
		                                 NoopExec));
		auto selection =
		    FunctionBinder::SelectFunction(*con.context, zeros, {FunctionCallArgument::Constant(Value::INTEGER(3))});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::BIGINT);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::ARRAY(LogicalType::DOUBLE, 3));
		REQUIRE(selection.GetBinding().variables.values.at("N") == Value::BIGINT(3));

		// any constant expression does, an argument that is not constant does not match
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(4)));
		selection = FunctionBinder::SelectFunction(*con.context, zeros, arguments);
		REQUIRE(selection.IsSuccess());
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::ARRAY(LogicalType::DOUBLE, 4));
		arguments.clear();
		arguments.push_back(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0U));
		selection = FunctionBinder::SelectFunction(*con.context, zeros, arguments);
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::CONSTANT_REQUIRED);
		REQUIRE(StringUtil::Contains(GetErrorMessage(selection), "Argument 1 has to be a constant"));

		// same<T, N: INTEGER>(a T[N], b T[N]) -> T[N]: the types of the arguments determine the variable
		ScalarFunctionSet same("same");
		same.AddFunction(ScalarFunction("same",
		                                FunctionSignature()
		                                    .AddVariable("T")
		                                    .AddVariable("N", TypeVariable::Kind::INTEGER)
		                                    .AddParameter("a", array_of(TypeName(Identifier("T")), "N"))
		                                    .AddParameter("b", array_of(TypeName(Identifier("T")), "N"))
		                                    .SetReturnType(array_of(TypeName(Identifier("T")), "N")),
		                                NoopExec));
		selection = FunctionBinder::SelectFunction(*con.context, same, {int_array, bigint_array});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == bigint_array);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == bigint_array);
		REQUIRE(selection.GetBinding().variables.types.at("T") == LogicalType::BIGINT);
		REQUIRE(selection.GetBinding().variables.values.at("N") == Value::BIGINT(3));

		// arguments that determine different values do not match the overload
		selection = FunctionBinder::SelectFunction(*con.context, same,
		                                           {int_array, LogicalType::ARRAY(LogicalType::INTEGER, 4)});
		auto &deduction = GetMismatches(selection)[0];
		REQUIRE(deduction.GetType() == FunctionMismatch::Type::TEMPLATE_DEDUCTION);
		REQUIRE(deduction.Cast<FunctionMismatch::TemplateDeduction>().template_name == "N");
		REQUIRE(deduction.Cast<FunctionMismatch::TemplateDeduction>().argument.GetIndex() == 1);
		REQUIRE(StringUtil::Contains(GetErrorMessage(selection), "it is determined to be both 3 and 4"));

		// a list says no size, but what it is a list of: it is passed as the array the arguments determine
		const auto bigint_list = LogicalType::LIST(LogicalType::BIGINT);
		selection = FunctionBinder::SelectFunction(*con.context, same, {int_array, bigint_list});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.types.at("T") == LogicalType::BIGINT);
		REQUIRE(selection.GetBinding().layout.arguments[1].type == bigint_array);

		// an argument that says nothing leaves the value to the other arguments
		selection = FunctionBinder::SelectFunction(*con.context, same, {LogicalType::SQLNULL, int_array});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.values.at("N") == Value::BIGINT(3));
		REQUIRE(selection.GetBinding().layout.arguments[0].type == int_array);
		// if none of them says it, the return type that uses it is not determined
		selection = FunctionBinder::SelectFunction(*con.context, same, {LogicalType::SQLNULL, LogicalType::SQLNULL});
		auto &unresolved = GetMismatches(selection)[0];
		REQUIRE(unresolved.GetType() == FunctionMismatch::Type::TEMPLATE_UNRESOLVED);
		REQUIRE(unresolved.Cast<FunctionMismatch::TemplateUnresolved>().template_name == "N");

		// rescaled<W: INTEGER, S: INTEGER>(x DECIMAL(W, S)) -> DECIMAL(W, S): also of the decimal a number is passed as
		TypeName decimal(Identifier("DECIMAL"), {TypeName(Identifier("W")), TypeName(Identifier("S"))});
		ScalarFunctionSet rescaled("rescaled");
		rescaled.AddFunction(ScalarFunction("rescaled",
		                                    FunctionSignature()
		                                        .AddVariable("W", TypeVariable::Kind::INTEGER)
		                                        .AddVariable("S", TypeVariable::Kind::INTEGER)
		                                        .AddParameter("x", decimal)
		                                        .SetReturnType(decimal),
		                                    NoopExec));
		selection = FunctionBinder::SelectFunction(*con.context, rescaled, {LogicalType::DECIMAL(12, 4)});
		REQUIRE(selection.IsSuccess());
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::DECIMAL(12, 4));
		selection = FunctionBinder::SelectFunction(*con.context, rescaled, {LogicalType::INTEGER});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::DECIMAL(10, 0));
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::DECIMAL(10, 0));
	});
}

TEST_CASE("A value the type of a parameter declares is the one an argument of its kind has",
          "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.context->RunFunctionInTransaction([&]() {
		// integral<W: INTEGER>(x DECIMAL(W, 0)) -> INTEGER
		ScalarFunctionSet integral("integral");
		integral.AddFunction(
		    ScalarFunction("integral",
		                   FunctionSignature()
		                       .AddVariable("W", TypeVariable::Kind::INTEGER)
		                       .AddParameter("x", TypeName::DECIMAL(TypeName::VARIABLE("W"), Literal::Integer(0)))
		                       .SetReturnType(LogicalType::INTEGER),
		                   NoopExec));
		// a decimal of the scale is taken as it is
		auto selection = FunctionBinder::SelectFunction(*con.context, integral, {LogicalType::DECIMAL(18, 0)});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::DECIMAL(18, 0));
		REQUIRE(selection.GetBinding().variables.values.at("W") == Value::BIGINT(18));
		// one of another scale is not passed as one of it
		selection = FunctionBinder::SelectFunction(*con.context, integral, {LogicalType::DECIMAL(18, 3)});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		REQUIRE(GetMismatches(selection)[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);
		// a number is passed as the decimal that holds it, and a NULL says nothing
		selection = FunctionBinder::SelectFunction(*con.context, integral, {LogicalType::INTEGER});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::DECIMAL(10, 0));
		selection = FunctionBinder::SelectFunction(*con.context, integral, {LogicalType::SQLNULL});
		REQUIRE(selection.IsSuccess());

		// triple<T>(x T[3]) -> T
		ScalarFunctionSet triple("triple");
		triple.AddFunction(ScalarFunction("triple",
		                                  FunctionSignature()
		                                      .AddVariable("T")
		                                      .AddParameter("x", TypeName::ARRAY(TypeName::VARIABLE("T"), 3))
		                                      .SetReturnType(TypeName::VARIABLE("T")),
		                                  NoopExec));
		const auto integers_3 = LogicalType::ARRAY(LogicalType::INTEGER, 3);
		selection = FunctionBinder::SelectFunction(*con.context, triple, {integers_3});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == integers_3);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::INTEGER);
		selection = FunctionBinder::SelectFunction(*con.context, triple, {LogicalType::ARRAY(LogicalType::INTEGER, 4)});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		// a list says no size: it is passed as an array of the one the name declares
		selection = FunctionBinder::SelectFunction(*con.context, triple, {LogicalType::LIST(LogicalType::INTEGER)});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == integers_3);
	});
}

TEST_CASE("A call no overload matches records why each overload fails", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto set = GetOverloadedSet();

	auto selection = FunctionBinder::SelectFunction(*con.context, set, {LogicalType::DATE});
	REQUIRE(!selection);
	auto message = GetErrorMessage(selection);
	REQUIRE(StringUtil::Contains(message, "No function matches"));

	auto &failed = GetMismatches(selection);
	REQUIRE(failed.size() == 3);
	// the last cannot take a DATE as its VARCHAR, which is closer than the first two leaving "b" unfilled
	REQUIRE(failed[0].GetOverloadIndex() == 2);
	REQUIRE(failed[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
	REQUIRE(failed[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);
	REQUIRE(failed[1].GetOverloadIndex() == 0);
	REQUIRE(failed[1].GetType() == FunctionMismatch::Type::MISSING_PARAMETER);
	REQUIRE(failed[1].Cast<FunctionMismatch::MissingParameter>().parameter == 1);
	REQUIRE(failed[2].GetOverloadIndex() == 1);
	REQUIRE(failed[0].GetDistance() < failed[1].GetDistance());
	REQUIRE(failed[1].GetDistance() == failed[2].GetDistance());
	REQUIRE(failed[0].Cast<FunctionMismatch::IncompatibleArgument>().parameter_type == LogicalType::VARCHAR);
	REQUIRE(selection.GetError().Cast<FunctionBindError::NoMatch>().GetClosest()->GetOverloadIndex() == 2);
	REQUIRE(StringUtil::Contains(message, "Candidates:\n    g(a VARCHAR) -> INTEGER\n        Argument 1 of type DATE "
	                                      "cannot be implicitly cast to VARCHAR"));

	// too many arguments, and a name no overload has
	vector<LogicalType> three_varchars {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	selection = FunctionBinder::SelectFunction(*con.context, set, three_varchars);
	REQUIRE(GetMismatches(selection)[2].GetOverloadIndex() == 2);
	REQUIRE(GetMismatches(selection)[2].GetType() == FunctionMismatch::Type::TOO_MANY_ARGUMENTS);
	// the two overloads taking two arguments are equally close, so neither is the closest
	REQUIRE(!selection.GetError().Cast<FunctionBindError::NoMatch>().GetClosest());

	// h(a VARCHAR, b INTEGER := 0) takes two arguments, but none named "c"
	FunctionSignature sig;
	sig.AddParameter("a", LogicalType::VARCHAR);
	sig.AddParameter("b", LogicalType::INTEGER, Value::INTEGER(0));
	sig.SetReturnType(LogicalType::INTEGER);
	ScalarFunctionSet defaulted("h");
	defaulted.AddFunction(ScalarFunction("h", std::move(sig), NoopExec));
	vector<pair<Identifier, LogicalType>> named_c {{"c", LogicalType::INTEGER}};
	selection = FunctionBinder::SelectFunction(*con.context, defaulted, {{LogicalType::VARCHAR}, named_c});
	REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::UNKNOWN_NAMED_ARGUMENT);
	REQUIRE(GetMismatches(selection)[0].Cast<FunctionMismatch::UnknownNamedArgument>().argument == 1);
	message = GetErrorMessage(selection);
	REQUIRE(StringUtil::Contains(message, "    h(a VARCHAR, b INTEGER := 0) -> INTEGER\n        Has no parameter "
	                                      "named \"c\""));
}

TEST_CASE("Fewer arguments that cannot be cast are a closer miss", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	// k(a DATE, b DATE), k(a INTEGER, b DATE)
	ScalarFunctionSet set("k");
	for (auto &first : {LogicalType::DATE, LogicalType::INTEGER}) {
		FunctionSignature sig;
		sig.AddParameter("a", first);
		sig.AddParameter("b", LogicalType::DATE);
		sig.SetReturnType(LogicalType::INTEGER);
		set.AddFunction(ScalarFunction("k", std::move(sig), NoopExec));
	}

	auto selection = FunctionBinder::SelectFunction(*con.context, set, {LogicalType::INTEGER, LogicalType::BOOLEAN});
	auto &failed = GetMismatches(selection);
	REQUIRE(failed[0].GetOverloadIndex() == 1);
	REQUIRE(failed[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 1);
	REQUIRE(failed[0].GetDistance() == 1);
	REQUIRE(failed[1].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);
	REQUIRE(failed[1].GetDistance() == 2);
}

TEST_CASE("A call several overloads match at the same cost is ambiguous", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto set = GetOverloadedSet();

	vector<LogicalType> two_integers {LogicalType::INTEGER, LogicalType::INTEGER};
	auto selection = FunctionBinder::SelectFunction(*con.context, set, two_integers);
	REQUIRE(!selection);
	REQUIRE(selection.GetError().GetType() == FunctionBindError::Type::AMBIGUOUS_MATCH);
	REQUIRE(StringUtil::Contains(GetErrorMessage(selection), "Could not choose a best candidate"));
	// only the overloads tied at the lowest cost
	auto &candidates = selection.GetError().Cast<FunctionBindError::AmbiguousMatch>().candidates;
	REQUIRE(candidates.size() == 2);
	REQUIRE(candidates[0].rank == candidates[1].rank);
}

TEST_CASE("Of the overloads that match at the same cost the most specific one is selected", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	// crs<C: STRING>(g GEOMETRY(C)) says more about its argument than crs(g GEOMETRY)
	ScalarFunctionSet crs("crs");
	crs.AddFunction(ScalarFunction("crs",
	                               FunctionSignature()
	                                   .AddVariable("C", TypeVariable::Kind::STRING)
	                                   .AddParameter("g", TypeName(Identifier("GEOMETRY"), {TypeName::VARIABLE("C")}))
	                                   .SetReturnType(LogicalType::VARCHAR),
	                               NoopExec));
	crs.AddFunction(ScalarFunction(
	    "crs", FunctionSignature().AddParameter("g", LogicalType::GEOMETRY()).SetReturnType(LogicalType::VARCHAR),
	    NoopExec));

	con.context->RunFunctionInTransaction([&]() {
		// a geometry of a coordinate reference system is taken as it is
		auto geometry_crs = LogicalType::GEOMETRY("OGC:CRS84");
		auto selection = FunctionBinder::SelectFunction(*con.context, crs, {geometry_crs});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 0);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == geometry_crs);

		// one without does not determine the variable
		selection = FunctionBinder::SelectFunction(*con.context, crs, {LogicalType::GEOMETRY()});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 1);

		// a NULL matches both
		selection = FunctionBinder::SelectFunction(*con.context, crs, {LogicalType::SQLNULL});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 0);
	});
}

//! A variable the parameter types of the selection cases use
struct SelectionVariable {
	const char *name;
	TypeVariable::Kind kind;
	//! The kind of type a type variable stands for, INVALID if it stands for any type
	LogicalTypeId constraint;
};

//! A call of the overloads of a function set, and the overload it selects
struct SelectionCase {
	string description;
	//! The parameter types of every overload
	vector<vector<TypeName>> overloads;
	vector<LogicalType> arguments;
	//! The position in "overloads" of the one that is selected, invalid if the call is ambiguous
	optional_idx selected;
};

//! A signature of parameters of the given types, that declares the variables the types use
static FunctionSignature GetSelectionSignature(const vector<TypeName> &parameters) {
	static const SelectionVariable VARIABLES[] = {{"T", TypeVariable::Kind::TYPE, LogicalTypeId::INVALID},
	                                              {"U", TypeVariable::Kind::TYPE, LogicalTypeId::INVALID},
	                                              {"TD", TypeVariable::Kind::TYPE, LogicalTypeId::DECIMAL},
	                                              {"UD", TypeVariable::Kind::TYPE, LogicalTypeId::DECIMAL},
	                                              {"TS", TypeVariable::Kind::TYPE, LogicalTypeId::STRUCT},
	                                              {"N", TypeVariable::Kind::INTEGER, LogicalTypeId::INVALID},
	                                              {"W", TypeVariable::Kind::INTEGER, LogicalTypeId::INVALID},
	                                              {"S", TypeVariable::Kind::INTEGER, LogicalTypeId::INVALID},
	                                              {"C", TypeVariable::Kind::STRING, LogicalTypeId::INVALID}};
	FunctionSignature sig;
	for (auto &variable : VARIABLES) {
		bool is_used = false;
		for (auto &parameter : parameters) {
			is_used = is_used || parameter.Contains(Identifier(variable.name));
		}
		if (!is_used) {
			continue;
		}
		sig.AddVariable(variable.name, variable.kind);
		if (variable.constraint != LogicalTypeId::INVALID) {
			sig.AddConstraint(variable.name, variable.constraint);
		}
	}
	for (auto &parameter : parameters) {
		sig.AddParameter(parameter);
	}
	sig.SetReturnType(LogicalType::INTEGER);
	return sig;
}

TEST_CASE("Overloads that match at the same cost are selected by how specific they are whatever their order",
          "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	// type variables: of any type, of a DECIMAL, of a STRUCT
	const auto t = TypeName::VARIABLE("T");
	const auto u = TypeName::VARIABLE("U");
	const auto t_decimal = TypeName::VARIABLE("TD");
	const auto u_decimal = TypeName::VARIABLE("UD");
	const auto t_struct = TypeName::VARIABLE("TS");
	const TypeName any(LogicalType::ANY);
	const auto t_list = TypeName::LIST(t);
	const auto any_list = TypeName::LIST(any);
	// types of value variables, and types that declare the values
	const TypeName geometry(LogicalType::GEOMETRY());
	const TypeName geometry_c(Identifier("GEOMETRY"), {TypeName::VARIABLE("C")});
	const auto decimal_ws = TypeName::DECIMAL(TypeName::VARIABLE("W"), TypeName::VARIABLE("S"));
	const TypeName decimal_18_3(LogicalType::DECIMAL(18, 3));
	const auto decimal_w0 = TypeName::DECIMAL(TypeName::VARIABLE("W"), Literal::Integer(0));
	const auto float_n = TypeName::ARRAY(LogicalType::FLOAT, TypeName::VARIABLE("N"));
	const auto float_3 = TypeName::ARRAY(LogicalType::FLOAT, 3);
	const auto t_n = TypeName::ARRAY(t, TypeName::VARIABLE("N"));
	const auto t_3 = TypeName::ARRAY(t, 3);
	const auto struct_a = TypeName::STRUCT({{"a", LogicalType::INTEGER}});
	const auto integer_list = TypeName::LIST(LogicalType::INTEGER);

	con.context->RunFunctionInTransaction([&]() {
		// the arguments of the calls
		const auto crs = LogicalType::GEOMETRY("OGC:CRS84");
		const auto no_crs = LogicalType::GEOMETRY();
		const auto decimal = LogicalType::DECIMAL(18, 3);
		const auto integral = LogicalType::DECIMAL(18, 0);
		const LogicalType integer = LogicalType::INTEGER;
		const auto floats_3 = LogicalType::ARRAY(LogicalType::FLOAT, 3);
		const auto floats_4 = LogicalType::ARRAY(LogicalType::FLOAT, 4);
		const auto integers_3 = LogicalType::ARRAY(LogicalType::INTEGER, 3);
		const auto a_struct = LogicalType::STRUCT({{"a", LogicalType::INTEGER}});
		const auto integers = LogicalType::LIST(LogicalType::INTEGER);
		const LogicalType null = LogicalType::SQLNULL;
		const optional_idx ambiguous;

		const vector<SelectionCase> cases {
		    // a name with arguments says more than the name
		    {"GEOMETRY(C) over GEOMETRY, a geometry of a CRS", {{geometry_c}, {geometry}}, {crs}, 0},
		    {"GEOMETRY(C) over GEOMETRY, NULL", {{geometry_c}, {geometry}}, {null}, 0},
		    {"DECIMAL(W, S) over T IS DECIMAL, a decimal", {{decimal_ws}, {t_decimal}}, {decimal}, 0},
		    {"DECIMAL(W, S) takes an integer, T IS DECIMAL does not", {{decimal_ws}, {t_decimal}}, {integer}, 0},
		    {"DECIMAL(W, S) over T IS DECIMAL, NULL", {{decimal_ws}, {t_decimal}}, {null}, 0},
		    // a value says more than a value variable
		    {"FLOAT[3] over FLOAT[N], FLOAT[3]", {{float_3}, {float_n}}, {floats_3}, 0},
		    {"FLOAT[3] over FLOAT[N], INTEGER[3]", {{float_3}, {float_n}}, {integers_3}, 0},
		    {"FLOAT[3] over FLOAT[N], NULL", {{float_3}, {float_n}}, {null}, 0},
		    {"DECIMAL(18, 3) over DECIMAL(W, S), DECIMAL(18, 3)", {{decimal_18_3}, {decimal_ws}}, {decimal}, 0},
		    {"DECIMAL(W, 0) over DECIMAL(W, S), DECIMAL(18, 0)", {{decimal_w0}, {decimal_ws}}, {integral}, 0},
		    {"DECIMAL(W, 0) over DECIMAL(W, S), an integer", {{decimal_w0}, {decimal_ws}}, {integer}, 0},
		    {"DECIMAL(W, 0) over DECIMAL(W, S), NULL", {{decimal_w0}, {decimal_ws}}, {null}, 0},
		    {"T[3] over T[N], FLOAT[3]", {{t_3}, {t_n}}, {floats_3}, 0},
		    // a type says more than a type variable of its kind
		    {"STRUCT(a INTEGER) over T IS STRUCT, the struct", {{struct_a}, {t_struct}}, {a_struct}, 0},
		    {"DECIMAL(18, 3) over T IS DECIMAL, DECIMAL(18, 3)", {{decimal_18_3}, {t_decimal}}, {decimal}, 0},
		    {"DECIMAL(18, 3) over T IS DECIMAL, NULL", {{decimal_18_3}, {t_decimal}}, {null}, 0},
		    // overloads that say as much, or of types that have nothing to do with each other
		    {"T and ANY, an integer", {{t}, {any}}, {integer}, ambiguous},
		    {"T and ANY, NULL", {{t}, {any}}, {null}, ambiguous},
		    {"T and U, an integer", {{t}, {u}}, {integer}, ambiguous},
		    {"T[] and ANY[], a list", {{t_list}, {any_list}}, {integers}, ambiguous},
		    {"T[] and ANY[], NULL", {{t_list}, {any_list}}, {null}, ambiguous},
		    {"T IS DECIMAL and U IS DECIMAL, a decimal", {{t_decimal}, {u_decimal}}, {decimal}, ambiguous},
		    {"T IS DECIMAL and U IS DECIMAL, NULL", {{t_decimal}, {u_decimal}}, {null}, ambiguous},
		    {"DATE and TIME, NULL", {{LogicalType::DATE}, {LogicalType::TIME}}, {null}, ambiguous},
		    // every argument counts: more for one of them and as much for the others is more
		    {"(GEOMETRY(C), GEOMETRY(C)) over (GEOMETRY(C), GEOMETRY)",
		     {{geometry_c, geometry_c}, {geometry_c, geometry}},
		     {crs, crs},
		     0},
		    {"(FLOAT[3], FLOAT[3]) over (FLOAT[3], FLOAT[N])",
		     {{float_3, float_3}, {float_3, float_n}},
		     {floats_3, floats_3},
		     0},
		    {"(FLOAT[N], DECIMAL(W, S)) over (FLOAT[N], T IS DECIMAL)",
		     {{float_n, decimal_ws}, {float_n, t_decimal}},
		     {floats_3, decimal},
		     0},
		    {"(FLOAT[3], FLOAT[3]) over (FLOAT[3], FLOAT[N]) and (FLOAT[N], FLOAT[N])",
		     {{float_3, float_n}, {float_n, float_n}, {float_3, float_3}},
		     {floats_3, floats_3},
		     2},
		    // more for one of them and less for another is neither
		    {"(GEOMETRY(C), GEOMETRY) and (GEOMETRY, GEOMETRY(C))",
		     {{geometry_c, geometry}, {geometry, geometry_c}},
		     {crs, crs},
		     ambiguous},
		    {"(FLOAT[3], FLOAT[N]) and (FLOAT[N], FLOAT[3])",
		     {{float_3, float_n}, {float_n, float_3}},
		     {floats_3, floats_3},
		     ambiguous},
		    {"(FLOAT[3], FLOAT[N]) and (FLOAT[N], FLOAT[3]), both over (FLOAT[N], FLOAT[N])",
		     {{float_3, float_n}, {float_n, float_n}, {float_n, float_3}},
		     {floats_3, floats_3},
		     ambiguous},
		    // no tie: only one of the overloads takes the argument
		    {"GEOMETRY, not GEOMETRY(C), a geometry of no CRS", {{geometry_c}, {geometry}}, {no_crs}, 1},
		    {"FLOAT[N], not FLOAT[3], FLOAT[4]", {{float_3}, {float_n}}, {floats_4}, 1},
		    {"DECIMAL(W, S), not DECIMAL(W, 0), DECIMAL(18, 3)", {{decimal_w0}, {decimal_ws}}, {decimal}, 1},
		    {"T[N], not T[3], FLOAT[4]", {{t_3}, {t_n}}, {floats_4}, 1},
		    // no tie: the cost decides, a type variable of any type costs more than any cast. A NULL that is all a
		    // type variable is passed costs what a NULL costs as itself, whatever the constraint of the variable
		    {"INTEGER at a lower cost than T, an integer", {{LogicalType::INTEGER}, {t}}, {integer}, 0},
		    {"INTEGER at a lower cost than T, NULL", {{LogicalType::INTEGER}, {t}}, {null}, 0},
		    {"T IS STRUCT at a lower cost than STRUCT(a INTEGER), NULL", {{struct_a}, {t_struct}}, {null}, 1},
		    {"T IS STRUCT at a lower cost than T, a struct", {{t_struct}, {t}}, {a_struct}, 0},
		    {"T IS DECIMAL at a lower cost than T, a decimal", {{t_decimal}, {t}}, {decimal}, 0},
		    {"T IS DECIMAL does not take an integer, T does", {{t_decimal}, {t}}, {integer}, 1},
		    {"INTEGER[] at a lower cost than T[], a list", {{integer_list}, {t_list}}, {integers}, 0},
		    {"T[] at a lower cost than T, a list", {{t_list}, {t}}, {integers}, 0},
		};

		for (auto &test : cases) {
			const auto count = test.overloads.size();
			// the overloads in the order of the case, and in the opposite order
			for (idx_t reversed = 0; reversed < 2; reversed++) {
				ScalarFunctionSet set("f");
				for (idx_t i = 0; i < count; i++) {
					auto &parameters = test.overloads[reversed ? count - 1 - i : i];
					set.AddFunction(ScalarFunction("f", GetSelectionSignature(parameters), NoopExec));
				}
				INFO(test.description << (reversed ? ", overloads in the opposite order" : ""));
				auto selection = FunctionBinder::SelectFunction(*con.context, set, test.arguments);
				if (!test.selected.IsValid()) {
					CHECK(!selection.IsSuccess());
					if (!selection.IsSuccess()) {
						CHECK(selection.GetError().GetType() == FunctionBindError::Type::AMBIGUOUS_MATCH);
					}
					continue;
				}
				CHECK(selection.IsSuccess());
				if (selection.IsSuccess()) {
					const auto index = selection.GetBinding().GetOverloadIndex();
					CHECK((reversed ? count - 1 - index : index) == test.selected.GetIndex());
				}
			}
		}
	});
}

//! A call of one overload, and the rank the call matches it at
struct RankCase {
	string description;
	FunctionSignature signature;
	vector<LogicalType> arguments;
	FunctionRank rank;
};

static FunctionRank MakeRank(idx_t unconstrained_arguments, idx_t cast_cost, idx_t variable_parameters,
                             idx_t declared_structure, idx_t declared_values) {
	FunctionRank rank;
	rank.unconstrained_arguments = unconstrained_arguments;
	rank.cast_cost = cast_cost;
	rank.variable_parameters = variable_parameters;
	rank.declared_structure = declared_structure;
	rank.declared_values = declared_values;
	return rank;
}

TEST_CASE("A call matches an overload at the rank the rules count", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto t = TypeName::VARIABLE("T");
	const auto t_struct = TypeName::VARIABLE("TS");
	const auto t_list = TypeName::LIST(t);
	const TypeName geometry_c(Identifier("GEOMETRY"), {TypeName::VARIABLE("C")});
	const auto decimal_ws = TypeName::DECIMAL(TypeName::VARIABLE("W"), TypeName::VARIABLE("S"));
	const auto decimal_w0 = TypeName::DECIMAL(TypeName::VARIABLE("W"), Literal::Integer(0));
	const TypeName decimal_18_3(LogicalType::DECIMAL(18, 3));
	const auto float_n = TypeName::ARRAY(LogicalType::FLOAT, TypeName::VARIABLE("N"));
	const auto t_3 = TypeName::ARRAY(t, 3);
	const auto map_varchar_t = TypeName::MAP(LogicalType::VARCHAR, t);

	// every(*args T): a parameter is counted for every argument it receives
	FunctionSignature variadic;
	variadic.AddVariable("T").AddArgs("args", t).SetReturnType(LogicalType::INTEGER);
	// defaulted(x INTEGER, y DECIMAL(18, 3) := 1): a parameter is not counted for its default
	FunctionSignature defaulted;
	defaulted.AddParameter("x", LogicalType::INTEGER);
	defaulted.AddParameter("y", decimal_18_3, Value::DECIMAL(int64_t(1000), 18, 3));
	defaulted.SetReturnType(LogicalType::INTEGER);

	con.context->RunFunctionInTransaction([&]() {
		const LogicalType integer = LogicalType::INTEGER;
		const LogicalType null = LogicalType::SQLNULL;
		const LogicalType unknown = LogicalType::UNKNOWN;
		const LogicalType string_literal(LogicalTypeId::STRING_LITERAL);
		const auto integers = LogicalType::LIST(LogicalType::INTEGER);
		const auto floats_3 = LogicalType::ARRAY(LogicalType::FLOAT, 3);
		const auto a_struct = LogicalType::STRUCT({{"a", LogicalType::INTEGER}});
		const auto a_map = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER);
		const auto crs = LogicalType::GEOMETRY("OGC:CRS84");

		const vector<RankCase> cases {
		    {"a declared type that is the type of the argument",
		     GetSelectionSignature({LogicalType::INTEGER}),
		     {integer},
		     MakeRank(0, 0, 0, 0, 0)},
		    {"rule 2: T, an integer", GetSelectionSignature({t}), {integer}, MakeRank(1, 0, 1, 0, 0)},
		    {"rule 2 and 3: T, a NULL is not counted and costs 110 as itself",
		     GetSelectionSignature({t}),
		     {null},
		     MakeRank(0, 110, 1, 0, 0)},
		    {"rule 3: T and T, a NULL costs the cast to the type the integer says",
		     GetSelectionSignature({t, t}),
		     {integer, null},
		     MakeRank(1, 102, 2, 0, 0)},
		    {"rule 3: T[], a NULL costs the cast to a list of NULL",
		     GetSelectionSignature({t_list}),
		     {null},
		     MakeRank(0, 160, 1, 2, 0)},
		    {"rule 3: FLOAT[N], a NULL is passed as it is and costs the cast to the type the parameter names",
		     GetSelectionSignature({float_n}),
		     {null},
		     MakeRank(0, 161, 0, 0, 1)},
		    {"rule 3: T[] and T, a string literal costs 20",
		     GetSelectionSignature({t_list, t}),
		     {string_literal, integer},
		     MakeRank(2, 20, 2, 2, 0)},
		    {"rule 3: T[] and T, a VARIANT is deferred and costs the cast to the list",
		     GetSelectionSignature({t_list, t}),
		     {LogicalType::VARIANT(), integer},
		     MakeRank(2, 160, 2, 2, 0)},
		    {"rule 4: T IS STRUCT, a struct", GetSelectionSignature({t_struct}), {a_struct}, MakeRank(0, 0, 1, 1, 0)},
		    {"rule 5: T[], a list", GetSelectionSignature({t_list}), {integers}, MakeRank(1, 0, 1, 2, 0)},
		    {"rule 5: MAP(VARCHAR, T), a map",
		     GetSelectionSignature({map_varchar_t}),
		     {a_map},
		     MakeRank(1, 0, 1, 4, 0)},
		    {"rule 5: T and T[], an integer and a list",
		     GetSelectionSignature({t, t_list}),
		     {integer, integers},
		     MakeRank(2, 0, 2, 2, 0)},
		    {"rule 5 and 6: T[3], an array", GetSelectionSignature({t_3}), {floats_3}, MakeRank(1, 0, 1, 2, 2)},
		    {"rule 6: DECIMAL(W, S), a decimal",
		     GetSelectionSignature({decimal_ws}),
		     {LogicalType::DECIMAL(18, 3)},
		     MakeRank(0, 0, 0, 0, 2)},
		    {"rule 6: DECIMAL(W, 0), a decimal",
		     GetSelectionSignature({decimal_w0}),
		     {LogicalType::DECIMAL(18, 0)},
		     MakeRank(0, 0, 0, 0, 3)},
		    {"rule 6: DECIMAL(18, 3), a decimal",
		     GetSelectionSignature({decimal_18_3}),
		     {LogicalType::DECIMAL(18, 3)},
		     MakeRank(0, 0, 0, 0, 4)},
		    {"rule 6: FLOAT[N], an array", GetSelectionSignature({float_n}), {floats_3}, MakeRank(0, 0, 0, 0, 1)},
		    {"rule 6: GEOMETRY(C), a geometry of a CRS",
		     GetSelectionSignature({geometry_c}),
		     {crs},
		     MakeRank(0, 0, 0, 0, 1)},
		    {"rules 4 to 6: *args T, once for every argument",
		     variadic,
		     {integer, integer, integer},
		     MakeRank(3, 0, 3, 0, 0)},
		    {"rules 4 to 6: not for a default", defaulted, {integer}, MakeRank(0, 0, 0, 0, 0)},
		    {"an unresolved parameter ranks nothing",
		     GetSelectionSignature({t_list, t}),
		     {unknown, integer},
		     FunctionRank()},
		};

		for (auto &test : cases) {
			ScalarFunctionSet set("f");
			set.AddFunction(ScalarFunction("f", test.signature, NoopExec));
			INFO(test.description);
			auto selection = FunctionBinder::SelectFunction(*con.context, set, test.arguments);
			CHECK(selection.IsSuccess());
			if (!selection.IsSuccess()) {
				continue;
			}
			auto &rank = selection.GetBinding().rank;
			CHECK(rank.legacy_casts == test.rank.legacy_casts);
			CHECK(rank.unconstrained_arguments == test.rank.unconstrained_arguments);
			CHECK(rank.cast_cost == test.rank.cast_cost);
			CHECK(rank.variable_parameters == test.rank.variable_parameters);
			CHECK(rank.declared_structure == test.rank.declared_structure);
			CHECK(rank.declared_values == test.rank.declared_values);
		}
	});
}

TEST_CASE("The rules of the rank decide between overloads in their order", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto t = TypeName::VARIABLE("T");
	const auto u = TypeName::VARIABLE("U");
	const auto t_decimal = TypeName::VARIABLE("TD");
	const auto t_struct = TypeName::VARIABLE("TS");
	const TypeName any(LogicalType::ANY);
	const auto t_list = TypeName::LIST(t);
	const auto any_list = TypeName::LIST(any);
	const auto float_n = TypeName::ARRAY(LogicalType::FLOAT, TypeName::VARIABLE("N"));
	const auto float_3 = TypeName::ARRAY(LogicalType::FLOAT, 3);
	const auto t_n = TypeName::ARRAY(t, TypeName::VARIABLE("N"));
	const auto t_3 = TypeName::ARRAY(t, 3);
	const auto struct_a = TypeName::STRUCT({{"a", LogicalType::INTEGER}});
	const auto map_varchar_t = TypeName::MAP(LogicalType::VARCHAR, t);
	const auto map_u_t = TypeName::MAP(u, t);
	const TypeName geometry_c(Identifier("GEOMETRY"), {TypeName::VARIABLE("C")});
	const TypeName geography_c(Identifier("GEOGRAPHY"), {TypeName::VARIABLE("C")});

	con.context->RunFunctionInTransaction([&]() {
		const LogicalType integer = LogicalType::INTEGER;
		const LogicalType null = LogicalType::SQLNULL;
		const auto decimal = LogicalType::DECIMAL(18, 3);
		const auto integers = LogicalType::LIST(LogicalType::INTEGER);
		const auto integers_3 = LogicalType::ARRAY(LogicalType::INTEGER, 3);
		const auto floats_3 = LogicalType::ARRAY(LogicalType::FLOAT, 3);
		const auto a_struct = LogicalType::STRUCT({{"a", LogicalType::INTEGER}});
		const auto a_map = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER);

		const vector<SelectionCase> cases {
		    {"rule 2: INTEGER over T, an integer", {{LogicalType::INTEGER}, {t}}, {integer}, 0},
		    {"rule 2 before rule 3: BIGINT with a cast over T without one, an integer",
		     {{LogicalType::BIGINT}, {t}},
		     {integer},
		     0},
		    {"rule 3: BIGINT over DOUBLE, an integer", {{LogicalType::BIGINT}, {LogicalType::DOUBLE}}, {integer}, 0},
		    {"rule 3: INTEGER over T, NULL", {{LogicalType::INTEGER}, {t}}, {null}, 0},
		    {"rule 3: GEOMETRY(C) over GEOGRAPHY(C), NULL is passed as it is",
		     {{geography_c}, {geometry_c}},
		     {null},
		     1},
		    {"rule 3 before rule 4: T IS DECIMAL without a cast over DOUBLE with one, a decimal",
		     {{t_decimal}, {LogicalType::DOUBLE}},
		     {decimal},
		     0},
		    {"rule 3 before rule 5: ANY over ANY[], an array is cast to a list", {{any_list}, {any}}, {integers_3}, 1},
		    {"rule 4: STRUCT(a INTEGER) over T IS STRUCT, the struct", {{struct_a}, {t_struct}}, {a_struct}, 0},
		    {"rule 5: T[] over T, a list", {{t_list}, {t}}, {integers}, 0},
		    {"rule 5: ANY[] over ANY, a list", {{any_list}, {any}}, {integers}, 0},
		    {"rule 5: MAP(VARCHAR, T) over MAP(U, T), a map", {{map_varchar_t}, {map_u_t}}, {a_map}, 0},
		    {"rule 3: (T IS STRUCT, U) over (T[], T), NULL and an integer: a NULL costs less as itself than a list",
		     {{t_list, t}, {t_struct, u}},
		     {null, integer},
		     1},
		    {"rule 5 before rule 6: (T[], FLOAT[N]) over (T, FLOAT[3])",
		     {{t_list, float_n}, {t, float_3}},
		     {integers, floats_3},
		     0},
		    {"rule 6: T[3] over T[N], an array", {{t_3}, {t_n}}, {floats_3}, 0},
		    {"rule 6: FLOAT[3] over FLOAT[N], an array", {{float_3}, {float_n}}, {floats_3}, 0},
		};

		for (auto &test : cases) {
			const auto count = test.overloads.size();
			for (idx_t reversed = 0; reversed < 2; reversed++) {
				ScalarFunctionSet set("f");
				for (idx_t i = 0; i < count; i++) {
					auto &parameters = test.overloads[reversed ? count - 1 - i : i];
					set.AddFunction(ScalarFunction("f", GetSelectionSignature(parameters), NoopExec));
				}
				INFO(test.description << (reversed ? ", overloads in the opposite order" : ""));
				auto selection = FunctionBinder::SelectFunction(*con.context, set, test.arguments);
				CHECK(selection.IsSuccess());
				if (selection.IsSuccess()) {
					const auto index = selection.GetBinding().GetOverloadIndex();
					CHECK((reversed ? count - 1 - index : index) == test.selected.GetIndex());
				}
			}
		}
	});
}

TEST_CASE("A cast that only old_implicit_casting allows ranks behind every other match", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET old_implicit_casting = true"));
	const auto t = TypeName::VARIABLE("T");

	con.context->RunFunctionInTransaction([&]() {
		const LogicalType integer = LogicalType::INTEGER;
		// rule 1: the cast of an integer to VARCHAR is counted
		ScalarFunctionSet text("f");
		text.AddFunction(ScalarFunction("f", GetSelectionSignature({LogicalType::VARCHAR}), NoopExec));
		auto selection = FunctionBinder::SelectFunction(*con.context, text, {integer});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().rank.legacy_casts == 1);

		// rule 1 before rule 2: T over VARCHAR, in either order
		for (idx_t reversed = 0; reversed < 2; reversed++) {
			ScalarFunctionSet set("f");
			const TypeName first = reversed ? t : TypeName(LogicalType::VARCHAR);
			const TypeName second = reversed ? TypeName(LogicalType::VARCHAR) : t;
			set.AddFunction(ScalarFunction("f", GetSelectionSignature({first}), NoopExec));
			set.AddFunction(ScalarFunction("f", GetSelectionSignature({second}), NoopExec));
			selection = FunctionBinder::SelectFunction(*con.context, set, {integer});
			REQUIRE(selection.IsSuccess());
			REQUIRE(selection.GetBinding().GetOverloadIndex() == (reversed ? 0 : 1));
			REQUIRE(selection.GetBinding().rank.legacy_casts == 0);
		}
	});
}

TEST_CASE("An argument that is not an instance of the type of its parameter is deferred", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto t = TypeName::VARIABLE("T");
	const auto variant = LogicalType::VARIANT();
	const auto integers = LogicalType::LIST(LogicalType::INTEGER);
	const LogicalType string_literal(LogicalTypeId::STRING_LITERAL);
	const auto t_decimal = TypeName::VARIABLE("TD");
	const auto decimal = LogicalType::DECIMAL(5, 2);
	// has(list T[], element T), single(list T[]), wrapped(list ANY[], element INTEGER)
	auto has = make_shared_ptr<ScalarFunction>("has", GetSelectionSignature({TypeName::LIST(t), t}), NoopExec);
	ScalarFunctionSet has_set("has");
	has_set.AddFunction(*has);
	ScalarFunctionSet single_set("single");
	single_set.AddFunction(ScalarFunction("single", GetSelectionSignature({TypeName::LIST(t)}), NoopExec));
	ScalarFunctionSet wrapped_set("wrapped");
	wrapped_set.AddFunction(ScalarFunction(
	    "wrapped", GetSelectionSignature({TypeName::LIST(LogicalType::ANY), LogicalType::INTEGER}), NoopExec));
	// plus(a T IS DECIMAL, b T IS DECIMAL), plus(a DOUBLE, b DOUBLE)
	ScalarFunctionSet plus_set("plus");
	plus_set.AddFunction(ScalarFunction("plus", GetSelectionSignature({t_decimal, t_decimal}), NoopExec));
	ScalarFunctionSet plus_double_set = plus_set;
	plus_double_set.AddFunction(
	    ScalarFunction("plus", GetSelectionSignature({LogicalType::DOUBLE, LogicalType::DOUBLE}), NoopExec));

	con.context->RunFunctionInTransaction([&]() {
		// it is cast to the type the other argument determines
		auto selection = FunctionBinder::SelectFunction(*con.context, has_set, {variant, LogicalType::INTEGER});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == integers);
		REQUIRE(selection.GetBinding().variables.types.at("T") == LogicalType::INTEGER);
		selection = FunctionBinder::SelectFunction(*con.context, has_set, {string_literal, LogicalType::INTEGER});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == integers);

		// which it has to cast implicitly to
		selection = FunctionBinder::SelectFunction(*con.context, has_set, {LogicalType::DATE, LogicalType::INTEGER});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		REQUIRE(GetMismatches(selection)[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);
		// unless the caller chose the function, and it is cast explicitly
		auto binding = FunctionBinder::MatchFunction(*con.context, has, {LogicalType::DATE, LogicalType::INTEGER});
		REQUIRE(binding.layout.arguments[0].type == integers);

		// it does not determine a variable
		selection = FunctionBinder::SelectFunction(*con.context, single_set, {variant});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		selection = FunctionBinder::SelectFunction(*con.context, single_set, {string_literal});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		selection = FunctionBinder::SelectFunction(*con.context, has_set, {variant, variant});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::LIST(variant));

		// nothing is deferred to ANY, which is the type of the argument itself
		selection = FunctionBinder::SelectFunction(*con.context, wrapped_set, {variant, LogicalType::INTEGER});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);

		// nor to a type variable by itself: the VARIANT is not passed as the decimal the other argument is
		selection = FunctionBinder::SelectFunction(*con.context, plus_set, {decimal, variant});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		REQUIRE(GetMismatches(selection)[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 1);
		selection = FunctionBinder::SelectFunction(*con.context, plus_double_set, {decimal, variant});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 1);
	});
}

TEST_CASE("A type of another kind than the type of its parameter says nothing about the variables",
          "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto t = TypeName::VARIABLE("T");
	const auto u = TypeName::VARIABLE("U");
	const auto int_varchar_map = LogicalType::MAP(LogicalType::INTEGER, LogicalType::VARCHAR);
	const auto int_int_map = LogicalType::MAP(LogicalType::INTEGER, LogicalType::INTEGER);
	const auto integers = LogicalType::LIST(LogicalType::INTEGER);
	const auto doubles = LogicalType::LIST(LogicalType::DOUBLE);
	const auto floats_3 = LogicalType::ARRAY(LogicalType::FLOAT, 3);
	const auto entry = LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}});
	const TypeName t_array(Identifier("ARRAY"), {t, TypeName(Identifier("N"))});
	// has(list T[], element T), single(list T[])
	auto has = make_shared_ptr<ScalarFunction>("has", GetSelectionSignature({TypeName::LIST(t), t}), NoopExec);
	ScalarFunctionSet has_set("has");
	has_set.AddFunction(*has);
	ScalarFunctionSet single_set("single");
	single_set.AddFunction(ScalarFunction("single", GetSelectionSignature({TypeName::LIST(t)}), NoopExec));
	// dot(a T[N], b T[N]), keys(m MAP(T, U)), entries(l TUPLE(T, U)[])
	ScalarFunctionSet dot_set("dot");
	dot_set.AddFunction(ScalarFunction("dot", GetSelectionSignature({t_array, t_array}), NoopExec));
	ScalarFunctionSet keys_set("keys");
	keys_set.AddFunction(ScalarFunction("keys", GetSelectionSignature({TypeName::MAP(t, u)}), NoopExec));
	ScalarFunctionSet entries_set("entries");
	entries_set.AddFunction(ScalarFunction(
	    "entries", GetSelectionSignature({TypeName::LIST(TypeName(Identifier("TUPLE"), {t, u}))}), NoopExec));

	con.context->RunFunctionInTransaction([&]() {
		// a map is not a list of its keys: it is the argument that cannot be passed, the variable has no conflict
		auto selection = FunctionBinder::SelectFunction(*con.context, has_set, {int_int_map, int_int_map});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		REQUIRE(GetMismatches(selection)[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);
		selection = FunctionBinder::SelectFunction(*con.context, has_set, {int_varchar_map, LogicalType::INTEGER});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		REQUIRE(GetMismatches(selection)[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);
		// the variable is what the other argument says, not the type of the keys
		auto binding = FunctionBinder::MatchFunction(*con.context, has, {int_varchar_map, LogicalType::DATE});
		REQUIRE(binding.variables.types.at("T") == LogicalType::DATE);
		REQUIRE(binding.layout.arguments[0].type == LogicalType::LIST(LogicalType::DATE));
		// and not determined if there is none
		selection = FunctionBinder::SelectFunction(*con.context, single_set, {int_varchar_map});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		// nor is a struct a map of its first two fields, or a list a map
		selection = FunctionBinder::SelectFunction(*con.context, keys_set, {entry});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		selection = FunctionBinder::SelectFunction(*con.context, keys_set, {integers});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);

		// a list and an array are of one kind: the elements of one say what the elements of the other are
		selection = FunctionBinder::SelectFunction(*con.context, has_set, {floats_3, LogicalType::DOUBLE});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.types.at("T") == LogicalType::DOUBLE);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == doubles);
		selection = FunctionBinder::SelectFunction(*con.context, dot_set, {doubles, floats_3});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.types.at("T") == LogicalType::DOUBLE);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::ARRAY(LogicalType::DOUBLE, 3));
		// and so are a struct with and without names
		selection = FunctionBinder::SelectFunction(*con.context, entries_set, {LogicalType::LIST(entry)});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.types.at("T") == LogicalType::INTEGER);
		REQUIRE(selection.GetBinding().variables.types.at("U") == LogicalType::VARCHAR);
	});
}

TEST_CASE("A value variable in the place of a type matches no call", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	// misplaced<N: INTEGER>(list N[], n N) -> INTEGER
	ScalarFunctionSet set("misplaced");
	set.AddFunction(ScalarFunction("misplaced",
	                               FunctionSignature()
	                                   .AddVariable("N", TypeVariable::Kind::INTEGER)
	                                   .AddParameter("list", TypeName::LIST(TypeName::VARIABLE("N")))
	                                   .AddParameter("n", TypeName::VARIABLE("N"))
	                                   .SetReturnType(LogicalType::INTEGER),
	                               NoopExec));
	const auto integers = LogicalType::LIST(LogicalType::INTEGER);
	con.context->RunFunctionInTransaction([&]() {
		// a list is not made of a value
		REQUIRE_THROWS_WITH(
		    FunctionBinder::SelectFunction(*con.context, set, {integers, FunctionCallArgument::Constant(Value(3))}),
		    Catch::Contains("does not accept type parameters"));
		// and a list is not passed as a list of a value that is not known
		auto selection = FunctionBinder::SelectFunction(*con.context, set, {integers, LogicalType::SQLNULL});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		REQUIRE(GetMismatches(selection)[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);
	});
}

TEST_CASE("An unresolved parameter takes the type the other arguments determine", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto t = TypeName::VARIABLE("T");
	const auto unknown = LogicalType(LogicalType::UNKNOWN);
	const auto integers = LogicalType::LIST(LogicalType::INTEGER);
	const auto floats_3 = LogicalType::ARRAY(LogicalType::FLOAT, 3);
	const TypeName float_array(Identifier("ARRAY"), {TypeName(LogicalType::FLOAT), TypeName(Identifier("N"))});
	// values<T>(*args T) -> T[]
	ScalarFunctionSet values("values");
	values.AddFunction(ScalarFunction(
	    "values", FunctionSignature().AddVariable("T").AddArgs("args", t).SetReturnType(TypeName::LIST(t)), NoopExec));
	// has(list T[], element T), distance(a FLOAT[N], b FLOAT[N]), plus(a INTEGER, b INTEGER)
	ScalarFunctionSet has("has");
	has.AddFunction(ScalarFunction("has", GetSelectionSignature({TypeName::LIST(t), t}), NoopExec));
	ScalarFunctionSet distance("distance");
	distance.AddFunction(ScalarFunction("distance", GetSelectionSignature({float_array, float_array}), NoopExec));
	ScalarFunctionSet plus("plus");
	plus.AddFunction(
	    ScalarFunction("plus", GetSelectionSignature({LogicalType::INTEGER, LogicalType::INTEGER}), NoopExec));

	con.context->RunFunctionInTransaction([&]() {
		// the type the parameter declares
		auto selection = FunctionBinder::SelectFunction(*con.context, plus, {LogicalType::INTEGER, unknown});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[1].type == LogicalType::INTEGER);

		// the type the other arguments say a variable is
		selection = FunctionBinder::SelectFunction(*con.context, values, {LogicalType::INTEGER, unknown});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[1].type == LogicalType::INTEGER);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == integers);
		selection = FunctionBinder::SelectFunction(*con.context, has, {integers, unknown});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[1].type == LogicalType::INTEGER);
		selection = FunctionBinder::SelectFunction(*con.context, distance, {unknown, floats_3});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == floats_3);

		// a type variable that only NULLs are passed as is the type of a NULL
		selection = FunctionBinder::SelectFunction(*con.context, values, {LogicalType::SQLNULL, LogicalType::SQLNULL});
		REQUIRE(selection.IsSuccess());
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::LIST(LogicalType::SQLNULL));
	});
}

TEST_CASE("A NULL and an unresolved parameter take the type the other arguments are promoted to",
          "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto t = TypeName::VARIABLE("T");
	const LogicalType null = LogicalType::SQLNULL;
	const auto unknown = LogicalType(LogicalType::UNKNOWN);
	const auto one = LogicalType::INTEGER_LITERAL(Value::INTEGER(1));
	const LogicalType string_literal(LogicalTypeId::STRING_LITERAL);
	const LogicalType tinyint = LogicalType::TINYINT;
	const LogicalType date = LogicalType::DATE;
	// values<T>(*args T) -> T[]
	ScalarFunctionSet values("values");
	values.AddFunction(ScalarFunction(
	    "values", FunctionSignature().AddVariable("T").AddArgs("args", t).SetReturnType(TypeName::LIST(t)), NoopExec));
	// both(a TD, b TD): a variable that only stands for a DECIMAL
	ScalarFunctionSet both("both");
	const auto t_decimal = TypeName::VARIABLE("TD");
	both.AddFunction(ScalarFunction("both", GetSelectionSignature({t_decimal, t_decimal}), NoopExec));

	con.context->RunFunctionInTransaction([&]() {
		for (auto &untyped : {null, unknown}) {
			// a literal stays a literal next to it, wherever it is passed: the literal still fits the TINYINT
			vector<vector<LogicalType>> calls {
			    {untyped, one, tinyint}, {one, untyped, tinyint}, {one, tinyint, untyped}};
			for (auto &call : calls) {
				auto selection = FunctionBinder::SelectFunction(*con.context, values, call);
				REQUIRE(selection.IsSuccess());
				REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::LIST(tinyint));
				for (auto &argument : selection.GetBinding().layout.arguments) {
					REQUIRE(argument.type == tinyint);
				}
			}
			// and a string literal is still passed as the DATE
			calls = {{untyped, string_literal, date}, {string_literal, untyped, date}, {string_literal, date, untyped}};
			for (auto &call : calls) {
				auto selection = FunctionBinder::SelectFunction(*con.context, values, call);
				REQUIRE(selection.IsSuccess());
				REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::LIST(date));
			}
			// a literal that nothing else is promoted with becomes its type at the end
			calls = {{untyped, one}, {one, untyped}};
			for (auto &call : calls) {
				auto selection = FunctionBinder::SelectFunction(*con.context, values, call);
				REQUIRE(selection.IsSuccess());
				REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::LIST(LogicalType::INTEGER));
			}
			calls = {{untyped, string_literal}, {string_literal, untyped}};
			for (auto &call : calls) {
				auto selection = FunctionBinder::SelectFunction(*con.context, values, call);
				REQUIRE(selection.IsSuccess());
				REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::LIST(LogicalType::VARCHAR));
			}
		}

		// a NULL is of any kind: the other argument says the DECIMAL
		auto selection = FunctionBinder::SelectFunction(*con.context, both, {null, LogicalType::DECIMAL(5, 2)});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::DECIMAL(5, 2));
		selection = FunctionBinder::SelectFunction(*con.context, both, {null, null});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == null);
	});
}

TEST_CASE("A constrained variable only stands for a type of its kind", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const LogicalType decimal = LogicalType::DECIMAL(5, 2);
	const LogicalType wider_decimal = LogicalType::DECIMAL(18, 3);
	const LogicalType a_struct = LogicalType::STRUCT({{"a", LogicalType::INTEGER}});
	const LogicalType a_tuple = LogicalType::TUPLE(vector<LogicalType> {LogicalType::INTEGER});
	// both(a TD, b TD): a variable that only stands for a DECIMAL, record(a TS): one that only stands for a struct
	ScalarFunctionSet both("both");
	const auto t_decimal = TypeName::VARIABLE("TD");
	both.AddFunction(ScalarFunction("both", GetSelectionSignature({t_decimal, t_decimal}), NoopExec));
	ScalarFunctionSet record("record");
	record.AddFunction(ScalarFunction("record", GetSelectionSignature({TypeName::VARIABLE("TS")}), NoopExec));
	// width(a DECIMAL(W, S)): a type of the kind that is declared with its arguments
	ScalarFunctionSet width("width");
	const auto decimal_ws = TypeName::DECIMAL(TypeName::VARIABLE("W"), TypeName::VARIABLE("S"));
	width.AddFunction(ScalarFunction("width", GetSelectionSignature({decimal_ws}), NoopExec));

	con.context->RunFunctionInTransaction([&]() {
		// types of the kind are promoted to the type the variable is
		auto selection = FunctionBinder::SelectFunction(*con.context, both, {decimal, wider_decimal});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.types.at("TD") == LogicalType::DECIMAL(18, 3));
		// a struct with and without names are of one kind
		for (auto &type : {a_struct, a_tuple}) {
			selection = FunctionBinder::SelectFunction(*con.context, record, {type});
			REQUIRE(selection.IsSuccess());
			REQUIRE(selection.GetBinding().variables.types.at("TS") == type);
		}

		// no type is converted to be of the kind: an integer is not a decimal, whatever the other argument is
		vector<vector<LogicalType>> calls {{LogicalType::INTEGER, LogicalType::INTEGER},
		                                   {decimal, LogicalType::INTEGER},
		                                   {LogicalType::INTEGER, decimal},
		                                   {LogicalType::HUGEINT, LogicalType::UHUGEINT},
		                                   {LogicalType::DOUBLE, decimal}};
		for (auto &call : calls) {
			selection = FunctionBinder::SelectFunction(*con.context, both, call);
			REQUIRE(!selection.IsSuccess());
			REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		}
		selection = FunctionBinder::SelectFunction(*con.context, record, {LogicalType::INTEGER});
		REQUIRE(!selection.IsSuccess());

		// it is passed as the decimal that holds it only to a DECIMAL that is declared with its arguments
		selection = FunctionBinder::SelectFunction(*con.context, width, {LogicalType::INTEGER});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::DECIMAL(10, 0));
		REQUIRE(selection.GetBinding().variables.values.at("W") == Value::BIGINT(10));
	});
}

TEST_CASE("A variable that only an unresolved parameter says leaves the call unresolved", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const string unresolved_message = "Parameter types could not be resolved";
	const auto t = TypeName::VARIABLE("T");
	const auto unknown = LogicalType(LogicalType::UNKNOWN);
	const LogicalType string_literal(LogicalTypeId::STRING_LITERAL);
	const TypeName float_array(Identifier("ARRAY"), {TypeName(LogicalType::FLOAT), TypeName(Identifier("N"))});
	// values<T>(*args T) -> T[], zeros<N: INTEGER>(n N) -> DOUBLE[N]
	ScalarFunctionSet values("values");
	values.AddFunction(ScalarFunction(
	    "values", FunctionSignature().AddVariable("T").AddArgs("args", t).SetReturnType(TypeName::LIST(t)), NoopExec));
	ScalarFunctionSet zeros("zeros");
	zeros.AddFunction(ScalarFunction(
	    "zeros",
	    FunctionSignature()
	        .AddVariable("N", TypeVariable::Kind::INTEGER)
	        .AddParameter("n", TypeName(Identifier("N")))
	        .SetReturnType(TypeName(Identifier("ARRAY"), {TypeName(LogicalType::DOUBLE), TypeName(Identifier("N"))})),
	    NoopExec));
	// has(list T[], element T), distance(a FLOAT[N], b FLOAT[N])
	ScalarFunctionSet has("has");
	has.AddFunction(ScalarFunction("has", GetSelectionSignature({TypeName::LIST(t), t}), NoopExec));
	ScalarFunctionSet distance("distance");
	distance.AddFunction(ScalarFunction("distance", GetSelectionSignature({float_array, float_array}), NoopExec));
	// pick(x INTEGER) -> INTEGER, pick(x T) -> T
	ScalarFunctionSet pick("pick");
	pick.AddFunction(ScalarFunction(
	    "pick", FunctionSignature().AddParameter("x", LogicalType::INTEGER).SetReturnType(LogicalType::INTEGER),
	    NoopExec));
	pick.AddFunction(
	    ScalarFunction("pick", FunctionSignature().AddVariable("T").AddParameter("x", t).SetReturnType(t), NoopExec));
	// wrapped(list ANY[], element T)
	ScalarFunctionSet wrapped("wrapped");
	wrapped.AddFunction(
	    ScalarFunction("wrapped", GetSelectionSignature({TypeName::LIST(LogicalType::ANY), t}), NoopExec));
	// same(a T, b T), one(a T), compare(a T, b U)
	ScalarFunctionSet same("same");
	same.AddFunction(ScalarFunction("same", GetSelectionSignature({t, t}), NoopExec));
	ScalarFunctionSet one("one");
	one.AddFunction(ScalarFunction("one", GetSelectionSignature({t}), NoopExec));
	ScalarFunctionSet compare("compare");
	compare.AddFunction(ScalarFunction("compare", GetSelectionSignature({t, TypeName::VARIABLE("U")}), NoopExec));
	// any(a ANY), any_list(a ANY[]), sized<N: INTEGER>(n N)
	ScalarFunctionSet any("any");
	any.AddFunction(ScalarFunction("any", GetSelectionSignature({LogicalType::ANY}), NoopExec));
	ScalarFunctionSet any_list("any_list");
	any_list.AddFunction(
	    ScalarFunction("any_list", GetSelectionSignature({TypeName::LIST(LogicalType::ANY)}), NoopExec));
	ScalarFunctionSet sized("sized");
	sized.AddFunction(ScalarFunction("sized",
	                                 FunctionSignature()
	                                     .AddVariable("N", TypeVariable::Kind::INTEGER)
	                                     .AddParameter("n", TypeName(Identifier("N")))
	                                     .SetReturnType(LogicalType::INTEGER),
	                                 NoopExec));
	// late(a INTEGER, b T) -> T
	ScalarFunctionSet late("late");
	late.AddFunction(ScalarFunction("late",
	                                FunctionSignature()
	                                    .AddVariable("T")
	                                    .AddParameter("a", LogicalType::INTEGER)
	                                    .AddParameter("b", t)
	                                    .SetReturnType(t),
	                                NoopExec));
	// two(a INTEGER, b T), two(a VARCHAR, b VARCHAR)
	ScalarFunctionSet two("two");
	two.AddFunction(ScalarFunction("two", GetSelectionSignature({LogicalType::INTEGER, t}), NoopExec));
	two.AddFunction(
	    ScalarFunction("two", GetSelectionSignature({LogicalType::VARCHAR, LogicalType::VARCHAR}), NoopExec));

	con.context->RunFunctionInTransaction([&]() {
		// the parameter is passed as a variable that the return type uses as well
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, values, {unknown}),
		                    Catch::Contains(unresolved_message));
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, zeros, {unknown}),
		                    Catch::Contains(unresolved_message));
		// or the parameter of another argument: one that says nothing about it, or another unresolved parameter
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, has, {string_literal, unknown}),
		                    Catch::Contains(unresolved_message));
		REQUIRE_THROWS_WITH(
		    FunctionBinder::MatchFunction(*con.context, has.GetSharedOverload(0), {string_literal, unknown}),
		    Catch::Contains(unresolved_message));
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, distance, {LogicalType::SQLNULL, unknown}),
		                    Catch::Contains(unresolved_message));
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, distance, {unknown, unknown}),
		                    Catch::Contains(unresolved_message));
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, same, {unknown, unknown}),
		                    Catch::Contains(unresolved_message));
		// a NULL does not say the type in the place of the parameter, whatever their order
		for (auto &set : {values, has}) {
			REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, set, {LogicalType::SQLNULL, unknown}),
			                    Catch::Contains(unresolved_message));
			REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, set, {unknown, LogicalType::SQLNULL}),
			                    Catch::Contains(unresolved_message));
		}

		// another overload that matches is not selected in its place
		vector<pair<Identifier, LogicalType>> named {{"x", unknown}};
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, pick, FunctionCallArguments({}, named)),
		                    Catch::Contains(unresolved_message));
		// and the other arguments are not checked before the parameter has a type
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, late, {LogicalType::DATE, unknown}),
		                    Catch::Contains(unresolved_message));

		// a variable that only the parameter is passed as is no different, at the matcher level: a function that
		// takes the type of the parameter from its other arguments itself, like a comparison, does so before it is
		// matched
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, one, {unknown}),
		                    Catch::Contains(unresolved_message));
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, compare, {unknown, LogicalType::INTEGER}),
		                    Catch::Contains(unresolved_message));
		REQUIRE_THROWS_WITH(
		    FunctionBinder::MatchFunction(*con.context, compare.GetSharedOverload(0), {unknown, LogicalType::INTEGER}),
		    Catch::Contains(unresolved_message));
		// nor is ANY, which is a type variable of its own wherever it is written, or a value variable
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, any, {unknown}),
		                    Catch::Contains(unresolved_message));
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, any_list, {unknown}),
		                    Catch::Contains(unresolved_message));
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, sized, {unknown}),
		                    Catch::Contains(unresolved_message));

		// so an overload with such a variable leaves the call unresolved whatever the other overloads are, and
		// whether or not its other arguments can be passed
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, two, {LogicalType::VARCHAR, unknown}),
		                    Catch::Contains(unresolved_message));
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, two, {LogicalType::DATE, unknown}),
		                    Catch::Contains(unresolved_message));
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, wrapped, {string_literal, unknown}),
		                    Catch::Contains(unresolved_message));

		// a NULL passed as such a variable is the type of a NULL, and no argument is passed as an unknown type
		auto selection = FunctionBinder::SelectFunction(*con.context, one, {LogicalType::SQLNULL});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::SQLNULL);
		selection = FunctionBinder::SelectFunction(*con.context, any, {LogicalType::SQLNULL});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::SQLNULL);
		selection = FunctionBinder::SelectFunction(*con.context, any_list, {LogicalType::SQLNULL});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::SQLNULL);
	});
}

TEST_CASE("A NULL that leaves a value variable unknown is passed as it is", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const LogicalType string_literal(LogicalTypeId::STRING_LITERAL);
	const TypeName float_array(Identifier("ARRAY"), {TypeName(LogicalType::FLOAT), TypeName(Identifier("N"))});
	// distance(a FLOAT[N], b FLOAT[N])
	ScalarFunctionSet distance("distance");
	distance.AddFunction(ScalarFunction("distance", GetSelectionSignature({float_array, float_array}), NoopExec));
	// sized<N: INTEGER>(a FLOAT[N]) -> FLOAT[N], sized(a VARCHAR) -> VARCHAR
	ScalarFunctionSet sized("sized");
	sized.AddFunction(ScalarFunction("sized",
	                                 FunctionSignature()
	                                     .AddVariable("N", TypeVariable::Kind::INTEGER)
	                                     .AddParameter("a", float_array)
	                                     .SetReturnType(float_array),
	                                 NoopExec));
	sized.AddFunction(ScalarFunction(
	    "sized", FunctionSignature().AddParameter("a", LogicalType::VARCHAR).SetReturnType(LogicalType::VARCHAR),
	    NoopExec));

	con.context->RunFunctionInTransaction([&]() {
		auto selection =
		    FunctionBinder::SelectFunction(*con.context, distance, {LogicalType::SQLNULL, LogicalType::SQLNULL});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.values.count("N") == 0);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::SQLNULL);
		REQUIRE(selection.GetBinding().layout.arguments[1].type == LogicalType::SQLNULL);

		// an argument that is deferred to the type cannot be passed
		selection = FunctionBinder::SelectFunction(*con.context, distance, {string_literal, LogicalType::SQLNULL});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		REQUIRE(GetMismatches(selection)[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);

		// an overload whose return type uses the variable is no candidate: the others are considered
		selection = FunctionBinder::SelectFunction(*con.context, sized, {LogicalType::SQLNULL});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 1);
	});
}

TEST_CASE("A NULL keeps the literal it is promoted with a literal only by the policy", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto one = LogicalType::INTEGER_LITERAL(Value::INTEGER(1));
	const LogicalType string_literal(LogicalTypeId::STRING_LITERAL);
	const LogicalType null_type(LogicalType::SQLNULL);
	const LogicalType unknown(LogicalType::UNKNOWN);
	const auto keep_literals = PromotionPolicy::NormalizeLiterals(false);

	con.context->RunFunctionInTransaction([&]() {
		auto &context = *con.context;
		LogicalType result;
		// by default a NULL and an unresolved parameter turn a literal into its default type
		REQUIRE(LogicalType::TryGetMaxLogicalType(context, null_type, one, result));
		REQUIRE(result == LogicalType::INTEGER);
		REQUIRE(LogicalType::TryGetMaxLogicalType(context, string_literal, unknown, result));
		REQUIRE(result == LogicalType::VARCHAR);
		REQUIRE(LogicalType::TryGetMaxLogicalType(&context, one, null_type, result, nullptr, PromotionPolicy()));
		REQUIRE(result == LogicalType::INTEGER);
		REQUIRE_THROWS_WITH(LogicalType::TryGetMaxLogicalType(context, null_type, unknown, result),
		                    Catch::Contains("Parameter types could not be resolved"));

		// with the policy they take the other type as it is - with and without a context
		REQUIRE(LogicalType::TryGetMaxLogicalType(&context, null_type, one, result, nullptr, keep_literals));
		REQUIRE(result == one);
		REQUIRE(LogicalType::TryGetMaxLogicalType(&context, string_literal, unknown, result, nullptr, keep_literals));
		REQUIRE(result == string_literal);
		REQUIRE(LogicalType::TryGetMaxLogicalType(nullptr, one, unknown, result, nullptr, keep_literals));
		REQUIRE(result == one);
		REQUIRE(LogicalType::TryGetMaxLogicalType(&context, null_type, unknown, result, nullptr, keep_literals));
		REQUIRE(result == unknown);
		// also inside a nested type
		REQUIRE(LogicalType::TryGetMaxLogicalType(&context, LogicalType::LIST(null_type), LogicalType::LIST(one),
		                                          result, nullptr, keep_literals));
		REQUIRE(result == LogicalType::LIST(one));
		// the literal still adapts to the type that follows
		REQUIRE(LogicalType::TryGetMaxLogicalType(&context, result, LogicalType::LIST(LogicalType::TINYINT), result,
		                                          nullptr, keep_literals));
		REQUIRE(result == LogicalType::LIST(LogicalType::TINYINT));
	});
}

TEST_CASE("A variable of a signature is not named like a built-in type", "[api][function_binding]") {
	for (auto &name : {"INTEGER", "any", "LIST", "decimal"}) {
		REQUIRE_THROWS_WITH(FunctionSignature().AddVariable(name), Catch::Contains("is named like a built-in type"));
	}
	REQUIRE_NOTHROW(FunctionSignature().AddVariable("T").AddVariable("N", TypeVariable::Kind::INTEGER));
}

TEST_CASE("A named argument passed twice matches no overload", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto set = GetOverloadedSet();

	vector<pair<Identifier, LogicalType>> named {{"a", LogicalType::VARCHAR}, {"a", LogicalType::VARCHAR}};
	auto selection = FunctionBinder::SelectFunction(*con.context, set, FunctionCallArguments({}, named));
	REQUIRE(!selection);
	REQUIRE(selection.GetError().GetType() == FunctionBindError::Type::DUPLICATE_ARGUMENT);
	REQUIRE(selection.GetError().Cast<FunctionBindError::DuplicateArgument>().argument == 1);
	REQUIRE(StringUtil::Contains(GetErrorMessage(selection), "Duplicate named argument"));
}

TEST_CASE("A named argument passed twice does not match a function the caller chose", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	// defaulted(a INTEGER, b INTEGER := 0) -> INTEGER: as many parameters as the call has arguments
	FunctionSignature sig;
	sig.AddParameter("a", LogicalType::INTEGER);
	sig.AddParameter("b", LogicalType::INTEGER, Value::INTEGER(0));
	sig.SetReturnType(LogicalType::INTEGER);
	auto function = make_shared_ptr<ScalarFunction>("defaulted", std::move(sig), NoopExec);

	vector<pair<Identifier, LogicalType>> named {{"a", LogicalType::INTEGER}, {"a", LogicalType::INTEGER}};
	REQUIRE_THROWS_WITH(FunctionBinder::MatchFunction(*con.context, function, FunctionCallArguments({}, named)),
	                    Catch::Contains("Duplicate named argument") && Catch::Contains("in 'defaulted'"));
	// passed once, the name fills its parameter and the default the other
	named.pop_back();
	auto binding = FunctionBinder::MatchFunction(*con.context, function, FunctionCallArguments({}, named));
	const vector<idx_t> parameters {0, 1};
	REQUIRE(ParameterIndexes(binding) == parameters);
}

TEST_CASE("An argument that is cast explicitly to a type of ANY has to have a type in the place of ANY",
          "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	// wrap(list ANY[]) -> INTEGER
	auto function = make_shared_ptr<ScalarFunction>(
	    "wrap",
	    FunctionSignature().AddParameter("list", TypeName::LIST(LogicalType::ANY)).SetReturnType(LogicalType::INTEGER),
	    NoopExec);
	ScalarFunctionSet set("wrap");
	set.AddFunction(*function);
	const auto integer_literal = LogicalType::INTEGER_LITERAL(Value::INTEGER(1));
	const auto string_literal = LogicalType(LogicalTypeId::STRING_LITERAL);

	con.context->RunFunctionInTransaction([&]() {
		// a literal is not a list, so it is not taken when the overload is selected
		for (auto &literal : {integer_literal, string_literal}) {
			auto selection = FunctionBinder::SelectFunction(*con.context, set, {literal});
			auto &failed = GetMismatches(selection);
			REQUIRE(failed.size() == 1);
			REQUIRE(failed[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
			REQUIRE(failed[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);
		}

		// nor when the caller chose the function: there is nothing a list of ANY is a list of
		for (auto &literal : {integer_literal, string_literal, LogicalType(LogicalType::DATE)}) {
			REQUIRE_THROWS_WITH(FunctionBinder::MatchFunction(*con.context, function, {literal}),
			                    Catch::Contains("cannot be implicitly cast to ANY[]"));
		}
		// a list is passed as it is, and an array as the list of what it holds
		auto binding = FunctionBinder::MatchFunction(*con.context, function, {LogicalType::LIST(integer_literal)});
		REQUIRE(binding.layout.arguments[0].type == LogicalType::LIST(LogicalType::INTEGER));
		binding = FunctionBinder::MatchFunction(*con.context, function, {LogicalType::ARRAY(LogicalType::DATE, 3)});
		REQUIRE(binding.layout.arguments[0].type == LogicalType::LIST(LogicalType::DATE));
	});
}

//! h(value ANY, depth BIGINT := 0, *args ANY) -> "value" nested in "depth" lists
static LogicalType NestedReturnType(const ResolveReturnTypeInput &input) {
	// the arguments are in the order of the signature, the default of a parameter the call leaves out included
	REQUIRE(input.GetArgumentCount() >= 2);
	REQUIRE(input.GetArgumentType(1) == LogicalType::BIGINT);
	auto depth = input.TryGetConstant(1);
	if (!depth || depth->IsNull()) {
		return LogicalType::SQLNULL;
	}
	auto result = input.GetArgumentType(0);
	for (int64_t i = 0; i < depth->GetValue<int64_t>(); i++) {
		result = LogicalType::LIST(result);
	}
	return result;
}

TEST_CASE("A return type callback resolves the type from the arguments of the bound call", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto function = make_shared_ptr<ScalarFunction>("h",
	                                                FunctionSignature()
	                                                    .AddParameter("value", LogicalType::ANY)
	                                                    .AddParameter("depth", LogicalType::BIGINT, Value::BIGINT(0))
	                                                    .AddArgs("args", LogicalType::ANY)
	                                                    .SetReturnType(ReturnType::ANY(NestedReturnType)),
	                                                NoopExec);
	REQUIRE(function->GetReturnType().IsAny());

	// selecting the overload does not resolve the type - binding the call does
	ScalarFunctionSet set("h");
	set.AddFunction(*function);
	auto selection = FunctionBinder::SelectFunction(*con.context, set, {LogicalType::DATE});
	REQUIRE(selection.IsSuccess());
	REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::INVALID);

	con.BeginTransaction();
	FunctionBinder binder(*con.context);
	auto bind = [&](vector<unique_ptr<Expression>> children, vector<pair<Identifier, unique_ptr<Expression>>> named) {
		return binder.BindScalarFunction(function, std::move(children), std::move(named), false, nullptr);
	};

	// an argument of a parameter that accepts any type has its own type
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(Value::DATE(2020, 1, 1)));
	REQUIRE(bind(std::move(children), {})->GetReturnType() == LogicalType::DATE);

	// a constant argument can be read, here after it is cast to the type of its parameter
	children.clear();
	children.push_back(make_uniq<BoundConstantExpression>(Value("a")));
	children.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(2)));
	children.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
	REQUIRE(bind(std::move(children), {})->GetReturnType() ==
	        LogicalType::LIST(LogicalType::LIST(LogicalType::VARCHAR)));

	// and so can an argument passed by name
	children.clear();
	children.push_back(make_uniq<BoundConstantExpression>(Value::DOUBLE(1)));
	vector<pair<Identifier, unique_ptr<Expression>>> named;
	named.emplace_back("depth", make_uniq<BoundConstantExpression>(Value::BIGINT(1)));
	REQUIRE(bind(std::move(children), std::move(named))->GetReturnType() == LogicalType::LIST(LogicalType::DOUBLE));

	// an argument that is not constant is not an error
	children.clear();
	children.push_back(make_uniq<BoundConstantExpression>(Value::DOUBLE(1)));
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0U));
	REQUIRE(bind(std::move(children), {})->GetReturnType() == LogicalType::SQLNULL);
	con.Rollback();
}

TEST_CASE("A signature declares packs, which print with their stars", "[api][function_binding]") {
	const auto t = TypeName::PACK("T");
	const auto kv = TypeName::NAMED_PACK("KV");
	const auto tuple = TypeName::TUPLE({t});
	const TypeName structure(Identifier("STRUCT"), {kv});

	auto declared = FunctionSignature().AddPack("T").AddNamedPack("KV").AddVariable("U");
	REQUIRE(declared.GetVariables().size() == 3);
	REQUIRE(declared.FindVariable("T")->IsPack());
	REQUIRE(declared.FindVariable("T")->GetMultiplicity() == TypeVariable::Multiplicity::POSITIONAL_PACK);
	REQUIRE(declared.FindVariable("KV")->GetMultiplicity() == TypeVariable::Multiplicity::NAMED_PACK);
	REQUIRE(!declared.FindVariable("T")->IsValue());
	REQUIRE(!declared.FindVariable("U")->IsPack());
	REQUIRE(declared.FindVariable("T")->ToString() == "*T");
	REQUIRE(declared.FindVariable("KV")->ToString() == "**KV");
	REQUIRE(declared.FindVariable("U")->ToString() == "U");
	// declaring a pack again is the same declaration
	REQUIRE(FunctionSignature().AddPack("T").AddPack("T").GetVariables().size() == 1);

	// struct_contains(struct TUPLE(*T), entry U) -> BOOLEAN
	ScalarFunction struct_contains("struct_contains",
	                               FunctionSignature()
	                                   .AddPack("T")
	                                   .AddVariable("U")
	                                   .AddParameter("struct", tuple)
	                                   .AddParameter("entry", TypeName::VARIABLE("U"))
	                                   .SetReturnType(LogicalType::BOOLEAN),
	                               NoopExec);
	REQUIRE_NOTHROW(struct_contains.GetSignature().Verify());
	REQUIRE(struct_contains.ToString() == "struct_contains(\"struct\" TUPLE(*T), entry U) -> BOOLEAN");

	// struct_extract(struct STRUCT(**KV), entry VARCHAR) -> ANY
	ScalarFunction struct_extract("struct_extract",
	                              FunctionSignature()
	                                  .AddNamedPack("KV")
	                                  .AddParameter("struct", structure)
	                                  .AddParameter("entry", LogicalType::VARCHAR)
	                                  .SetReturnType(ReturnType::ANY()),
	                              NoopExec);
	REQUIRE_NOTHROW(struct_extract.GetSignature().Verify());
	REQUIRE(struct_extract.ToString() == "struct_extract(\"struct\" STRUCT(**KV), entry VARCHAR) -> ANY");

	// row(*args *T) -> TUPLE(*T)
	ScalarFunction row("row", FunctionSignature().AddPack("T").AddArgs("args", t).SetReturnType(tuple), NoopExec);
	REQUIRE_NOTHROW(row.GetSignature().Verify());
	REQUIRE(row.ToString() == "\"row\"(*args *T) -> TUPLE(*T)");
	REQUIRE(row.GetSignature().GetParameter(0).ToString() == "*args *T");

	// struct_pack(**kwargs **KV) -> STRUCT(**KV)
	ScalarFunction struct_pack("struct_pack",
	                           FunctionSignature().AddNamedPack("KV").AddKwargs("kwargs", kv).SetReturnType(structure),
	                           NoopExec);
	REQUIRE_NOTHROW(struct_pack.GetSignature().Verify());
	REQUIRE(struct_pack.ToString() == "struct_pack(**kwargs **KV) -> STRUCT(**KV)");

	// "*args T" with a single variable is every argument as a T, and prints as before
	const auto single_t = TypeName::VARIABLE("T");
	ScalarFunction items(
	    "items", FunctionSignature().AddVariable("T").AddArgs("args", single_t).SetReturnType(TypeName::LIST(single_t)),
	    NoopExec);
	REQUIRE_NOTHROW(items.GetSignature().Verify());
	REQUIRE(items.ToString() == "items(*args T) -> T[]");

	// a pack below another type, and as the argument of a type that is not built in, which is not checked
	REQUIRE_NOTHROW(FunctionSignature().AddPack("T").AddParameter("x", TypeName::LIST(tuple)).Verify());
	REQUIRE_NOTHROW(FunctionSignature().AddPack("T").AddParameter("x", TypeName(Identifier("my_type"), {t})).Verify());
	REQUIRE_NOTHROW(
	    FunctionSignature().AddNamedPack("KV").AddParameter("x", TypeName(Identifier("UNION"), {kv})).Verify());

	// TUPLE(T) takes one type and TUPLE(*T) any number: the use says which, so they are two type names and two
	// overloads
	const auto single_tuple = TypeName::TUPLE({single_t});
	const auto named_tuple = TypeName::TUPLE({TypeName::NAMED_PACK("T")});
	REQUIRE(single_tuple.ToString() == "TUPLE(T)");
	REQUIRE(single_tuple != tuple);
	REQUIRE(single_tuple.Hash() != tuple.Hash());
	REQUIRE(named_tuple != tuple);
	REQUIRE(named_tuple.Hash() != tuple.Hash());
	REQUIRE(tuple == TypeName::TUPLE({TypeName::PACK("T")}));
	REQUIRE(tuple.Hash() == TypeName::TUPLE({TypeName::PACK("T")}).Hash());
	auto single = FunctionSignature().AddVariable("T").AddParameter("x", single_tuple);
	auto pack = FunctionSignature().AddPack("T").AddParameter("x", tuple);
	REQUIRE(!single.IsSameOverload(pack));
	REQUIRE(!single.Equal(pack));
	REQUIRE(single != pack);
	REQUIRE(pack.IsSameOverload(FunctionSignature().AddPack("T").AddParameter("y", tuple)));
	REQUIRE(!pack.IsSameOverload(FunctionSignature().AddNamedPack("T").AddParameter("x", named_tuple)));
}

TEST_CASE("A signature that misuses a pack is rejected", "[api][function_binding]") {
	const auto t = TypeName::PACK("T");
	const auto kv = TypeName::NAMED_PACK("KV");
	const auto tuple = TypeName::TUPLE({t});
	const TypeName structure(Identifier("STRUCT"), {kv});

	// a pack is a variable of its own: not also a single variable or the other kind of pack, not a built-in type,
	// and not constrained
	REQUIRE_THROWS_WITH(FunctionSignature().AddVariable("T").AddPack("T"), Catch::Contains("declared twice"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddVariable("T"), Catch::Contains("declared twice"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddNamedPack("T"), Catch::Contains("declared twice"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("TUPLE"), Catch::Contains("is named like a built-in type"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddNamedPack("any"), Catch::Contains("is named like a built-in type"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddConstraint("T", LogicalTypeId::STRUCT),
	                    Catch::Contains("not a type variable of the signature"));

	// every use is written as the variable is declared: a pack with its stars, a single variable without
	const auto unstarred = TypeName::VARIABLE("T");
	const auto unstarred_kv = TypeName::VARIABLE("KV");
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddParameter("x", TypeName::TUPLE({unstarred})).Verify(),
	                    Catch::Contains("Pack '*T' is used as 'T': a pack is written with its stars where it is used"));
	REQUIRE_THROWS_WITH(
	    FunctionSignature()
	        .AddNamedPack("KV")
	        .AddParameter("x", TypeName(Identifier("STRUCT"), {unstarred_kv}))
	        .Verify(),
	    Catch::Contains("Pack '**KV' is used as 'KV': a pack is written with its stars where it is used"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddArgs("args", unstarred).Verify(),
	                    Catch::Contains("Pack '*T' is used as 'T'"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddVariable("T").AddParameter("x", tuple).Verify(),
	                    Catch::Contains("Variable 'T' is not a pack: it is used as 'T', not as '*T'"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddVariable("KV").AddParameter("x", structure).Verify(),
	                    Catch::Contains("Variable 'KV' is not a pack: it is used as 'KV', not as '**KV'"));
	REQUIRE_THROWS_WITH(FunctionSignature()
	                        .AddVariable("N", TypeVariable::Kind::INTEGER)
	                        .AddParameter("x", TypeName::TUPLE({TypeName::PACK("N")}))
	                        .Verify(),
	                    Catch::Contains("Variable 'N: INTEGER' is not a pack: it is used as 'N', not as '*N'"));
	REQUIRE_THROWS_WITH(FunctionSignature()
	                        .AddPack("T")
	                        .AddParameter("x", TypeName(Identifier("STRUCT"), {TypeName::NAMED_PACK("T")}))
	                        .Verify(),
	                    Catch::Contains("Pack '*T' is used as '**T': its types have no labels"));
	REQUIRE_THROWS_WITH(
	    FunctionSignature().AddNamedPack("KV").AddParameter("x", TypeName::TUPLE({TypeName::PACK("KV")})).Verify(),
	    Catch::Contains("Pack '**KV' is used as '*KV': using a named pack without its labels is not supported"));
	REQUIRE_THROWS_WITH(FunctionSignature()
	                        .AddNamedPack("KV")
	                        .AddParameter("x", structure)
	                        .SetReturnType(TypeName::TUPLE({TypeName::PACK("KV")}))
	                        .Verify(),
	                    Catch::Contains("Pack '**KV' is used as '*KV'"));
	// and a starred name is a pack the signature declares
	REQUIRE_THROWS_WITH(FunctionSignature().AddParameter("x", tuple).Verify(),
	                    Catch::Contains("'*T' is not a pack the signature declares"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddParameter("x", LogicalType::INTEGER).SetReturnType(structure).Verify(),
	                    Catch::Contains("'**KV' is not a pack the signature declares"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddParameter("x", TypeName::TUPLE({TypeName::PACK("INTEGER")})).Verify(),
	                    Catch::Contains("'*INTEGER' is not a pack the signature declares"));

	// an expansion is over one pack, which is expanded once
	const auto lists = TypeName::EXPAND(TypeName::LIST(TypeName::VARIABLE("T")));
	REQUIRE_THROWS_WITH(
	    FunctionSignature().AddPack("T").AddParameter("x", TypeName::TUPLE({TypeName::EXPAND(t)})).Verify(),
	    Catch::Contains("'*(*T)' expands '*T', which is an expansion itself: a pack is expanded once"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddArgs("args", TypeName::EXPAND(TypeName::LIST(t))).Verify(),
	                    Catch::Contains("'*((*T)[])' expands '*T', which is an expansion itself"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddVariable("T").AddArgs("args", lists).Verify(),
	                    Catch::Contains("'*(T[])' expands no pack: an expansion is over one pack of the signature"));
	REQUIRE_THROWS_WITH(
	    FunctionSignature().AddArgs("args", TypeName::EXPAND(TypeName::LIST(LogicalType::INTEGER))).Verify(),
	    Catch::Contains("'*(INTEGER[])' expands no pack"));
	REQUIRE_THROWS_WITH(
	    FunctionSignature()
	        .AddPack("T")
	        .AddPack("U")
	        .AddArgs("args", TypeName::EXPAND(TypeName::MAP(TypeName::VARIABLE("T"), TypeName::VARIABLE("U"))))
	        .Verify(),
	    Catch::Contains("'*(MAP(T, U))' expands more than one pack: an expansion is over one pack of the signature"));
	// of the kind of the expansion
	REQUIRE_THROWS_WITH(FunctionSignature()
	                        .AddPack("T")
	                        .AddKwargs("kwargs", TypeName::EXPAND_NAMED(TypeName::LIST(unstarred)))
	                        .Verify(),
	                    Catch::Contains("Pack '*T' is used as '**(T[])': its types have no labels"));
	REQUIRE_THROWS_WITH(
	    FunctionSignature().AddNamedPack("KV").AddArgs("args", TypeName::EXPAND(TypeName::LIST(unstarred_kv))).Verify(),
	    Catch::Contains("Pack '**KV' is used as '*(KV[])': using a named pack without its labels is not supported"));
	// a pattern is expanded as the type of a variadic parameter of its kind: every argument is matched against it
	REQUIRE_NOTHROW(FunctionSignature().AddPack("T").AddArgs("args", lists).Verify());
	REQUIRE_NOTHROW(FunctionSignature()
	                    .AddNamedPack("KV")
	                    .AddKwargs("kwargs", TypeName::EXPAND_NAMED(TypeName::LIST(unstarred_kv)))
	                    .Verify());
	REQUIRE_NOTHROW(FunctionSignature()
	                    .AddVariable("K")
	                    .AddPack("T")
	                    .AddArgs("args", TypeName::EXPAND(TypeName::MAP(TypeName::VARIABLE("K"), unstarred)))
	                    .Verify());
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddParameter("x", lists).Verify(),
	                    Catch::Contains("a pack '*(T[])' is the type of a '*args' parameter"));
	// a type takes a pack by itself
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddParameter("x", TypeName::TUPLE({lists})).Verify(),
	                    Catch::Contains("'*(T[])' expands a pattern in 'TUPLE(*(T[]))': a pattern is only expanded as "
	                                    "the type of a variadic parameter, a type takes a pack by itself, as '*T'"));
	REQUIRE_THROWS_WITH(FunctionSignature()
	                        .AddPack("T")
	                        .AddArgs("args", TypeName::PACK("T"))
	                        .SetReturnType(TypeName::TUPLE({lists}))
	                        .Verify(),
	                    Catch::Contains("'*(T[])' expands a pattern in 'TUPLE(*(T[]))'"));

	// every declared pack is used
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddParameter("x", LogicalType::INTEGER).Verify(),
	                    Catch::Contains("Pack '*T' is declared but not used"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddNamedPack("KV").AddParameter("x", tuple).Verify(),
	                    Catch::Contains("Pack '**KV' is declared but not used"));

	// a pack is the only argument of the type name that takes it, without a label
	REQUIRE_THROWS_WITH(
	    FunctionSignature().AddPack("T").AddParameter("x", TypeName::TUPLE({LogicalType::INTEGER, t})).Verify(),
	    Catch::Contains("Pack '*T' has to be the only argument of 'TUPLE(INTEGER, *T)'"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddParameter("x", TypeName::TUPLE({t, t})).Verify(),
	                    Catch::Contains("has to be the only argument"));
	REQUIRE_THROWS_WITH(
	    FunctionSignature().AddNamedPack("KV").AddParameter("x", TypeName::STRUCT({{"a", kv}})).Verify(),
	    Catch::Contains("Pack '**KV' has to be the only argument of 'STRUCT(a **KV)', without a label"));

	// a built-in type takes a pack the way its constructor takes its parameters: TUPLE by position, STRUCT and
	// UNION by name, and a type that takes a fixed number of parameters, or values, takes none
	REQUIRE_THROWS_WITH(
	    FunctionSignature().AddPack("T").AddParameter("x", TypeName(Identifier("STRUCT"), {t})).Verify(),
	    Catch::Contains("Type STRUCT does not take the types of a pack '*T'"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddParameter("x", TypeName(Identifier("UNION"), {t})).Verify(),
	                    Catch::Contains("Type UNION does not take the types of a pack '*T'"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddNamedPack("KV").AddParameter("x", TypeName::TUPLE({kv})).Verify(),
	                    Catch::Contains("Type TUPLE does not take the types of a pack '**KV'"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddParameter("x", TypeName::LIST(t)).Verify(),
	                    Catch::Contains("Type LIST does not take the types of a pack '*T'"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddParameter("x", TypeName(Identifier("ENUM"), {t})).Verify(),
	                    Catch::Contains("Type ENUM does not take the types of a pack '*T'"));

	// a pack by itself is not a type: it is what "*args" receives, or "**kwargs" if it is a named pack
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddParameter("x", t).Verify(),
	                    Catch::Contains("Parameter 'x *T' is not a type: a pack '*T' is the type of a '*args'"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddNamedPack("KV").AddArgs("args", kv).Verify(),
	                    Catch::Contains("a pack '**KV' is the type of a '**kwargs' parameter"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddKwargs("kwargs", t).Verify(),
	                    Catch::Contains("a pack '*T' is the type of a '*args' parameter"));
	REQUIRE_THROWS_WITH(FunctionSignature().AddPack("T").AddParameter("x", tuple).SetReturnType(ReturnType(t)).Verify(),
	                    Catch::Contains("The return type '*T' is not a type"));

	// a pack is a type variable where ANY may not be used next to one
	REQUIRE_THROWS_WITH(
	    FunctionSignature().AddPack("T").AddParameter("x", TypeName::MAP(LogicalType::ANY, tuple)).Verify(),
	    Catch::Contains("uses ANY and a type variable in the same type"));
}

TEST_CASE("A pack holds its types in order, read from a type or given as a list", "[api][function_binding]") {
	const auto tuple = LogicalType::TUPLE({LogicalType::INTEGER, LogicalType::VARCHAR});
	const auto structure = LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}});

	// the types of a tuple have no labels
	auto from_tuple = TypePack::ParametersOf(tuple);
	REQUIRE(from_tuple.Count() == 2);
	REQUIRE(from_tuple.GetType(0) == LogicalType::INTEGER);
	REQUIRE(from_tuple.GetType(1) == LogicalType::VARCHAR);
	REQUIRE(!from_tuple.GetLabel(0));
	REQUIRE(!from_tuple.GetLabel(1));
	REQUIRE(from_tuple.ToString() == "(INTEGER, VARCHAR)");

	// the types of a struct, and of a union, have theirs
	auto from_struct = TypePack::ParametersOf(structure);
	REQUIRE(from_struct.Count() == 2);
	REQUIRE(from_struct.GetType(1) == LogicalType::VARCHAR);
	REQUIRE(*from_struct.GetLabel(0) == Identifier("a"));
	REQUIRE(*from_struct.GetLabel(1) == Identifier("b"));
	REQUIRE(from_struct.ToString() == "(a INTEGER, b VARCHAR)");
	auto from_union =
	    TypePack::ParametersOf(LogicalType::UNION({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));
	REQUIRE(from_union.ToString() == "(a INTEGER, b VARCHAR)");

	// a list of types is the same pack as the types of the tuple, a list of labelled types as those of the struct:
	// a pack made from a type holds its own copy of the elements
	auto listed = TypePack::Positional({LogicalType::INTEGER, LogicalType::VARCHAR});
	REQUIRE(listed.Count() == 2);
	REQUIRE(listed.GetType(0) == LogicalType::INTEGER);
	REQUIRE(!listed.GetLabel(0));
	REQUIRE(listed.ToString() == "(INTEGER, VARCHAR)");
	REQUIRE(listed == from_tuple);
	REQUIRE(from_tuple == listed);
	auto labelled = TypePack::Named({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}});
	REQUIRE(*labelled.GetLabel(1) == Identifier("b"));
	REQUIRE(labelled.ToString() == "(a INTEGER, b VARCHAR)");
	REQUIRE(labelled == from_struct);
	REQUIRE(labelled == from_union);

	// packs differ by a label, a type, the order and the number of their elements
	REQUIRE(listed != labelled);
	REQUIRE(from_tuple != from_struct);
	REQUIRE(listed != TypePack::Positional({LogicalType::VARCHAR, LogicalType::INTEGER}));
	REQUIRE(listed != TypePack::Positional({LogicalType::INTEGER, LogicalType::BIGINT}));
	REQUIRE(listed != TypePack::Positional({LogicalType::INTEGER}));
	REQUIRE(labelled != TypePack::Named({{"a", LogicalType::INTEGER}, {"c", LogicalType::VARCHAR}}));

	// an empty pack
	auto empty_tuple = TypePack::ParametersOf(LogicalType::TUPLE(vector<LogicalType>()));
	REQUIRE(empty_tuple.Count() == 0);
	REQUIRE(empty_tuple.ToString() == "()");
	REQUIRE(empty_tuple == TypePack::Positional(vector<LogicalType>()));
	REQUIRE(empty_tuple != listed);

	// a label is what makes an element one of a named pack, and a type whose parameters are not types has no types
	REQUIRE_THROWS_WITH(TypePack::Named({{"", LogicalType::INTEGER}}), Catch::Contains("has no label"));
	REQUIRE_THROWS_WITH(TypePack::ParametersOf(LogicalType::DECIMAL(18, 3)),
	                    Catch::Contains("is not a type, so it is not an element of a pack of types"));

	// the variables of a call hold a pack by the name of its variable
	FunctionVariables variables;
	variables.packs.emplace("T", from_tuple);
	REQUIRE(variables.packs.at("T") == listed);
	REQUIRE(variables.types.empty());
}

//! search(struct TUPLE(*T), entry U) -> BOOLEAN
static FunctionSignature GetTupleSearchSignature() {
	return FunctionSignature()
	    .AddPack("T")
	    .AddVariable("U")
	    .AddParameter("struct", TypeName::TUPLE({TypeName::PACK("T")}))
	    .AddParameter("entry", TypeName::VARIABLE("U"))
	    .SetReturnType(LogicalType::BOOLEAN);
}

TEST_CASE("A positional pack is the types of the tuple that is passed as it", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const LogicalType integer = LogicalType::INTEGER;
	const auto tuple = LogicalType::TUPLE({LogicalType::INTEGER, LogicalType::VARCHAR});
	const auto empty_tuple = LogicalType::TUPLE(vector<LogicalType>());
	const auto nested_tuple = LogicalType::TUPLE({tuple, LogicalType::LIST(LogicalType::DOUBLE)});
	const auto structure = LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}});
	const auto empty_struct = LogicalType::STRUCT(child_list_t<LogicalType>());
	ScalarFunctionSet search("search");
	search.AddFunction(ScalarFunction("search", GetTupleSearchSignature(), NoopExec));
	REQUIRE(search.GetOverload(0).GetSignature().ToString() == "(\"struct\" TUPLE(*T), entry U) -> BOOLEAN");

	con.context->RunFunctionInTransaction([&]() {
		// a tuple says the pack: its types in order - it is passed as it is, at no cost
		auto selection = FunctionBinder::SelectFunction(*con.context, search, {tuple, integer});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("T") ==
		        TypePack::Positional({LogicalType::INTEGER, LogicalType::VARCHAR}));
		REQUIRE(selection.GetBinding().variables.types.count("T") == 0);
		REQUIRE(selection.GetBinding().variables.types.at("U") == integer);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == tuple);
		REQUIRE(selection.GetBinding().rank.cast_cost == 0);
		selection = FunctionBinder::SelectFunction(*con.context, search, {nested_tuple, integer});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("T") ==
		        TypePack::Positional({tuple, LogicalType::LIST(LogicalType::DOUBLE)}));
		REQUIRE(selection.GetBinding().layout.arguments[0].type == nested_tuple);

		// a tuple without types says the pack without elements
		selection = FunctionBinder::SelectFunction(*con.context, search, {empty_tuple, integer});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("T").Count() == 0);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == empty_tuple);

		// a struct with names is not a tuple: it says nothing about the pack, and cannot be passed - like any type of
		// another kind
		selection = FunctionBinder::SelectFunction(*con.context, search, {structure, integer});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		REQUIRE(GetMismatches(selection)[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);
		REQUIRE_THAT(GetErrorMessage(selection),
		             Catch::Contains("Argument 1 of type STRUCT(a INTEGER, b VARCHAR) cannot be implicitly cast to "
		                             "TUPLE(*T)"));

		// a struct without fields has no names either: it says the pack without elements, and is cast to the tuple
		// without elements, which costs the change of kind
		selection = FunctionBinder::SelectFunction(*con.context, search, {empty_struct, integer});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("T").Count() == 0);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == empty_tuple);
		REQUIRE(selection.GetBinding().rank.cast_cost == 1);

		// a NULL has no elements to say: it is passed as it is, and costs the cast of a NULL to a tuple
		selection = FunctionBinder::SelectFunction(*con.context, search, {LogicalType::SQLNULL, integer});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.count("T") == 0);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::SQLNULL);
		REQUIRE(selection.GetBinding().rank.cast_cost == 161);

		// an unresolved parameter is what could say the pack: the call is unresolved
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, search, {LogicalType::UNKNOWN, integer}),
		                    Catch::Contains("Parameter types could not be resolved"));

		// a type of another kind says nothing, and cannot be passed
		for (auto &other : {LogicalType::LIST(LogicalType::INTEGER), LogicalType(LogicalType::INTEGER),
		                    LogicalType::MAP(LogicalType::INTEGER, LogicalType::VARCHAR)}) {
			selection = FunctionBinder::SelectFunction(*con.context, search, {other, integer});
			REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
			REQUIRE(GetMismatches(selection)[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);
			REQUIRE_THAT(GetErrorMessage(selection), Catch::Contains("cannot be implicitly cast to TUPLE(*T)"));
		}
	});

	// one parameter uses a pack, so that one argument says what it is
	REQUIRE_THROWS_WITH(FunctionSignature()
	                        .AddPack("T")
	                        .AddParameter("x", TypeName::TUPLE({TypeName::PACK("T")}))
	                        .AddParameter("y", TypeName::TUPLE({TypeName::PACK("T")}))
	                        .Verify(),
	                    Catch::Contains("Pack '*T' is used by the parameters more than once"));
}

TEST_CASE("A pack ranks like a type variable in the type that takes it", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto t = TypeName::VARIABLE("T");
	const auto pack_tuple = TypeName::TUPLE({TypeName::PACK("P")});
	const auto int_varchar_tuple = TypeName::TUPLE({LogicalType::INTEGER, LogicalType::VARCHAR});
	// f(x TUPLE(*P)), f(x T[]), f(x T), f(x TUPLE(INTEGER, VARCHAR))
	const auto packed =
	    FunctionSignature().AddPack("P").AddParameter("x", pack_tuple).SetReturnType(LogicalType::INTEGER);
	const auto listed = GetSelectionSignature({TypeName::LIST(t)});
	const auto generic = GetSelectionSignature({t});
	const auto concrete = GetSelectionSignature({int_varchar_tuple});

	con.context->RunFunctionInTransaction([&]() {
		const auto tuple = LogicalType::TUPLE({LogicalType::INTEGER, LogicalType::VARCHAR});
		const LogicalType null = LogicalType::SQLNULL;

		// a tuple: as T[] ranks for a list - rule 2 counts the argument, rule 4 the parameter, rule 5 the type name
		ScalarFunctionSet set("f");
		set.AddFunction(ScalarFunction("f", packed, NoopExec));
		auto selection = FunctionBinder::SelectFunction(*con.context, set, {tuple});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().rank == MakeRank(1, 0, 1, 2, 0));
		// a NULL is not counted by rule 2, and costs the cast of a NULL to a tuple
		selection = FunctionBinder::SelectFunction(*con.context, set, {null});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().rank == MakeRank(0, 161, 1, 2, 0));

		// rule 5: TUPLE(*P) over T, a tuple
		set.AddFunction(ScalarFunction("f", generic, NoopExec));
		selection = FunctionBinder::SelectFunction(*con.context, set, {tuple});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 0);
		// rule 2: TUPLE(INTEGER, VARCHAR) over TUPLE(*P), the tuple
		set.AddFunction(ScalarFunction("f", concrete, NoopExec));
		selection = FunctionBinder::SelectFunction(*con.context, set, {tuple});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 2);

		// rule 3: T[] over TUPLE(*P), a NULL - an untyped value is a list rather than a tuple
		ScalarFunctionSet nested("g");
		nested.AddFunction(ScalarFunction("g", packed, NoopExec));
		nested.AddFunction(ScalarFunction("g", listed, NoopExec));
		selection = FunctionBinder::SelectFunction(*con.context, nested, {null});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 1);
		// and only TUPLE(*P) takes a tuple
		selection = FunctionBinder::SelectFunction(*con.context, nested, {tuple});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 0);
	});
}

//! keys(struct STRUCT(**KV)) -> INTEGER
static FunctionSignature GetStructKeysSignature() {
	return FunctionSignature()
	    .AddNamedPack("KV")
	    .AddParameter("struct", TypeName(Identifier("STRUCT"), {TypeName::NAMED_PACK("KV")}))
	    .SetReturnType(LogicalType::INTEGER);
}

TEST_CASE("A named pack is the fields of the struct that is passed as it", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto structure = LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}});
	const auto nested_struct = LogicalType::STRUCT({{"s", structure}, {"l", LogicalType::LIST(LogicalType::DOUBLE)}});
	const auto empty_struct = LogicalType::STRUCT(child_list_t<LogicalType>());
	const auto tuple = LogicalType::TUPLE({LogicalType::INTEGER, LogicalType::VARCHAR});
	const auto empty_tuple = LogicalType::TUPLE(vector<LogicalType>());
	const auto union_type = LogicalType::UNION({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}});
	ScalarFunctionSet keys("keys");
	keys.AddFunction(ScalarFunction("keys", GetStructKeysSignature(), NoopExec));
	REQUIRE(keys.GetOverload(0).GetSignature().ToString() == "(\"struct\" STRUCT(**KV)) -> INTEGER");
	// tag("union" UNION(**KV)) -> INTEGER
	ScalarFunctionSet tag("tag");
	tag.AddFunction(
	    ScalarFunction("tag",
	                   FunctionSignature()
	                       .AddNamedPack("KV")
	                       .AddParameter("union", TypeName(Identifier("UNION"), {TypeName::NAMED_PACK("KV")}))
	                       .SetReturnType(LogicalType::INTEGER),
	                   NoopExec));

	con.context->RunFunctionInTransaction([&]() {
		// a struct says the pack: its fields in order, each with its name - it is passed as it is, at no cost
		auto selection = FunctionBinder::SelectFunction(*con.context, keys, {structure});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("KV") ==
		        TypePack::Named({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));
		REQUIRE(selection.GetBinding().variables.types.count("KV") == 0);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == structure);
		REQUIRE(selection.GetBinding().rank.cast_cost == 0);
		selection = FunctionBinder::SelectFunction(*con.context, keys, {nested_struct});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("KV") ==
		        TypePack::Named({{"s", structure}, {"l", LogicalType::LIST(LogicalType::DOUBLE)}}));
		REQUIRE(selection.GetBinding().layout.arguments[0].type == nested_struct);

		// a struct without fields says the pack without elements
		selection = FunctionBinder::SelectFunction(*con.context, keys, {empty_struct});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("KV").Count() == 0);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == empty_struct);

		// the labels go to the type that is constructed from the pack: a struct with an alias is a type of its own,
		// and is cast explicitly to the struct of its fields by a caller that knows the function it calls
		const auto aliased = structure.WithAlias("pair");
		auto binding = FunctionBinder::MatchFunction(*con.context, keys.GetSharedOverload(0), {aliased});
		REQUIRE(binding.variables.packs.at("KV") ==
		        TypePack::Named({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));
		REQUIRE(binding.layout.arguments[0].type == structure);
		REQUIRE(binding.layout.arguments[0].type.GetAlias().empty());

		// a union says the members of UNION(**KV)
		selection = FunctionBinder::SelectFunction(*con.context, tag, {union_type});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("KV") ==
		        TypePack::Named({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));
		REQUIRE(selection.GetBinding().layout.arguments[0].type == union_type);
		selection = FunctionBinder::SelectFunction(*con.context, tag, {structure});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
		REQUIRE_THAT(GetErrorMessage(selection), Catch::Contains("cannot be implicitly cast to UNION(**KV)"));

		// a tuple without elements says the pack without elements, and is cast to the struct without fields, which
		// costs the change of kind
		selection = FunctionBinder::SelectFunction(*con.context, keys, {empty_tuple});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("KV").Count() == 0);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == empty_struct);
		REQUIRE(selection.GetBinding().rank.cast_cost == 1);

		// a tuple with elements is not a struct with names: it says nothing about the pack, and cannot be passed -
		// like any type of another kind
		for (auto &other :
		     {tuple, LogicalType::LIST(LogicalType::INTEGER), union_type, LogicalType(LogicalType::INTEGER),
		      LogicalType::MAP(LogicalType::INTEGER, LogicalType::VARCHAR)}) {
			selection = FunctionBinder::SelectFunction(*con.context, keys, {other});
			REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
			REQUIRE(GetMismatches(selection)[0].Cast<FunctionMismatch::IncompatibleArgument>().argument == 0);
			REQUIRE_THAT(GetErrorMessage(selection), Catch::Contains("cannot be implicitly cast to STRUCT(**KV)"));
		}
		selection = FunctionBinder::SelectFunction(*con.context, keys, {tuple});
		REQUIRE_THAT(GetErrorMessage(selection),
		             Catch::Contains("Argument 1 of type TUPLE(INTEGER, VARCHAR) cannot be implicitly cast to "
		                             "STRUCT(**KV)"));

		// a NULL has no fields to say: it is passed as it is, and costs the cast of a NULL to a struct
		selection = FunctionBinder::SelectFunction(*con.context, keys, {LogicalType::SQLNULL});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.count("KV") == 0);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::SQLNULL);
		REQUIRE(selection.GetBinding().rank.cast_cost == 160);

		// an unresolved parameter is what could say the pack: the call is unresolved
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, keys, {LogicalType::UNKNOWN}),
		                    Catch::Contains("Parameter types could not be resolved"));
	});
}

TEST_CASE("A struct with names and a tuple each select the overload of their kind", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto t = TypeName::VARIABLE("T");
	const auto named = GetStructKeysSignature();
	const auto positional = FunctionSignature().AddPack("P").AddParameter("x", TypeName::TUPLE({TypeName::PACK("P")}));
	const auto generic = GetSelectionSignature({t});
	const auto listed = GetSelectionSignature({TypeName::LIST(t)});
	const auto concrete = GetSelectionSignature({TypeName::STRUCT({{"a", LogicalType::INTEGER}})});

	con.context->RunFunctionInTransaction([&]() {
		const auto structure = LogicalType::STRUCT({{"a", LogicalType::INTEGER}});
		const auto tuple = LogicalType::TUPLE({LogicalType::INTEGER});
		const LogicalType null = LogicalType::SQLNULL;

		// a struct: as TUPLE(*T) ranks for a tuple
		ScalarFunctionSet set("f");
		set.AddFunction(ScalarFunction("f", named, NoopExec));
		auto selection = FunctionBinder::SelectFunction(*con.context, set, {structure});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().rank == MakeRank(1, 0, 1, 2, 0));
		selection = FunctionBinder::SelectFunction(*con.context, set, {null});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().rank == MakeRank(0, 160, 1, 2, 0));

		// f(struct STRUCT(**KV)) and f(x TUPLE(*P)): only one takes a struct, only the other a tuple, and a NULL is
		// a struct rather than a tuple (rule 3: 160 against 161)
		set.AddFunction(ScalarFunction("f", positional, NoopExec));
		selection = FunctionBinder::SelectFunction(*con.context, set, {structure});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 0);
		selection = FunctionBinder::SelectFunction(*con.context, set, {tuple});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 1);
		selection = FunctionBinder::SelectFunction(*con.context, set, {null});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 0);

		// a struct without fields and a tuple without elements fit both, and each selects the overload of its own
		// kind (rule 3: 0 against the 1 that the change of kind costs)
		const auto empty_struct = LogicalType::STRUCT(child_list_t<LogicalType>());
		const auto empty_tuple = LogicalType::TUPLE(vector<LogicalType>());
		selection = FunctionBinder::SelectFunction(*con.context, set, {empty_struct});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 0);
		REQUIRE(selection.GetBinding().rank.cast_cost == 0);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == empty_struct);
		selection = FunctionBinder::SelectFunction(*con.context, set, {empty_tuple});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 1);
		REQUIRE(selection.GetBinding().rank.cast_cost == 0);
		REQUIRE(selection.GetBinding().layout.arguments[0].type == empty_tuple);

		// rule 5: STRUCT(**KV) over T, a struct
		set.AddFunction(ScalarFunction("f", generic, NoopExec));
		selection = FunctionBinder::SelectFunction(*con.context, set, {structure});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 0);
		// rule 2: STRUCT(a INTEGER) over STRUCT(**KV), the struct
		set.AddFunction(ScalarFunction("f", concrete, NoopExec));
		selection = FunctionBinder::SelectFunction(*con.context, set, {structure});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 3);

		// g(struct STRUCT(**KV)) and g(x T[]): a NULL is as much a struct as a list - both cost 160 and rank the same
		ScalarFunctionSet nested("g");
		nested.AddFunction(ScalarFunction("g", named, NoopExec));
		nested.AddFunction(ScalarFunction("g", listed, NoopExec));
		selection = FunctionBinder::SelectFunction(*con.context, nested, {structure});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().GetOverloadIndex() == 0);
		selection = FunctionBinder::SelectFunction(*con.context, nested, {null});
		REQUIRE_THAT(GetErrorMessage(selection), Catch::Contains("Could not choose a best candidate function"));
	});
}

TEST_CASE("A pattern that a variadic parameter expands is matched by every argument", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto t = TypeName::VARIABLE("T");
	const auto lists = TypeName::EXPAND(TypeName::LIST(t));
	// zip(*lists *(T[])) -> TUPLE(*T)[]
	ScalarFunctionSet zip("zip");
	zip.AddFunction(ScalarFunction("zip",
	                               FunctionSignature()
	                                   .AddPack("T")
	                                   .AddArgs("lists", lists)
	                                   .SetReturnType(TypeName::LIST(TypeName::TUPLE({TypeName::PACK("T")}))),
	                               NoopExec));
	// any_lists(*lists ANY[])
	ScalarFunctionSet any_lists("any_lists");
	any_lists.AddFunction(ScalarFunction(
	    "any_lists",
	    FunctionSignature().AddArgs("lists", TypeName::LIST(LogicalType::ANY)).SetReturnType(LogicalType::INTEGER),
	    NoopExec));
	// tagged(**kwargs **(KV[])) -> STRUCT(**KV)
	const TypeName named_struct(Identifier("STRUCT"), {TypeName::NAMED_PACK("KV")});
	ScalarFunctionSet tagged("tagged");
	tagged.AddFunction(
	    ScalarFunction("tagged",
	                   FunctionSignature()
	                       .AddNamedPack("KV")
	                       .AddKwargs("kwargs", TypeName::EXPAND_NAMED(TypeName::LIST(TypeName::VARIABLE("KV"))))
	                       .SetReturnType(named_struct),
	                   NoopExec));
	// keyed(*maps *(MAP(K, V))) -> K: one key type for all the maps, a value type for each
	const auto k = TypeName::VARIABLE("K");
	ScalarFunctionSet keyed("keyed");
	keyed.AddFunction(ScalarFunction("keyed",
	                                 FunctionSignature()
	                                     .AddVariable("K")
	                                     .AddPack("V")
	                                     .AddArgs("maps", TypeName::EXPAND(TypeName::MAP(k, TypeName::VARIABLE("V"))))
	                                     .SetReturnType(k),
	                                 NoopExec));

	const auto integers = LogicalType::LIST(LogicalType::INTEGER);
	const auto strings = LogicalType::LIST(LogicalType::VARCHAR);
	const auto integer_array = LogicalType::ARRAY(LogicalType::INTEGER, 3);
	const auto empty_list = LogicalType::LIST(LogicalType::SQLNULL);
	const LogicalType string_literal(LogicalTypeId::STRING_LITERAL);
	const auto unknown = LogicalType(LogicalType::UNKNOWN);

	con.context->RunFunctionInTransaction([&]() {
		// no argument: the pack of no elements
		auto selection = FunctionBinder::SelectFunction(*con.context, zip, {});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("T").Count() == 0);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) ==
		        LogicalType::LIST(LogicalType::TUPLE(vector<LogicalType>())));

		// every argument says its element in the pattern, and is passed as the pattern with it
		selection = FunctionBinder::SelectFunction(*con.context, zip, {integers, strings});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("T") ==
		        TypePack::Positional({LogicalType::INTEGER, LogicalType::VARCHAR}));
		REQUIRE(selection.GetBinding().layout.arguments[0].type == integers);
		REQUIRE(selection.GetBinding().layout.arguments[1].type == strings);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) ==
		        LogicalType::LIST(LogicalType::TUPLE({LogicalType::INTEGER, LogicalType::VARCHAR})));

		// an array is passed as a list, a NULL as it is, and a list of no type is a list of NULLs
		selection =
		    FunctionBinder::SelectFunction(*con.context, zip, {integer_array, LogicalType::SQLNULL, empty_list});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("T") ==
		        TypePack::Positional({LogicalType::INTEGER, LogicalType::SQLNULL, LogicalType::SQLNULL}));
		REQUIRE(selection.GetBinding().layout.arguments[0].type == integers);
		REQUIRE(selection.GetBinding().layout.arguments[1].type == LogicalType::SQLNULL);
		REQUIRE(selection.GetBinding().layout.arguments[2].type == empty_list);
		REQUIRE(
		    ReturnTypeOf(con, selection.GetBinding()) ==
		    LogicalType::LIST(LogicalType::TUPLE({LogicalType::INTEGER, LogicalType::SQLNULL, LogicalType::SQLNULL})));

		// a call through the pattern ranks like the same call through ANY[]
		auto through_any =
		    FunctionBinder::SelectFunction(*con.context, any_lists, {integer_array, LogicalType::SQLNULL, empty_list});
		REQUIRE(through_any.IsSuccess());
		REQUIRE(selection.GetBinding().rank == through_any.GetBinding().rank);
		selection = FunctionBinder::SelectFunction(*con.context, zip, {integers, strings});
		through_any = FunctionBinder::SelectFunction(*con.context, any_lists, {integers, strings});
		REQUIRE(selection.GetBinding().rank == through_any.GetBinding().rank);

		// an argument that has no parts for the pattern says no element and cannot be passed
		for (auto &other : {LogicalType(LogicalType::INTEGER), string_literal, LogicalType::VARIANT()}) {
			selection = FunctionBinder::SelectFunction(*con.context, zip, {integers, other});
			auto &mismatch = GetMismatches(selection)[0];
			REQUIRE(mismatch.GetType() == FunctionMismatch::Type::INCOMPATIBLE_ARGUMENT);
			REQUIRE(mismatch.Cast<FunctionMismatch::IncompatibleArgument>().argument == 1);
			REQUIRE_THAT(GetErrorMessage(selection), Catch::Contains("cannot be implicitly cast to T[]"));
			through_any = FunctionBinder::SelectFunction(*con.context, any_lists, {integers, other});
			REQUIRE(!through_any.IsSuccess());
		}

		// an unresolved parameter is an element nothing else says
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, zip, {integers, unknown}),
		                    Catch::Contains("Parameter types could not be resolved"));

		// "**kwargs": the element has the name of its argument as the label
		vector<pair<Identifier, LogicalType>> named {{"a", integers}, {"b", strings}};
		selection = FunctionBinder::SelectFunction(*con.context, tagged, FunctionCallArguments({}, named));
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("KV") ==
		        TypePack::Named({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) ==
		        LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));

		// a variable next to the pack in the pattern is one type for all the arguments
		const auto first_map = LogicalType::MAP(LogicalType::INTEGER, LogicalType::VARCHAR);
		const auto second_map = LogicalType::MAP(LogicalType::BIGINT, LogicalType::DATE);
		selection = FunctionBinder::SelectFunction(*con.context, keyed, {first_map, second_map});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.types.at("K") == LogicalType::BIGINT);
		REQUIRE(selection.GetBinding().variables.packs.at("V") ==
		        TypePack::Positional({LogicalType::VARCHAR, LogicalType::DATE}));
		REQUIRE(selection.GetBinding().layout.arguments[0].type ==
		        LogicalType::MAP(LogicalType::BIGINT, LogicalType::VARCHAR));
		REQUIRE(selection.GetBinding().layout.arguments[1].type == second_map);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::BIGINT);
	});
}

TEST_CASE("A pack on a variadic parameter is the arguments it receives", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto t = TypeName::PACK("T");
	// row(*args *T) -> TUPLE(*T), wrap(tuple TUPLE(*T)) -> TUPLE(*T)[]
	ScalarFunctionSet row("row");
	row.AddFunction(ScalarFunction(
	    "row", FunctionSignature().AddPack("T").AddArgs("args", t).SetReturnType(TypeName::TUPLE({t})), NoopExec));
	ScalarFunctionSet wrap("wrap");
	wrap.AddFunction(ScalarFunction("wrap",
	                                FunctionSignature()
	                                    .AddPack("T")
	                                    .AddParameter("tuple", TypeName::TUPLE({t}))
	                                    .SetReturnType(TypeName::LIST(TypeName::TUPLE({t}))),
	                                NoopExec));

	// wrap(struct STRUCT(**KV)) -> STRUCT(**KV)[]
	const TypeName named_struct(Identifier("STRUCT"), {TypeName::NAMED_PACK("KV")});
	ScalarFunctionSet wrap_named("wrap");
	wrap_named.AddFunction(ScalarFunction("wrap",
	                                      FunctionSignature()
	                                          .AddNamedPack("KV")
	                                          .AddParameter("struct", named_struct)
	                                          .SetReturnType(TypeName::LIST(named_struct)),
	                                      NoopExec));

	// pack(**kwargs **KV) -> STRUCT(**KV), insert(struct STRUCT(**KV), **kwargs **N)
	ScalarFunctionSet pack("pack");
	pack.AddFunction(ScalarFunction("pack",
	                                FunctionSignature()
	                                    .AddNamedPack("KV")
	                                    .AddKwargs("kwargs", TypeName::NAMED_PACK("KV"))
	                                    .SetReturnType(named_struct),
	                                NoopExec));
	ScalarFunctionSet insert("insert");
	insert.AddFunction(ScalarFunction("insert",
	                                  FunctionSignature()
	                                      .AddNamedPack("KV")
	                                      .AddNamedPack("N")
	                                      .AddParameter("struct", named_struct)
	                                      .AddKwargs("kwargs", TypeName::NAMED_PACK("N"))
	                                      .SetReturnType(LogicalType::INTEGER),
	                                  NoopExec));
	// any(*args ANY)
	ScalarFunctionSet any("any");
	any.AddFunction(ScalarFunction(
	    "any", FunctionSignature().AddArgs("args", LogicalType::ANY).SetReturnType(LogicalType::INTEGER), NoopExec));

	const LogicalType string_literal(LogicalTypeId::STRING_LITERAL);
	const auto integer_literal = LogicalType::INTEGER_LITERAL(Value::INTEGER(1));
	const auto unknown = LogicalType(LogicalType::UNKNOWN);
	const string unresolved_message = "Parameter types could not be resolved";

	con.context->RunFunctionInTransaction([&]() {
		// no argument: the pack of no elements
		auto selection = FunctionBinder::SelectFunction(*con.context, row, {});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("T").Count() == 0);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::TUPLE(vector<LogicalType>()));

		// every argument is one element, and is passed as it: a literal as its default type, a NULL as it is
		selection = FunctionBinder::SelectFunction(*con.context, row, {LogicalType::BIGINT});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::BIGINT);
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::TUPLE({LogicalType::BIGINT}));
		selection =
		    FunctionBinder::SelectFunction(*con.context, row, {integer_literal, string_literal, LogicalType::SQLNULL});
		REQUIRE(selection.IsSuccess());
		auto &elements = selection.GetBinding().variables.packs.at("T");
		REQUIRE(elements == TypePack::Positional({LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::SQLNULL}));
		for (idx_t i = 0; i < elements.Count(); i++) {
			REQUIRE(selection.GetBinding().layout.arguments[i].type == elements.GetType(i));
		}
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) ==
		        LogicalType::TUPLE({LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::SQLNULL}));

		// a call through a pack ranks like the same call through ANY
		auto through_any =
		    FunctionBinder::SelectFunction(*con.context, any, {integer_literal, string_literal, LogicalType::SQLNULL});
		REQUIRE(through_any.IsSuccess());
		REQUIRE(selection.GetBinding().rank == through_any.GetBinding().rank);
		selection = FunctionBinder::SelectFunction(*con.context, row, {LogicalType::DATE, LogicalType::DOUBLE});
		through_any = FunctionBinder::SelectFunction(*con.context, any, {LogicalType::DATE, LogicalType::DOUBLE});
		REQUIRE(selection.GetBinding().rank == through_any.GetBinding().rank);

		// an unresolved parameter is an element nothing else says
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, row, {LogicalType::INTEGER, unknown}),
		                    Catch::Contains(unresolved_message));

		// "**kwargs": the names of the arguments are the labels, in the order of the call
		vector<pair<Identifier, LogicalType>> named {{"a", integer_literal}, {"b", LogicalType::VARCHAR}};
		selection = FunctionBinder::SelectFunction(*con.context, pack, FunctionCallArguments({}, named));
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("KV") ==
		        TypePack::Named({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) ==
		        LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));
		named = {{"b", LogicalType::VARCHAR}, {"a", LogicalType::SQLNULL}};
		selection = FunctionBinder::SelectFunction(*con.context, pack, FunctionCallArguments({}, named));
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) ==
		        LogicalType::STRUCT({{"b", LogicalType::VARCHAR}, {"a", LogicalType::SQLNULL}}));
		selection = FunctionBinder::SelectFunction(*con.context, pack, {});
		REQUIRE(ReturnTypeOf(con, selection.GetBinding()) == LogicalType::STRUCT(child_list_t<LogicalType>()));
		named = {{"a", LogicalType::INTEGER}, {"b", unknown}};
		REQUIRE_THROWS_WITH(FunctionBinder::SelectFunction(*con.context, pack, FunctionCallArguments({}, named)),
		                    Catch::Contains(unresolved_message));
		// no overload takes a name twice
		named = {{"a", LogicalType::INTEGER}, {"a", LogicalType::VARCHAR}};
		selection = FunctionBinder::SelectFunction(*con.context, pack, FunctionCallArguments({}, named));
		REQUIRE(!selection.IsSuccess());
		REQUIRE(selection.GetError().GetType() == FunctionBindError::Type::DUPLICATE_ARGUMENT);

		// two packs in one signature: the one of the struct and the one of the named arguments
		const auto structure = LogicalType::STRUCT({{"a", LogicalType::INTEGER}});
		named = {{"b", LogicalType::VARCHAR}};
		selection = FunctionBinder::SelectFunction(*con.context, insert, FunctionCallArguments({structure}, named));
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variables.packs.at("KV") == TypePack::Named({{"a", LogicalType::INTEGER}}));
		REQUIRE(selection.GetBinding().variables.packs.at("N") == TypePack::Named({{"b", LogicalType::VARCHAR}}));

		// a pack the return type uses that no argument says is named as it is used
		selection = FunctionBinder::SelectFunction(*con.context, wrap, {LogicalType::SQLNULL});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::TEMPLATE_UNRESOLVED);
		REQUIRE_THAT(GetErrorMessage(selection), Catch::Contains("Cannot deduce template type '*T'"));
		selection = FunctionBinder::SelectFunction(*con.context, wrap_named, {LogicalType::SQLNULL});
		REQUIRE(GetMismatches(selection)[0].GetType() == FunctionMismatch::Type::TEMPLATE_UNRESOLVED);
		REQUIRE_THAT(GetErrorMessage(selection), Catch::Contains("Cannot deduce template type '**KV'"));
	});
}
