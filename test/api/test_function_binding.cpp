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
		REQUIRE(selection.GetBinding().variable_types.at("T") == LogicalType::INTEGER);
		REQUIRE(selection.GetBinding().variable_types.at("U") == LogicalType::VARCHAR);
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
		REQUIRE(selection.GetBinding().variable_values.at("N") == Value::BIGINT(3));

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
		REQUIRE(selection.GetBinding().variable_types.at("T") == LogicalType::BIGINT);
		REQUIRE(selection.GetBinding().variable_values.at("N") == Value::BIGINT(3));

		// arguments that determine different values do not match the overload
		selection = FunctionBinder::SelectFunction(*con.context, same,
		                                           {int_array, LogicalType::ARRAY(LogicalType::INTEGER, 4)});
		auto &deduction = GetMismatches(selection)[0];
		REQUIRE(deduction.GetType() == FunctionMismatch::Type::TEMPLATE_DEDUCTION);
		REQUIRE(deduction.Cast<FunctionMismatch::TemplateDeduction>().template_name == "N");
		REQUIRE(deduction.Cast<FunctionMismatch::TemplateDeduction>().argument.GetIndex() == 1);
		REQUIRE(StringUtil::Contains(GetErrorMessage(selection), "it is determined to be both 3 and 4"));

		// an argument that says nothing leaves the value to the other arguments, or unknown: an array of any size
		selection = FunctionBinder::SelectFunction(*con.context, same, {LogicalType::SQLNULL, int_array});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variable_values.at("N") == Value::BIGINT(3));
		selection = FunctionBinder::SelectFunction(*con.context, same, {LogicalType::SQLNULL, LogicalType::SQLNULL});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().variable_values.at("N").IsNull());
		REQUIRE(ArrayType::IsAnySize(selection.GetBinding().layout.arguments[0].type));

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
		REQUIRE(selection.GetBinding().variable_values.at("W") == Value::BIGINT(18));
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
		    {"DECIMAL(W, S) over T IS DECIMAL, an integer", {{decimal_ws}, {t_decimal}}, {integer}, 0},
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
		    {"STRUCT(a INTEGER) over T IS STRUCT, NULL", {{struct_a}, {t_struct}}, {null}, 0},
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
		    // no tie: the cost decides, a type variable of any type costs more than any cast - but less for a NULL
		    {"INTEGER at a lower cost than T, an integer", {{LogicalType::INTEGER}, {t}}, {integer}, 0},
		    {"T at a lower cost than INTEGER, NULL", {{LogicalType::INTEGER}, {t}}, {null}, 1},
		    {"T IS STRUCT at a lower cost than T, a struct", {{t_struct}, {t}}, {a_struct}, 0},
		    {"T IS DECIMAL at a lower cost than T, an integer", {{t_decimal}, {t}}, {integer}, 0},
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
		    {"rule 2 and 3: T, a NULL is not counted and costs 5",
		     GetSelectionSignature({t}),
		     {null},
		     MakeRank(0, 5, 1, 0, 0)},
		    {"rule 3: T[] and T, a string literal costs 20",
		     GetSelectionSignature({t_list, t}),
		     {string_literal, integer},
		     MakeRank(2, 20, 2, 2, 0)},
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
		    {"rule 3: T over INTEGER, NULL", {{LogicalType::INTEGER}, {t}}, {null}, 1},
		    {"rule 3 before rule 4: T IS DECIMAL without a cast over DOUBLE with one, a decimal",
		     {{t_decimal}, {LogicalType::DOUBLE}},
		     {decimal},
		     0},
		    {"rule 3 before rule 5: ANY over ANY[], an array is cast to a list", {{any_list}, {any}}, {integers_3}, 1},
		    {"rule 4: STRUCT(a INTEGER) over T IS STRUCT, the struct", {{struct_a}, {t_struct}}, {a_struct}, 0},
		    {"rule 5: T[] over T, a list", {{t_list}, {t}}, {integers}, 0},
		    {"rule 5: ANY[] over ANY, a list", {{any_list}, {any}}, {integers}, 0},
		    {"rule 5: MAP(VARCHAR, T) over MAP(U, T), a map", {{map_varchar_t}, {map_u_t}}, {a_map}, 0},
		    {"rule 5: (T[], T) over (T IS STRUCT, U), NULL and an integer",
		     {{t_list, t}, {t_struct, u}},
		     {null, integer},
		     0},
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

TEST_CASE("An argument that is cast explicitly to a type of ANY is passed as the type a literal defaults to",
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

		// a caller that knows the function casts it to a list of what the literal is
		auto binding = FunctionBinder::MatchFunction(*con.context, function, {integer_literal});
		REQUIRE(binding.layout.arguments[0].type == LogicalType::LIST(LogicalType::INTEGER));
		binding = FunctionBinder::MatchFunction(*con.context, function, {string_literal});
		REQUIRE(binding.layout.arguments[0].type == LogicalType::LIST(LogicalType::VARCHAR));
		binding = FunctionBinder::MatchFunction(*con.context, function, {LogicalType::DATE});
		REQUIRE(binding.layout.arguments[0].type == LogicalType::LIST(LogicalType::DATE));

		// a default that is not a list is passed as one as well, however the overload is bound
		FunctionSignature sig;
		sig.AddParameter("list", TypeName::LIST(LogicalType::ANY), Value::INTEGER(1));
		sig.SetReturnType(LogicalType::INTEGER);
		ScalarFunctionSet defaulted("wrap_default");
		defaulted.AddFunction(ScalarFunction("wrap_default", std::move(sig), NoopExec));
		auto selection = FunctionBinder::SelectFunction(*con.context, defaulted, FunctionCallArguments());
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().layout.arguments[0].type == LogicalType::LIST(LogicalType::INTEGER));
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
