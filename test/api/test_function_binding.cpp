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
	REQUIRE(selection.GetBinding().return_type == LogicalType::INTEGER);
	const vector<idx_t> identity {0, 1};
	REQUIRE(selection.GetBinding().argument_to_parameters == identity);

	// An omitted parameter still contributes its default to template deduction.
	selection = FunctionBinder::SelectFunction(*con.context, set, {LogicalType::INTEGER});
	REQUIRE(selection.IsSuccess());
	REQUIRE(selection.GetBinding().return_type == LogicalType::BIGINT);

	vector<LogicalType> no_positional;
	vector<pair<Identifier, LogicalType>> reversed {{"b", LogicalType::BIGINT}, {"a", LogicalType::INTEGER}};
	selection = FunctionBinder::SelectFunction(*con.context, set, {no_positional, reversed});
	REQUIRE(selection.IsSuccess());
	const vector<idx_t> reordered {1, 0};
	REQUIRE(selection.GetBinding().argument_to_parameters == reordered);
	REQUIRE(selection.GetBinding().return_type == LogicalType::BIGINT);

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
		REQUIRE(binding.argument_to_parameters == expected_parameters);
		const vector<LogicalType> expected_types {LogicalType::LIST(LogicalType::INTEGER), LogicalType::INTEGER,
		                                          LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::ANY};
		REQUIRE(binding.parameter_types == expected_types);
		REQUIRE(binding.return_type == LogicalType::INTEGER);
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
		auto &parameter_types = selection.GetBinding().parameter_types;
		REQUIRE(parameter_types[0] == LogicalType::LIST(LogicalType::BIGINT));
		REQUIRE(parameter_types[1] == LogicalType::BIGINT);
		REQUIRE(selection.GetBinding().return_type == LogicalType::BIGINT);

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
		REQUIRE(selection.GetBinding().return_type == LogicalType::BIGINT);

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
		REQUIRE(selection.GetBinding().parameter_types[0] == LogicalType::BIGINT);
		REQUIRE(selection.GetBinding().return_type == LogicalType::ARRAY(LogicalType::DOUBLE, 3));
		REQUIRE(selection.GetBinding().variable_values.at("N") == Value::BIGINT(3));

		// any constant expression does, an argument that is not constant does not match
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(4)));
		selection = FunctionBinder::SelectFunction(*con.context, zeros, arguments);
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().return_type == LogicalType::ARRAY(LogicalType::DOUBLE, 4));
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
		REQUIRE(selection.GetBinding().parameter_types[0] == bigint_array);
		REQUIRE(selection.GetBinding().return_type == bigint_array);
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
		REQUIRE(ArrayType::IsAnySize(selection.GetBinding().parameter_types[0]));

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
		REQUIRE(selection.GetBinding().return_type == LogicalType::DECIMAL(12, 4));
		selection = FunctionBinder::SelectFunction(*con.context, rescaled, {LogicalType::INTEGER});
		REQUIRE(selection.IsSuccess());
		REQUIRE(selection.GetBinding().parameter_types[0] == LogicalType::DECIMAL(10, 0));
		REQUIRE(selection.GetBinding().return_type == LogicalType::DECIMAL(10, 0));
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
	REQUIRE(candidates[0].cost == candidates[1].cost);
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
		REQUIRE(selection.GetBinding().parameter_types[0] == geometry_crs);
		REQUIRE(selection.GetBinding().variable_values.at("C") == Value("OGC:CRS84"));

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
	REQUIRE(selection.GetBinding().return_type == LogicalType::ANY);

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
