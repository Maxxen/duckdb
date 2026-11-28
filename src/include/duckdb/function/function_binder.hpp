//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/type_cache.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/query_location.hpp"
#include "duckdb/common/result.hpp"

#include <variant>

namespace duckdb {
class AggregateFunctionCatalogEntry;

class WindowFunctionCatalogEntry;
class BoundFunctionExpression;
struct BoundPragmaInfo;

//! One ORDER BY key of an exported ordered aggregate state: the buffered struct column it sorts on and the modifiers.
struct SortedAggregateStateOrder {
	idx_t column;
	OrderType order_type;
	OrderByNullType null_order;
};

//! A table function bound to the constants it is called with, before its bind callback is invoked
struct ResolvedTableFunction {
	explicit ResolvedTableFunction(BoundTableFunction function_p, vector<Value> parameters_p = {},
	                               named_argument_map_t named_parameters_p = {})
	    : function(std::move(function_p)), parameters(std::move(parameters_p)),
	      named_parameters(std::move(named_parameters_p)) {
	}

	BoundTableFunction function;
	vector<Value> parameters;
	named_argument_map_t named_parameters;
};

//! The types a pack of a signature stands for in a call, in order - each with a label, for a named pack. A pack
//! is a list of its own, whether it is made from the parameters of a type an argument has, as TUPLE(*T) passed a
//! tuple, or from the types of the arguments "*args *T" receives
class TypePack {
public:
	//! The pack of the given types, without labels
	DUCKDB_API static TypePack Positional(vector<LogicalType> types);
	//! The pack of the given types, each with its label
	DUCKDB_API static TypePack Named(child_list_t<LogicalType> labelled_types);
	//! The pack of the parameters of a type, e.g. the types of a tuple, or the labelled types of a struct
	DUCKDB_API static TypePack ParametersOf(const LogicalType &type);

public:
	DUCKDB_API idx_t Count() const;
	DUCKDB_API const LogicalType &GetType(idx_t index) const;
	//! The label of an element, if it has one: the elements of a named pack do
	DUCKDB_API optional_ptr<const Identifier> GetLabel(idx_t index) const;

	//! As the elements are written: (INTEGER, VARCHAR), (a INTEGER, b VARCHAR)
	DUCKDB_API string ToString() const;
	//! Whether both have the same types, with the same labels, in the same order
	DUCKDB_API bool operator==(const TypePack &other) const;
	DUCKDB_API bool operator!=(const TypePack &other) const;

private:
	struct Element {
		LogicalType type;
		optional<Identifier> label;
	};

	explicit TypePack(vector<Element> elements);

private:
	vector<Element> elements;
};

//! What a call determines a variable of a signature to be: the type of a type variable - normalized, of the kind of
//! a constrained variable, and SQLNULL if only NULLs are passed as it - the value of a value variable, a NULL of its
//! values if only NULLs say it, or the elements of a pack
using FunctionVariable = std::variant<LogicalType, Value, TypePack>;

//! What a call determines the variables of a signature to be, by their names. A variable it does not determine is
//! not in it
struct FunctionVariables {
	bool IsDetermined(const Identifier &name) const {
		return determined.count(name) > 0;
	}
	//! What a variable the call determines is - it has to be a variable of that kind
	const LogicalType &GetType(const Identifier &name) const {
		return std::get<LogicalType>(determined.at(name));
	}
	const Value &GetValue(const Identifier &name) const {
		return std::get<Value>(determined.at(name));
	}
	const TypePack &GetPack(const Identifier &name) const {
		return std::get<TypePack>(determined.at(name));
	}

	identifier_map_t<FunctionVariable> determined;
};

//! An argument a function is bound with
struct BoundArgument {
	//! The parameter of the overload that receives it
	idx_t parameter_index;
	//! The argument of the call it is, the positional arguments counted before the named ones - invalid for the
	//! default of a parameter the call leaves out
	optional_idx argument_index;
	//! The type it is passed as: the type of the parameter that receives it, where ANY stands for the type of the
	//! argument itself
	LogicalType type;

	bool IsDefaultArgument() const {
		return !argument_index.IsValid();
	}
};

//! How the arguments of a call are passed to the parameters of an overload
struct ArgumentLayout {
	//! The arguments the function is bound with, ordered by the parameter that receives them: an argument of the
	//! call, or the default of a parameter the call leaves out. "*args" and "**kwargs" receive any number of them
	vector<BoundArgument> arguments;
	//! How many of them are passed by position, and the names of the ones that follow: the keyword-only parameters,
	//! and what "**kwargs" receives
	idx_t positional_count = 0;
	vector<Identifier> named_arguments;

	//! The types the arguments are passed as
	vector<LogicalType> GetArgumentTypes() const {
		vector<LogicalType> result;
		result.reserve(arguments.size());
		for (auto &argument : arguments) {
			result.push_back(argument.type);
		}
		return result;
	}
};

//! How well an overload matches a call. The overload of a function set that a call binds to is selected in four steps.
//! A type variable of a signature is constrained if it only stands for types of one kind (T IS ENUM), else it is
//! unconstrained. ANY is an unconstrained type variable of its own every time it is written.
//!
//! 1. Shape. An overload is a candidate only if the arguments of the call can be laid out over its parameters: a
//!    positional argument is passed to the parameter at its position or to "*args", a named argument to the
//!    parameter of its name or to "**kwargs", and every parameter the call leaves out has a default.
//!
//! 2. Types. The type of every argument is paired with the type of the parameter that receives it, part by part,
//!    and says what the variables are that the parameter has in the place of its parts: INTEGER[3] passed to T[]
//!    says that T is INTEGER, and so does INTEGER[] passed to T[N]. A type of another kind than the parameter's
//!    says nothing: a MAP passed to T[] is not a list of its keys. A type variable is the promotion of the types
//!    the arguments say it is, in the order of the arguments. A NULL and an unresolved parameter take the other
//!    type in the promotion, wherever they are passed: [1, NULL, 2::TINYINT] is a TINYINT[] like [1, 2::TINYINT].
//!    So a type variable is the type of a NULL if only NULLs are passed as it. The promoted type is then made
//!    final: a literal becomes its type, and a constrained variable has to be of its kind, which the type of a NULL
//!    is for every kind. No type is converted to be of the kind: an INTEGER is not a T IS DECIMAL, although it is
//!    passed to DECIMAL(W, S) as DECIMAL(10, 0). A value variable is the one value the arguments say it is, and not
//!    determined if none says one. A pack is the types the one argument that is passed as it has in its place, in
//!    their order, and for a named pack each with its label: a tuple passed to TUPLE(*T) says that T is the types
//!    of the tuple, a struct passed to STRUCT(**KV) that KV is its fields. Only a type of the kind of the name has
//!    them: a struct with names says nothing about TUPLE(*T) and cannot be passed to it, nor a tuple to
//!    STRUCT(**KV), like a list. A struct or a tuple that has no parts says the pack of no elements for either:
//!    it is cast to the other kind, which costs 1, so an overload of its own kind is preferred. A NULL has no
//!    types to say: a pack that only a NULL is passed as is the pack of no elements, so a pack is always
//!    determined, and the NULL is passed as the struct or tuple without parts. A pack that the type of a variadic
//!    parameter expands is the arguments it receives, one element each: for row(*args *T), T is their types, a
//!    literal as its type and a NULL as the type of a NULL, and for struct_pack(**kwargs **KV) each with the name
//!    of its argument as its label. Where a pattern is expanded, every argument is matched against the pattern and
//!    the element is what it says the pack is there: for list_zip(*lists *(T[])), T is the element types of the
//!    lists, and each list is passed as a list of its own element type. An argument that has no parts for the
//!    pattern says no element and cannot be passed. Without an argument it is the pack of no elements. The
//!    function still receives its arguments one by one. In the return type a named pack can be used without its labels:
//!    a function declared (s STRUCT(**KW)) -> TUPLE(*KW) returns the tuple of the types of the fields. An overload is
//!    not a candidate if the arguments disagree about a variable, or promote a constrained variable out of its kind
//!    (two enums are promoted to VARCHAR). A variable that is left to an unresolved parameter - a type variable, ANY, a
//!    value variable or a pack that the parameter is passed as, and that no other argument says - leaves the call
//!    unresolved (step 4). Every argument is then passed as the type of its parameter with the variables as the
//!    arguments determine them, and has to be passable to it. The pairing says whether an argument fits the type
//!    of its parameter: every part of the one is paired with a part of the other - a type variable with a type it
//!    stands for, a value variable with a value, a pack with all the parts, and a value the parameter gives with
//!    that value. INTEGER[3] fits T[] and T[N], INTEGER fits DECIMAL(W, S) as DECIMAL(10, 0), and anything fits an
//!    unconstrained type variable by itself. A VARIANT, a string literal and a MAP do not fit T[], nor INTEGER[]
//!    T[N], nor a tuple of three TUPLE(K, V).
//!    - An argument that fits a type that uses a type variable is cast explicitly to the type it is passed as - no
//!      implicit cast to that type is asked for: BOOLEAN and INTEGER fit (T, T), and both are passed as INTEGER. A
//!      setting could ask for that implicit cast as well.
//!    - An argument that does not fit such a type has to cast implicitly to the type it is passed as. A VARIANT and
//!      a string literal are passed to T[] this way. There is no such way to a type variable by itself: a type that
//!      a constrained variable does not stand for cannot be passed as it.
//!    - An argument has to cast implicitly to a type that uses no type variable (INTEGER, DECIMAL(W, S), FLOAT[N]),
//!      which is that type whatever the other arguments are.
//!    - A NULL and an unresolved parameter are passed as the type of the parameter - a NULL as it is, if that type
//!      cannot be made, e.g. of a value variable that only NULLs say. An argument that fits a type that the
//!      constructor does not make is passed as it is as well: it is that type already.
//!    - A parameter that is declared as ANY takes the type of its argument.
//!    An overload is not a candidate if no argument says what a variable is: an argument that has no parts for the
//!    type of its parameter cannot be passed. The type the overload returns is made of the variables once it is
//!    selected: a call whose return type cannot be made is an error, e.g. FLOAT[N] where only NULLs say N.
//!
//! 3. Rank. Every candidate has a rank of six numbers. Two candidates are compared number by number in the order of
//!    the rules below, and the first difference decides:
//!    1. legacy_casts, fewer is better: the arguments whose cast is only implicit because of the
//!       old_implicit_casting setting.
//!    2. unconstrained_arguments, fewer is better: the arguments, other than a NULL, that are passed to a parameter
//!       whose type uses an unconstrained type variable. A pack counts as one here and in rules 4 and 5.
//!    3. cast_cost, lower is better: the sum of the implicit cast costs of the arguments. An argument costs the
//!       implicit cast to the type it is passed as - and nothing if it is cast explicitly (step 2) and there is no
//!       implicit cast to that type. A NULL costs the cast of a NULL to the type it is passed as.
//!       That is the type of a NULL if a type variable is passed nothing else, constrained or not: nothing is
//!       taken from the constraint. A NULL that is passed as it is costs the cast to the type the parameter names
//!       (FLOAT[N] an array, GEOMETRY(C) a geometry, TUPLE(*T) a tuple).
//!    4. variable_parameters, fewer is better: the arguments that are passed to a parameter whose type uses a type
//!       variable, constrained or not.
//!    5. declared_structure, more is better: over the arguments of rule 4, 2 for every type that the type of the
//!       parameter names and 1 for every constrained type variable in it. T[], TUPLE(*T) and STRUCT(**KV) count 2,
//!       MAP(VARCHAR, T) counts 4, T IS ENUM counts 1, an unconstrained type variable counts 0.
//!    6. declared_values, more is better: over all arguments, 2 for every literal and 1 for every value variable in
//!       the type of the parameter. DECIMAL(18, 3) counts 4, DECIMAL(W, 0) counts 3, FLOAT[N] and GEOMETRY(C) count 1.
//!    Rules 4 to 6 count a parameter once for every argument of the call it receives, and not for a default. When the
//!    arguments are cast explicitly, nothing is ranked.
//!
//! 4. Result. If an overload leaves a variable to an unresolved parameter, the call is unresolved: it is bound
//!    again once the parameter has a type, and no other overload is selected in its place, as that type may make
//!    this overload the better match, or no match. That holds once the shape of the overload fits and the arguments
//!    agree about the variables, whether or not its other arguments can be passed: they are only checked against
//!    the types the parameter determines. Else the candidate that ranks before every other one is selected. Several
//!    candidates of the best rank make the call ambiguous, which is an error - unless a positional argument is an
//!    unresolved parameter: all candidates of such a call rank the same, and the call is unresolved as well. So no
//!    function is bound with an argument of an unknown type, however it is called: an unresolved parameter is
//!    passed as a type the overload declares, or as one the other arguments determine. A function that takes the
//!    type of such a parameter from its other arguments by a rule of its own does so before it is matched - a
//!    comparison compares the parameter as the type of its other side.
struct FunctionRank {
	//! Rule 1: the arguments that are only cast implicitly because of the old_implicit_casting setting
	idx_t legacy_casts = 0;
	//! Rule 2: the arguments, other than a NULL, passed through an unconstrained type variable
	idx_t unconstrained_arguments = 0;
	//! Rule 3: the sum of the implicit cast costs of the arguments
	idx_t cast_cost = 0;
	//! Rule 4: the arguments passed to a parameter whose type uses a type variable
	idx_t variable_parameters = 0;
	//! Rule 5: the types and kinds that the types of those parameters declare
	idx_t declared_structure = 0;
	//! Rule 6: the literals and value variables that the types of the parameters declare
	idx_t declared_values = 0;

	//! Whether this rank comes before the other one: the first rule they differ in decides
	bool operator<(const FunctionRank &other) const {
		if (legacy_casts != other.legacy_casts) {
			return legacy_casts < other.legacy_casts;
		}
		if (unconstrained_arguments != other.unconstrained_arguments) {
			return unconstrained_arguments < other.unconstrained_arguments;
		}
		if (cast_cost != other.cast_cost) {
			return cast_cost < other.cast_cost;
		}
		if (variable_parameters != other.variable_parameters) {
			return variable_parameters < other.variable_parameters;
		}
		if (declared_structure != other.declared_structure) {
			return declared_structure > other.declared_structure;
		}
		return declared_values > other.declared_values;
	}
	bool operator==(const FunctionRank &other) const {
		return legacy_casts == other.legacy_casts && unconstrained_arguments == other.unconstrained_arguments &&
		       cast_cost == other.cast_cost && variable_parameters == other.variable_parameters &&
		       declared_structure == other.declared_structure && declared_values == other.declared_values;
	}
	bool operator!=(const FunctionRank &other) const {
		return !(*this == other);
	}
};

//! An overload that matches a call, and how the arguments of the call are passed to it
struct FunctionBinding {
	//! The index of the overload in the function set
	idx_t GetOverloadIndex() const {
		return overload;
	}
	//! The overload the call binds to
	const SimpleFunction &GetFunction() const {
		return *function;
	}
	//! Whether the call binds better to this overload - an order of how well the overloads match, not of which
	//! overload they are
	bool operator<(const FunctionBinding &other) const {
		return rank < other.rank;
	}
	template <class T>
	shared_ptr<const T> GetFunction() const {
		D_ASSERT(function->GetKind() == T::KIND);
		return shared_ptr_cast<const SimpleFunction, const T>(function);
	}

	//! How well the call matches the overload
	FunctionRank rank;
	//! The arguments the function is bound with
	ArgumentLayout layout;
	//! What the call determines the variables of the overload to be: the type of a type variable, the value of a
	//! value variable, the elements of a pack. The return type follows from them
	FunctionVariables variables;

private:
	friend class FunctionBinder;
	idx_t overload = 0;
	shared_ptr<const SimpleFunction> function;
};

//! One argument of a call - a positional one if it has no name
struct FunctionCallArgument {
	FunctionCallArgument(LogicalType type, Identifier name = Identifier(), // NOLINT: allow implicit conversion
	                     QueryLocation location = QueryLocation())
	    : type(std::move(type)), name(std::move(name)), location(location) {
	}
	//! For the type constants, e.g. LogicalType::INTEGER
	FunctionCallArgument(LogicalTypeId id) // NOLINT: allow implicit conversion
	    : FunctionCallArgument(LogicalType(id)) {
	}

	//! An argument whose value is known, e.g. to determine a value variable of a signature
	static FunctionCallArgument Constant(Value value, Identifier name = Identifier()) {
		FunctionCallArgument result(value.type(), std::move(name));
		result.constant = std::move(value);
		return result;
	}

	LogicalType type;
	Identifier name;
	QueryLocation location;
	//! The value of the argument, if the caller knows it
	optional<Value> constant;
	//! The expression of the argument, if it has one: its value is used if it is constant
	optional_ptr<const Expression> expression;
};

//! The arguments of a call to select an overload for - the positional arguments, followed by the named ones
class FunctionCallArguments {
public:
	FunctionCallArguments() = default;
	//! E.g. {LogicalType::INTEGER, {LogicalType::BIGINT, "opt"}} - the positional arguments come first
	FunctionCallArguments(std::initializer_list<FunctionCallArgument> arguments); // NOLINT: allow implicit conversion
	FunctionCallArguments(vector<LogicalType> positional,                         // NOLINT: allow implicit conversion
	                      vector<pair<Identifier, LogicalType>> named = {});
	//! The types and locations of bound arguments
	FunctionCallArguments(const vector<unique_ptr<Expression>> &positional, // NOLINT: allow implicit conversion
	                      const vector<pair<Identifier, unique_ptr<Expression>>> &named = {});

public:
	idx_t GetArgumentCount() const {
		return arguments.size();
	}
	idx_t GetPositionalCount() const {
		return positional_count;
	}
	//! An argument - the positional arguments, followed by the named ones
	const FunctionCallArgument &GetArgument(idx_t argument) const {
		return arguments[argument];
	}
	//! The types of the positional arguments
	vector<LogicalType> GetPositionalTypes() const;
	//! The names and types of the named arguments
	vector<pair<Identifier, LogicalType>> GetNamedTypes() const;

private:
	void AddArgument(FunctionCallArgument argument);

private:
	vector<FunctionCallArgument> arguments;
	idx_t positional_count = 0;
};

class FunctionBindError;

//! Why an overload does not match a call. Arguments are numbered as the positional arguments, followed by the named
//! ones - what the call itself says about an argument, its type and name, is not repeated here
class FunctionMismatch {
public:
	enum class Type : uint8_t {
		MISSING_PARAMETER,
		TOO_MANY_ARGUMENTS,
		UNKNOWN_NAMED_ARGUMENT,
		ALREADY_PASSED_BY_POSITION,
		INCOMPATIBLE_ARGUMENT,
		TEMPLATE_DEDUCTION,
		TEMPLATE_UNRESOLVED,
		CONSTANT_REQUIRED
	};

	//! A parameter without a default is not filled by the call
	struct MissingParameter {
		static constexpr auto TYPE = Type::MISSING_PARAMETER;
		idx_t parameter;
		//! The name of the parameter - empty if it has none
		Identifier name;

		string ToString(const SimpleFunction &function, const FunctionBindError &error) const;
	};
	//! The call passes more arguments than the overload takes, starting at this one
	struct TooManyArguments {
		static constexpr auto TYPE = Type::TOO_MANY_ARGUMENTS;
		idx_t argument;
		//! The number of arguments the overload accepts at most
		idx_t accepted;
		//! Whether that is the number of arguments it accepts by position
		bool positional;

		string ToString(const SimpleFunction &function, const FunctionBindError &error) const;
	};
	//! A named argument names no parameter, and the overload has no "**kwargs" to receive it
	struct UnknownNamedArgument {
		static constexpr auto TYPE = Type::UNKNOWN_NAMED_ARGUMENT;
		idx_t argument;

		string ToString(const SimpleFunction &function, const FunctionBindError &error) const;
	};
	//! A named argument names a parameter that a positional argument already fills
	struct AlreadyPassedByPosition {
		static constexpr auto TYPE = Type::ALREADY_PASSED_BY_POSITION;
		idx_t argument;

		string ToString(const SimpleFunction &function, const FunctionBindError &error) const;
	};
	//! An argument cannot be implicitly cast to the type of the parameter that receives it
	struct IncompatibleArgument {
		static constexpr auto TYPE = Type::INCOMPATIBLE_ARGUMENT;
		idx_t argument;
		//! The parameter that receives the argument
		idx_t parameter;
		//! The type the argument is matched against
		LogicalType parameter_type;
		//! Whether the parameter reads as it is declared: the type of an unconstrained type variable stands for any
		bool reads_as_declared;

		string ToString(const SimpleFunction &function, const FunctionBindError &error) const;
	};
	//! The arguments determine conflicting types for a template of the overload
	struct TemplateDeduction {
		static constexpr auto TYPE = Type::TEMPLATE_DEDUCTION;
		//! The argument that conflicts - invalid if it is the default of a parameter
		optional_idx argument;
		string template_name;
		//! The types the template was inferred to be, in the order they were inferred, followed by the type that
		//! conflicts with them
		vector<LogicalType> inferred_types;
		//! For a value variable: the value it was determined to be, and the value that conflicts with it
		vector<Value> inferred_values;

		string ToString(const SimpleFunction &function, const FunctionBindError &error) const;
	};
	//! Neither the arguments nor the defaults determine a template of the types the call uses
	struct TemplateUnresolved {
		static constexpr auto TYPE = Type::TEMPLATE_UNRESOLVED;
		string template_name;
		//! Whether the variable stands for a value rather than a type
		bool is_value = false;

		string ToString(const SimpleFunction &function, const FunctionBindError &error) const;
	};
	//! An argument that determines a value variable of the overload is not a constant
	struct ConstantRequired {
		static constexpr auto TYPE = Type::CONSTANT_REQUIRED;
		idx_t argument;
		string variable_name;

		string ToString(const SimpleFunction &function, const FunctionBindError &error) const;
	};

	//! The alternatives are in the order of Type
	using Reason = std::variant<MissingParameter, TooManyArguments, UnknownNamedArgument, AlreadyPassedByPosition,
	                            IncompatibleArgument, TemplateDeduction, TemplateUnresolved, ConstantRequired>;

	FunctionMismatch(idx_t overload, shared_ptr<const SimpleFunction> function, idx_t distance, Reason reason)
	    : overload(overload), function(std::move(function)), distance(distance), reason(std::move(reason)) {
	}

public:
	//! The index of the overload in the function set
	idx_t GetOverloadIndex() const {
		return overload;
	}
	//! The overload the call does not match
	const SimpleFunction &GetFunction() const {
		return *function;
	}
	//! How far the call is from matching the overload, lower is closer. Any number of arguments that cannot be cast
	//! is closer than a call of the wrong shape - missing parameters, extra arguments or unknown names
	idx_t GetDistance() const {
		return distance;
	}
	//! Whether the call is closer to matching this overload - an order of how close the overloads are, not of which
	//! overload they are
	bool operator<(const FunctionMismatch &other) const {
		return distance < other.distance;
	}
	Type GetType() const {
		return static_cast<Type>(reason.index());
	}
	template <class T>
	const T &Cast() const {
		D_ASSERT(GetType() == T::TYPE);
		return std::get<T>(reason);
	}
	bool IsTemplateMismatch() const {
		return GetType() == Type::TEMPLATE_DEDUCTION || GetType() == Type::TEMPLATE_UNRESOLVED;
	}
	//! Why the call of the error does not match, e.g. "argument 1 of type DATE cannot be implicitly cast to VARCHAR"
	string ToString(const FunctionBindError &error) const;

private:
	idx_t overload;
	shared_ptr<const SimpleFunction> function;
	idx_t distance;
	Reason reason;
};

//! Why no overload of a function set could be selected for a call
class FunctionBindError {
public:
	enum class Type : uint8_t { NO_MATCH, AMBIGUOUS_MATCH, DUPLICATE_ARGUMENT };

	//! No overload matches the call
	struct NoMatch {
		static constexpr auto TYPE = Type::NO_MATCH;
		//! Why every overload does not match, closest first
		vector<FunctionMismatch> mismatches;

		//! The closest mismatch, if no other is as close
		optional_ptr<const FunctionMismatch> GetClosest() const;
		ErrorData ToError(const FunctionBindError &error) const;
	};
	//! Several overloads match at the best rank
	struct AmbiguousMatch {
		static constexpr auto TYPE = Type::AMBIGUOUS_MATCH;
		vector<FunctionBinding> candidates;

		ErrorData ToError(const FunctionBindError &error) const;
	};
	//! A named argument repeats the name of an earlier one, so no overload can match
	struct DuplicateArgument {
		static constexpr auto TYPE = Type::DUPLICATE_ARGUMENT;
		idx_t argument;
		//! The kind of the functions of the set - the call matches none of them, so none of them is at hand
		FunctionKind function_kind;

		ErrorData ToError(const FunctionBindError &error) const;
	};

	//! The alternatives are in the order of Type
	using Kind = std::variant<NoMatch, AmbiguousMatch, DuplicateArgument>;

	//! No overload of the set matches the call - the mismatches are sorted, closest first
	static FunctionBindError CreateNoMatch(Identifier name, FunctionCallArguments call,
	                                       vector<FunctionMismatch> mismatches) {
		return FunctionBindError(std::move(name), std::move(call), NoMatch {std::move(mismatches)});
	}
	//! Several overloads match the call at the best rank
	static FunctionBindError CreateAmbiguousMatch(Identifier name, FunctionCallArguments call,
	                                              vector<FunctionBinding> candidates) {
		return FunctionBindError(std::move(name), std::move(call), AmbiguousMatch {std::move(candidates)});
	}
	//! A named argument of the call repeats the name of an earlier one
	static FunctionBindError CreateDuplicateArgument(Identifier name, FunctionCallArguments call, idx_t argument,
	                                                 FunctionKind function_kind) {
		return FunctionBindError(std::move(name), std::move(call), DuplicateArgument {argument, function_kind});
	}

public:
	//! The name of the function set the selection was made from
	const Identifier &GetName() const {
		return name;
	}
	//! The call the selection was made for
	const FunctionCallArguments &GetCall() const {
		return call;
	}
	Type GetType() const {
		return static_cast<Type>(kind.index());
	}
	template <class T>
	const T &Cast() const {
		D_ASSERT(GetType() == T::TYPE);
		return std::get<T>(kind);
	}
	//! The error to report for the call, pointing at the argument it is about if its location is known
	operator ErrorData() const; // NOLINT: allow implicit conversion
	[[noreturn]] void Throw() const;
	//! The messages of the error, as ErrorData::Message() and ErrorData::RawMessage()
	string Message() const;
	string RawMessage() const;

private:
	FunctionBindError(Identifier name, FunctionCallArguments call, Kind kind)
	    : name(std::move(name)), call(std::move(call)), kind(std::move(kind)) {
	}

private:
	Identifier name;
	FunctionCallArguments call;
	Kind kind;
};

//! The outcome of selecting the overload of a function set that a call binds to
class FunctionBindResult {
public:
	FunctionBindResult(FunctionBinding binding) : result(std::move(binding)) { // NOLINT: allow implicit conversion
	}
	FunctionBindResult(FunctionBindError error) : result(std::move(error)) { // NOLINT: allow implicit conversion
	}

public:
	bool IsSuccess() const {
		return std::holds_alternative<FunctionBinding>(result);
	}
	explicit operator bool() const {
		return IsSuccess();
	}
	//! The overload the call binds to - only if the selection succeeded
	const FunctionBinding &GetBinding() const {
		D_ASSERT(IsSuccess());
		return std::get<FunctionBinding>(result);
	}
	//! Why the selection failed - only if it did not succeed
	const FunctionBindError &GetError() const {
		D_ASSERT(!IsSuccess());
		return std::get<FunctionBindError>(result);
	}

private:
	std::variant<FunctionBinding, FunctionBindError> result;
};

//! The FunctionBinder class is responsible for binding functions
class FunctionBinder {
	//! These bind a function that no call is matched to, from the types it declares
	friend class FunctionSerializer;

public:
	DUCKDB_API explicit FunctionBinder(Binder &binder);
	DUCKDB_API explicit FunctionBinder(ClientContext &context);
	//! Binds outside of a query within a fixed decimal width, if one is given: a deserialized function is rebound
	//! within the width it was bound in
	DUCKDB_API FunctionBinder(ClientContext &context, optional_ptr<const DecimalWidthLimit> decimal_width_limit);

	optional_ptr<Binder> binder;
	ClientContext &context;
	//! The fixed limit of the DECIMAL widths a bind outside of a query constructs, if any
	optional_ptr<const DecimalWidthLimit> decimal_width_limit;
	//! Set when the arguments were bound before, so their collations are not pushed again
	bool arguments_collated = false;
	//! The types a binder outside of a query constructed
	TypeCache type_cache;

public:
	//! The types this binder constructed: those of the query if it binds within one, else its own
	TypeCache &GetTypeCache();

public:
	//! Overload selection: the overload of a function set that a call with the given arguments binds to, by the steps
	//! and the ranking that FunctionRank describes.
	//! Selection needs no binder - without a ClientContext only the built-in implicit casts are considered. With
	//! explicit casts, an argument that cannot be implicitly cast to its parameter is accepted too. Template types
	//! unify decimals within the limit, if one is given, else within the max_decimal_width setting
	DUCKDB_API static FunctionBindResult
	SelectFunction(optional_ptr<ClientContext> context, const BaseFunctionSet &functions,
	               const FunctionCallArguments &call, bool explicit_casts = false,
	               optional_ptr<const DecimalWidthLimit> decimal_width_limit = nullptr);
	//! The fixed limit of the DECIMAL widths the binder constructs, if any: set while the keys of an index are bound,
	//! and while a function is deserialized
	optional_ptr<const DecimalWidthLimit> FixedDecimalWidthLimit() const;
	//! The type the overload of a binding returns for the call. INVALID if its signature marks the return type as
	//! ANY: its resolver or its bind callback determines it
	DUCKDB_API static LogicalType BindReturnType(TypeCache &cache, const FunctionBinding &binding);

	//! The type a type name stands for: a type of the catalog, or LAMBDA / TABLE. Without a ClientContext only the
	//! built-in types are found
	DUCKDB_API static LogicalType BindTypeName(optional_ptr<ClientContext> context, const TypeName &name);

	//! How a call binds to a function it is known to call - the arguments are cast explicitly. Throws if the call does
	//! not fit the signature of the function
	DUCKDB_API static FunctionBinding
	MatchFunction(optional_ptr<ClientContext> context, shared_ptr<const SimpleFunction> function,
	              const FunctionCallArguments &call,
	              optional_ptr<const DecimalWidthLimit> decimal_width_limit = nullptr);

	//! Bind a function the caller knows it calls to the argument types of the call. Only the types are resolved: no
	//! argument is cast and no callback of the function runs, the caller supplies what they would. A type only
	//! unresolved parameters determine stays unknown. The function is copied into a definition of its own
	DUCKDB_API static BoundScalarFunction BindScalarFunction(optional_ptr<ClientContext> context,
	                                                         const ScalarFunction &function,
	                                                         const FunctionCallArguments &call);
	DUCKDB_API static BoundAggregateFunction BindAggregateFunction(optional_ptr<ClientContext> context,
	                                                               const AggregateFunction &function,
	                                                               const FunctionCallArguments &call);
	DUCKDB_API static BoundWindowFunction BindWindowFunction(optional_ptr<ClientContext> context,
	                                                         const WindowFunction &function,
	                                                         const FunctionCallArguments &call);
	//! Bind an aggregate to types the caller resolved itself: nothing the function declares is bound
	DUCKDB_API static BoundAggregateFunction
	BindAggregateFunction(const AggregateFunction &function, vector<LogicalType> arguments, LogicalType return_type);
	//! The same, as the expression of the call with the bind data the caller supplies
	DUCKDB_API static unique_ptr<BoundFunctionExpression> BindScalarFunction(optional_ptr<ClientContext> context,
	                                                                         const ScalarFunction &function,
	                                                                         vector<unique_ptr<Expression>> children,
	                                                                         unique_ptr<FunctionData> bind_data,
	                                                                         bool is_operator = false);
	DUCKDB_API static BoundTableFunction BindTableFunction(optional_ptr<ClientContext> context,
	                                                       const TableFunction &function,
	                                                       const FunctionCallArguments &call);

	//! Bind a table function: select the overload, check the named arguments against its signature, and fold the
	//! arguments to the constants it is called with, cast to the types it declares. Returns nullptr and sets the error
	//! if no overload matches
	DUCKDB_API unique_ptr<ResolvedTableFunction>
	BindTableFunction(const TableFunctionSet &functions, vector<unique_ptr<Expression>> &positional_arguments,
	                  vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error);
	//! Select a table in-out function using input column types and bind its named scalar options.
	DUCKDB_API unique_ptr<ResolvedTableFunction>
	BindTableInOutFunction(const TableFunctionSet &functions, const vector<LogicalType> &input_types,
	                       vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error);
	//! Bind a pragma function, as BindTableFunction binds a table function
	DUCKDB_API unique_ptr<BoundPragmaInfo>
	BindPragmaFunction(const PragmaFunctionSet &functions, vector<unique_ptr<Expression>> &positional_arguments,
	                   vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error);

	//! Casts a constant to the type of the parameter it fills, as an argument of any function is cast - a parameter
	//! whose type is not concrete, such as ANY, takes it as it is
	DUCKDB_API static Value CastToParameterType(ClientContext &context, Value value, const LogicalType &parameter_type);

	DUCKDB_API unique_ptr<Expression>
	BindScalarFunction(const Identifier &schema, const Identifier &name, vector<unique_ptr<Expression>> children,
	                   ErrorData &error, bool is_operator = false, optional_ptr<Binder> binder = nullptr,
	                   Identifier alias = Identifier(), QueryLocation query_location = QueryLocation());

	//! Throws the binding error instead of returning nullptr
	DUCKDB_API unique_ptr<Expression>
	BindScalarFunction(const Identifier &schema, const Identifier &name, vector<unique_ptr<Expression>> children,
	                   bool is_operator = false, optional_ptr<Binder> binder = nullptr, Identifier alias = Identifier(),
	                   QueryLocation query_location = QueryLocation());

	DUCKDB_API unique_ptr<Expression>
	BindScalarFunction(const ScalarFunctionCatalogEntry &function, vector<unique_ptr<Expression>> children,
	                   ErrorData &error, bool is_operator = false, optional_ptr<Binder> binder = nullptr,
	                   Identifier alias = Identifier(), QueryLocation query_location = QueryLocation());

	DUCKDB_API unique_ptr<Expression>
	BindScalarFunction(const shared_ptr<const ScalarFunction> &function, vector<unique_ptr<Expression>> children,
	                   bool is_operator = false, optional_ptr<Binder> binder = nullptr, Identifier alias = Identifier(),
	                   QueryLocation query_location = QueryLocation());

	//! Bind a function that does not come from a function set - the function is copied into a definition of its own
	DUCKDB_API unique_ptr<Expression>
	BindScalarFunction(const ScalarFunction &function, vector<unique_ptr<Expression>> children,
	                   bool is_operator = false, optional_ptr<Binder> binder = nullptr, Identifier alias = Identifier(),
	                   QueryLocation query_location = QueryLocation());

	//! Bind a scalar function from a catalog entry given the full list of (maybe-named) bound arguments. The
	//! positional/named split is resolved per candidate overload (overloads flagged to capture argument aliases
	//! treat every argument as positional and keep its alias).
	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const ScalarFunctionCatalogEntry &function,
	                                                     vector<pair<Identifier, unique_ptr<Expression>>> arguments,
	                                                     ErrorData &error, bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr,
	                                                     Identifier alias = Identifier(),
	                                                     QueryLocation query_location = QueryLocation());

	DUCKDB_API unique_ptr<Expression>
	BindScalarFunction(const shared_ptr<const ScalarFunction> &function, vector<unique_ptr<Expression>> children,
	                   vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, bool is_operator = false,
	                   optional_ptr<Binder> binder = nullptr, Identifier alias = Identifier(),
	                   QueryLocation query_location = QueryLocation());

	DUCKDB_API unique_ptr<Expression>
	BindScalarFunction(const ScalarFunction &function, vector<unique_ptr<Expression>> children,
	                   vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, bool is_operator = false,
	                   optional_ptr<Binder> binder = nullptr, Identifier alias = Identifier(),
	                   QueryLocation query_location = QueryLocation());

	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(const shared_ptr<const AggregateFunction> &function, vector<unique_ptr<Expression>> children,
	                      unique_ptr<Expression> filter = nullptr,
	                      AggregateType aggr_type = AggregateType::NON_DISTINCT);

	//! Bind a function that does not come from a function set - the function is copied into a definition of its own
	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(const AggregateFunction &function, vector<unique_ptr<Expression>> children,
	                      unique_ptr<Expression> filter = nullptr,
	                      AggregateType aggr_type = AggregateType::NON_DISTINCT);

	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(const shared_ptr<const AggregateFunction> &function, vector<unique_ptr<Expression>> children,
	                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, unique_ptr<Expression> filter,
	                      AggregateType aggr_type);

	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(const AggregateFunction &function, vector<unique_ptr<Expression>> children,
	                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, unique_ptr<Expression> filter,
	                      AggregateType aggr_type);

	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(const AggregateFunctionCatalogEntry &function,
	                      vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
	                      unique_ptr<Expression> filter = nullptr,
	                      AggregateType aggr_type = AggregateType::NON_DISTINCT);
	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(const AggregateFunctionCatalogEntry &function, vector<unique_ptr<Expression>> children,
	                      ErrorData &error, unique_ptr<Expression> filter = nullptr,
	                      AggregateType aggr_type = AggregateType::NON_DISTINCT);

	DUCKDB_API static void BindSortedAggregate(ClientContext &context, BoundAggregateExpression &expr,
	                                           const vector<unique_ptr<Expression>> &groups,
	                                           optional_ptr<vector<GroupingSet>> grouping_sets);
	DUCKDB_API static void BindSortedAggregate(ClientContext &context, BoundWindowExpression &expr);

	//! Computes the exported buffer layout of an ordered aggregate: the struct of buffered columns (arguments first,
	//! then any appended sort keys), the per-key column + modifiers, and the number of leading argument columns.
	//! Mirrors the matching done by the sorted aggregate bind data so the export type matches the runtime buffer.
	DUCKDB_API static void GetSortedAggregateStateLayout(const BoundAggregateExpression &expr,
	                                                     LogicalType &buffer_struct,
	                                                     vector<SortedAggregateStateOrder> &orders,
	                                                     idx_t &argument_count);
	//! Reconstructs a sorted aggregate wrapper from an exported buffer state so finalize/combine operate on the buffer:
	//! finalize sorts by the keys and runs the (already re-bound) inner aggregate, combine concatenates buffers.
	//! Returns the wrapper function and its bind data.
	DUCKDB_API static pair<AggregateFunction, unique_ptr<FunctionData>>
	BindSortedAggregateState(ClientContext &context, const BoundAggregateFunction &inner_function,
	                         unique_ptr<FunctionData> inner_bind_info, const LogicalType &buffer_struct,
	                         const vector<SortedAggregateStateOrder> &orders, idx_t argument_count);

	DUCKDB_API unique_ptr<BoundWindowExpression>
	BindWindowFunction(const shared_ptr<const WindowFunction> &function, vector<unique_ptr<Expression>> children,
	                   vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, vector<LogicalType> &order_types,
	                   vector<LogicalType> &arg_order_types);

	DUCKDB_API unique_ptr<BoundWindowExpression>
	BindWindowFunction(const WindowFunctionCatalogEntry &function,
	                   vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
	                   vector<LogicalType> &order_types, vector<LogicalType> &arg_order_types);

	DUCKDB_API unique_ptr<Expression> BindScalarWindowFunction(BoundWindowExpression &wexpr,
	                                                           const vector<LogicalType> &input_types);

	//! Turn the overload a call binds to into a bound function. The arguments are passed as they were to the
	//! selection, positional ones first. The overload is kept as the definition of the bound function
	DUCKDB_API pair<BoundScalarFunction, unique_ptr<FunctionData>>
	ResolveScalarFunction(const FunctionBinding &binding, vector<unique_ptr<Expression>> &children,
	                      vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args);
	DUCKDB_API pair<BoundAggregateFunction, unique_ptr<FunctionData>>
	ResolveAggregateFunction(const FunctionBinding &binding, vector<unique_ptr<Expression>> &children,
	                         vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args);
	//! Also resolves the types of the ORDER BY arguments, if they are passed
	DUCKDB_API pair<BoundWindowFunction, unique_ptr<FunctionData>>
	ResolveWindowFunction(const FunctionBinding &binding, vector<unique_ptr<Expression>> &children,
	                      vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
	                      optional_ptr<vector<LogicalType>> order_types = nullptr,
	                      optional_ptr<vector<LogicalType>> arg_order_types = nullptr);

private:
	//! The same, the bind callback is only invoked if asked for: without it only the types are resolved
	pair<BoundScalarFunction, unique_ptr<FunctionData>>
	ResolveScalarFunction(const FunctionBinding &binding, vector<unique_ptr<Expression>> &children,
	                      vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args, bool invoke_bind);

	unique_ptr<Expression> BindScalarFunction(const FunctionBinding &binding, vector<unique_ptr<Expression>> children,
	                                          vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
	                                          bool is_operator, optional_ptr<Binder> binder, Identifier alias,
	                                          QueryLocation query_location);
	unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(FunctionBinding binding, vector<unique_ptr<Expression>> children,
	                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, unique_ptr<Expression> filter,
	                      AggregateType aggr_type);
	unique_ptr<BoundWindowExpression> BindWindowFunction(const FunctionBinding &binding,
	                                                     vector<unique_ptr<Expression>> children,
	                                                     vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
	                                                     vector<LogicalType> &order_types,
	                                                     vector<LogicalType> &arg_order_types);
	//! Cast the arguments of a call to the types of the parameters they fill
	void CastToFunctionArguments(const Identifier &function_name, const vector<LogicalType> &argument_types,
	                             vector<unique_ptr<Expression>> &children);

	//! Select the best matching overload for the given full (maybe-named) argument list, and split it into the
	//! positional and named arguments. Names positional arguments by their alias if that is the only way the call
	//! matches an overload that captures argument aliases
	optional<FunctionBinding> SelectFunctionFromArguments(const BaseFunctionSet &functions,
	                                                      vector<pair<Identifier, unique_ptr<Expression>>> arguments,
	                                                      vector<unique_ptr<Expression>> &positional,
	                                                      vector<pair<Identifier, unique_ptr<Expression>>> &named,
	                                                      ErrorData &error);

	//! Whether an overload matches a call - with how the call binds to it if it does, and why not if it does not. With
	//! explicit casts, an argument that cannot be implicitly cast to its parameter is accepted too
	static Result<FunctionBinding, FunctionMismatch>
	MatchOverload(TypeCache &cache, idx_t overload, shared_ptr<const SimpleFunction> function,
	              const FunctionCallArguments &call, bool explicit_casts,
	              optional_ptr<const DecimalWidthLimit> decimal_width_limit = nullptr);
	//! SelectFunction, with the types constructed before
	static FunctionBindResult SelectFunction(TypeCache &cache, const BaseFunctionSet &functions,
	                                         const FunctionCallArguments &call, bool explicit_casts,
	                                         optional_ptr<const DecimalWidthLimit> decimal_width_limit);
	//! The function of a binding, bound to the types the binding resolved. A return type the signature marks as ANY
	//! is left unset: the resolver or the bind callback of the function sets it
	template <class BOUND>
	static BOUND CreateBoundFunction(TypeCache &cache, const FunctionBinding &binding);
};

} // namespace duckdb
