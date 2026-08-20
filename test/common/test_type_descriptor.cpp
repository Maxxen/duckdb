#include "catch.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/type_descriptor.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/parser/column_definition.hpp"

using namespace duckdb;

namespace {

//! The property that makes an incremental migration safe: describing a bound type and binding the
//! description back must yield exactly the type you started with, without a catalog.
void RequireRoundTrip(const LogicalType &type) {
	auto descriptor = TypeDescriptor::FromLogicalType(type);
	INFO("type: " << type.ToString() << " -> descriptor: " << descriptor.ToString());
	auto bound = descriptor.DefaultBind();
	REQUIRE(bound == type);
	REQUIRE(bound.id() == type.id());
	// describing the round-tripped type produces an identical descriptor
	REQUIRE(TypeDescriptor::FromLogicalType(bound) == descriptor);
}

LogicalType MakeEnum(const duckdb::vector<string> &values) {
	Vector enum_vector(LogicalType::VARCHAR, values.size());
	auto writer = FlatVector::Writer<string_t>(enum_vector, values.size());
	for (auto &value : values) {
		writer.WriteValue(string_t(value));
	}
	return LogicalType::ENUM(enum_vector, values.size());
}

} // namespace

TEST_CASE("TypeDescriptor round-trips every primitive built-in type", "[type_descriptor]") {
	const LogicalType primitives[] = {
	    LogicalType::SQLNULL,      LogicalType::BOOLEAN,      LogicalType::TINYINT,     LogicalType::SMALLINT,
	    LogicalType::INTEGER,      LogicalType::BIGINT,       LogicalType::HUGEINT,     LogicalType::UTINYINT,
	    LogicalType::USMALLINT,    LogicalType::UINTEGER,     LogicalType::UBIGINT,     LogicalType::UHUGEINT,
	    LogicalType::FLOAT,        LogicalType::DOUBLE,       LogicalType::DATE,        LogicalType::TIME,
	    LogicalType::TIME_TZ,      LogicalType::TIMESTAMP,    LogicalType::TIMESTAMP_S, LogicalType::TIMESTAMP_MS,
	    LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL,    LogicalType::VARCHAR,
	    LogicalType::BLOB,         LogicalType::BIT,          LogicalType::UUID,        LogicalType::BIGNUM,
	};
	for (auto &type : primitives) {
		RequireRoundTrip(type);
	}
}

TEST_CASE("TypeDescriptor round-trips parameterized types", "[type_descriptor]") {
	SECTION("decimal") {
		RequireRoundTrip(LogicalType::DECIMAL(18, 3));
		RequireRoundTrip(LogicalType::DECIMAL(4, 0));
		RequireRoundTrip(LogicalType::DECIMAL(38, 38));
	}
	SECTION("list and array") {
		RequireRoundTrip(LogicalType::LIST(LogicalType::INTEGER));
		RequireRoundTrip(LogicalType::LIST(LogicalType::LIST(LogicalType::VARCHAR)));
		RequireRoundTrip(LogicalType::ARRAY(LogicalType::DOUBLE, 3));
	}
	SECTION("struct and tuple") {
		child_list_t<LogicalType> children;
		children.emplace_back(Identifier("a"), LogicalType::INTEGER);
		children.emplace_back(Identifier("b"), LogicalType::LIST(LogicalType::VARCHAR));
		RequireRoundTrip(LogicalType::STRUCT(children));

		duckdb::vector<LogicalType> unnamed {LogicalType::INTEGER, LogicalType::VARCHAR};
		RequireRoundTrip(LogicalType::TUPLE(unnamed));
	}
	SECTION("map") {
		RequireRoundTrip(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::INTEGER));
	}
	SECTION("union") {
		child_list_t<LogicalType> members;
		members.emplace_back(Identifier("num"), LogicalType::INTEGER);
		members.emplace_back(Identifier("str"), LogicalType::VARCHAR);
		RequireRoundTrip(LogicalType::UNION(members));
	}
	SECTION("enum") {
		RequireRoundTrip(MakeEnum({"a", "b", "c"}));
	}
	SECTION("deeply nested") {
		child_list_t<LogicalType> children;
		children.emplace_back(Identifier("id"), LogicalType::INTEGER);
		children.emplace_back(Identifier("tags"), LogicalType::LIST(LogicalType::VARCHAR));
		children.emplace_back(Identifier("scores"),
		                      LogicalType::MAP(LogicalType::VARCHAR, LogicalType::DECIMAL(10, 2)));
		RequireRoundTrip(LogicalType::LIST(LogicalType::STRUCT(children)));
	}
}

TEST_CASE("TypeDescriptor describes a type by its parts", "[type_descriptor]") {
	SECTION("a nominal type is named by its alias") {
		auto aliased = LogicalType(LogicalType::VARCHAR).WithAlias("json");
		auto described = TypeDescriptor::FromLogicalType(aliased);
		REQUIRE(described.Name().Name() == Identifier("json"));
	}
	SECTION("a caller defining a type strips the alias, so the definition does not name the entry") {
		auto aliased = LogicalType(LogicalType::VARCHAR).WithAlias("json");
		auto described = TypeDescriptor::FromLogicalType(aliased.WithAlias(""));
		REQUIRE(described.Name().Name() == Identifier("VARCHAR"));
		REQUIRE(described.DefaultBind() == LogicalType::VARCHAR);
	}
	SECTION("a collation is a modifier, and rebuilds without a context") {
		auto collated = LogicalType::VARCHAR_COLLATION("nocase");
		auto described = TypeDescriptor::FromLogicalType(collated);
		REQUIRE(described.DefaultBind() == collated);
	}
}

TEST_CASE("TypeDescriptor rejects what is not a type", "[type_descriptor]") {
	SECTION("a non-builtin name does not silently bind") {
		TypeDescriptor unknown(QualifiedName(Identifier("no_such_type")));
		REQUIRE_THROWS_AS(unknown.DefaultBind(), BinderException);
	}
	SECTION("a catalog-qualified name is not a builtin") {
		TypeDescriptor qualified(QualifiedName(Identifier("mydb"), Identifier("main"), Identifier("integer")));
		REQUIRE_THROWS_AS(qualified.DefaultBind(), BinderException);
	}
}

TEST_CASE("TypeDescriptor compares structurally", "[type_descriptor]") {
	auto a = TypeDescriptor::FromLogicalType(LogicalType::LIST(LogicalType::INTEGER));
	auto b = TypeDescriptor::FromLogicalType(LogicalType::LIST(LogicalType::INTEGER));
	auto c = TypeDescriptor::FromLogicalType(LogicalType::LIST(LogicalType::BIGINT));
	REQUIRE(a == b);
	REQUIRE(a != c);

	// copying is a deep copy - the nested descriptor is owned, not shared
	auto copy = a;
	REQUIRE(copy == a);
	REQUIRE(copy.Parameters()[0].GetType() == a.Parameters()[0].GetType());
}

TEST_CASE("TypeDescriptor declines the bare built-in templates", "[type_descriptor]") {
	// DefaultTypeGenerator stores LogicalType(id) with no type info as the base a bind_function is applied
	// to. That is a template, not a type - describing it must fail cleanly rather than deref its missing info.
	const LogicalTypeId parametric[] = {LogicalTypeId::DECIMAL, LogicalTypeId::LIST, LogicalTypeId::ARRAY,
	                                    LogicalTypeId::STRUCT,  LogicalTypeId::MAP,  LogicalTypeId::UNION,
	                                    LogicalTypeId::ENUM};
	for (auto id : parametric) {
		LogicalType bare(id);
		INFO(EnumUtil::ToString(id));
		REQUIRE(!bare.AuxInfo());
		REQUIRE(TypeDescriptor::TryFromLogicalType(bare) == nullptr);
		REQUIRE_THROWS(TypeDescriptor::FromLogicalType(bare));
	}
}
