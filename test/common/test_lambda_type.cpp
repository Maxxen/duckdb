#include "catch.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types.hpp"

using namespace duckdb; // NOLINT

namespace {
LogicalType RoundTrip(const LogicalType &type, StorageVersion storage_version) {
	MemoryStream stream;
	SerializationOptions options;
	options.storage_compatibility.storage_version = storage_version;
	BinarySerializer serializer(stream, options);
	serializer.Begin();
	serializer.WriteProperty(100, "type", type);
	serializer.End();

	stream.Rewind();
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto result = deserializer.ReadProperty<LogicalType>(100, "type");
	deserializer.End();
	return result;
}
} // namespace

TEST_CASE("LAMBDA type carries parameter and return types", "[lambda_type]") {
	const LogicalType untyped(LogicalTypeId::LAMBDA);
	REQUIRE(!untyped.HasParameters());
	REQUIRE(untyped.ToString() == "LAMBDA");

	const auto typed = LogicalType::LAMBDA({LogicalType::BIGINT, LogicalType::VARCHAR}, LogicalType::BOOLEAN);
	REQUIRE(typed.id() == LogicalTypeId::LAMBDA);
	REQUIRE(typed.HasParameters());
	REQUIRE(typed.ToString() == "LAMBDA(BIGINT, VARCHAR -> BOOLEAN)");
	REQUIRE(LambdaType::GetParameterTypes(typed).size() == 2);
	REQUIRE(LambdaType::GetParameterTypes(typed)[1] == LogicalType::VARCHAR);
	REQUIRE(LambdaType::GetReturnType(typed) == LogicalType::BOOLEAN);
	REQUIRE(LogicalType::LAMBDA({}, LogicalType::INTEGER).ToString() == "LAMBDA(-> INTEGER)");

	// equality compares the signature
	REQUIRE(typed == LogicalType::LAMBDA({LogicalType::BIGINT, LogicalType::VARCHAR}, LogicalType::BOOLEAN));
	REQUIRE(typed != LogicalType::LAMBDA({LogicalType::BIGINT}, LogicalType::BOOLEAN));
	REQUIRE(typed != LogicalType::LAMBDA({LogicalType::BIGINT, LogicalType::VARCHAR}, LogicalType::INTEGER));
	REQUIRE(typed != untyped);
	REQUIRE(typed.IsComplete());
}

TEST_CASE("Variadic LAMBDA types", "[lambda_type]") {
	const auto variadic = LogicalType::LAMBDA({LogicalType::BIGINT}, LogicalType::TEMPLATE("U"), LogicalType::ANY);
	REQUIRE(LambdaType::IsVariadic(variadic));
	REQUIRE(LambdaType::GetVarargsType(variadic) == LogicalType::ANY);
	REQUIRE(LambdaType::GetParameterTypes(variadic).size() == 1);
	REQUIRE(variadic.ToString() == "LAMBDA(BIGINT, ANY... -> U)");
	REQUIRE(LogicalType::LAMBDA({}, LogicalType::INTEGER, LogicalType::ANY).ToString() == "LAMBDA(ANY... -> INTEGER)");

	const auto fixed = LogicalType::LAMBDA({LogicalType::BIGINT}, LogicalType::TEMPLATE("U"));
	REQUIRE(!LambdaType::IsVariadic(fixed));
	REQUIRE(variadic != fixed);
	REQUIRE(variadic.IsTemplated());

	// the varargs type is visited and replaced like the other children
	auto resolved = TypeVisitor::VisitReplace(variadic, [&](const LogicalType &child) {
		if (child.id() == LogicalTypeId::TEMPLATE) {
			return LogicalType(LogicalType::DOUBLE);
		}
		if (child.id() == LogicalTypeId::ANY) {
			return LogicalType(LogicalType::VARCHAR);
		}
		return child;
	});
	REQUIRE(resolved == LogicalType::LAMBDA({LogicalType::BIGINT}, LogicalType::DOUBLE, LogicalType::VARCHAR));
}

TEST_CASE("LAMBDA type participates in type visiting", "[lambda_type]") {
	const auto templated = LogicalType::LAMBDA({LogicalType::TEMPLATE("T"), LogicalType::BIGINT},
	                                           LogicalType::LIST(LogicalType::TEMPLATE("U")));
	REQUIRE(templated.IsTemplated());
	REQUIRE(!templated.IsComplete());

	duckdb::vector<LogicalTypeId> visited;
	auto resolved = TypeVisitor::VisitReplace(templated, [&](const LogicalType &child) {
		visited.push_back(child.id());
		if (child.id() != LogicalTypeId::TEMPLATE) {
			return child;
		}
		return TemplateType::GetName(child) == "T" ? LogicalType(LogicalType::VARCHAR)
		                                           : LogicalType(LogicalType::DOUBLE);
	});
	const duckdb::vector<LogicalTypeId> expected {LogicalTypeId::TEMPLATE, LogicalTypeId::BIGINT,
	                                              LogicalTypeId::TEMPLATE, LogicalTypeId::LIST, LogicalTypeId::LAMBDA};
	REQUIRE(visited == expected);
	REQUIRE(resolved ==
	        LogicalType::LAMBDA({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::LIST(LogicalType::DOUBLE)));
	REQUIRE(!resolved.IsTemplated());
	REQUIRE(resolved.IsComplete());
}

TEST_CASE("LAMBDA type serialization drops the signature before v2.1.0", "[lambda_type]") {
	const auto typed = LogicalType::LAMBDA({LogicalType::BIGINT}, LogicalType::LIST(LogicalType::VARCHAR));
	REQUIRE(RoundTrip(typed, StorageVersion::V2_1_0) == typed);
	REQUIRE(RoundTrip(typed, StorageVersion::LATEST) == typed);

	const auto legacy = RoundTrip(typed, StorageVersion::V2_0_0);
	REQUIRE(legacy.id() == LogicalTypeId::LAMBDA);
	REQUIRE(!legacy.HasParameters());

	const LogicalType untyped(LogicalTypeId::LAMBDA);
	REQUIRE(RoundTrip(untyped, StorageVersion::LATEST) == untyped);
	REQUIRE(RoundTrip(untyped, StorageVersion::V1_5_0) == untyped);

	const auto variadic = LogicalType::LAMBDA({LogicalType::BIGINT}, LogicalType::VARCHAR, LogicalType::INTEGER);
	REQUIRE(RoundTrip(variadic, StorageVersion::LATEST) == variadic);
	REQUIRE(RoundTrip(variadic, StorageVersion::V2_0_0) == untyped);

	// nested inside another type
	const auto nested = LogicalType::LIST(typed);
	REQUIRE(RoundTrip(nested, StorageVersion::LATEST) == nested);
	REQUIRE(RoundTrip(nested, StorageVersion::V2_0_0) == LogicalType::LIST(untyped));
}
