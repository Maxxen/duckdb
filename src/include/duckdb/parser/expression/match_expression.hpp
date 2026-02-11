//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/match_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

enum class MatchPatternType : uint8_t {
    INVALID = 0,
    WILDCARD = 1,
    IDENTIFIER = 2,
    LITERAL = 3,
    LIST = 4,
    REST = 5,
    STRUCT = 5,
};

class MatchPattern {
public:
    MatchPatternType GetType() const { return type; }

    virtual ~MatchPattern() = default;

    template<class T>
    const T &Cast() const {
		D_ASSERT(GetType() == T::TYPE);
		return static_cast<const T &>(*this);
	}

    virtual string ToString() const = 0;

    virtual bool Equals(const MatchPattern &other) const = 0;
    virtual unique_ptr<MatchPattern> Copy() const = 0;
    virtual void Serialize(Serializer &serializer) const;

    static unique_ptr<MatchPattern> Deserialize(Deserializer &deserializer);

protected:
    explicit MatchPattern(MatchPatternType type) : type(type) {
	}
private:
    MatchPatternType type;
};

class MatchPatternWildcard : public MatchPattern {
public:
	static constexpr const MatchPatternType TYPE = MatchPatternType::WILDCARD;

	MatchPatternWildcard() : MatchPattern(TYPE) {
	}

public:
    string ToString() const override;
    bool Equals(const MatchPattern &other) const override;
    unique_ptr<MatchPattern> Copy() const override;
	void Serialize(Serializer &serializer) const override;
    static unique_ptr<MatchPattern> Deserialize(Deserializer &deserializer);
};

class MatchPatternRest : public MatchPattern {
public:
    static constexpr const MatchPatternType TYPE = MatchPatternType::REST;

    MatchPatternRest() : MatchPattern(TYPE) {
    }

public:
    string ToString() const override;
    bool Equals(const MatchPattern &other) const override;
    unique_ptr<MatchPattern> Copy() const override;
    void Serialize(Serializer &serializer) const override;
    static unique_ptr<MatchPattern> Deserialize(Deserializer &deserializer);
};


class MatchPatternIdentifier final : public MatchPattern {
public:
    static constexpr const MatchPatternType TYPE = MatchPatternType::IDENTIFIER;

    MatchPatternIdentifier(string identifier) : MatchPattern(TYPE), identifier(std::move(identifier)) {
	}

    string identifier;

public:
    string ToString() const override;
    bool Equals(const MatchPattern &other) const override;
    unique_ptr<MatchPattern> Copy() const override;
    void Serialize(Serializer &serializer) const override;
    static unique_ptr<MatchPattern> Deserialize(Deserializer &deserializer);
};

class MatchPatternLiteral final : public MatchPattern {
public:
    static constexpr const MatchPatternType TYPE = MatchPatternType::LITERAL;

    MatchPatternLiteral(Value value) : MatchPattern(TYPE), value(std::move(value)) {
    }

    Value value;

public:

    string ToString() const override;
    bool Equals(const MatchPattern &other) const override;
    unique_ptr<MatchPattern> Copy() const override;
    void Serialize(Serializer &serializer) const override;
    static unique_ptr<MatchPattern> Deserialize(Deserializer &deserializer);
};

class MatchPatternList final : public MatchPattern {
public:
    static constexpr const MatchPatternType TYPE = MatchPatternType::LIST;

    MatchPatternList(vector<unique_ptr<MatchPattern>> &&elements) : MatchPattern(TYPE), elements(std::move(elements)) {}

	vector<unique_ptr<MatchPattern>> elements;

public:

    string ToString() const override;
    bool Equals(const MatchPattern &other) const override;
    unique_ptr<MatchPattern> Copy() const override;
    void Serialize(Serializer &serializer) const override;
    static unique_ptr<MatchPattern> Deserialize(Deserializer &deserializer);
};


class MatchPatternStruct final : public MatchPattern {
public:
    static constexpr const MatchPatternType TYPE = MatchPatternType::STRUCT;

    MatchPatternStruct(child_list_t<unique_ptr<MatchPattern>> &&fields) : MatchPattern(TYPE), fields(std::move(fields)) {}

    child_list_t<unique_ptr<MatchPattern>> fields;

public:
    string ToString() const override;
    bool Equals(const MatchPattern &other) const override;
    unique_ptr<MatchPattern> Copy() const override;
    void Serialize(Serializer &serializer) const override;
    static unique_ptr<MatchPattern> Deserialize(Deserializer &deserializer);
};

class MatchArm {
public:
    MatchArm();

    unique_ptr<MatchPattern> &GetPattern() { return pattern; }
    unique_ptr<ParsedExpression> &GetExpression() { return expression; }
    unique_ptr<ParsedExpression> &GetGuard() { return guard; }

    unique_ptr<MatchPattern> pattern;
    unique_ptr<ParsedExpression> guard;
    unique_ptr<ParsedExpression> expression;

public:
    string ToString() const;
    bool Equals(const MatchArm &other) const;
    unique_ptr<MatchArm> Copy() const;
    void Serialize(Serializer &serializer) const;
    static unique_ptr<MatchArm> Deserialize(Deserializer &deserializer);
};

class MatchExpression final : public ParsedExpression {
public:
    static constexpr const ExpressionClass TYPE = ExpressionClass::MATCH;

    DUCKDB_API MatchExpression();

    unique_ptr<ParsedExpression> arg;
    vector<unique_ptr<MatchArm>> arms;
    unique_ptr<ParsedExpression> else_expr;

public:
    string ToString() const override;

    static bool Equal(const MatchExpression &a, const MatchExpression &b);

    unique_ptr<ParsedExpression> Copy() const override;

    void Serialize(Serializer &serializer) const override;

    static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);
};

}