//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/diagnostic.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/span.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! The severity of a diagnostic (ariadne/codespan-style)
enum class DiagnosticSeverity : uint8_t { ERROR = 0, WARNING = 1, NOTE = 2, HELP = 3 };

DUCKDB_API const char *DiagnosticSeverityToString(DiagnosticSeverity severity);

//! A label attaches a message to a source span. A primary label marks the main location of the diagnostic,
//! secondary labels provide supporting context.
struct DiagnosticLabel {
	Span span;
	string message;
	bool primary = false;
};

//! A structured diagnostic report: a headline message plus labels over source spans and free-floating notes.
//! The rendering of this model (carets, colors, ...) is left to the consumer (e.g. the shell).
struct Diagnostic {
	DiagnosticSeverity severity = DiagnosticSeverity::ERROR;
	//! The top-level headline message
	string message;
	//! An optional machine-readable code / subtype (e.g. "type_mismatch")
	string code;
	//! Labels attaching messages to source spans
	vector<DiagnosticLabel> labels;
	//! Free-floating notes (not tied to a span)
	vector<string> notes;

	//! Serialize this diagnostic to a JSON string (used to transport it inside an error's extra_info)
	DUCKDB_API string ToJSON() const;
	//! Parse a diagnostic from a JSON string. Returns false if the input is not a valid diagnostic.
	DUCKDB_API static bool TryParse(const string &json, Diagnostic &result);
};

//! Builder for constructing a Diagnostic and throwing it as an exception.
class DiagnosticBuilder {
public:
	DUCKDB_API explicit DiagnosticBuilder(string message, DiagnosticSeverity severity = DiagnosticSeverity::ERROR);

	//! Set the machine-readable code / subtype
	DUCKDB_API DiagnosticBuilder &Code(string code);
	//! Add a primary label (the main location of the diagnostic)
	DUCKDB_API DiagnosticBuilder &Primary(Span span, string message);
	//! Add a secondary (supporting context) label
	DUCKDB_API DiagnosticBuilder &Secondary(Span span, string message);
	//! Add a free-floating note
	DUCKDB_API DiagnosticBuilder &Note(string note);

	//! Get the constructed diagnostic
	DUCKDB_API Diagnostic Build();
	//! Throw the diagnostic as an exception (serialized into the error's extra_info)
	[[noreturn]] DUCKDB_API void Throw(ExceptionType type = ExceptionType::BINDER) const;

private:
	Diagnostic diagnostic;
};

} // namespace duckdb
