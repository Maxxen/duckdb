#include "duckdb/common/diagnostic.hpp"

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

const char *DiagnosticSeverityToString(DiagnosticSeverity severity) {
	switch (severity) {
	case DiagnosticSeverity::ERROR:
		return "error";
	case DiagnosticSeverity::WARNING:
		return "warning";
	case DiagnosticSeverity::NOTE:
		return "note";
	case DiagnosticSeverity::HELP:
		return "help";
	default:
		return "error";
	}
}

string Diagnostic::ToJSON() const {
	JSONWriter writer;
	auto obj = writer.CreateObject();
	obj.Add("severity", writer.CreateUnsignedInteger(static_cast<uint64_t>(severity)));
	obj.AddString("message", message);
	if (!code.empty()) {
		obj.AddString("code", code);
	}
	auto labels_arr = writer.CreateArray();
	for (auto &label : labels) {
		auto label_obj = writer.CreateObject();
		label_obj.Add("start", writer.CreateUnsignedInteger(label.span.offset));
		label_obj.Add("length", writer.CreateUnsignedInteger(label.span.length));
		label_obj.AddString("message", label.message);
		label_obj.Add("primary", writer.CreateBoolean(label.primary));
		labels_arr.Append(label_obj);
	}
	obj.Add("labels", labels_arr);
	auto notes_arr = writer.CreateArray();
	for (auto &note : notes) {
		notes_arr.AppendString(note);
	}
	obj.Add("notes", notes_arr);
	writer.SetRoot(obj);
	return writer.ToString();
}

bool Diagnostic::TryParse(const string &json, Diagnostic &result) {
	JSONParseError error;
	auto doc = JSONDocument::TryParse(json.c_str(), json.size(), error);
	if (!doc) {
		return false;
	}
	auto root = doc->GetRoot();
	if (!root.IsObject()) {
		return false;
	}
	auto severity = root.GetMember("severity");
	if (severity.IsInteger()) {
		result.severity = static_cast<DiagnosticSeverity>(severity.GetUnsignedInteger());
	}
	auto message = root.GetMember("message");
	if (message.IsString()) {
		result.message = message.GetString();
	}
	auto code = root.GetMember("code");
	if (code.IsString()) {
		result.code = code.GetString();
	}
	auto labels = root.GetMember("labels");
	if (labels.IsArray()) {
		labels.IterateArray([&](JSONValue label_value) {
			DiagnosticLabel label;
			auto start = label_value.GetMember("start");
			auto length = label_value.GetMember("length");
			idx_t start_idx = start.IsInteger() ? start.GetUnsignedInteger() : idx_t(Span::INVALID_OFFSET);
			idx_t length_idx = length.IsInteger() ? length.GetUnsignedInteger() : 0;
			label.span = Span(start_idx, length_idx);
			auto label_message = label_value.GetMember("message");
			if (label_message.IsString()) {
				label.message = label_message.GetString();
			}
			auto primary = label_value.GetMember("primary");
			label.primary = primary.IsValid() && primary.GetBoolean();
			result.labels.push_back(std::move(label));
		});
	}
	auto notes = root.GetMember("notes");
	if (notes.IsArray()) {
		notes.IterateArray([&](JSONValue note_value) {
			if (note_value.IsString()) {
				result.notes.push_back(note_value.GetString());
			}
		});
	}
	return true;
}

//! Render a single label as "LINE n: <source line>" followed by an underline (^ primary, - secondary) and message.
//! Byte-column based: correct for ASCII source (Unicode-width alignment is left for a later pass).
static string RenderLabel(const string &source, const DiagnosticLabel &label) {
	if (!label.span.IsValid() || label.span.offset > source.size()) {
		return "  " + label.message;
	}
	idx_t offset = label.span.offset;
	// find the start of the line containing the label and the line number
	idx_t line_start = 0;
	idx_t line_number = 1;
	for (idx_t i = 0; i < offset; i++) {
		if (source[i] == '\n') {
			line_number++;
			line_start = i + 1;
		}
	}
	// find the end of the line
	idx_t line_end = source.size();
	for (idx_t i = offset; i < source.size(); i++) {
		if (source[i] == '\n' || source[i] == '\r') {
			line_end = i;
			break;
		}
	}
	auto source_line = source.substr(line_start, line_end - line_start);
	auto gutter = "LINE " + to_string(line_number) + ": ";
	idx_t column = offset - line_start;
	idx_t line_length = line_end - line_start;
	idx_t caret_count = label.span.length > 0 ? label.span.length : 1;
	if (column + caret_count > line_length) {
		caret_count = line_length > column ? line_length - column : 1;
	}
	char caret = label.primary ? '^' : '-';
	string result = "  " + gutter + source_line + "\n";
	result += "  " + string(gutter.size() + column, ' ') + string(caret_count, caret);
	if (!label.message.empty()) {
		result += " " + label.message;
	}
	return result;
}

string Diagnostic::Render(const string &source) const {
	string result = string(DiagnosticSeverityToString(severity)) + ": " + message;
	for (auto &label : labels) {
		result += "\n" + RenderLabel(source, label);
	}
	for (auto &note : notes) {
		result += "\n  = note: " + note;
	}
	return result;
}

DiagnosticBuilder::DiagnosticBuilder(string message, DiagnosticSeverity severity) {
	diagnostic.message = std::move(message);
	diagnostic.severity = severity;
}

DiagnosticBuilder &DiagnosticBuilder::Code(string code) {
	diagnostic.code = std::move(code);
	return *this;
}

DiagnosticBuilder &DiagnosticBuilder::Primary(Span span, string message) {
	DiagnosticLabel label;
	label.span = span;
	label.message = std::move(message);
	label.primary = true;
	diagnostic.labels.push_back(std::move(label));
	return *this;
}

DiagnosticBuilder &DiagnosticBuilder::Secondary(Span span, string message) {
	DiagnosticLabel label;
	label.span = span;
	label.message = std::move(message);
	label.primary = false;
	diagnostic.labels.push_back(std::move(label));
	return *this;
}

DiagnosticBuilder &DiagnosticBuilder::Note(string note) {
	diagnostic.notes.push_back(std::move(note));
	return *this;
}

Diagnostic DiagnosticBuilder::Build() {
	return diagnostic;
}

void DiagnosticBuilder::Throw(ExceptionType type) const {
	unordered_map<string, string> extra_info;
	extra_info["diagnostic"] = diagnostic.ToJSON();
	if (!diagnostic.code.empty()) {
		extra_info["error_subtype"] = diagnostic.code;
	}
	// point the (legacy) caret rendering at the primary label so existing consumers still get a location
	for (auto &label : diagnostic.labels) {
		if (label.primary && label.span.IsValid()) {
			Exception::SetQueryLocation(label.span, extra_info);
			break;
		}
	}
	throw Exception(extra_info, type, diagnostic.message);
}

} // namespace duckdb
