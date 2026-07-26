#include "duckdb/common/diagnostic.hpp"

#include "duckdb/common/json_document.hpp"
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
