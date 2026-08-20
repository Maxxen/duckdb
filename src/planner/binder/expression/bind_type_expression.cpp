#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/type_descriptor.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"

namespace duckdb {

static bool IsValidTypeLookup(optional_ptr<CatalogEntry> entry) {
	if (!entry) {
		return false;
	}
	return entry->Cast<TypeCatalogEntry>().IsValid();
}

//! Resolve the catalog entry a type expression names. The qualification is resolved the same way a table
//! reference is: a leading component is the catalog when it names an attached database, and otherwise the
//! outermost schema of a (possibly nested) schema path.
TypeCatalogEntry &Binder::LookupTypeEntry(const TypeExpression &type_expr) {
	return LookupTypeEntry(type_expr.GetQualifiedName(), QueryErrorContext(type_expr));
}

TypeCatalogEntry &Binder::LookupTypeEntry(const QualifiedName &qualified_name, QueryErrorContext error_context) {
	auto &type_name = qualified_name.Name();

	EntryLookupInfo type_lookup(CatalogType::TYPE_ENTRY, QualifiedName(type_name), error_context);

	optional_ptr<CatalogEntry> entry = nullptr;

	// Resolve the qualification the same way a table reference is resolved: a leading component is the catalog when
	// it names an attached database, and otherwise the outermost schema of a (possibly nested) schema path.
	auto bound_name = Binder::BindTableName(EntryRetriever(), qualified_name);
	auto &type_catalog = bound_name.Catalog();
	bool is_qualified = bound_name.Path().size() > 1;

	if (type_catalog.empty() && !DatabaseManager::Get(context).HasDefaultDatabase()) {
		// Look in the system catalog if no catalog was specified
		entry =
		    entry_retriever.GetEntry(EntryLookupInfo(type_lookup, bound_name.WithCatalog(Identifier::SystemCatalog())));
	} else {
		// Try to search from most specific to least specific
		// The search path should already have been set to the correct catalog/schema,
		// in case we are looking for a type in the same schema as a table we are creating

		entry = entry_retriever.GetEntry(EntryLookupInfo(type_lookup, bound_name), OnEntryNotFound::RETURN_NULL);

		if (!IsValidTypeLookup(entry)) {
			if (is_qualified) {
				// re-run the lookup to report the qualification that was given
				entry = entry_retriever.GetEntry(EntryLookupInfo(type_lookup, bound_name),
				                                 OnEntryNotFound::THROW_EXCEPTION);
			}
			entry = entry_retriever.GetEntry(
			    EntryLookupInfo(type_lookup, QualifiedName(type_catalog, Identifier::InvalidSchema(),
			                                               type_lookup.GetEntryIdentifier())),
			    OnEntryNotFound::RETURN_NULL);
		}
		if (!IsValidTypeLookup(entry)) {
			entry = entry_retriever.GetEntry(
			    EntryLookupInfo(type_lookup, QualifiedName(Identifier::InvalidCatalog(), Identifier::InvalidSchema(),
			                                               type_lookup.GetEntryIdentifier())),
			    OnEntryNotFound::RETURN_NULL);
		}
		if (!IsValidTypeLookup(entry)) {
			entry = entry_retriever.GetEntry(
			    EntryLookupInfo(type_lookup, QualifiedName(Identifier::SystemCatalog(), Identifier::DefaultSchema(),
			                                               type_lookup.GetEntryIdentifier())),
			    OnEntryNotFound::THROW_EXCEPTION);
		}
	}

	// By this point we have to have found a type in the catalog
	D_ASSERT(entry != nullptr);
	return entry->Cast<TypeCatalogEntry>();
}

void Binder::QualifyTypeExpression(TypeExpression &type_expr) {
	auto &type_entry = LookupTypeEntry(type_expr);
	type_expr.SetQualifiedName(type_entry.ParentCatalog().GetName(), type_entry.ParentSchema().name,
	                           type_expr.GetTypeName());
	for (auto &child : type_expr.GetChildren()) {
		if (child->GetExpressionClass() == ExpressionClass::TYPE) {
			QualifyTypeExpression(child->Cast<TypeExpression>());
		}
	}
}

BindResult ExpressionBinder::BindExpression(TypeExpression &type_expr, idx_t depth) {
	auto &type_name = type_expr.GetTypeName();
	auto &type_entry = binder.LookupTypeEntry(type_expr);

	// Now handle type parameters
	auto &unbound_parameters = type_expr.GetChildren();

	if (!type_entry.bind_function) {
		if (!unbound_parameters.empty()) {
			// This type does not support type parameters
			throw BinderException(type_expr, "Type '%s' does not take any type parameters", type_name);
		}

		// Otherwise, return the user type directly!
		auto result_expr = make_uniq<BoundConstantExpression>(Value::TYPE(type_entry.GetType(context)));
		result_expr->SetQueryLocation(type_expr.GetQueryLocation());
		return BindResult(std::move(result_expr));
	}

	// Bind value parameters
	vector<TypeArgument> bound_parameters;

	for (auto &param : unbound_parameters) {
		// Otherwise, try to fold it to a constant value
		ConstantBinder binder(this->binder, context, StringUtil::Format("Type parameter for type '%s'", type_name));

		auto expr = param->Copy();
		auto bound_expr = binder.Bind(expr);

		if (!bound_expr->IsFoldable()) {
			throw BinderException(type_expr, "Type parameter expression for type '%s' is not a constant", type_name);
		}

		// Shortcut for constant expressions
		if (bound_expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &const_expr = bound_expr->Cast<BoundConstantExpression>();
			bound_parameters.emplace_back(param->GetAlias().GetIdentifierName(), const_expr.GetValue());
			continue;
		}

		// Otherwise we need to evaluate the expression
		auto bound_param = ExpressionExecutor::EvaluateScalar(context, *bound_expr);
		bound_parameters.emplace_back(param->GetAlias().GetIdentifierName(), bound_param);
	};

	// Call the bind function
	BindLogicalTypeInput input {context, bound_parameters};
	auto result_type = type_entry.bind_function(input);

	// Return the resulting type!
	auto result_expr = make_uniq<BoundConstantExpression>(Value::TYPE(result_type));
	result_expr->SetQueryLocation(type_expr.GetQueryLocation());
	return BindResult(std::move(result_expr));
}

LogicalType Binder::BindTypeDescriptor(const TypeDescriptor &descriptor) {
	if (!descriptor.Name().Schema().empty() || !descriptor.Name().Catalog().empty()) {
		// qualified: it can only be a catalog entry
	} else if (DefaultTypeGenerator::GetDefaultType(descriptor.Name().Name()) != LogicalTypeId::INVALID) {
		// a built-in is defined by the compiled-in table; its catalog entry is generated from that table, so
		// resolving it there would only lead back here
		return descriptor.DefaultBind();
	}
	auto &type_entry = LookupTypeEntry(descriptor.Name(), QueryErrorContext());
	auto &type_name = descriptor.Name().Name();

	if (!type_entry.bind_function) {
		if (!descriptor.Parameters().empty()) {
			throw BinderException("Type '%s' does not take any type parameters", type_name);
		}
		return type_entry.GetType(context);
	}

	// a descriptor's parameters are already folded, so there is nothing to evaluate here
	vector<TypeArgument> bound_parameters;
	for (auto &param : descriptor.Parameters()) {
		auto value = param.IsType() ? Value::TYPE(BindTypeDescriptor(param.GetType())) : param.GetValue();
		bound_parameters.emplace_back(param.Label().GetIdentifierName(), std::move(value));
	}

	BindLogicalTypeInput input {context, bound_parameters};
	return type_entry.bind_function(input);
}

} // namespace duckdb
