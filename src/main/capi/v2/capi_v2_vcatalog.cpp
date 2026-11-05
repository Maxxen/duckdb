#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb::capiv2 {

//! What every database attached with a vcatalog shares: its name, callbacks and user data.
struct CV2VCatalogDefinition {
	string name;
	duckdb_v2_vcatalog_attach_callback_fn attach_cb = nullptr;
	duckdb_v2_vcatalog_transaction_begin_callback_fn begin_cb = nullptr;
	duckdb_v2_vcatalog_transaction_commit_callback_fn commit_cb = nullptr;
	duckdb_v2_vcatalog_transaction_rollback_callback_fn rollback_cb = nullptr;
	shared_ptr<CV2UserData> user_data;

	void *GetUserData() const {
		return user_data ? user_data->GetData() : nullptr;
	}
};

//! Invokes a callback with a live error slot, rethrowing the error it reports.
template <class FN>
static void InvokeCallback(FN &&callback) {
	CV2ErrorInfo err;
	auto err_ptr = Convert(&err);
	callback(&err_ptr);
	if (err.HasError()) {
		err.ThrowAsException();
	}
}

static void *GetState(const unique_ptr<CV2UserData> &state) {
	return state ? state->GetData() : nullptr;
}

//----------------------------------------------------------------------------------------------------------------------
// Catalog
//----------------------------------------------------------------------------------------------------------------------
class CV2VirtualCatalog final : public Catalog {
public:
	CV2VirtualCatalog(AttachedDatabase &db, CV2VCatalogDefinition definition_p, string path_p,
	                  unique_ptr<CV2UserData> catalog_state_p)
	    : Catalog(db), definition(std::move(definition_p)), path(std::move(path_p)),
	      catalog_state(std::move(catalog_state_p)) {
	}

	void Initialize(bool load_builtin) override {
	}

	string GetCatalogType() override {
		return definition.name;
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override {
		throw NotImplementedException("Creating schemas is not supported in %s databases", definition.name);
	}

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override {
		return LookupSchemaPath(transaction, schema_lookup, if_not_found,
		                        [](const EntryLookupInfo &) -> optional_ptr<CatalogEntry> { return nullptr; });
	}

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override {
	}

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override {
		throw NotImplementedException("CREATE TABLE AS is not supported in %s databases", definition.name);
	}

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override {
		throw NotImplementedException("INSERT is not supported in %s databases", definition.name);
	}

	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override {
		throw NotImplementedException("DELETE is not supported in %s databases", definition.name);
	}

	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override {
		throw NotImplementedException("UPDATE is not supported in %s databases", definition.name);
	}

	DatabaseSize GetDatabaseSize(ClientContext &context) override {
		return DatabaseSize();
	}

	bool InMemory() override {
		return false;
	}

	string GetDBPath() override {
		return path;
	}

	void *GetCatalogState() const {
		return GetState(catalog_state);
	}

public:
	const CV2VCatalogDefinition definition;
	const string path;

private:
	void DropSchema(ClientContext &context, DropInfo &info) override {
		throw NotImplementedException("Dropping schemas is not supported in %s databases", definition.name);
	}

private:
	//! Destroyed after every transaction state: the attached database destroys its transaction manager first
	unique_ptr<CV2UserData> catalog_state;
};

//----------------------------------------------------------------------------------------------------------------------
// Attach Info
//----------------------------------------------------------------------------------------------------------------------
class CV2VCatalogAttachInfo {
public:
	CV2VCatalogAttachInfo(const CV2VCatalogDefinition &definition, const string &name, const string &path,
	                      bool read_only, const unordered_map<string, Value> &options)
	    : definition(definition), name(name), path(path), read_only(read_only), options(options) {
	}

	//! Finds the option named `option_name`, marking it as recognized.
	optional_ptr<const Value> FindOption(const string &option_name) {
		for (auto &entry : options) {
			if (StringUtil::CIEquals(entry.first, option_name)) {
				recognized_options.insert(entry.first);
				return &entry.second;
			}
		}
		return nullptr;
	}

	void CheckOptionsRecognized() const {
		for (auto &entry : options) {
			if (recognized_options.find(entry.first) == recognized_options.end()) {
				throw BinderException("Unrecognized option for attach \"%s\"", entry.first);
			}
		}
	}

public:
	const CV2VCatalogDefinition &definition;
	const string &name;
	const string &path;
	const bool read_only;
	unique_ptr<CV2UserData> catalog_state;

private:
	const unordered_map<string, Value> &options;
	unordered_set<string> recognized_options;
};

static auto Convert(duckdb_v2_vcatalog_attach_info_handle info) -> CV2VCatalogAttachInfo * {
	return reinterpret_cast<CV2VCatalogAttachInfo *>(info);
}
static auto Convert(CV2VCatalogAttachInfo *info) -> duckdb_v2_vcatalog_attach_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_attach_info_handle>(info);
}

//----------------------------------------------------------------------------------------------------------------------
// Transaction
//----------------------------------------------------------------------------------------------------------------------
class CV2VCatalogTransaction final : public Transaction {
public:
	CV2VCatalogTransaction(TransactionManager &manager, ClientContext &context) : Transaction(manager, context) {
	}

	unique_ptr<CV2UserData> transaction_state;
};

//! Backs the begin info handle.
class CV2VCatalogBeginInfo {
public:
	CV2VCatalogBeginInfo(CV2VirtualCatalog &catalog, CV2VCatalogTransaction &transaction, bool read_only)
	    : catalog(catalog), transaction(transaction), read_only(read_only) {
	}

	CV2VirtualCatalog &catalog;
	CV2VCatalogTransaction &transaction;
	const bool read_only;
};

static auto Convert(duckdb_v2_vcatalog_transaction_begin_info_handle info) -> CV2VCatalogBeginInfo * {
	return reinterpret_cast<CV2VCatalogBeginInfo *>(info);
}
static auto Convert(CV2VCatalogBeginInfo *info) -> duckdb_v2_vcatalog_transaction_begin_info_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_transaction_begin_info_handle>(info);
}

//! Backs the commit and rollback info handles.
class CV2VCatalogEndInfo {
public:
	CV2VCatalogEndInfo(CV2VirtualCatalog &catalog, CV2VCatalogTransaction &transaction)
	    : catalog(catalog), transaction(transaction) {
	}

	CV2VirtualCatalog &catalog;
	CV2VCatalogTransaction &transaction;
};

static auto Convert(duckdb_v2_vcatalog_transaction_commit_info_handle info) -> CV2VCatalogEndInfo * {
	return reinterpret_cast<CV2VCatalogEndInfo *>(info);
}
static auto Convert(duckdb_v2_vcatalog_transaction_rollback_info_handle info) -> CV2VCatalogEndInfo * {
	return reinterpret_cast<CV2VCatalogEndInfo *>(info);
}

class CV2VCatalogTransactionManager final : public TransactionManager {
public:
	CV2VCatalogTransactionManager(AttachedDatabase &db, CV2VirtualCatalog &catalog)
	    : TransactionManager(db), catalog(catalog) {
	}

	//! Runs under the lock of the client's MetaTransaction.
	Transaction &StartTransaction(ClientContext &context) override {
		auto transaction = make_uniq<CV2VCatalogTransaction>(*this, context);
		auto begin_cb = catalog.definition.begin_cb;
		if (begin_cb) {
			auto read_only = db.IsReadOnly() ||
			                 (context.transaction.HasActiveTransaction() && MetaTransaction::Get(context).IsReadOnly());
			CV2VCatalogBeginInfo info(catalog, *transaction, read_only);
			InvokeCallback([&](duckdb_v2_error_info_handle *err) { begin_cb(Convert(&info), err); });
		}
		auto &result = *transaction;
		lock_guard<mutex> guard(transaction_lock);
		transactions[result] = std::move(transaction);
		return result;
	}

	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override {
		auto &vcatalog_transaction = transaction.Cast<CV2VCatalogTransaction>();
		ErrorData error;
		auto commit_cb = catalog.definition.commit_cb;
		if (commit_cb) {
			try {
				CV2VCatalogEndInfo info(catalog, vcatalog_transaction);
				InvokeCallback([&](duckdb_v2_error_info_handle *err) {
					commit_cb(reinterpret_cast<duckdb_v2_vcatalog_transaction_commit_info_handle>(&info), err);
				});
			} catch (std::exception &ex) {
				error = ErrorData(ex);
			}
		}
		if (error.HasError()) {
			// DuckDB does not roll back a transaction whose commit failed; the commit error is the one reported
			TryRollback(vcatalog_transaction);
		}
		RemoveTransaction(transaction);
		return error;
	}

	void RollbackTransaction(Transaction &transaction) override {
		auto error = TryRollback(transaction.Cast<CV2VCatalogTransaction>());
		RemoveTransaction(transaction);
		if (error.HasError()) {
			error.Throw();
		}
	}

	void Checkpoint(ClientContext &context, bool force) override {
	}

private:
	ErrorData TryRollback(CV2VCatalogTransaction &transaction) {
		auto rollback_cb = catalog.definition.rollback_cb;
		if (!rollback_cb) {
			return ErrorData();
		}
		try {
			CV2VCatalogEndInfo info(catalog, transaction);
			InvokeCallback([&](duckdb_v2_error_info_handle *err) {
				rollback_cb(reinterpret_cast<duckdb_v2_vcatalog_transaction_rollback_info_handle>(&info), err);
			});
		} catch (std::exception &ex) {
			return ErrorData(ex);
		}
		return ErrorData();
	}

	//! Destroys the transaction, and with it its state, after releasing the lock: the state's destructor may block.
	void RemoveTransaction(Transaction &transaction) {
		unique_ptr<CV2VCatalogTransaction> removed;
		lock_guard<mutex> guard(transaction_lock);
		auto entry = transactions.find(transaction);
		if (entry == transactions.end()) {
			return;
		}
		removed = std::move(entry->second);
		transactions.erase(entry);
	}

private:
	CV2VirtualCatalog &catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<CV2VCatalogTransaction>> transactions;
};

//----------------------------------------------------------------------------------------------------------------------
// Storage Extension
//----------------------------------------------------------------------------------------------------------------------
class CV2VCatalogStorageInfo final : public StorageExtensionInfo {
public:
	explicit CV2VCatalogStorageInfo(CV2VCatalogDefinition definition_p) : definition(std::move(definition_p)) {
	}

	const CV2VCatalogDefinition definition;
};

static unique_ptr<Catalog> AttachVCatalog(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AttachOptions &options) {
	auto &definition = static_cast<CV2VCatalogStorageInfo &>(*storage_info).definition;
	CV2VCatalogAttachInfo attach_info(definition, name, info.path, options.access_mode == AccessMode::READ_ONLY,
	                                  options.options);
	CV2CallbackContext callback_context(context);
	InvokeCallback([&](duckdb_v2_error_info_handle *err) {
		definition.attach_cb(Convert(&attach_info), Convert(&callback_context), err);
	});
	attach_info.CheckOptionsRecognized();
	return make_uniq<CV2VirtualCatalog>(db, definition, info.path, std::move(attach_info.catalog_state));
}

static unique_ptr<TransactionManager> CreateVCatalogTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                       AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<CV2VCatalogTransactionManager>(db, catalog.Cast<CV2VirtualCatalog>());
}

//----------------------------------------------------------------------------------------------------------------------
// Builder
//----------------------------------------------------------------------------------------------------------------------
class CV2VCatalog {
public:
	explicit CV2VCatalog(DatabaseInstance &db) : db(db) {
	}

	//! Validates the configuration and registers the vcatalog on `target`.
	void Register(DatabaseInstance &target) {
		CheckRegistrationTarget(db, target, "vcatalog");
		if (definition.name.empty()) {
			throw InvalidInputException("A name must be set for the vcatalog.");
		}
		if (!definition.attach_cb) {
			throw InvalidInputException("An attach callback must be set for the vcatalog.");
		}
		// DuckDB's own storage is selected by this name, so a vcatalog registered under it could never be attached
		if (StringUtil::CIEquals(definition.name, "duckdb")) {
			throw InvalidInputException("\"%s\" is the name of DuckDB's own database type.", definition.name);
		}
		auto &config = DBConfig::GetConfig(target);
		if (StorageExtension::Find(config, definition.name)) {
			throw InvalidInputException("A database type named \"%s\" is already registered.", definition.name);
		}
		auto extension = make_shared_ptr<StorageExtension>();
		extension->attach = AttachVCatalog;
		extension->create_transaction_manager = CreateVCatalogTransactionManager;
		extension->storage_info = make_shared_ptr<CV2VCatalogStorageInfo>(definition);
		StorageExtension::Register(config, definition.name, std::move(extension));
	}

public:
	//! The database it was created for: the only one it can be registered on.
	DatabaseInstance &db;
	CV2VCatalogDefinition definition;
};

static auto Convert(duckdb_v2_vcatalog_handle vcatalog) -> CV2VCatalog * {
	return reinterpret_cast<CV2VCatalog *>(vcatalog);
}
static auto Convert(CV2VCatalog *vcatalog) -> duckdb_v2_vcatalog_handle {
	return reinterpret_cast<duckdb_v2_vcatalog_handle>(vcatalog);
}

static void SetState(unique_ptr<CV2UserData> &state, const duckdb_v2_opaque &data) {
	state = make_uniq<CV2UserData>(data.ptr, data.destroy, data.equals);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_vcatalog_create(duckdb_v2_factory_handle factory, duckdb_v2_vcatalog_handle *vcatalog,
                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(vcatalog);
	*vcatalog = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2VCatalog>(Convert(factory)->GetDatabase());
		*vcatalog = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_destroy(duckdb_v2_vcatalog_handle *vcatalog) {
	return WithErrorHandler(nullptr, [&]() {
		if (!vcatalog) {
			return;
		}
		if (*vcatalog) {
			delete Convert(*vcatalog);
			*vcatalog = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_name(duckdb_v2_vcatalog_handle vcatalog, const char *name, idx_t name_length,
                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	return WithErrorHandler(
	    err, [&]() { Convert(vcatalog)->definition.name = duckdb::string(ConvertIdentifierName(name, name_length)); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_user_data(duckdb_v2_vcatalog_handle vcatalog, duckdb_v2_opaque *data,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(vcatalog)->definition.user_data =
		    duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_set_attach_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                       duckdb_v2_vcatalog_attach_callback_fn callback,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.attach_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_set_transaction_begin_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                  duckdb_v2_vcatalog_transaction_begin_callback_fn callback,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.begin_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_set_transaction_commit_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                   duckdb_v2_vcatalog_transaction_commit_callback_fn callback,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.commit_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_set_transaction_rollback_callback(duckdb_v2_vcatalog_handle vcatalog,
                                                     duckdb_v2_vcatalog_transaction_rollback_callback_fn callback,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->definition.rollback_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_get_user_data(duckdb_v2_vcatalog_attach_info_handle info, void **data,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->definition.GetUserData(); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_get_name(duckdb_v2_vcatalog_attach_info_handle info, const char **name,
                                                   idx_t *name_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err, [&]() {
		auto &attach_name = Convert(info)->name;
		*name = attach_name.data();
		*name_length = attach_name.size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_get_path(duckdb_v2_vcatalog_attach_info_handle info, const char **path,
                                                   idx_t *path_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(path);
	DUCKDB_CHECK_ARG(path_length);
	return WithErrorHandler(err, [&]() {
		auto &attach_path = Convert(info)->path;
		*path = attach_path.data();
		*path_length = attach_path.size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_is_read_only(duckdb_v2_vcatalog_attach_info_handle info, bool *read_only,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(read_only);
	return WithErrorHandler(err, [&]() { *read_only = Convert(info)->read_only; });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_get_option(duckdb_v2_vcatalog_attach_info_handle info, const char *name,
                                                     idx_t name_length, duckdb_v2_value_handle *value, bool *present,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	DUCKDB_CHECK_ARG(value);
	DUCKDB_CHECK_ARG(present);
	return WithErrorHandler(err, [&]() {
		auto option = Convert(info)->FindOption(duckdb::string(Convert(name, name_length)));
		*present = option != nullptr;
		if (option) {
			*value = Convert(new duckdb::Value(*option));
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_attach_set_catalog_state(duckdb_v2_vcatalog_attach_info_handle info,
                                                            duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { SetState(Convert(info)->catalog_state, *data); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_begin_get_user_data(duckdb_v2_vcatalog_transaction_begin_info_handle info, void **data,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->catalog.definition.GetUserData(); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_begin_get_catalog_state(duckdb_v2_vcatalog_transaction_begin_info_handle info,
                                                       void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->catalog.GetCatalogState(); });
}

DUCKDB_V2_ERROR duckdb_v2_vcatalog_transaction_begin_is_read_only(duckdb_v2_vcatalog_transaction_begin_info_handle info,
                                                                  bool *read_only, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(read_only);
	return WithErrorHandler(err, [&]() { *read_only = Convert(info)->read_only; });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_begin_set_transaction_state(duckdb_v2_vcatalog_transaction_begin_info_handle info,
                                                           duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { SetState(Convert(info)->transaction.transaction_state, *data); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_commit_get_user_data(duckdb_v2_vcatalog_transaction_commit_info_handle info, void **data,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->catalog.definition.GetUserData(); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_commit_get_catalog_state(duckdb_v2_vcatalog_transaction_commit_info_handle info,
                                                        void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->catalog.GetCatalogState(); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_commit_get_transaction_state(duckdb_v2_vcatalog_transaction_commit_info_handle info,
                                                            void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetState(Convert(info)->transaction.transaction_state); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_rollback_get_user_data(duckdb_v2_vcatalog_transaction_rollback_info_handle info,
                                                      void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->catalog.definition.GetUserData(); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_rollback_get_catalog_state(duckdb_v2_vcatalog_transaction_rollback_info_handle info,
                                                          void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->catalog.GetCatalogState(); });
}

DUCKDB_V2_ERROR
duckdb_v2_vcatalog_transaction_rollback_get_transaction_state(duckdb_v2_vcatalog_transaction_rollback_info_handle info,
                                                              void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = GetState(Convert(info)->transaction.transaction_state); });
}

DUCKDB_V2_ERROR duckdb_v2_connection_register_vcatalog(duckdb_v2_connection_handle conn,
                                                       duckdb_v2_vcatalog_handle vcatalog,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(err, [&]() { Convert(vcatalog)->Register(*Convert(conn)->context->db); });
}

DUCKDB_V2_ERROR duckdb_v2_extension_register_vcatalog(duckdb_v2_extension_handle extension,
                                                      duckdb_v2_vcatalog_handle vcatalog,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(vcatalog);
	return WithErrorHandler(
	    err, [&]() { Convert(vcatalog)->Register(GetExtensionLoader(extension).GetDatabaseInstance()); });
}
