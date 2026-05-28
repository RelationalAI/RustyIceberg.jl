/// Transaction support for iceberg_rust_ffi
///
/// This module provides FFI bindings for the iceberg-rust Transaction API,
/// enabling Julia to create transactions, add data files via FastAppendAction,
/// and commit changes to Iceberg tables.
use crate::catalog::IcebergCatalog;
use crate::error_codes::{classified_error, classify_iceberg, IcebergErrorCode};
use crate::response::IcebergBoxedResponse;
use crate::table::IcebergTable;
use iceberg::spec::{DataContentType, DataFile, Datum, ManifestContentType};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use std::collections::HashMap;

// FFI exports
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};
use std::ffi::c_void;

/// Opaque transaction handle for FFI
/// Stores the Transaction struct which holds table reference and pending actions
pub struct IcebergTransaction {
    transaction: Option<Transaction>,
}

unsafe impl Send for IcebergTransaction {}
unsafe impl Sync for IcebergTransaction {}

impl IcebergTransaction {
    /// Create a new transaction from a table
    pub fn new(table: &iceberg::table::Table) -> Self {
        IcebergTransaction {
            transaction: Some(Transaction::new(table)),
        }
    }

    /// Take ownership of the inner transaction (for operations that consume it)
    pub fn take(&mut self) -> Option<Transaction> {
        self.transaction.take()
    }

    /// Replace the inner transaction
    pub fn replace(&mut self, tx: Transaction) {
        self.transaction = Some(tx);
    }
}

/// Opaque handle for data files produced by a writer
/// This holds the Vec<DataFile> that results from closing a writer
pub struct IcebergDataFiles {
    pub data_files: Vec<DataFile>,
}

unsafe impl Send for IcebergDataFiles {}
unsafe impl Sync for IcebergDataFiles {}

/// Opaque handle for accumulating data files for a FastAppendAction
///
/// Since iceberg-rust's FastAppendAction is not publicly exported, we store
/// the accumulated data files here and create the actual action at apply time.
pub struct IcebergFastAppendAction {
    data_files: Vec<DataFile>,
}

unsafe impl Send for IcebergFastAppendAction {}
unsafe impl Sync for IcebergFastAppendAction {}

impl IcebergFastAppendAction {
    /// Create a new empty fast append action
    pub fn new() -> Self {
        IcebergFastAppendAction {
            data_files: Vec::new(),
        }
    }

    /// Add data files to the accumulated list
    pub fn add_data_files(&mut self, files: Vec<DataFile>) {
        self.data_files.extend(files);
    }

    /// Take all accumulated data files
    pub fn take_data_files(&mut self) -> Vec<DataFile> {
        std::mem::take(&mut self.data_files)
    }
}

/// Opaque handle accumulating data files for an OverwriteAction.
#[derive(Default)]
pub struct IcebergOverwriteAction {
    added_files: Vec<DataFile>,
    deleted_files: Vec<DataFile>,
}

unsafe impl Send for IcebergOverwriteAction {}
unsafe impl Sync for IcebergOverwriteAction {}

impl IcebergOverwriteAction {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Type alias for transaction response
pub type IcebergTransactionResponse = IcebergBoxedResponse<IcebergTransaction>;

/// Type alias for data files list response
pub type IcebergDataFilesResponse = IcebergBoxedResponse<IcebergDataFiles>;

/// Free a transaction
#[no_mangle]
pub extern "C" fn iceberg_transaction_free(transaction: *mut IcebergTransaction) {
    if !transaction.is_null() {
        unsafe {
            let _ = Box::from_raw(transaction);
        }
    }
}

/// Free data files handle
#[no_mangle]
pub extern "C" fn iceberg_data_files_free(data_files: *mut IcebergDataFiles) {
    if !data_files.is_null() {
        unsafe {
            let _ = Box::from_raw(data_files);
        }
    }
}

/// Return the number of data files in the handle (0 if null).
#[no_mangle]
pub extern "C" fn iceberg_data_files_len(data_files: *const IcebergDataFiles) -> usize {
    if data_files.is_null() {
        return 0;
    }
    unsafe { (*data_files).data_files.len() }
}

/// Return a JSON array of objects describing every data file in the handle.
///
/// Each object contains all metadata fields from the Iceberg DataFile spec:
/// content, file_path, file_format, record_count, file_size_in_bytes,
/// partition_spec_id, column_sizes, value_counts, null_value_counts,
/// nan_value_counts, lower_bounds, upper_bounds, split_offsets,
/// sort_order_id, equality_ids, first_row_id, referenced_data_file,
/// content_offset, content_size_in_bytes.
///
/// Returns null on error or if `data_files` is null.
/// The caller must free the returned string with `iceberg_destroy_cstring`.
#[no_mangle]
pub extern "C" fn iceberg_data_files_to_json(
    data_files: *const IcebergDataFiles,
) -> *mut std::ffi::c_char {
    #[derive(serde::Serialize)]
    struct DataFileJson<'a> {
        content: &'static str,
        file_path: &'a str,
        file_format: String,
        record_count: u64,
        file_size_in_bytes: u64,
        column_sizes: &'a HashMap<i32, u64>,
        value_counts: &'a HashMap<i32, u64>,
        null_value_counts: &'a HashMap<i32, u64>,
        nan_value_counts: &'a HashMap<i32, u64>,
        lower_bounds: HashMap<i32, &'a Datum>,
        upper_bounds: HashMap<i32, &'a Datum>,
        split_offsets: Option<&'a [i64]>,
        sort_order_id: Option<i32>,
        equality_ids: Option<Vec<i32>>,
        first_row_id: Option<i64>,
        referenced_data_file: Option<String>,
        content_offset: Option<i64>,
        content_size_in_bytes: Option<i64>,
    }

    if data_files.is_null() {
        return std::ptr::null_mut();
    }
    let df_ref = unsafe { &*data_files };

    let entries: Vec<DataFileJson> = df_ref
        .data_files
        .iter()
        .map(|f| DataFileJson {
            content: match f.content_type() {
                DataContentType::Data => "data",
                DataContentType::PositionDeletes => "position_deletes",
                DataContentType::EqualityDeletes => "equality_deletes",
            },
            file_path: f.file_path(),
            file_format: f.file_format().to_string(),
            record_count: f.record_count(),
            file_size_in_bytes: f.file_size_in_bytes(),
            column_sizes: f.column_sizes(),
            value_counts: f.value_counts(),
            null_value_counts: f.null_value_counts(),
            nan_value_counts: f.nan_value_counts(),
            lower_bounds: f.lower_bounds().iter().map(|(k, v)| (*k, v)).collect(),
            upper_bounds: f.upper_bounds().iter().map(|(k, v)| (*k, v)).collect(),
            split_offsets: f.split_offsets(),
            sort_order_id: f.sort_order_id(),
            equality_ids: f.equality_ids(),
            first_row_id: f.first_row_id(),
            referenced_data_file: f.referenced_data_file(),
            content_offset: f.content_offset(),
            content_size_in_bytes: f.content_size_in_bytes(),
        })
        .collect();

    match serde_json::to_string(&entries) {
        Ok(json) => match std::ffi::CString::new(json) {
            Ok(c_str) => c_str.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        Err(_) => std::ptr::null_mut(),
    }
}

/// Free a fast append action
#[no_mangle]
pub extern "C" fn iceberg_fast_append_action_free(action: *mut IcebergFastAppendAction) {
    if !action.is_null() {
        unsafe {
            let _ = Box::from_raw(action);
        }
    }
}

/// Create a new FastAppendAction
///
/// This is a synchronous operation. The action can accumulate data files
/// via `iceberg_fast_append_action_add_data_files` before being applied
/// to the transaction with `iceberg_fast_append_action_apply`.
///
/// # Safety
/// The returned action must be freed with `iceberg_fast_append_action_free` when no longer needed.
#[no_mangle]
pub extern "C" fn iceberg_fast_append_action_new() -> *mut IcebergFastAppendAction {
    let action = IcebergFastAppendAction::new();
    Box::into_raw(Box::new(action))
}

/// Add data files to a FastAppendAction
///
/// This can be called multiple times to accumulate data files from multiple
/// writers before applying the action.
///
/// The data_files handle is consumed by this operation - the data files are
/// moved into the action and the handle becomes empty.
///
/// Returns 0 on success, non-zero on error.
#[no_mangle]
pub extern "C" fn iceberg_fast_append_action_add_data_files(
    action: *mut IcebergFastAppendAction,
    data_files: *mut IcebergDataFiles,
    error_message_out: *mut *mut std::ffi::c_char,
) -> i32 {
    let set_error = |msg: &str, out: *mut *mut std::ffi::c_char| {
        if !out.is_null() {
            if let Ok(c_str) = std::ffi::CString::new(msg) {
                unsafe {
                    *out = c_str.into_raw();
                }
            }
        }
    };

    if action.is_null() {
        set_error("Null action pointer provided", error_message_out);
        return 1;
    }
    if data_files.is_null() {
        set_error("Null data_files pointer provided", error_message_out);
        return 1;
    }

    let action_ref = unsafe { &mut *action };
    let df_ref = unsafe { &mut *data_files };

    // Take the data files
    let files = std::mem::take(&mut df_ref.data_files);

    // Add data files to the action
    action_ref.add_data_files(files);

    0
}

/// Apply a FastAppendAction to a transaction
///
/// This consumes the action's data files and applies them to the transaction.
/// The action handle should be freed after calling this.
///
/// Returns 0 on success, non-zero on error.
#[no_mangle]
pub extern "C" fn iceberg_fast_append_action_apply(
    action: *mut IcebergFastAppendAction,
    transaction: *mut IcebergTransaction,
    error_message_out: *mut *mut std::ffi::c_char,
) -> i32 {
    let set_error = |msg: &str, out: *mut *mut std::ffi::c_char| {
        if !out.is_null() {
            if let Ok(c_str) = std::ffi::CString::new(msg) {
                unsafe {
                    *out = c_str.into_raw();
                }
            }
        }
    };

    if action.is_null() {
        set_error("Null action pointer provided", error_message_out);
        return 1;
    }
    if transaction.is_null() {
        set_error("Null transaction pointer provided", error_message_out);
        return 1;
    }

    let action_ref = unsafe { &mut *action };
    let tx_ref = unsafe { &mut *transaction };

    // Take the accumulated data files
    let files = action_ref.take_data_files();

    // Take the transaction
    let tx = match tx_ref.take() {
        Some(t) => t,
        None => {
            set_error(
                &format!(
                    "{}\t{}\tTransaction already consumed",
                    IcebergErrorCode::STATE_TRANSACTION_CONSUMED as u32,
                    "Transaction has already been committed or rolled back"
                ),
                error_message_out,
            );
            return 1;
        }
    };

    // Create the actual FastAppendAction from the transaction and add all files
    let fast_append_action = tx.fast_append().add_data_files(files);

    // Apply the action to the transaction
    match fast_append_action.apply(tx) {
        Ok(new_tx) => {
            tx_ref.replace(new_tx);
            0
        }
        Err(e) => {
            set_error(
                &format!("Failed to apply fast append: {}", e),
                error_message_out,
            );
            1
        }
    }
}

/// Create a new transaction from a table
///
/// This is a synchronous operation since creating a transaction doesn't involve I/O.
/// Returns a pointer to the new transaction, or null if the table pointer is null.
///
/// # Safety
/// The table pointer must be valid and the returned transaction must be freed
/// with `iceberg_transaction_free` when no longer needed.
#[no_mangle]
pub extern "C" fn iceberg_transaction_new(table: *mut IcebergTable) -> *mut IcebergTransaction {
    if table.is_null() {
        return std::ptr::null_mut();
    }

    // Get reference to table
    let table_ref = unsafe { &*table };

    // Create transaction
    let tx = IcebergTransaction::new(&table_ref.table);

    Box::into_raw(Box::new(tx))
}

// Commit a transaction to the catalog
//
// This consumes the transaction and returns the updated table.
// After calling this, the transaction handle should be freed but the
// transaction itself has been consumed and cannot be used again.
export_runtime_op!(
    iceberg_transaction_commit,
    crate::IcebergTableResponse,
    || {
        if transaction.is_null() {
            return Err(classified_error(IcebergErrorCode::STATE_RESOURCE_FREED, "Resource has been freed", "Null transaction pointer provided"));
        }
        if catalog.is_null() {
            return Err(classified_error(IcebergErrorCode::STATE_RESOURCE_FREED, "Resource has been freed", "Null catalog pointer provided"));
        }

        // Get references
        let tx_ref = unsafe { &mut *transaction };
        let catalog_ref = unsafe { &*catalog };

        Ok((tx_ref, catalog_ref))
    },
    result_tuple,
    async {
        let (tx_ref, catalog_ref) = result_tuple;

        let tx = tx_ref.take()
            .ok_or_else(|| classified_error(IcebergErrorCode::STATE_TRANSACTION_CONSUMED, "Transaction has already been committed or rolled back", "Transaction already consumed"))?;

        let catalog = catalog_ref.get_catalog()
            .ok_or_else(|| classified_error(IcebergErrorCode::STATE_RESOURCE_FREED, "Resource has been freed", "Catalog not initialized"))?;

        let updated_table = tx.commit(catalog).await.map_err(|e| classify_iceberg(e))?;

        Ok::<IcebergTable, anyhow::Error>(IcebergTable { table: updated_table })
    },
    transaction: *mut IcebergTransaction,
    catalog: *mut IcebergCatalog
);

/// Free an overwrite action.
#[no_mangle]
pub extern "C" fn iceberg_overwrite_action_free(action: *mut IcebergOverwriteAction) {
    if !action.is_null() {
        unsafe {
            let _ = Box::from_raw(action);
        }
    }
}

/// Create a new OverwriteAction.
///
/// # Safety
/// The returned action must be freed with `iceberg_overwrite_action_free` when no longer needed.
#[no_mangle]
pub extern "C" fn iceberg_overwrite_action_new() -> *mut IcebergOverwriteAction {
    Box::into_raw(Box::new(IcebergOverwriteAction::new()))
}

/// Add data files to write in the overwrite snapshot.
///
/// The data_files handle is consumed — its files are moved into the action.
/// Returns 0 on success, non-zero on error.
#[no_mangle]
pub extern "C" fn iceberg_overwrite_action_add_data_files(
    action: *mut IcebergOverwriteAction,
    data_files: *mut IcebergDataFiles,
    error_message_out: *mut *mut std::ffi::c_char,
) -> i32 {
    let set_error = |msg: &str, out: *mut *mut std::ffi::c_char| {
        if !out.is_null() {
            if let Ok(c_str) = std::ffi::CString::new(msg) {
                unsafe {
                    *out = c_str.into_raw();
                }
            }
        }
    };
    if action.is_null() {
        set_error("Null action pointer provided", error_message_out);
        return 1;
    }
    if data_files.is_null() {
        set_error("Null data_files pointer provided", error_message_out);
        return 1;
    }
    let action_ref = unsafe { &mut *action };
    let df_ref = unsafe { &mut *data_files };
    action_ref
        .added_files
        .extend(std::mem::take(&mut df_ref.data_files));
    0
}

/// Mark data files for deletion in the overwrite snapshot.
///
/// The data_files handle is consumed — its files are moved into the action.
/// Returns 0 on success, non-zero on error.
#[no_mangle]
pub extern "C" fn iceberg_overwrite_action_delete_data_files(
    action: *mut IcebergOverwriteAction,
    data_files: *mut IcebergDataFiles,
    error_message_out: *mut *mut std::ffi::c_char,
) -> i32 {
    let set_error = |msg: &str, out: *mut *mut std::ffi::c_char| {
        if !out.is_null() {
            if let Ok(c_str) = std::ffi::CString::new(msg) {
                unsafe {
                    *out = c_str.into_raw();
                }
            }
        }
    };
    if action.is_null() {
        set_error("Null action pointer provided", error_message_out);
        return 1;
    }
    if data_files.is_null() {
        set_error("Null data_files pointer provided", error_message_out);
        return 1;
    }
    let action_ref = unsafe { &mut *action };
    let df_ref = unsafe { &mut *data_files };
    action_ref
        .deleted_files
        .extend(std::mem::take(&mut df_ref.data_files));
    0
}

/// Apply an OverwriteAction to a transaction.
///
/// Consumes the action's file lists and applies them to the transaction via
/// `Transaction::overwrite()`. The action handle should be freed after this call.
/// Returns 0 on success, non-zero on error.
#[no_mangle]
pub extern "C" fn iceberg_overwrite_action_apply(
    action: *mut IcebergOverwriteAction,
    transaction: *mut IcebergTransaction,
    error_message_out: *mut *mut std::ffi::c_char,
) -> i32 {
    let set_error = |msg: &str, out: *mut *mut std::ffi::c_char| {
        if !out.is_null() {
            if let Ok(c_str) = std::ffi::CString::new(msg) {
                unsafe {
                    *out = c_str.into_raw();
                }
            }
        }
    };
    if action.is_null() {
        set_error("Null action pointer provided", error_message_out);
        return 1;
    }
    if transaction.is_null() {
        set_error("Null transaction pointer provided", error_message_out);
        return 1;
    }
    let action_ref = unsafe { &mut *action };
    let tx_ref = unsafe { &mut *transaction };

    let added = std::mem::take(&mut action_ref.added_files);
    let deleted = std::mem::take(&mut action_ref.deleted_files);

    let tx = match tx_ref.take() {
        Some(t) => t,
        None => {
            set_error(
                &format!(
                    "{}\t{}\tTransaction already consumed",
                    IcebergErrorCode::STATE_TRANSACTION_CONSUMED as u32,
                    "Transaction has already been committed or rolled back"
                ),
                error_message_out,
            );
            return 1;
        }
    };

    let overwrite_action = tx
        .overwrite()
        .add_data_files(added)
        .delete_data_files(deleted);

    match overwrite_action.apply(tx) {
        Ok(new_tx) => {
            tx_ref.replace(new_tx);
            0
        }
        Err(e) => {
            set_error(
                &format!("Failed to apply overwrite: {}", e),
                error_message_out,
            );
            1
        }
    }
}

// List all live data files in the current snapshot of a table.
// Returns an IcebergDataFiles handle (free with iceberg_data_files_free).
// Returns an empty list if the table has no snapshot.
export_runtime_op!(
    iceberg_table_list_data_files,
    crate::IcebergDataFilesResponse,
    || {
        if table.is_null() {
            return Err(classified_error(
                IcebergErrorCode::STATE_RESOURCE_FREED,
                "Resource has been freed",
                "Null table pointer provided",
            ));
        }
        let table_ref = unsafe { &*table };
        Ok(table_ref)
    },
    table_ref,
    async {
        let table = &table_ref.table;
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok::<IcebergDataFiles, anyhow::Error>(IcebergDataFiles {
                data_files: vec![],
            });
        };

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load manifest list: {e}"))?;

        let mut data_files = Vec::new();
        for manifest_entry in manifest_list.entries() {
            if manifest_entry.content != ManifestContentType::Data {
                continue;
            }
            if !manifest_entry.has_added_files() && !manifest_entry.has_existing_files() {
                continue;
            }
            let manifest = manifest_entry
                .load_manifest(table.file_io())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to load manifest: {e}"))?;
            for entry in manifest.entries() {
                if entry.is_alive() {
                    data_files.push(entry.data_file().clone());
                }
            }
        }

        Ok::<IcebergDataFiles, anyhow::Error>(IcebergDataFiles { data_files })
    },
    table: *mut crate::table::IcebergTable
);
