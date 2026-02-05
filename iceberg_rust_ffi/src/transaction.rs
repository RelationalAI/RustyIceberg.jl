/// Transaction support for iceberg_rust_ffi
///
/// This module provides FFI bindings for the iceberg-rust Transaction API,
/// enabling Julia to create transactions, add data files via FastAppendAction,
/// and commit changes to Iceberg tables.
use crate::catalog::IcebergCatalog;
use crate::response::IcebergBoxedResponse;
use crate::table::IcebergTable;
use iceberg::spec::DataFile;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

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

/// Type alias for transaction response
pub type IcebergTransactionResponse = IcebergBoxedResponse<IcebergTransaction>;

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
            set_error("Transaction already consumed", error_message_out);
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
            return Err(anyhow::anyhow!("Null transaction pointer provided"));
        }
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        // Get references
        let tx_ref = unsafe { &mut *transaction };
        let catalog_ref = unsafe { &*catalog };

        Ok((tx_ref, catalog_ref))
    },
    result_tuple,
    async {
        let (tx_ref, catalog_ref) = result_tuple;

        // Take the transaction
        let tx = tx_ref.take()
            .ok_or_else(|| anyhow::anyhow!("Transaction already consumed"))?;

        // Get the catalog reference
        let catalog = catalog_ref.get_catalog()
            .ok_or_else(|| anyhow::anyhow!("Catalog not initialized"))?;

        // Commit the transaction
        let updated_table = tx.commit(catalog).await?;

        Ok::<IcebergTable, anyhow::Error>(IcebergTable { table: updated_table })
    },
    transaction: *mut IcebergTransaction,
    catalog: *mut IcebergCatalog
);
