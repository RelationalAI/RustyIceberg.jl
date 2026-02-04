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

/// Add data files to a transaction using fast append
///
/// This is a synchronous operation that:
/// 1. Creates a FastAppendAction
/// 2. Adds the data files from the handle
/// 3. Applies the action to the transaction (which just adds it to the action list)
///
/// The data_files handle is consumed by this operation - the data files are
/// moved into the action and the handle becomes empty.
///
/// Returns 0 on success, non-zero on error.
/// On error, the error message is written to error_message_out if provided.
#[no_mangle]
pub extern "C" fn iceberg_transaction_fast_append(
    transaction: *mut IcebergTransaction,
    data_files: *mut IcebergDataFiles,
    error_message_out: *mut *mut std::ffi::c_char,
) -> i32 {
    // Helper to set error message
    let set_error = |msg: &str, out: *mut *mut std::ffi::c_char| {
        if !out.is_null() {
            if let Ok(c_str) = std::ffi::CString::new(msg) {
                unsafe {
                    *out = c_str.into_raw();
                }
            }
        }
    };

    if transaction.is_null() {
        set_error("Null transaction pointer provided", error_message_out);
        return 1;
    }
    if data_files.is_null() {
        set_error("Null data_files pointer provided", error_message_out);
        return 1;
    }

    // Get mutable references
    let tx_ref = unsafe { &mut *transaction };
    let df_ref = unsafe { &mut *data_files };

    // Take the transaction and data files
    let tx = match tx_ref.take() {
        Some(t) => t,
        None => {
            set_error("Transaction already consumed", error_message_out);
            return 1;
        }
    };
    let files = std::mem::take(&mut df_ref.data_files);

    // Create fast append action and add files
    let action = tx.fast_append().add_data_files(files);

    // Apply the action to the transaction (this is synchronous)
    match action.apply(tx) {
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
