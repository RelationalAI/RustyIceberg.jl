use std::ffi::{c_char, c_void, CStr};
use std::ptr;

use iceberg::scan::incremental::{IncrementalTableScan, IncrementalTableScanBuilder};
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, Context, NotifyGuard, RawResponse,
    ResponseGuard, RT,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::scan_common::*;
use crate::{IcebergArrowStream, IcebergTable};

/// Sentinel value for optional snapshot IDs in the C API.
/// When -1 is passed as from_snapshot_id or to_snapshot_id, it means None (use default).
const SNAPSHOT_ID_NONE: i64 = -1;

/// Struct for incremental scan builder and scan
#[repr(C)]
pub struct IcebergIncrementalScan {
    pub builder: Option<IncrementalTableScanBuilder<'static>>,
    pub scan: Option<IncrementalTableScan>,
}

unsafe impl Send for IcebergIncrementalScan {}

/// Response type for unzipped streams (two separate Arrow streams)
#[repr(C)]
pub struct IcebergUnzippedStreamsResponse {
    result: CResult,
    inserts_stream: *mut IcebergArrowStream,
    deletes_stream: *mut IcebergArrowStream,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergUnzippedStreamsResponse {}

impl RawResponse for IcebergUnzippedStreamsResponse {
    type Payload = (IcebergArrowStream, IcebergArrowStream);

    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }

    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }

    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }

    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload {
            Some((inserts, deletes)) => {
                self.inserts_stream = Box::into_raw(Box::new(inserts));
                self.deletes_stream = Box::into_raw(Box::new(deletes));
            }
            None => {
                self.inserts_stream = ptr::null_mut();
                self.deletes_stream = ptr::null_mut();
            }
        }
    }
}

/// Create a new incremental scan builder
///
/// # Arguments
/// * `table` - The table to scan
/// * `from_snapshot_id` - Starting snapshot ID, or `SNAPSHOT_ID_NONE` (-1) to scan from the root (oldest) snapshot
/// * `to_snapshot_id` - Ending snapshot ID, or `SNAPSHOT_ID_NONE` (-1) to scan to the current (latest) snapshot
#[no_mangle]
pub extern "C" fn iceberg_new_incremental_scan(
    table: *mut IcebergTable,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> *mut IcebergIncrementalScan {
    if table.is_null() {
        return ptr::null_mut();
    }
    let table_ref = unsafe { &*table };

    // Convert SNAPSHOT_ID_NONE to None for optional snapshot IDs
    let from_id = if from_snapshot_id == SNAPSHOT_ID_NONE {
        None
    } else {
        Some(from_snapshot_id)
    };

    let to_id = if to_snapshot_id == SNAPSHOT_ID_NONE {
        None
    } else {
        Some(to_snapshot_id)
    };

    let scan_builder = table_ref.table.incremental_scan(from_id, to_id);
    Box::into_raw(Box::new(IcebergIncrementalScan {
        builder: Some(scan_builder),
        scan: None,
    }))
}

// Use macros from scan_common for shared functionality
impl_select_columns!(iceberg_incremental_select_columns, IcebergIncrementalScan);

impl_scan_builder_method!(
    iceberg_incremental_scan_with_data_file_concurrency_limit,
    IcebergIncrementalScan,
    with_concurrency_limit_data_files,
    n: usize
);

impl_scan_builder_method!(
    iceberg_incremental_scan_with_manifest_entry_concurrency_limit,
    IcebergIncrementalScan,
    with_concurrency_limit_manifest_entries,
    n: usize
);

impl_with_batch_size!(
    iceberg_incremental_scan_with_batch_size,
    IcebergIncrementalScan
);

impl_scan_builder_method!(
    iceberg_incremental_scan_with_file_column,
    IcebergIncrementalScan,
    with_file_column
);

impl_scan_build!(iceberg_incremental_scan_build, IcebergIncrementalScan);

// Get unzipped Arrow streams from incremental scan (async)
// Returns two separate streams: one for inserts, one for deletes
export_runtime_op!(
    iceberg_incremental_arrow_stream_unzipped,
    IcebergUnzippedStreamsResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ref = unsafe { &(*scan).scan };
        if scan_ref.is_none() {
            return Err(anyhow::anyhow!("Incremental scan not initialized"));
        }

        Ok(scan_ref.as_ref().unwrap())
    },
    scan_ref,
    async {
        // Get unzipped streams (separate append and delete streams)
        let (inserts_stream, deletes_stream) = scan_ref.to_unzipped_arrow().await?;

        let inserts = IcebergArrowStream {
            stream: AsyncMutex::new(inserts_stream),
        };

        let deletes = IcebergArrowStream {
            stream: AsyncMutex::new(deletes_stream),
        };

        Ok::<(IcebergArrowStream, IcebergArrowStream), anyhow::Error>((inserts, deletes))
    },
    scan: *mut IcebergIncrementalScan
);

impl_scan_free!(iceberg_free_incremental_scan, IcebergIncrementalScan);
