use std::ffi::{c_char, c_void, CStr};
use std::ptr;

use iceberg::scan::{TableScan, TableScanBuilder};
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::scan_common::*;
use crate::{IcebergArrowStream, IcebergArrowStreamResponse, IcebergTable};

/// Struct for regular (full) scan builder and scan
#[repr(C)]
pub struct IcebergScan {
    pub builder: Option<TableScanBuilder<'static>>,
    pub scan: Option<TableScan>,
}

unsafe impl Send for IcebergScan {}

/// Create a new scan builder
#[no_mangle]
pub extern "C" fn iceberg_new_scan(table: *mut IcebergTable) -> *mut IcebergScan {
    if table.is_null() {
        return ptr::null_mut();
    }
    let table_ref = unsafe { &*table };
    let scan_builder = table_ref.table.scan();
    Box::into_raw(Box::new(IcebergScan {
        builder: Some(scan_builder),
        scan: None,
    }))
}

// Use macros from scan_common for shared functionality
impl_select_columns!(iceberg_select_columns, IcebergScan);

impl_scan_builder_method!(
    iceberg_scan_with_data_file_concurrency_limit,
    IcebergScan,
    with_data_file_concurrency_limit,
    n: usize
);

impl_scan_builder_method!(
    iceberg_scan_with_manifest_file_concurrency_limit,
    IcebergScan,
    with_manifest_file_concurrency_limit,
    n: usize
);

impl_scan_builder_method!(
    iceberg_scan_with_manifest_entry_concurrency_limit,
    IcebergScan,
    with_manifest_entry_concurrency_limit,
    n: usize
);

impl_with_batch_size!(iceberg_scan_with_batch_size, IcebergScan);

impl_scan_builder_method!(iceberg_scan_with_file_column, IcebergScan, with_file_column);

impl_scan_builder_method!(iceberg_scan_with_pos_column, IcebergScan, with_pos_column);

impl_scan_build!(iceberg_scan_build, IcebergScan);

// Async function to initialize stream from a table scan
export_runtime_op!(
    iceberg_arrow_stream,
    IcebergArrowStreamResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ref = unsafe { &(*scan).scan };
        if scan_ref.is_none() {
            return Err(anyhow::anyhow!("Scan not initialized"));
        }

        Ok(scan_ref.as_ref().unwrap())
    },
    scan_ref,
    async {
        let stream = scan_ref.to_arrow().await?;
        Ok::<IcebergArrowStream, anyhow::Error>(IcebergArrowStream {
            stream: AsyncMutex::new(stream),
        })
    },
    scan: *mut IcebergScan
);

impl_scan_free!(iceberg_scan_free, IcebergScan);
