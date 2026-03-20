use std::ffi::{c_char, c_void, CStr};
use std::ptr;

use iceberg::io::FileIO;
use iceberg::scan::{TableScan, TableScanBuilder};
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::scan_common::*;
use crate::{IcebergArrowStream, IcebergArrowStreamResponse, IcebergTable};

/// FFI wrapper for a regular (full) table scan.
///
/// In addition to the builder/scan from iceberg-rust, we store copies of
/// file_io, batch_size, and data_file_concurrency_limit so that
/// `create_reader` can build an ArrowReaderBuilder without accessing
/// private fields on TableScan.
#[repr(C)]
pub struct IcebergScan {
    pub builder: Option<TableScanBuilder<'static>>,
    pub scan: Option<TableScan>,
    /// 0 = auto-detect (num_cpus)
    pub serialization_concurrency: usize,
    /// Cloned from Table at scan creation time for use by create_reader
    pub file_io: Option<FileIO>,
    /// 0 = no batch size override (use reader default)
    pub batch_size: usize,
    /// 0 = auto-detect (num_cpus)
    pub data_file_concurrency_limit: usize,
}

unsafe impl Send for IcebergScan {}

/// Create a new scan builder
#[no_mangle]
pub extern "C" fn iceberg_new_scan(table: *mut IcebergTable) -> *mut IcebergScan {
    if table.is_null() {
        return ptr::null_mut();
    }
    let table_ref = unsafe { &*table };
    // Clone FileIO for later use by create_reader.
    // FileIO is Arc-based internally so this is cheap.
    let file_io = table_ref.table.file_io().clone();
    let scan_builder = table_ref.table.scan();
    Box::into_raw(Box::new(IcebergScan {
        builder: Some(scan_builder),
        scan: None,
        serialization_concurrency: 0,
        file_io: Some(file_io),
        batch_size: 0,
        data_file_concurrency_limit: 0,
    }))
}

// Use macros from scan_common for shared functionality
impl_select_columns!(iceberg_select_columns, IcebergScan);

/// Sets data file concurrency and stores the value for create_reader.
#[no_mangle]
pub extern "C" fn iceberg_scan_with_data_file_concurrency_limit(
    scan: &mut *mut IcebergScan,
    n: usize,
) -> CResult {
    if scan.is_null() || (*scan).is_null() {
        return CResult::Error;
    }
    let mut scan_val = *unsafe { Box::from_raw(*scan) };
    if scan_val.builder.is_none() {
        *scan = Box::into_raw(Box::new(scan_val));
        return CResult::Error;
    }
    scan_val.builder = scan_val.builder.map(|b| b.with_data_file_concurrency_limit(n));
    scan_val.data_file_concurrency_limit = n;
    *scan = Box::into_raw(Box::new(scan_val));
    CResult::Ok
}

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

impl_with_serialization_concurrency_limit!(
    iceberg_scan_with_serialization_concurrency_limit,
    IcebergScan
);

impl_scan_builder_method!(
    iceberg_scan_with_snapshot_id,
    IcebergScan,
    snapshot_id,
    snapshot_id: i64
);

// Async function to initialize stream from a table scan
export_runtime_op!(
    iceberg_arrow_stream,
    IcebergArrowStreamResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ptr = unsafe { &*scan };
        let scan_ref = &scan_ptr.scan;
        if scan_ref.is_none() {
            return Err(anyhow::anyhow!("Scan not initialized"));
        }

        // Determine concurrency (0 = auto-detect)
        let serialization_concurrency = scan_ptr.serialization_concurrency;
        let serialization_concurrency = if serialization_concurrency == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            serialization_concurrency
        };

        Ok((scan_ref.as_ref().unwrap(), serialization_concurrency))
    },
    result_tuple,
    async {
        let (scan_ref, serialization_concurrency) = result_tuple;

        let stream = scan_ref.to_arrow().await?;

        // Transform stream: RecordBatch -> ArrowBatch with parallel serialization
        let serialized_stream = crate::transform_stream_with_parallel_serialization(
            stream,
            serialization_concurrency
        );

        Ok::<IcebergArrowStream, anyhow::Error>(IcebergArrowStream {
            stream: AsyncMutex::new(serialized_stream),
        })
    },
    scan: *mut IcebergScan
);

impl_scan_free!(iceberg_scan_free, IcebergScan);
