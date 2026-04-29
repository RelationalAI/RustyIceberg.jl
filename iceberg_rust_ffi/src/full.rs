use std::ffi::{c_char, c_void, CStr};
use std::ptr;

use iceberg::io::FileIO;
use iceberg::scan::{TableScan, TableScanBuilder};
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};
use crate::scan_common::*;
use crate::{
    IcebergArrowStream, IcebergArrowStreamResponse, IcebergFileScanStream,
    IcebergFileScanStreamResponse, IcebergTable,
};

/// Holds state for a full table scan across its lifecycle:
///   1. Construction: `builder` is set, everything else is None/0.
///   2. Configuration: Julia calls with_* methods that transform `builder`.
///   3. Build: `builder` is consumed → `scan` is populated.
///   4. Stream: `scan` + `file_io` + `batch_size` are consumed by
///      `iceberg_arrow_stream` to create the file-parallel pipeline.
#[repr(C)]
pub struct IcebergScan {
    pub builder: Option<TableScanBuilder<'static>>,
    pub scan: Option<TableScan>,
    /// Serialization thread count (0 = auto-detect). Currently unused by the
    /// pipeline (kept for future use / backward compat with incremental scan).
    pub serialization_concurrency: usize,
    /// Cloned from the Table at construction time. Passed to the pipeline so
    /// each per-file ArrowReader can open its own parquet file.
    pub file_io: FileIO,
    /// Captured when Julia calls with_batch_size. Forwarded to each per-file
    /// ArrowReaderBuilder inside the pipeline.
    pub batch_size: Option<usize>,
    /// How many parquet files to process concurrently in the pipeline.
    /// Set by with_data_file_concurrency_limit (0 = auto-detect).
    pub file_concurrency: usize,
}

unsafe impl Send for IcebergScan {}

/// Create a new scan builder from an opened table.
/// Captures `file_io` from the table for later use by the pipeline.
#[no_mangle]
pub extern "C" fn iceberg_new_scan(table: *mut IcebergTable) -> *mut IcebergScan {
    if table.is_null() {
        return ptr::null_mut();
    }
    let table_ref = unsafe { &*table };
    let file_io = table_ref.table.file_io().clone();
    let scan_builder = table_ref.table.scan();
    Box::into_raw(Box::new(IcebergScan {
        builder: Some(scan_builder),
        scan: None,
        serialization_concurrency: 0,
        file_io,
        batch_size: None,
        file_concurrency: 0,
    }))
}

// ── Scan builder configuration (via macros from scan_common.rs) ─────────

impl_select_columns!(iceberg_select_columns, IcebergScan);

/// Set file concurrency. Hand-written (not macro-generated) because we need
/// to both (a) forward the value to the iceberg-rs scan builder and (b)
/// capture it in `file_concurrency` for our pipeline.
#[no_mangle]
pub extern "C" fn iceberg_scan_with_data_file_concurrency_limit(
    scan: &mut *mut IcebergScan,
    n: usize,
) -> CResult {
    if scan.is_null() || (*scan).is_null() {
        return CResult::Error;
    }
    let scan_ref = unsafe { Box::from_raw(*scan) };
    if scan_ref.builder.is_none() {
        return CResult::Error;
    }
    *scan = Box::into_raw(Box::new(IcebergScan {
        builder: scan_ref
            .builder
            .map(|b| b.with_data_file_concurrency_limit(n)),
        scan: scan_ref.scan,
        serialization_concurrency: scan_ref.serialization_concurrency,
        file_io: scan_ref.file_io,
        batch_size: scan_ref.batch_size,
        file_concurrency: n,
    }));
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

// ── Stream creation ─────────────────────────────────────────────────────
//
// Instead of calling iceberg-rs's `scan.to_arrow()` (which uses
// `try_for_each_concurrent` internally and interleaves batches across
// files in arbitrary order), we:
//   1. Call `scan.plan_files()` to get an ordered list of FileScanTasks.
//   2. Feed them into our own file-parallel pipeline
//      (ordered_file_pipeline.rs) which processes N files concurrently
//      but yields batches in strict file-then-row order.

export_runtime_op!(
    iceberg_arrow_stream,
    IcebergArrowStreamResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ptr = unsafe { &mut *scan };
        let scan_ref = &scan_ptr.scan;
        if scan_ref.is_none() {
            return Err(anyhow::anyhow!("Scan not initialized"));
        }

        // File pipeline concurrency (0 = auto-detect from available CPUs)
        let concurrency = scan_ptr.file_concurrency;
        let concurrency = if concurrency == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            concurrency
        };

        let file_io = scan_ptr.file_io.clone();
        let batch_size = scan_ptr.batch_size;

        Ok((scan_ref.as_ref().unwrap(), concurrency, file_io, batch_size))
    },
    result_tuple,
    async {
        let (scan_ref, concurrency, file_io, batch_size) = result_tuple;

        // Collect the ordered file task list from iceberg-rs.
        use futures::TryStreamExt;
        let tasks: Vec<iceberg::scan::FileScanTask> =
            scan_ref.plan_files().await?.try_collect().await?;

        // Hand off to the file-parallel pipeline.
        let stream = crate::ordered_file_pipeline::create_pipeline(
            tasks, file_io, batch_size, concurrency,
        )
        .await?;

        Ok::<IcebergArrowStream, anyhow::Error>(stream)
    },
    scan: *mut IcebergScan
);

impl_scan_free!(iceberg_scan_free, IcebergScan);

// ── Nested stream creation ───────────────────────────────────────────────
//
// Returns an IcebergFileScanStream whose items are per-file (filename,
// record_count, inner-batch-stream) tuples, yielded in strict file order.
// The flat iceberg_arrow_stream is implemented as a flattening wrapper over
// this same nested pipeline.

export_runtime_op!(
    iceberg_file_scan_stream,
    IcebergFileScanStreamResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ptr = unsafe { &mut *scan };
        let scan_ref = &scan_ptr.scan;
        if scan_ref.is_none() {
            return Err(anyhow::anyhow!("Scan not initialized"));
        }

        let concurrency = scan_ptr.file_concurrency;
        let concurrency = if concurrency == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            concurrency
        };

        let file_io = scan_ptr.file_io.clone();
        let batch_size = scan_ptr.batch_size;

        Ok((scan_ref.as_ref().unwrap(), concurrency, file_io, batch_size))
    },
    result_tuple,
    async {
        let (scan_ref, concurrency, file_io, batch_size) = result_tuple;

        use futures::TryStreamExt;
        let tasks: Vec<iceberg::scan::FileScanTask> =
            scan_ref.plan_files().await?.try_collect().await?;

        let stream = crate::ordered_file_pipeline::create_nested_pipeline(
            tasks, file_io, batch_size, concurrency,
        )
        .await?;

        Ok::<IcebergFileScanStream, anyhow::Error>(stream)
    },
    scan: *mut IcebergScan
);
