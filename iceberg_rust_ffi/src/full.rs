use std::ffi::{c_char, c_void, CStr};
use std::ptr;

use crate::scan_common::*;
use crate::{
    IcebergArrowStream, IcebergArrowStreamResponse, IcebergFileScanStream,
    IcebergFileScanStreamResponse, IcebergTable,
};
use iceberg::io::FileIO;
use iceberg::scan::{TableScan, TableScanBuilder};
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
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
    /// Dead field kept for FFI ABI compatibility and shape-symmetry with
    /// `IncrementalScan` (whose twin *is* still used, but only for the
    /// incremental delete-stream's parallel serialization — the per-file
    /// append/full pipeline uses `tokio::task::spawn_blocking` directly,
    /// with parallelism implicitly bounded by `file_prefetch_depth`).
    /// The FFI setter `iceberg_scan_with_serialization_concurrency_limit`
    /// writes this field, but nothing reads it.
    pub serialization_concurrency: usize,
    /// Cloned from the Table at construction time. Passed to the pipeline so
    /// each per-file ArrowReader can open its own parquet file.
    pub file_io: FileIO,
    /// Captured when Julia calls with_batch_size. Forwarded to each per-file
    /// ArrowReaderBuilder inside the pipeline.
    pub batch_size: Option<usize>,
    /// Dead field kept for FFI ABI compatibility and shape-symmetry with
    /// `IncrementalScan` (whose `file_concurrency` is likewise dead — both
    /// pipelines are now bounded by `file_prefetch_depth` alone via
    /// `Stream::buffered`). The FFI setter
    /// `iceberg_scan_with_data_file_concurrency_limit` is a no-op and
    /// does not write here; nothing reads this. Removing it would force
    /// `scan_common.rs` macros to switch to functional-record-update
    /// just to skip one field on one of the two structs they handle.
    pub file_concurrency: usize,
    /// How many FileScan items the full-scan pipeline keeps in flight (i.e.
    /// `Stream::buffered(prefetch_depth)`). 0 = auto (= cpu_count()). This is
    /// the only concurrency knob; see `nested_pipeline`'s module-level
    /// "Concurrency invariant" doc for the cap on alive `process_file` tasks.
    pub file_prefetch_depth: usize,
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
        file_prefetch_depth: 0,
    }))
}

// ── Scan builder configuration (via macros from scan_common.rs) ─────────

impl_select_columns!(iceberg_select_columns, IcebergScan);

/// FFI-stable no-op kept for API compatibility with Julia callers. The
/// full-scan pipeline used to take this as a separate concurrency cap, but
/// it overlapped with `file_prefetch_depth` and is now replaced by
/// `Stream::buffered(prefetch_depth)`. The iceberg-rs scan builder's own
/// `with_data_file_concurrency_limit` only affects `to_arrow()` (which we
/// don't call — we use `plan_files()` directly), so forwarding is unnecessary.
#[no_mangle]
pub extern "C" fn iceberg_scan_with_data_file_concurrency_limit(
    scan: &mut *mut IcebergScan,
    _n: usize,
) -> CResult {
    if scan.is_null() || (*scan).is_null() {
        return CResult::Error;
    }
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

impl_with_file_prefetch_depth!(iceberg_scan_with_file_prefetch_depth, IcebergScan);

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
//      (nested_pipeline.rs) which processes N files concurrently
//      but yields batches in strict file-then-row order.

/// Resolve the pipeline tuning parameters from a configured scan.
/// Returns `(prefetch_depth, file_io, batch_size)`. A stored
/// `file_prefetch_depth` of 0 means "auto" → defaults to `cpu_count()`.
fn resolve_pipeline_params(scan: &IcebergScan) -> (usize, FileIO, Option<usize>) {
    let prefetch_depth = if scan.file_prefetch_depth == 0 {
        crate::cpu_count()
    } else {
        scan.file_prefetch_depth
    };
    (prefetch_depth, scan.file_io.clone(), scan.batch_size)
}

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
        let (prefetch_depth, file_io, batch_size) = resolve_pipeline_params(scan_ptr);
        Ok((scan_ref.as_ref().unwrap(), prefetch_depth, file_io, batch_size))
    },
    result_tuple,
    async {
        let (scan_ref, prefetch_depth, file_io, batch_size) = result_tuple;

        // Collect the ordered file task list from iceberg-rs.
        use futures::TryStreamExt;
        let tasks: Vec<iceberg::scan::FileScanTask> =
            scan_ref.plan_files().await?.try_collect().await?;

        // Hand off to the file-parallel pipeline. `STATS.reset()` is called
        // inside `create_nested_pipeline`, which this composes with.
        let stream = crate::full_pipeline::create_pipeline(
            tasks, file_io, batch_size, prefetch_depth,
        )
        .await;

        Ok::<IcebergArrowStream, anyhow::Error>(stream)
    },
    scan: *mut IcebergScan
);

impl_scan_free!(iceberg_scan_free, IcebergScan);

// ── Nested stream creation ───────────────────────────────────────────────
//
// Returns an IcebergFileScanStream whose items are per-file (filename,
// record_count, inner-batch-stream) tuples, yielded in strict file order.
// The flat iceberg_arrow_stream above is implemented as a flattening wrapper
// over this same nested pipeline.

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
        let (prefetch_depth, file_io, batch_size) = resolve_pipeline_params(scan_ptr);
        Ok((scan_ref.as_ref().unwrap(), prefetch_depth, file_io, batch_size))
    },
    result_tuple,
    async {
        let (scan_ref, prefetch_depth, file_io, batch_size) = result_tuple;

        use futures::TryStreamExt;
        let tasks: Vec<iceberg::scan::FileScanTask> =
            scan_ref.plan_files().await?.try_collect().await?;

        // `STATS.reset()` is called inside `create_nested_pipeline`.
        let stream = crate::full_pipeline::create_full_scan_pipeline(
            tasks, file_io, batch_size, prefetch_depth,
        )
        .await;

        Ok::<IcebergFileScanStream, anyhow::Error>(stream)
    },
    scan: *mut IcebergScan
);
