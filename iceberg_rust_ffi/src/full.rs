use std::ffi::{c_char, c_void, CStr};
use std::ptr;

use crate::error_codes::{classified_error, classify_iceberg, IcebergErrorCode};
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
///   1. Construction: `builder` is set from the table + perf config; the perf
///      fields (`batch_size`, `file_prefetch_depth`, `serialization_concurrency`)
///      are stored on the struct.
///   2. Configuration: Julia calls non-perf with_* methods (select, file/pos
///      column, snapshot id) that transform `builder`.
///   3. Build: `builder` is consumed → `scan` is populated.
///   4. Stream: `scan` + `file_io` + `batch_size` are consumed by
///      `iceberg_arrow_stream` to create the file-parallel pipeline.
#[repr(C)]
pub struct IcebergScan {
    pub builder: Option<TableScanBuilder<'static>>,
    pub scan: Option<TableScan>,
    /// Dead field kept for shape-symmetry with `IncrementalScan` (whose twin
    /// *is* still used, but only for the incremental delete-stream's parallel
    /// serialization — the per-file append/full pipeline uses
    /// `tokio::task::spawn_blocking` directly, with parallelism implicitly
    /// bounded by `file_prefetch_depth`). Populated from the perf config at
    /// construction, but nothing reads it on the full-scan path.
    pub serialization_concurrency: usize,
    /// Cloned from the Table at construction time. Passed to the pipeline so
    /// each per-file ArrowReader can open its own parquet file.
    pub file_io: FileIO,
    /// Set from the perf config at construction. Forwarded to each per-file
    /// `ArrowReaderBuilder` inside the pipeline.
    pub batch_size: usize,
    /// How many FileScan items the full-scan pipeline keeps in flight (i.e.
    /// `Stream::buffered(prefetch_depth)`). Supplied verbatim by Julia (the
    /// single source of tuning defaults) and used as-is. See `nested_pipeline`'s
    /// module-level "Concurrency invariant" doc for the cap on alive
    /// `process_file` tasks.
    pub file_prefetch_depth: usize,
}

unsafe impl Send for IcebergScan {}

/// Create a new scan from an opened table and a complete `IcebergPerfConfigFFI`
/// (supplied by Julia — the single source of tuning defaults). The manifest
/// concurrency knobs and batch size are applied to the iceberg-rs builder here;
/// `batch_size` / `file_prefetch_depth` / `serialization_concurrency` are also
/// stored on the struct for the pipeline to read. Captures `file_io` from the
/// table for later use by the pipeline.
#[no_mangle]
pub extern "C" fn iceberg_new_scan(
    table: *mut IcebergTable,
    perf: IcebergPerfConfigFFI,
) -> *mut IcebergScan {
    if table.is_null() {
        return ptr::null_mut();
    }
    let table_ref = unsafe { &*table };
    let file_io = table_ref.table.file_io().clone();
    let scan_builder = table_ref
        .table
        .scan()
        .with_batch_size(Some(perf.batch_size as usize))
        .with_manifest_file_concurrency_limit(perf.manifest_file_concurrency_limit as usize)
        .with_manifest_entry_concurrency_limit(perf.manifest_entry_concurrency_limit as usize);
    Box::into_raw(Box::new(IcebergScan {
        builder: Some(scan_builder),
        scan: None,
        serialization_concurrency: perf.serialization_concurrency_limit as usize,
        file_io,
        batch_size: perf.batch_size as usize,
        file_prefetch_depth: perf.file_prefetch_depth as usize,
    }))
}

// ── Scan builder configuration (via macros from scan_common.rs) ─────────

impl_select_columns!(iceberg_select_columns, IcebergScan);

impl_scan_builder_method!(iceberg_scan_with_file_column, IcebergScan, with_file_column);

impl_scan_builder_method!(iceberg_scan_with_pos_column, IcebergScan, with_pos_column);

impl_scan_build!(iceberg_scan_build, IcebergScan);

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
//   1. Call `scan.plan_files()` to get an ordered stream of FileScanTasks.
//   2. Feed that stream straight into our own file-parallel pipeline
//      (nested_pipeline.rs), which processes N files concurrently but
//      yields batches in strict file-then-row order.

/// Resolve the pipeline tuning parameters from a configured scan.
/// Returns `(prefetch_depth, file_io, batch_size)`. Both `file_prefetch_depth`
/// and `batch_size` are supplied verbatim by Julia at construction (the single
/// source of tuning defaults) and used as-is — there is no "0 = auto" fallback.
fn resolve_pipeline_params(scan: &IcebergScan) -> anyhow::Result<(usize, FileIO, usize)> {
    Ok((
        scan.file_prefetch_depth,
        scan.file_io.clone(),
        scan.batch_size,
    ))
}

export_runtime_op!(
    iceberg_arrow_stream,
    IcebergArrowStreamResponse,
    || {
        if scan.is_null() {
            return Err(classified_error(IcebergErrorCode::STATE_RESOURCE_FREED, "Resource has been freed", "Null scan pointer provided"));
        }
        let scan_ptr = unsafe { &mut *scan };
        let scan_ref = &scan_ptr.scan;
        if scan_ref.is_none() {
            return Err(classified_error(IcebergErrorCode::STATE_RESOURCE_FREED, "Resource has been freed", "Scan not initialized"));
        }
        let (prefetch_depth, file_io, batch_size) = resolve_pipeline_params(scan_ptr)?;
        Ok((scan_ref.as_ref().unwrap(), prefetch_depth, file_io, batch_size))
    },
    result_tuple,
    async {
        let (scan_ref, prefetch_depth, file_io, batch_size) = result_tuple;

        // Hand off to the file-parallel pipeline. We pass the planner's
        // task stream directly — no `try_collect` into a `Vec`, since the
        // nested pipeline already drives `plan_files()` through
        // `Stream::buffered(prefetch_depth)`.
        let tasks = scan_ref.plan_files().await.map_err(|e| classify_iceberg(e))?;
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
            return Err(classified_error(IcebergErrorCode::STATE_RESOURCE_FREED, "Resource has been freed", "Null scan pointer provided"));
        }
        let scan_ptr = unsafe { &mut *scan };
        let scan_ref = &scan_ptr.scan;
        if scan_ref.is_none() {
            return Err(classified_error(IcebergErrorCode::STATE_RESOURCE_FREED, "Resource has been freed", "Scan not initialized"));
        }
        let (prefetch_depth, file_io, batch_size) = resolve_pipeline_params(scan_ptr)?;
        Ok((scan_ref.as_ref().unwrap(), prefetch_depth, file_io, batch_size))
    },
    result_tuple,
    async {
        let (scan_ref, prefetch_depth, file_io, batch_size) = result_tuple;

        // Pass the planner's task stream directly into the pipeline.
        let tasks = scan_ref.plan_files().await.map_err(|e| classify_iceberg(e))?;
        let stream = crate::full_pipeline::create_full_scan_pipeline(
            tasks, file_io, batch_size, prefetch_depth,
        )
        .await;

        Ok::<IcebergFileScanStream, anyhow::Error>(stream)
    },
    scan: *mut IcebergScan
);
