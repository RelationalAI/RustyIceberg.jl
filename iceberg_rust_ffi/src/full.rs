use std::ffi::{c_char, c_void, CStr};
use std::ptr;

use futures::{stream, StreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::{TableScan, TableScanBuilder};
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::scan_common::*;
use crate::{
    IcebergArrowReaderContext, IcebergArrowStream,
    IcebergArrowStreamResponse, IcebergFileScanTask, IcebergFileScanTaskStream,
    IcebergFileScanTaskStreamResponse, IcebergNextFileScanTaskResponse, IcebergTable,
};

/// Struct for regular (full) scan builder and scan
#[repr(C)]
pub struct IcebergScan {
    pub builder: Option<TableScanBuilder<'static>>,
    pub scan: Option<TableScan>,
    pub file_io: Option<FileIO>,
    /// 0 = auto-detect (num_cpus)
    pub serialization_concurrency: usize,
    /// stored separately so create_reader can use it; 0 = use reader default
    pub data_file_concurrency_limit: usize,
    /// stored separately so create_reader can use it; 0 = no override
    pub batch_size: usize,
    /// File scan task prefetch depth for plan_files; 0 = DEFAULT_TASK_PREFETCH_DEPTH
    pub task_prefetch_depth: usize,
}

unsafe impl Send for IcebergScan {}

/// Create a new scan builder
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
        file_io: Some(file_io),
        serialization_concurrency: 1,
        data_file_concurrency_limit: 0,
        batch_size: 0,
        task_prefetch_depth: 0,
    }))
}

// Use macros from scan_common for shared functionality
impl_select_columns!(iceberg_select_columns, IcebergScan);

/// Sets data file concurrency on the builder and stores the value for create_reader.
#[no_mangle]
pub extern "C" fn iceberg_scan_with_data_file_concurrency_limit(
    scan: &mut *mut IcebergScan,
    n: usize,
) -> CResult {
    if scan.is_null() || (*scan).is_null() {
        return CResult::Error;
    }
    let scan_ref = unsafe { &mut **scan };
    let builder = scan_ref.builder.take();
    if builder.is_none() {
        return CResult::Error;
    }
    scan_ref.builder = builder.map(|b| b.with_data_file_concurrency_limit(n));
    scan_ref.data_file_concurrency_limit = n;
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

/// Sets batch size on the builder and stores the value for create_reader.
#[no_mangle]
pub extern "C" fn iceberg_scan_with_batch_size(
    scan: &mut *mut IcebergScan,
    n: usize,
) -> CResult {
    if scan.is_null() || (*scan).is_null() {
        return CResult::Error;
    }
    let scan_ref = unsafe { &mut **scan };
    if scan_ref.builder.is_none() {
        return CResult::Error;
    }
    let builder = scan_ref.builder.take();
    scan_ref.builder = builder.map(|b| b.with_batch_size(Some(n)));
    scan_ref.batch_size = n;
    CResult::Ok
}

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

/// Sets the file scan task prefetch depth (tasks buffered ahead of consumer).
#[no_mangle]
pub extern "C" fn iceberg_scan_with_task_prefetch_depth(
    scan: &mut *mut IcebergScan,
    n: usize,
) -> CResult {
    if scan.is_null() || (*scan).is_null() {
        return CResult::Error;
    }
    unsafe { (*(*scan)).task_prefetch_depth = n };
    CResult::Ok
}

// Async: get a single Arrow stream for the entire scan (non-split API)
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

// ---------------------------------------------------------------------------
// Split-scan API: plan_files → create_reader → next_file_scan → read_file_scan
// ---------------------------------------------------------------------------

// Async: plan which files to read. Returns a concurrent-safe task stream.
export_runtime_op!(
    iceberg_plan_files,
    IcebergFileScanTaskStreamResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ptr = unsafe { &*scan };
        let scan_ref = scan_ptr.scan.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Scan not built — call build! first"))?;
        let task_prefetch_depth = if scan_ptr.task_prefetch_depth == 0 {
            crate::table::DEFAULT_TASK_PREFETCH_DEPTH
        } else {
            scan_ptr.task_prefetch_depth
        };
        Ok((scan_ref, task_prefetch_depth))
    },
    result_tuple,
    async {
        let (scan_ref, task_prefetch_depth) = result_tuple;
        let stream = scan_ref.plan_files().await?;
        Ok::<IcebergFileScanTaskStream, anyhow::Error>(
            IcebergFileScanTaskStream::new(stream, task_prefetch_depth)
        )
    },
    scan: *mut IcebergScan
);

/// Sync: create a shared ArrowReader context from the scan's configuration.
/// Pass the returned context to every read_file_scan call.
/// `reader_concurrency` overrides the scan-level data_file_concurrency_limit when > 0.
#[no_mangle]
pub extern "C" fn iceberg_create_reader(
    scan: *mut IcebergScan,
    reader_concurrency: usize,
) -> *mut IcebergArrowReaderContext {
    if scan.is_null() {
        return ptr::null_mut();
    }
    let scan_ptr = unsafe { &*scan };
    let Some(file_io) = scan_ptr.file_io.as_ref() else {
        return ptr::null_mut();
    };

    let mut builder = ArrowReaderBuilder::new(file_io.clone())
        .with_row_group_filtering_enabled(true)
        .with_row_selection_enabled(false);

    let data_file_concurrency = if reader_concurrency > 0 {
        reader_concurrency
    } else {
        scan_ptr.data_file_concurrency_limit
    };
    if data_file_concurrency > 0 {
    builder = builder.with_data_file_concurrency_limit(data_file_concurrency);
    }
    if scan_ptr.batch_size > 0 {
        builder = builder.with_batch_size(scan_ptr.batch_size);
    }

    let serialization_concurrency = if scan_ptr.serialization_concurrency == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        scan_ptr.serialization_concurrency
    };

    let batch_size = if scan_ptr.batch_size > 0 { Some(scan_ptr.batch_size) } else { None };

    Box::into_raw(Box::new(IcebergArrowReaderContext {
        reader: builder.build(),
        serialization_concurrency,
        batch_size,
    }))
}

// Async: pull the next file scan task. Returns null payload at end-of-stream.
export_runtime_op!(
    iceberg_next_file_scan_task,
    IcebergNextFileScanTaskResponse,
    || {
        if task_stream.is_null() {
            return Err(anyhow::anyhow!("Null task stream pointer"));
        }
        let stream_ref = unsafe { &*task_stream };
        Ok(stream_ref)
    },
    stream_ref,
    async {
        stream_ref.next().await
    },
    task_stream: *mut IcebergFileScanTaskStream
);

// Async: read a single file scan task into an Arrow stream.
// Clones the ArrowReader (cheap — shares delete-file cache via Arc).
// Consumes the task — caller must NOT call free_file_scan after this.
export_runtime_op!(
    iceberg_read_file_scan_task,
    IcebergArrowStreamResponse,
    || {
        if reader_ctx.is_null() {
            return Err(anyhow::anyhow!("Null reader context pointer"));
        }
        if task.is_null() {
            return Err(anyhow::anyhow!("Null task pointer"));
        }
        let ctx = unsafe { &*reader_ctx };
        let reader = ctx.reader.clone();
        let serialization_concurrency = ctx.serialization_concurrency;
        let task_ref = unsafe { Box::from_raw(task) };
        let file_scan_task = task_ref.task;
        Ok((reader, serialization_concurrency, file_scan_task))
    },
    result_tuple,
    async {
        let (reader, serialization_concurrency, file_scan_task) = result_tuple;

        // Wrap the single task in a stream for the reader API, which expects a stream
        // of tasks
        let task_stream = stream::once(async { Ok(file_scan_task) }).boxed();
        let record_batch_stream = reader.read(task_stream)?;
        let serialized_stream = crate::transform_stream_with_parallel_serialization(
            record_batch_stream,
            serialization_concurrency,
        );
        Ok::<IcebergArrowStream, anyhow::Error>(IcebergArrowStream {
            stream: AsyncMutex::new(serialized_stream),
        })
    },
    reader_ctx: *mut IcebergArrowReaderContext,
    task: *mut IcebergFileScanTask
);

/// Returns the record count for a file scan task, or -1 if not available.
#[no_mangle]
pub extern "C" fn iceberg_file_scan_task_record_count(
    task: *const IcebergFileScanTask,
) -> i64 {
    if task.is_null() {
        return -1;
    }
    let task_ref = unsafe { &*task };
    task_ref.task.record_count.map_or(-1, |n| n as i64)
}

/// Returns the data file path for a file scan task as an allocated C string.
/// Caller must free with `iceberg_destroy_cstring`.
#[no_mangle]
pub extern "C" fn iceberg_file_scan_task_file_path(
    task: *const IcebergFileScanTask,
) -> *mut std::ffi::c_char {
    if task.is_null() {
        return std::ptr::null_mut();
    }
    let task_ref = unsafe { &*task };
    match std::ffi::CString::new(task_ref.task.data_file_path.as_str()) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}
