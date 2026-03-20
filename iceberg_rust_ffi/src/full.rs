use std::ffi::{c_char, c_void, CStr};
use std::ptr;

use futures::stream;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::{TableScan, TableScanBuilder};
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::scan_common::*;
use crate::{
    IcebergArrowReaderContext, IcebergArrowStream, IcebergArrowStreamResponse, IcebergFileScanTask,
    IcebergFileScanTaskStreamResponse, IcebergNextFileScanTaskResponse, IcebergTable,
    IcebergTaskBatchResponse,
};

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
    scan_val.builder = scan_val
        .builder
        .map(|b| b.with_data_file_concurrency_limit(n));
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

// ---------------------------------------------------------------------------
// Split-scan API: plan_files → create_reader → next_task → read_task
//
// This separates scan planning from reading, allowing multiple consumers
// to concurrently pull tasks from a shared stream and read them with a
// shared ArrowReader (which caches loaded delete files via Arc).
// ---------------------------------------------------------------------------

// Async: plan which files to read. Returns a shared task stream.
// Multiple consumers can call next_task on the same stream concurrently.
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
        Ok(scan_ref)
    },
    scan_ref,
    async {
        let stream = scan_ref.plan_files().await?;
        Ok::<crate::IcebergFileScanTaskStream, anyhow::Error>(crate::IcebergFileScanTaskStream {
            stream: AsyncMutex::new(stream),
        })
    },
    scan: *mut IcebergScan
);

/// Sync: create a shared ArrowReader context from the scan's configuration.
///
/// The returned context should be passed to every read_task call.
/// Cloning the inner ArrowReader is cheap and shares the delete-file cache.
///
/// For full scans, row_group_filtering is enabled and row_selection is
/// disabled, matching the defaults in TableScan::to_arrow().
#[no_mangle]
pub extern "C" fn iceberg_create_reader(
    scan: *mut IcebergScan,
) -> *mut crate::IcebergArrowReaderContext {
    if scan.is_null() {
        return std::ptr::null_mut();
    }
    let scan_ptr = unsafe { &*scan };

    let Some(file_io) = scan_ptr.file_io.as_ref() else {
        return std::ptr::null_mut();
    };

    let mut builder = ArrowReaderBuilder::new(file_io.clone())
        .with_row_group_filtering_enabled(true)
        .with_row_selection_enabled(false);

    if scan_ptr.data_file_concurrency_limit > 0 {
        builder = builder.with_data_file_concurrency_limit(scan_ptr.data_file_concurrency_limit);
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

    Box::into_raw(Box::new(crate::IcebergArrowReaderContext {
        reader: builder.build(),
        serialization_concurrency,
    }))
}

// Async: pull the next file scan task from the stream.
// Returns null payload when the stream is exhausted (end of planning).
// Safe to call concurrently from multiple consumers — the stream is
// behind an AsyncMutex.
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
        let mut guard = stream_ref.stream.lock().await;
        match guard.try_next().await {
            Ok(Some(task)) => Ok(Some(crate::IcebergFileScanTask { task })),
            Ok(None) => Ok(None),
            Err(e) => Err(anyhow::anyhow!("Error pulling next task: {}", e)),
        }
    },
    task_stream: *mut crate::IcebergFileScanTaskStream
);

// Async: read a single file scan task into an Arrow stream.
//
// Clones the ArrowReader from the shared context — this shares the
// CachingDeleteFileLoader cache across all concurrent consumers.
// The task is wrapped in a one-element stream and fed to
// ArrowReader::read().
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
        // Clone the reader — cheap, shares delete-file cache via Arc
        let reader = ctx.reader.clone();
        let serialization_concurrency = ctx.serialization_concurrency;

        // Consume the task — caller must NOT call free_task after this
        let task_ref = unsafe { Box::from_raw(task) };
        let file_scan_task = task_ref.task;

        Ok((reader, serialization_concurrency, file_scan_task))
    },
    result_tuple,
    async {
        let (reader, serialization_concurrency, file_scan_task) = result_tuple;

        // Wrap single task in a one-element stream for ArrowReader::read()
        let task_stream = stream::once(async { Ok(file_scan_task) }).boxed();
        let record_batch_stream = reader.read(task_stream)?;

        let serialized_stream = crate::transform_stream_with_parallel_serialization(
            record_batch_stream,
            serialization_concurrency,
        );

        Ok::<crate::IcebergArrowStream, anyhow::Error>(crate::IcebergArrowStream {
            stream: AsyncMutex::new(serialized_stream),
        })
    },
    reader_ctx: *mut crate::IcebergArrowReaderContext,
    task: *mut crate::IcebergFileScanTask
);

// ---------------------------------------------------------------------------
// Batch API: pull/read multiple tasks at once
// ---------------------------------------------------------------------------

// Async: pull up to `count` file scan tasks. count=0 drains all remaining.
export_runtime_op!(
    iceberg_next_file_scan_tasks,
    IcebergTaskBatchResponse,
    || {
        if task_stream.is_null() {
            return Err(anyhow::anyhow!("Null task stream pointer"));
        }
        let stream_ref = unsafe { &*task_stream };
        Ok((stream_ref, count))
    },
    result_tuple,
    async {
        let (stream_ref, count) = result_tuple;
        let mut guard = stream_ref.stream.lock().await;
        let mut tasks: Vec<IcebergFileScanTask> = Vec::new();
        loop {
            if count > 0 && tasks.len() >= count {
                break;
            }
            match guard.try_next().await {
                Ok(Some(task)) => tasks.push(IcebergFileScanTask { task }),
                Ok(None) => break,
                Err(e) => return Err(anyhow::anyhow!("Error pulling next task: {}", e)),
            }
        }
        // Convert to void pointers after async work is done (raw ptrs aren't Send)
        let ptrs = tasks.into_iter()
            .map(|t| Box::into_raw(Box::new(t)) as *mut c_void)
            .collect();
        Ok::<Vec<*mut c_void>, anyhow::Error>(ptrs)
    },
    task_stream: *mut crate::IcebergFileScanTaskStream,
    count: usize
);

// Async: read multiple file scan tasks into a single Arrow stream.
// Consumes all provided tasks — caller must NOT free them afterwards.
export_runtime_op!(
    iceberg_read_file_scan_tasks,
    IcebergArrowStreamResponse,
    || {
        if reader_ctx.is_null() {
            return Err(anyhow::anyhow!("Null reader context pointer"));
        }
        if tasks_ptr.is_null() || tasks_len == 0 {
            return Err(anyhow::anyhow!("Empty tasks array"));
        }
        let ctx = unsafe { &*reader_ctx };
        let reader = ctx.reader.clone();
        let serialization_concurrency = ctx.serialization_concurrency;

        // Consume all tasks from the pointer array
        let task_slice = unsafe { std::slice::from_raw_parts(tasks_ptr, tasks_len) };
        let file_scan_tasks: Vec<_> = task_slice
            .iter()
            .map(|&task_ptr| {
                let task_box = unsafe { Box::from_raw(task_ptr as *mut IcebergFileScanTask) };
                task_box.task
            })
            .collect();

        Ok((reader, serialization_concurrency, file_scan_tasks))
    },
    result_tuple,
    async {
        let (reader, serialization_concurrency, file_scan_tasks) = result_tuple;

        let task_stream = stream::iter(file_scan_tasks.into_iter().map(Ok)).boxed();
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
    tasks_ptr: *const *mut c_void,
    tasks_len: usize
);
