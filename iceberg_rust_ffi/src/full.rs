use std::ffi::{c_char, c_void, CStr};
use std::ptr;

use futures::{stream, StreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::TableScan;
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::scan_common::*;
use crate::{
    IcebergArrowReaderContext, IcebergArrowStream, IcebergArrowStreamResponse, IcebergFileScanTask,
    IcebergFileScanTaskStream, IcebergFileScanTaskStreamResponse, IcebergNextFileScanTaskResponse,
    IcebergTable,
};

const SNAPSHOT_ID_NONE: i64 = -1;

/// Struct for a built full table scan.
#[repr(C)]
pub struct IcebergScan {
    pub scan: TableScan,
    pub file_io: FileIO,
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

/// Create and build a new scan.
///
/// Numeric parameters use -1 as "not set / use default" sentinel.
/// Returns NULL on any error (invalid table, failed build, bad column names, etc.)
#[no_mangle]
pub extern "C" fn iceberg_new_scan(
    table: *mut IcebergTable,
    column_names: *const *const c_char, // NULL = all columns
    column_names_len: usize,
    data_file_concurrency_limit: i64,      // -1 = use reader default
    manifest_file_concurrency_limit: i64,  // -1 = don't set
    manifest_entry_concurrency_limit: i64, // -1 = don't set
    batch_size: i64,                       // -1 = no override
    file_column: u8,                       // 0 = no, non-zero = yes
    pos_column: u8,
    serialization_concurrency: i64, // -1 = auto-detect
    snapshot_id: i64,               // -1 = current (SNAPSHOT_ID_NONE)
    task_prefetch_depth: i64,       // -1 = use DEFAULT_TASK_PREFETCH_DEPTH
) -> *mut IcebergScan {
    if table.is_null() {
        return ptr::null_mut();
    }
    let table_ref = unsafe { &*table };
    let file_io = table_ref.table.file_io().clone();
    let mut builder = table_ref.table.scan();

    // Apply column selection
    if !column_names.is_null() && column_names_len > 0 {
        let mut columns = Vec::new();
        for i in 0..column_names_len {
            let col_ptr = unsafe { *column_names.add(i) };
            if col_ptr.is_null() {
                return ptr::null_mut();
            }
            let col_str = unsafe {
                match CStr::from_ptr(col_ptr).to_str() {
                    Ok(s) => s,
                    Err(_) => return ptr::null_mut(),
                }
            };
            columns.push(col_str.to_string());
        }
        builder = builder.select(columns);
    }

    // Apply builder methods
    if manifest_file_concurrency_limit >= 0 {
        builder =
            builder.with_manifest_file_concurrency_limit(manifest_file_concurrency_limit as usize);
    }
    if manifest_entry_concurrency_limit >= 0 {
        builder = builder
            .with_manifest_entry_concurrency_limit(manifest_entry_concurrency_limit as usize);
    }
    if batch_size >= 0 {
        builder = builder.with_batch_size(Some(batch_size as usize));
    }
    if file_column != 0 {
        builder = builder.with_file_column();
    }
    if pos_column != 0 {
        builder = builder.with_pos_column();
    }
    if snapshot_id != SNAPSHOT_ID_NONE {
        builder = builder.snapshot_id(snapshot_id);
    }
    if data_file_concurrency_limit >= 0 {
        builder = builder.with_data_file_concurrency_limit(data_file_concurrency_limit as usize);
    }

    let scan = match builder.build() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let resolved_serialization_concurrency = if serialization_concurrency < 0 {
        0
    } else {
        serialization_concurrency as usize
    };
    let resolved_data_file_concurrency = if data_file_concurrency_limit < 0 {
        0
    } else {
        data_file_concurrency_limit as usize
    };
    let resolved_batch_size = if batch_size < 0 {
        0
    } else {
        batch_size as usize
    };
    let resolved_task_prefetch_depth = if task_prefetch_depth < 0 {
        0
    } else {
        task_prefetch_depth as usize
    };
    tracing::info!(
        data_file_concurrency = resolved_data_file_concurrency,
        manifest_file_concurrency = manifest_file_concurrency_limit,
        manifest_entry_concurrency = manifest_entry_concurrency_limit,
        batch_size = resolved_batch_size,
        serialization_concurrency = resolved_serialization_concurrency,
        task_prefetch_depth = resolved_task_prefetch_depth,
        "iceberg_new_scan"
    );
    Box::into_raw(Box::new(IcebergScan {
        scan,
        file_io,
        serialization_concurrency: resolved_serialization_concurrency,
        data_file_concurrency_limit: resolved_data_file_concurrency,
        batch_size: resolved_batch_size,
        task_prefetch_depth: resolved_task_prefetch_depth,
    }))
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

        // Determine concurrency (0 = auto-detect)
        let serialization_concurrency = scan_ptr.serialization_concurrency;
        let serialization_concurrency = if serialization_concurrency == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            serialization_concurrency
        };

        Ok((&scan_ptr.scan, serialization_concurrency))
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
// Split-scan API: plan_files → create_reader → next_file → read_file_scan
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
        let task_prefetch_depth = if scan_ptr.task_prefetch_depth == 0 {
            crate::table::DEFAULT_TASK_PREFETCH_DEPTH
        } else {
            scan_ptr.task_prefetch_depth
        };
        Ok((&scan_ptr.scan, task_prefetch_depth))
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
    batch_prefetch_depth: usize,
) -> *mut IcebergArrowReaderContext {
    if scan.is_null() {
        return ptr::null_mut();
    }
    let scan_ptr = unsafe { &*scan };

    let mut builder = ArrowReaderBuilder::new(scan_ptr.file_io.clone())
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

    let batch_size = if scan_ptr.batch_size > 0 {
        Some(scan_ptr.batch_size)
    } else {
        None
    };

    let batch_prefetch_depth = if batch_prefetch_depth == 0 { 4 } else { batch_prefetch_depth };
    Box::into_raw(Box::new(IcebergArrowReaderContext {
        reader: builder.build(),
        serialization_concurrency,
        batch_size,
        batch_prefetch_depth,
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
        let batch_prefetch_depth = ctx.batch_prefetch_depth;
        let task_ref = unsafe { Box::from_raw(task) };
        let file_scan_task = task_ref.task;
        Ok((reader, serialization_concurrency, batch_prefetch_depth, file_scan_task))
    },
    result_tuple,
    async {
        let (reader, serialization_concurrency, batch_prefetch_depth, file_scan_task) = result_tuple;

        // Wrap the single task in a one-element stream, as reader.read() takes a FileScanTaskStream as input
        let task_stream = stream::once(async { Ok(file_scan_task) }).boxed();
        let record_batch_stream = reader.read(task_stream)?;
        let serialized_stream = crate::transform_stream_with_parallel_serialization(
            record_batch_stream,
            serialization_concurrency,
        );

        // Eagerly drain into a bounded channel so batches are prefetched while
        // Julia processes the previous one. The background task runs independently
        // of Julia's next_batch calls.
        let (tx, rx) = tokio::sync::mpsc::channel(batch_prefetch_depth);
        tokio::spawn(async move {
            futures::pin_mut!(serialized_stream);
            while let Some(item) = serialized_stream.next().await {
                if tx.send(item).await.is_err() {
                    break; // Julia dropped the stream
                }
            }
        });
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })
        .boxed();
        Ok::<IcebergArrowStream, anyhow::Error>(IcebergArrowStream {
            stream: AsyncMutex::new(stream),
        })
    },
    reader_ctx: *mut IcebergArrowReaderContext,
    task: *mut IcebergFileScanTask
);

/// Returns the record count for a file scan task, or -1 if not available.
#[no_mangle]
pub extern "C" fn iceberg_file_scan_task_record_count(task: *const IcebergFileScanTask) -> i64 {
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
