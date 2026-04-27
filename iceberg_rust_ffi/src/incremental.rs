use std::ffi::{c_char, c_void, CStr};
use std::ptr;

use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::incremental::IncrementalTableScan;
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, Context, NotifyGuard, RawResponse,
    ResponseGuard, RT,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::scan_common::*;
use crate::table::{
    read_incremental_append_file, IcebergArrowReaderContext, IcebergIncrementalAppendFile,
    IcebergIncrementalAppendFileStream, IcebergIncrementalPosDeleteFile,
    IcebergIncrementalPosDeleteFileStream, DEFAULT_DELETE_BATCH_SIZE, DEFAULT_TASK_PREFETCH_DEPTH,
};
use crate::{IcebergArrowStream, IcebergArrowStreamResponse, IcebergTable};

/// Sentinel value for optional snapshot IDs in the C API.
/// When -1 is passed as from_snapshot_id or to_snapshot_id, it means None (use default).
const SNAPSHOT_ID_NONE: i64 = -1;

/// Struct for a built incremental table scan.
#[repr(C)]
pub struct IcebergIncrementalScan {
    pub scan: IncrementalTableScan,
    /// 0 = auto-detect (num_cpus)
    pub serialization_concurrency: usize,
    /// Stored for create_reader; set at scan creation from table.file_io()
    pub file_io: FileIO,
    /// 0 = use reader default
    pub data_file_concurrency_limit: usize,
    /// 0 = no override
    pub batch_size: usize,
    /// 0 = DEFAULT_TASK_PREFETCH_DEPTH
    pub task_prefetch_depth: usize,
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

/// Create a new incremental scan builder with all parameters.
///
/// Numeric parameters use -1 as "not set / use default" sentinel.
///
/// # Arguments
/// * `table` - The table to scan
/// * `from_snapshot_id` - Starting snapshot ID, or -1 (SNAPSHOT_ID_NONE) for oldest
/// * `to_snapshot_id` - Ending snapshot ID, or -1 (SNAPSHOT_ID_NONE) for current
/// * All remaining parameters use -1 to mean "not set / use default"
#[no_mangle]
pub extern "C" fn iceberg_new_incremental_scan(
    table: *mut IcebergTable,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    column_names: *const *const c_char, // NULL = all columns
    column_names_len: usize,
    data_file_concurrency_limit: i64,      // -1 = use reader default
    manifest_file_concurrency_limit: i64,  // -1 = don't set
    manifest_entry_concurrency_limit: i64, // -1 = don't set
    batch_size: i64,                       // -1 = no override
    file_column: u8,                       // 0 = no, non-zero = yes
    pos_column: u8,
    serialization_concurrency: i64, // -1 = auto-detect
    task_prefetch_depth: i64,       // -1 = use DEFAULT_TASK_PREFETCH_DEPTH
) -> *mut IcebergIncrementalScan {
    if table.is_null() {
        return ptr::null_mut();
    }
    let table_ref = unsafe { &*table };

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

    let file_io = table_ref.table.file_io().clone();
    let mut builder = table_ref.table.incremental_scan(from_id, to_id);

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
        "iceberg_new_incremental_scan"
    );
    Box::into_raw(Box::new(IcebergIncrementalScan {
        scan,
        serialization_concurrency: resolved_serialization_concurrency,
        file_io,
        data_file_concurrency_limit: resolved_data_file_concurrency,
        batch_size: resolved_batch_size,
        task_prefetch_depth: resolved_task_prefetch_depth,
    }))
}

// Get unzipped Arrow streams from incremental scan (async)
// Returns two separate streams: one for inserts, one for deletes
export_runtime_op!(
    iceberg_incremental_arrow_stream_unzipped,
    IcebergUnzippedStreamsResponse,
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

        // Get unzipped streams (separate append and delete streams)
        let (inserts_stream, deletes_stream) = scan_ref.to_unzipped_arrow().await?;

        // Transform both streams with parallel serialization
        let inserts = IcebergArrowStream {
            stream: AsyncMutex::new(crate::transform_stream_with_parallel_serialization(
                inserts_stream,
                serialization_concurrency
            )),
        };

        let deletes = IcebergArrowStream {
            stream: AsyncMutex::new(crate::transform_stream_with_parallel_serialization(
                deletes_stream,
                serialization_concurrency
            )),
        };

        Ok::<(IcebergArrowStream, IcebergArrowStream), anyhow::Error>((inserts, deletes))
    },
    scan: *mut IcebergIncrementalScan
);

impl_scan_free!(iceberg_free_incremental_scan, IcebergIncrementalScan);

// ---------------------------------------------------------------------------
// Incremental split-scan API
// ---------------------------------------------------------------------------

/// Response for plan_files: returns pointers to two separate task streams.
#[repr(C)]
pub struct IcebergIncrementalTaskStreamsResponse {
    result: CResult,
    append_stream: *mut IcebergIncrementalAppendFileStream,
    delete_stream: *mut IcebergIncrementalPosDeleteFileStream,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergIncrementalTaskStreamsResponse {}

impl RawResponse for IcebergIncrementalTaskStreamsResponse {
    type Payload = (
        IcebergIncrementalAppendFileStream,
        IcebergIncrementalPosDeleteFileStream,
    );

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
            Some((appends, deletes)) => {
                self.append_stream = Box::into_raw(Box::new(appends));
                self.delete_stream = Box::into_raw(Box::new(deletes));
            }
            None => {
                self.append_stream = ptr::null_mut();
                self.delete_stream = ptr::null_mut();
            }
        }
    }
}

/// Response for next_append_file/next_pos_delete_file — null value = end-of-stream.
#[repr(transparent)]
pub struct IcebergIncrementalNextAppendFileResponse(
    pub crate::response::IcebergBoxedResponse<IcebergIncrementalAppendFile>,
);

unsafe impl Send for IcebergIncrementalNextAppendFileResponse {}

impl RawResponse for IcebergIncrementalNextAppendFileResponse {
    type Payload = Option<IcebergIncrementalAppendFile>;
    fn result_mut(&mut self) -> &mut CResult {
        &mut self.0.result
    }
    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.0.context
    }
    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.0.error_message
    }
    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload.flatten() {
            Some(task) => self.0.value = Box::into_raw(Box::new(task)),
            None => self.0.value = ptr::null_mut(),
        }
    }
}

#[repr(transparent)]
pub struct IcebergIncrementalNextPosDeleteFileResponse(
    pub crate::response::IcebergBoxedResponse<IcebergIncrementalPosDeleteFile>,
);

unsafe impl Send for IcebergIncrementalNextPosDeleteFileResponse {}

impl RawResponse for IcebergIncrementalNextPosDeleteFileResponse {
    type Payload = Option<IcebergIncrementalPosDeleteFile>;
    fn result_mut(&mut self) -> &mut CResult {
        &mut self.0.result
    }
    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.0.context
    }
    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.0.error_message
    }
    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload.flatten() {
            Some(task) => self.0.value = Box::into_raw(Box::new(task)),
            None => self.0.value = ptr::null_mut(),
        }
    }
}

// Async: plan which files to read for an incremental scan.
// Returns two task streams: one for append tasks, one for positional-delete tasks.
export_runtime_op!(
    iceberg_incremental_plan_files,
    IcebergIncrementalTaskStreamsResponse,
    || {
        if scan.is_null() {
            return Err(anyhow::anyhow!("Null scan pointer provided"));
        }
        let scan_ptr = unsafe { &*scan };
        let scan_ref = &scan_ptr.scan;
        let task_prefetch_depth = if scan_ptr.task_prefetch_depth == 0 {
            DEFAULT_TASK_PREFETCH_DEPTH
        } else {
            scan_ptr.task_prefetch_depth
        };
        Ok((scan_ref, task_prefetch_depth))
    },
    result_tuple,
    async {
        let (scan_ref, task_prefetch_depth) = result_tuple;
        let (append_stream, delete_stream) = scan_ref.plan_files().await?;
        Ok::<(IcebergIncrementalAppendFileStream, IcebergIncrementalPosDeleteFileStream), anyhow::Error>((
            IcebergIncrementalAppendFileStream::new(append_stream, task_prefetch_depth),
            IcebergIncrementalPosDeleteFileStream::new(delete_stream, task_prefetch_depth),
        ))
    },
    scan: *mut IcebergIncrementalScan
);

/// Sync: create a shared ArrowReader context from the incremental scan's configuration.
/// Pass the returned context to every read_append_file and read_pos_delete_file call.
/// `reader_concurrency` overrides data_file_concurrency_limit when > 0.
#[no_mangle]
pub extern "C" fn iceberg_incremental_create_reader(
    scan: *const IcebergIncrementalScan,
    reader_concurrency: usize,
) -> *mut IcebergArrowReaderContext {
    if scan.is_null() {
        return ptr::null_mut();
    }
    let scan_ptr = unsafe { &*scan };
    let file_io = scan_ptr.file_io.clone();

    let mut builder = ArrowReaderBuilder::new(file_io);

    let concurrency = if reader_concurrency > 0 {
        reader_concurrency
    } else {
        scan_ptr.data_file_concurrency_limit
    };
    if concurrency > 0 {
        builder = builder.with_data_file_concurrency_limit(concurrency);
    }

    let batch_size = if scan_ptr.batch_size > 0 {
        Some(scan_ptr.batch_size)
    } else {
        None
    };
    if let Some(n) = batch_size {
        builder = builder.with_batch_size(n);
    }

    let serialization_concurrency = if scan_ptr.serialization_concurrency == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        scan_ptr.serialization_concurrency
    };

    Box::into_raw(Box::new(IcebergArrowReaderContext {
        reader: builder.build(),
        serialization_concurrency,
        batch_size,
    }))
}

// Async: pull the next append task. Returns null payload at end-of-stream.
export_runtime_op!(
    iceberg_incremental_next_append_file,
    IcebergIncrementalNextAppendFileResponse,
    || {
        if stream.is_null() {
            return Err(anyhow::anyhow!("Null append task stream pointer"));
        }
        Ok(unsafe { &*stream })
    },
    stream_ref,
    async {
        stream_ref.next().await
    },
    stream: *mut IcebergIncrementalAppendFileStream
);

// Async: pull the next positional-delete task. Returns null payload at end-of-stream.
export_runtime_op!(
    iceberg_incremental_next_pos_delete_file,
    IcebergIncrementalNextPosDeleteFileResponse,
    || {
        if stream.is_null() {
            return Err(anyhow::anyhow!("Null pos-delete task stream pointer"));
        }
        Ok(unsafe { &*stream })
    },
    stream_ref,
    async {
        stream_ref.next().await
    },
    stream: *mut IcebergIncrementalPosDeleteFileStream
);

// Async: read a single append task into an Arrow stream. Consumes the task.
export_runtime_op!(
    iceberg_incremental_read_append_file,
    IcebergArrowStreamResponse,
    || {
        if reader_ctx.is_null() {
            return Err(anyhow::anyhow!("Null reader context pointer"));
        }
        if task.is_null() {
            return Err(anyhow::anyhow!("Null append task pointer"));
        }
        let ctx = unsafe { &*reader_ctx };
        let reader = ctx.reader.clone();
        let serialization_concurrency = ctx.serialization_concurrency;
        let task_ref = unsafe { Box::from_raw(task) };
        let append_task = task_ref.task;
        Ok((reader, serialization_concurrency, append_task))
    },
    result_tuple,
    async {
        let (reader, serialization_concurrency, append_task) = result_tuple;
        let arrow_stream = read_incremental_append_file(reader, append_task)
            .map_err(|e| anyhow::anyhow!("Failed to read append task: {}", e))?;
        let serialized = crate::transform_stream_with_parallel_serialization(
            arrow_stream,
            serialization_concurrency,
        );
        Ok::<IcebergArrowStream, anyhow::Error>(IcebergArrowStream {
            stream: AsyncMutex::new(serialized),
        })
    },
    reader_ctx: *mut IcebergArrowReaderContext,
    task: *mut IcebergIncrementalAppendFile
);

// Async: convert a positional-delete task into an Arrow stream of (file_path, pos) pairs.
// Consumes the task.
export_runtime_op!(
    iceberg_incremental_read_pos_delete_file,
    IcebergArrowStreamResponse,
    || {
        if reader_ctx.is_null() {
            return Err(anyhow::anyhow!("Null reader context pointer"));
        }
        if task.is_null() {
            return Err(anyhow::anyhow!("Null pos-delete task pointer"));
        }
        let ctx = unsafe { &*reader_ctx };
        let serialization_concurrency = ctx.serialization_concurrency;
        let batch_size = ctx.batch_size.unwrap_or(DEFAULT_DELETE_BATCH_SIZE);
        let task_ref = unsafe { Box::from_raw(task) };
        Ok((serialization_concurrency, batch_size, task_ref.file_path, task_ref.positions))
    },
    result_tuple,
    async {
        let (serialization_concurrency, batch_size, file_path, positions) = result_tuple;
        let arrow_stream = crate::pos_delete_positions_to_arrow_stream(
            file_path,
            positions,
            batch_size,
        )?;
        let serialized = crate::transform_stream_with_parallel_serialization(
            arrow_stream,
            serialization_concurrency,
        );
        Ok::<IcebergArrowStream, anyhow::Error>(IcebergArrowStream {
            stream: AsyncMutex::new(serialized),
        })
    },
    reader_ctx: *mut IcebergArrowReaderContext,
    task: *mut IcebergIncrementalPosDeleteFile
);

/// Returns the record count for an append task, or -1 if not available.
#[no_mangle]
pub extern "C" fn iceberg_incremental_append_file_record_count(
    task: *const IcebergIncrementalAppendFile,
) -> i64 {
    if task.is_null() {
        return -1;
    }
    let task_ref = unsafe { &*task };
    task_ref.task.base.record_count.map_or(-1, |n| n as i64)
}

/// Returns the number of deleted row positions in a positional-delete file.
#[no_mangle]
pub extern "C" fn iceberg_incremental_pos_delete_file_record_count(
    task: *const IcebergIncrementalPosDeleteFile,
) -> i64 {
    if task.is_null() {
        return -1;
    }
    let task_ref = unsafe { &*task };
    task_ref.positions.len() as i64
}

/// Returns the data file path for an incremental append file as an allocated C string.
/// Caller must free with `iceberg_destroy_cstring`.
#[no_mangle]
pub extern "C" fn iceberg_incremental_append_file_path(
    task: *const IcebergIncrementalAppendFile,
) -> *mut std::ffi::c_char {
    if task.is_null() {
        return ptr::null_mut();
    }
    let task_ref = unsafe { &*task };
    match std::ffi::CString::new(task_ref.task.base.data_file_path.as_str()) {
        Ok(s) => s.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Returns the data file path for a positional-delete file as an allocated C string.
/// Caller must free with `iceberg_destroy_cstring`.
#[no_mangle]
pub extern "C" fn iceberg_incremental_pos_delete_file_path(
    task: *const IcebergIncrementalPosDeleteFile,
) -> *mut std::ffi::c_char {
    if task.is_null() {
        return ptr::null_mut();
    }
    let task_ref = unsafe { &*task };
    match std::ffi::CString::new(task_ref.file_path.as_str()) {
        Ok(s) => s.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}
