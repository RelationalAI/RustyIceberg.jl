use std::ffi::{c_char, c_void, CStr};
use std::ptr;

use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::incremental::{IncrementalTableScan, IncrementalTableScanBuilder};
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, Context, NotifyGuard, RawResponse,
    ResponseGuard, RT,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::scan_common::*;
use crate::table::{
    read_incremental_append_file, IcebergArrowReaderContext,
    IcebergIncrementalAppendFile, IcebergIncrementalAppendFileStream,
    IcebergIncrementalPosDeleteFile, IcebergIncrementalPosDeleteFileStream,
    DEFAULT_DELETE_BATCH_SIZE, DEFAULT_TASK_PREFETCH_DEPTH,
};
use crate::{IcebergArrowStream, IcebergArrowStreamResponse, IcebergTable};

/// Sentinel value for optional snapshot IDs in the C API.
/// When -1 is passed as from_snapshot_id or to_snapshot_id, it means None (use default).
const SNAPSHOT_ID_NONE: i64 = -1;

/// Struct for incremental scan builder and scan
#[repr(C)]
pub struct IcebergIncrementalScan {
    pub builder: Option<IncrementalTableScanBuilder<'static>>,
    pub scan: Option<IncrementalTableScan>,
    /// 0 = auto-detect (num_cpus)
    pub serialization_concurrency: usize,
    /// Stored for create_reader; set at scan creation from table.file_io()
    pub file_io: Option<FileIO>,
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

    let file_io = table_ref.table.file_io().clone();
    let scan_builder = table_ref.table.incremental_scan(from_id, to_id);
    Box::into_raw(Box::new(IcebergIncrementalScan {
        builder: Some(scan_builder),
        scan: None,
        serialization_concurrency: 0,
        file_io: Some(file_io),
        data_file_concurrency_limit: 0,
        batch_size: 0,
        task_prefetch_depth: 0,
    }))
}

// Use macros from scan_common for shared functionality
impl_select_columns!(iceberg_incremental_select_columns, IcebergIncrementalScan);

impl_scan_builder_method!(
    iceberg_incremental_scan_with_manifest_file_concurrency_limit,
    IcebergIncrementalScan,
    with_manifest_file_concurrency_limit,
    n: usize
);

/// Sets data file concurrency and stores the value for create_reader.
#[no_mangle]
pub extern "C" fn iceberg_incremental_scan_with_data_file_concurrency_limit(
    scan: &mut *mut IcebergIncrementalScan,
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
    iceberg_incremental_scan_with_manifest_entry_concurrency_limit,
    IcebergIncrementalScan,
    with_manifest_entry_concurrency_limit,
    n: usize
);

/// Sets batch size on the builder and stores the value for create_reader.
#[no_mangle]
pub extern "C" fn iceberg_incremental_scan_with_batch_size(
    scan: &mut *mut IcebergIncrementalScan,
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

impl_scan_builder_method!(
    iceberg_incremental_scan_with_file_column,
    IcebergIncrementalScan,
    with_file_column
);

impl_scan_builder_method!(
    iceberg_incremental_scan_with_pos_column,
    IcebergIncrementalScan,
    with_pos_column
);

impl_scan_build!(iceberg_incremental_scan_build, IcebergIncrementalScan);

impl_with_serialization_concurrency_limit!(
    iceberg_incremental_scan_with_serialization_concurrency_limit,
    IcebergIncrementalScan
);

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
        let scan_ref = &scan_ptr.scan;
        if scan_ref.is_none() {
            return Err(anyhow::anyhow!("Incremental scan not initialized"));
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
    type Payload = (IcebergIncrementalAppendFileStream, IcebergIncrementalPosDeleteFileStream);

    fn result_mut(&mut self) -> &mut CResult { &mut self.result }
    fn context_mut(&mut self) -> &mut *const Context { &mut self.context }
    fn error_message_mut(&mut self) -> &mut *mut c_char { &mut self.error_message }

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
    fn result_mut(&mut self) -> &mut CResult { &mut self.0.result }
    fn context_mut(&mut self) -> &mut *const Context { &mut self.0.context }
    fn error_message_mut(&mut self) -> &mut *mut c_char { &mut self.0.error_message }
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
    fn result_mut(&mut self) -> &mut CResult { &mut self.0.result }
    fn context_mut(&mut self) -> &mut *const Context { &mut self.0.context }
    fn error_message_mut(&mut self) -> &mut *mut c_char { &mut self.0.error_message }
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
        let scan_ref = scan_ptr.scan.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Incremental scan not built — call build! first"))?;
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
    let file_io = match scan_ptr.file_io.as_ref() {
        Some(f) => f.clone(),
        None => return ptr::null_mut(),
    };

    let mut builder = ArrowReaderBuilder::new(file_io);

    let concurrency = if reader_concurrency > 0 {
        reader_concurrency
    } else {
        scan_ptr.data_file_concurrency_limit
    };
    if concurrency > 0 {
        builder = builder.with_data_file_concurrency_limit(concurrency);
    }

    let batch_size = if scan_ptr.batch_size > 0 { Some(scan_ptr.batch_size) } else { None };
    if let Some(n) = batch_size {
        builder = builder.with_batch_size(n);
    }

    let serialization_concurrency = if scan_ptr.serialization_concurrency == 0 {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
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
