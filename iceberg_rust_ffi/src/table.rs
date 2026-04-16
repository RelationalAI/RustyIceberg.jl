use crate::response::IcebergBoxedResponse;
/// Table and streaming support for iceberg_rust_ffi
use crate::{CResult, Context, RawResponse};
use iceberg::arrow::ArrowReader;
use iceberg::io::{FileIOBuilder, OpenDalRoutingStorageFactory};
use iceberg::scan::incremental::{AppendedFileScanTask, DeleteScanTask};
use iceberg::scan::FileScanTask;
use iceberg::scan::FileScanTaskStream;
use iceberg::table::StaticTable;
use iceberg::table::Table;
use iceberg::TableIdent;
use std::ffi::{c_char, c_void, CString};
use std::ptr;
use tokio::sync::Mutex as AsyncMutex;

// FFI exports
use object_store_ffi::{export_runtime_op, with_cancellation, NotifyGuard, ResponseGuard, RT};

// Utility imports
use crate::util::{parse_c_string, parse_properties};
use crate::PropertyEntry;

/// Direct table structure - no opaque wrapper
#[repr(C)]
pub struct IcebergTable {
    pub table: Table,
}

/// Default number of serialized batches buffered ahead of the consumer.
/// Keeps the Rust pipeline (Parquet decode → IPC serialize) running independently
/// of how fast Julia pulls batches, while bounding memory usage.
pub(crate) const DEFAULT_PREFETCH_DEPTH: usize = 8;

/// Default number of file scan tasks buffered ahead of the consumer.
/// Only a few tasks need to be planned ahead — task planning is cheap compared
/// to reading/serializing batches.
pub(crate) const DEFAULT_TASK_PREFETCH_DEPTH: usize = 4;

/// Stream wrapper for FFI — eagerly drains serialized batches into a bounded
/// channel so the producer runs independently of the Julia consumer.
///
/// Error handling:
/// - Stream errors are sent through the channel as `Err(...)` values; the producer
///   stops after forwarding the first error.
/// - If the producer task panics, the sender is dropped, `recv()` returns `None`,
///   and the `JoinHandle` is checked to surface the panic message as an error.
/// - Dropping the `IcebergArrowStream` (via `iceberg_arrow_stream_free`) drops the
///   receiver, which causes the producer to exit on its next `send()`.
pub struct IcebergArrowStream {
    receiver: AsyncMutex<tokio::sync::mpsc::Receiver<Result<ArrowBatch, iceberg::Error>>>,
    producer_handle: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
}

unsafe impl Send for IcebergArrowStream {}

impl IcebergArrowStream {
    /// Spawn a background tokio task that eagerly drains `stream` into a bounded
    /// mpsc channel of capacity `prefetch_depth`.
    pub fn new(
        stream: futures::stream::BoxStream<'static, Result<ArrowBatch, iceberg::Error>>,
        prefetch_depth: usize,
    ) -> Self {
        use futures::StreamExt;
        let (tx, rx) = tokio::sync::mpsc::channel(prefetch_depth);
        let handle = tokio::spawn(async move {
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let is_err = item.is_err();
                // If the receiver is dropped (stream freed early), send() fails and we exit.
                if tx.send(item).await.is_err() {
                    break;
                }
                // Stop producing after forwarding an error — the stream is in an
                // undefined state and the consumer will stop pulling anyway.
                if is_err {
                    break;
                }
            }
        });
        Self {
            receiver: AsyncMutex::new(rx),
            producer_handle: AsyncMutex::new(Some(handle)),
        }
    }

    /// Pull the next batch from the internal buffer.
    /// Returns `Ok(Some(batch))` for data, `Ok(None)` for end-of-stream,
    /// or `Err(...)` for stream/producer errors.
    pub async fn next(&self) -> Result<Option<ArrowBatch>, anyhow::Error> {
        let mut rx = self.receiver.lock().await;
        match rx.recv().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(anyhow::anyhow!("Stream error: {}", e)),
            None => {
                // Channel closed — either clean end-of-stream or producer panicked.
                // Check the JoinHandle to distinguish.
                let mut handle_guard = self.producer_handle.lock().await;
                if let Some(handle) = handle_guard.take() {
                    match handle.await {
                        Ok(()) => Ok(None), // clean EOF
                        Err(e) => Err(anyhow::anyhow!("Batch producer task panicked: {}", e)),
                    }
                } else {
                    // Handle already consumed (second call after EOF) — just return None.
                    Ok(None)
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Split-scan API types
//
// These types support a two-phase scan workflow:
//   1. plan_files()  -> produces a task stream (file-level work items)
//   2. create_reader() -> builds a shared ArrowReader (with delete-file cache)
//   3. next_task()   -> concurrent-safe pull of one task from the stream
//   4. read_task()   -> reads one task using a clone of the shared reader
//
// The ArrowReader is Clone; cloning shares the CachingDeleteFileLoader via
// Arc, so delete files loaded by one consumer are cached for all others.
// Task streams are wrapped in AsyncMutex so multiple Julia tasks can call
// next_task concurrently.
// ---------------------------------------------------------------------------

/// Shared ArrowReader context for the split-scan API.
///
/// Created once via `create_reader`, then passed to every `read_task` call.
/// Each `read_task` clones the inner ArrowReader -- this is cheap because
/// the CachingDeleteFileLoader shares its cache via Arc<RwLock<...>>.
pub struct IcebergArrowReaderContext {
    pub reader: ArrowReader,
    pub serialization_concurrency: usize,
    pub prefetch_depth: usize,
}

unsafe impl Send for IcebergArrowReaderContext {}

/// File scan task stream for full scans — eagerly drains planned tasks into
/// a bounded channel so task planning runs ahead of Julia's consumption.
pub struct IcebergFileScanTaskStream {
    receiver: AsyncMutex<tokio::sync::mpsc::Receiver<Result<FileScanTask, iceberg::Error>>>,
    producer_handle: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
}

unsafe impl Send for IcebergFileScanTaskStream {}

impl IcebergFileScanTaskStream {
    pub fn new(stream: FileScanTaskStream, prefetch_depth: usize) -> Self {
        use futures::StreamExt;
        let (tx, rx) = tokio::sync::mpsc::channel(prefetch_depth);
        let handle = tokio::spawn(async move {
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let is_err = item.is_err();
                if tx.send(item).await.is_err() {
                    break;
                }
                if is_err {
                    break;
                }
            }
        });
        Self {
            receiver: AsyncMutex::new(rx),
            producer_handle: AsyncMutex::new(Some(handle)),
        }
    }

    pub async fn next(&self) -> Result<Option<FileScanTask>, anyhow::Error> {
        let mut rx = self.receiver.lock().await;
        match rx.recv().await {
            Some(Ok(task)) => Ok(Some(task)),
            Some(Err(e)) => Err(anyhow::anyhow!("Task planning error: {}", e)),
            None => {
                let mut handle_guard = self.producer_handle.lock().await;
                if let Some(handle) = handle_guard.take() {
                    match handle.await {
                        Ok(()) => Ok(None),
                        Err(e) => Err(anyhow::anyhow!("Task planner panicked: {}", e)),
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// A single file scan task pulled from the stream (full scan).
pub struct IcebergFileScanTask {
    pub task: FileScanTask,
}

unsafe impl Send for IcebergFileScanTask {}

/// Incremental file scan task streams (appends + deletes) — each eagerly
/// drained into a bounded channel for prefetching.
pub struct IcebergIncrementalFileScanTaskStreams {
    pub appends_rx: AsyncMutex<tokio::sync::mpsc::Receiver<Result<AppendedFileScanTask, iceberg::Error>>>,
    pub deletes_rx: AsyncMutex<tokio::sync::mpsc::Receiver<Result<DeleteScanTask, iceberg::Error>>>,
    _appends_handle: tokio::task::JoinHandle<()>,
    _deletes_handle: tokio::task::JoinHandle<()>,
}

unsafe impl Send for IcebergIncrementalFileScanTaskStreams {}

impl IcebergIncrementalFileScanTaskStreams {
    pub fn new(
        appends_stream: futures::stream::BoxStream<'static, Result<AppendedFileScanTask, iceberg::Error>>,
        deletes_stream: futures::stream::BoxStream<'static, Result<DeleteScanTask, iceberg::Error>>,
        prefetch_depth: usize,
    ) -> Self {
        use futures::StreamExt;

        let (atx, arx) = tokio::sync::mpsc::channel(prefetch_depth);
        let appends_handle = tokio::spawn(async move {
            futures::pin_mut!(appends_stream);
            while let Some(item) = appends_stream.next().await {
                let is_err = item.is_err();
                if atx.send(item).await.is_err() { break; }
                if is_err { break; }
            }
        });

        // TODO @vustef: I'm not sure, but why don't we use the existing concurrency limits?
        let (dtx, drx) = tokio::sync::mpsc::channel(prefetch_depth);
        let deletes_handle = tokio::spawn(async move {
            futures::pin_mut!(deletes_stream);
            while let Some(item) = deletes_stream.next().await {
                let is_err = item.is_err();
                if dtx.send(item).await.is_err() { break; }
                if is_err { break; }
            }
        });

        Self {
            appends_rx: AsyncMutex::new(arx),
            deletes_rx: AsyncMutex::new(drx),
            _appends_handle: appends_handle,
            _deletes_handle: deletes_handle,
        }
    }
}

/// A single appended file scan task pulled from the incremental stream.
pub struct IcebergAppendTask {
    pub task: AppendedFileScanTask,
}

unsafe impl Send for IcebergAppendTask {}

/// A single delete scan task pulled from the incremental stream.
pub struct IcebergDeleteTask {
    pub task: DeleteScanTask,
}

unsafe impl Send for IcebergDeleteTask {}

/// Arrow batch serialized for FFI
#[repr(C)]
pub struct ArrowBatch {
    pub data: *const u8,
    pub length: usize,
    pub rust_ptr: *mut std::ffi::c_void,
}

// SAFETY: ArrowBatch contains raw pointers that are owned by the FFI layer.
// The pointers are allocated in Rust and deallocated via iceberg_arrow_batch_free,
// making it safe to send between threads.
unsafe impl Send for ArrowBatch {}

/// Type aliases for response types
pub type IcebergTableResponse = IcebergBoxedResponse<IcebergTable>;
pub type IcebergArrowStreamResponse = IcebergBoxedResponse<IcebergArrowStream>;
pub type IcebergArrowReaderContextResponse = IcebergBoxedResponse<IcebergArrowReaderContext>;
pub type IcebergFileScanTaskStreamResponse = IcebergBoxedResponse<IcebergFileScanTaskStream>;
pub type IcebergFileScanTaskResponse = IcebergBoxedResponse<IcebergFileScanTask>;
pub type IcebergIncrementalFileScanTaskStreamsResponse =
    IcebergBoxedResponse<IcebergIncrementalFileScanTaskStreams>;
pub type IcebergAppendTaskResponse = IcebergBoxedResponse<IcebergAppendTask>;
pub type IcebergDeleteTaskResponse = IcebergBoxedResponse<IcebergDeleteTask>;

/// Batch response - same memory layout as IcebergBoxedResponse<ArrowBatch>
/// but with Option<ArrowBatch> payload to handle end-of-stream (None) case.
/// Uses #[repr(transparent)] to ensure identical FFI layout.
#[repr(transparent)]
pub struct IcebergBatchResponse(pub IcebergBoxedResponse<ArrowBatch>);

unsafe impl Send for IcebergBatchResponse {}

impl RawResponse for IcebergBatchResponse {
    type Payload = Option<ArrowBatch>;
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
            Some(arrow_batch) => {
                self.0.value = Box::into_raw(Box::new(arrow_batch));
            }
            None => self.0.value = ptr::null_mut(),
        }
    }
}

/// Response for next_task operations that may return None (end of stream).
/// Uses the same pattern as IcebergBatchResponse.
#[repr(transparent)]
pub struct IcebergOptionalTaskResponse<T>(pub IcebergBoxedResponse<T>);

unsafe impl<T: Send> Send for IcebergOptionalTaskResponse<T> {}

impl<T> RawResponse for IcebergOptionalTaskResponse<T> {
    type Payload = Option<T>;
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
            Some(val) => {
                self.0.value = Box::into_raw(Box::new(val));
            }
            None => self.0.value = ptr::null_mut(),
        }
    }
}

pub type IcebergNextFileScanTaskResponse = IcebergOptionalTaskResponse<IcebergFileScanTask>;
pub type IcebergNextAppendTaskResponse = IcebergOptionalTaskResponse<IcebergAppendTask>;
pub type IcebergNextDeleteTaskResponse = IcebergOptionalTaskResponse<IcebergDeleteTask>;

/// Synchronous operations for table and batch management

/// Free a table
#[no_mangle]
pub extern "C" fn iceberg_table_free(table: *mut IcebergTable) {
    if !table.is_null() {
        unsafe {
            let _ = Box::from_raw(table);
        }
    }
}

/// Free an arrow batch
#[no_mangle]
pub extern "C" fn iceberg_arrow_batch_free(batch: *mut ArrowBatch) {
    if batch.is_null() {
        return;
    }

    unsafe {
        let batch_ref = Box::from_raw(batch);
        if !batch_ref.rust_ptr.is_null() {
            let _ = Box::from_raw(batch_ref.rust_ptr as *mut Vec<u8>);
        }
    }
}

/// Free an arrow stream
#[no_mangle]
pub extern "C" fn iceberg_arrow_stream_free(stream: *mut IcebergArrowStream) {
    if !stream.is_null() {
        unsafe {
            let _ = Box::from_raw(stream);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_arrow_reader_context_free(ctx: *mut IcebergArrowReaderContext) {
    if !ctx.is_null() {
        unsafe {
            let _ = Box::from_raw(ctx);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_file_scan_task_stream_free(stream: *mut IcebergFileScanTaskStream) {
    if !stream.is_null() {
        unsafe {
            let _ = Box::from_raw(stream);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_file_scan_task_free(task: *mut IcebergFileScanTask) {
    if !task.is_null() {
        unsafe {
            let _ = Box::from_raw(task);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_incremental_file_scan_task_streams_free(
    streams: *mut IcebergIncrementalFileScanTaskStreams,
) {
    if !streams.is_null() {
        unsafe {
            let _ = Box::from_raw(streams);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_append_task_free(task: *mut IcebergAppendTask) {
    if !task.is_null() {
        unsafe {
            let _ = Box::from_raw(task);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_delete_task_free(task: *mut IcebergDeleteTask) {
    if !task.is_null() {
        unsafe {
            let _ = Box::from_raw(task);
        }
    }
}

// FFI Export functions for table operations
// These functions are exported to be called from Julia via the FFI

// Open a table from metadata file
export_runtime_op!(
    iceberg_table_open,
    IcebergTableResponse,
    || {
        let snapshot_path_str = parse_c_string(snapshot_path, "snapshot_path")?;
        let props = parse_properties(properties, properties_len)?;

        // Convert HashMap to Vec of tuples for compatibility with FileIOBuilder::with_props
        let props_vec: Vec<(String, String)> = props.into_iter().collect();
        Ok((snapshot_path_str, props_vec))
    },
    result_tuple,
    async {
        let (full_metadata_path, props) = result_tuple;

        // Create file IO using routing factory that infers scheme from metadata location
        let factory = std::sync::Arc::new(OpenDalRoutingStorageFactory);
        let file_io = FileIOBuilder::new(factory)
            .with_props(props)
            .with_prop("iceberg.internal.metadata-location", &full_metadata_path)
            .build();

        // Create table identifier
        let table_ident = TableIdent::from_strs(["default", "table"])?;

        // Load the static table
        let static_table =
            StaticTable::from_metadata_file(&full_metadata_path, table_ident, file_io).await?;

        Ok::<IcebergTable, anyhow::Error>(IcebergTable { table: static_table.into_table() })
    },
    snapshot_path: *const c_char,
    properties: *const PropertyEntry,
    properties_len: usize
);

// Get next batch from stream (pops from the internal prefetch buffer).
export_runtime_op!(
    iceberg_next_batch,
    IcebergBatchResponse,
    || {
        if stream.is_null() {
            return Err(anyhow::anyhow!("Null stream pointer provided"));
        }
        let stream_ref = unsafe { &*stream };
        Ok(stream_ref)
    },
    stream_ref,
    async {
        stream_ref.next().await
    },
    stream: *mut IcebergArrowStream
);

// Synchronous table property functions

/// Get table location
#[no_mangle]
pub extern "C" fn iceberg_table_location(table: *mut IcebergTable) -> *mut c_char {
    if table.is_null() {
        return ptr::null_mut();
    }
    let table_ref = unsafe { &*table };
    let location = table_ref.table.metadata().location().to_string();
    match std::ffi::CString::new(location) {
        Ok(c_str) => c_str.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Get table UUID
#[no_mangle]
pub extern "C" fn iceberg_table_uuid(table: *mut IcebergTable) -> *mut c_char {
    if table.is_null() {
        return ptr::null_mut();
    }
    let table_ref = unsafe { &*table };
    let uuid = table_ref.table.metadata().uuid().to_string();
    match std::ffi::CString::new(uuid) {
        Ok(c_str) => c_str.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Get table format version (returns 0 on error)
#[no_mangle]
pub extern "C" fn iceberg_table_format_version(table: *mut IcebergTable) -> i64 {
    if table.is_null() {
        return 0;
    }
    let table_ref = unsafe { &*table };
    match table_ref.table.metadata().format_version() {
        iceberg::spec::FormatVersion::V1 => 1,
        iceberg::spec::FormatVersion::V2 => 2,
        iceberg::spec::FormatVersion::V3 => 3,
    }
}

/// Get table last sequence number (returns -1 on error)
#[no_mangle]
pub extern "C" fn iceberg_table_last_sequence_number(table: *mut IcebergTable) -> i64 {
    if table.is_null() {
        return -1;
    }
    let table_ref = unsafe { &*table };
    table_ref.table.metadata().last_sequence_number()
}

/// Get table last updated timestamp in milliseconds (returns -1 on error)
#[no_mangle]
pub extern "C" fn iceberg_table_last_updated_ms(table: *mut IcebergTable) -> i64 {
    if table.is_null() {
        return -1;
    }
    let table_ref = unsafe { &*table };
    table_ref.table.metadata().last_updated_ms()
}

/// Get table current schema as JSON string
#[no_mangle]
pub extern "C" fn iceberg_table_schema(table: *mut IcebergTable) -> *mut c_char {
    if table.is_null() {
        return ptr::null_mut();
    }
    let table_ref = unsafe { &*table };
    let schema = table_ref.table.metadata().current_schema();
    match serde_json::to_string(schema.as_ref()) {
        Ok(json) => match std::ffi::CString::new(json) {
            Ok(c_str) => c_str.into_raw(),
            Err(_) => ptr::null_mut(),
        },
        Err(_) => ptr::null_mut(),
    }
}

/// Returns a newly allocated C string with the file path from a FileScanTask.
/// Caller must free with `iceberg_destroy_cstring`.
#[no_mangle]
pub extern "C" fn iceberg_file_scan_task_data_file_path(
    task: *const IcebergFileScanTask,
) -> *mut c_char {
    if task.is_null() {
        return ptr::null_mut();
    }
    let task_ref = unsafe { &*task };
    let path = task_ref.task.data_file_path();
    match CString::new(path) {
        Ok(cstr) => cstr.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Returns a newly allocated C string with the file path from an AppendedFileScanTask.
/// Caller must free with `iceberg_destroy_cstring`.
#[no_mangle]
pub extern "C" fn iceberg_append_task_data_file_path(
    task: *const IcebergAppendTask,
) -> *mut c_char {
    if task.is_null() {
        return ptr::null_mut();
    }
    let task_ref = unsafe { &*task };
    let path = task_ref.task.data_file_path();
    match CString::new(path) {
        Ok(cstr) => cstr.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Returns a newly allocated C string with the file path from a DeleteScanTask.
/// Caller must free with `iceberg_destroy_cstring`.
#[no_mangle]
pub extern "C" fn iceberg_delete_task_data_file_path(
    task: *const IcebergDeleteTask,
) -> *mut c_char {
    if task.is_null() {
        return ptr::null_mut();
    }
    let task_ref = unsafe { &*task };
    let path = match &task_ref.task {
        DeleteScanTask::DeletedFile(inner) => inner.data_file_path(),
        DeleteScanTask::PositionalDeletes(path, _) => path.as_str(),
        DeleteScanTask::EqualityDeletes(inner) => inner.data_file_path(),
    };
    match CString::new(path) {
        Ok(cstr) => cstr.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Sentinel value for unknown record counts (when Option<u64> is None).
const RECORD_COUNT_UNKNOWN: i64 = -1;

/// Returns the record count from a FileScanTask.
/// Returns -1 if the record count is not available (partial file read).
#[no_mangle]
pub extern "C" fn iceberg_file_scan_task_record_count(task: *const IcebergFileScanTask) -> i64 {
    if task.is_null() {
        return RECORD_COUNT_UNKNOWN;
    }
    let task_ref = unsafe { &*task };
    task_ref
        .task
        .record_count
        .map_or(RECORD_COUNT_UNKNOWN, |n| n as i64)
}

/// Returns the record count from an AppendedFileScanTask.
/// Returns -1 if the record count is not available.
#[no_mangle]
pub extern "C" fn iceberg_append_task_record_count(task: *const IcebergAppendTask) -> i64 {
    if task.is_null() {
        return RECORD_COUNT_UNKNOWN;
    }
    let task_ref = unsafe { &*task };
    task_ref
        .task
        .base
        .record_count
        .map_or(RECORD_COUNT_UNKNOWN, |n| n as i64)
}

/// Returns the record count from a DeleteScanTask.
/// Returns -1 if the record count is not available.
#[no_mangle]
pub extern "C" fn iceberg_delete_task_record_count(task: *const IcebergDeleteTask) -> i64 {
    if task.is_null() {
        return RECORD_COUNT_UNKNOWN;
    }
    let task_ref = unsafe { &*task };
    match &task_ref.task {
        DeleteScanTask::DeletedFile(inner) => inner
            .base
            .record_count
            .map_or(RECORD_COUNT_UNKNOWN, |n| n as i64),
        DeleteScanTask::PositionalDeletes(_, _) => RECORD_COUNT_UNKNOWN,
        DeleteScanTask::EqualityDeletes(inner) => inner
            .base
            .record_count
            .map_or(RECORD_COUNT_UNKNOWN, |n| n as i64),
    }
}
