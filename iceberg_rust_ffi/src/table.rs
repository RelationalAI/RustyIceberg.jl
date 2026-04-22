use crate::response::IcebergBoxedResponse;
/// Table and streaming support for iceberg_rust_ffi
use crate::{CResult, Context, RawResponse};
use iceberg::arrow::{ArrowReader, StreamsInto, UnzippedIncrementalBatchRecordStream};
use iceberg::io::{FileIOBuilder, OpenDalRoutingStorageFactory};
use iceberg::scan::incremental::{AppendedFileScanTask, DeleteScanTask};
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::table::StaticTable;
use iceberg::table::Table;
use iceberg::TableIdent;
use std::ffi::{c_char, c_void};
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

/// Stream wrapper for FFI - using async mutex to avoid blocking calls
#[repr(C)]
pub struct IcebergArrowStream {
    // TODO: Maybe remove this mutex and let this be handled in Julia?
    pub stream: AsyncMutex<futures::stream::BoxStream<'static, Result<ArrowBatch, iceberg::Error>>>,
}

unsafe impl Send for IcebergArrowStream {}

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

/// Default task prefetch depth for IcebergFileScanTaskStream producers.
pub(crate) const DEFAULT_TASK_PREFETCH_DEPTH: usize = 4;

// ---------------------------------------------------------------------------
// Split-scan API types
// ---------------------------------------------------------------------------

/// Shared ArrowReader context — created once via `create_reader`, then passed
/// to every `read_file_scan` call. Cloning the reader is cheap: the internal
/// CachingDeleteFileLoader is shared via Arc.
pub struct IcebergArrowReaderContext {
    pub reader: ArrowReader,
    pub serialization_concurrency: usize,
    /// Batch size used for positional-delete Arrow conversion; None = use DEFAULT_DELETE_BATCH_SIZE.
    pub batch_size: Option<usize>,
}

pub(crate) const DEFAULT_DELETE_BATCH_SIZE: usize = 1024;

unsafe impl Send for IcebergArrowReaderContext {}

/// A single file scan task (byte range of a Parquet file) returned by next_file_scan.
pub struct IcebergFileScanTask {
    pub task: FileScanTask,
}

unsafe impl Send for IcebergFileScanTask {}

/// Stream of file scan tasks produced by plan_files. Eagerly drains the
/// planning stream into a bounded channel so task planning runs ahead of
/// Julia's consumption.
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

    pub async fn next(&self) -> Result<Option<IcebergFileScanTask>, anyhow::Error> {
        let mut rx = self.receiver.lock().await;
        match rx.recv().await {
            Some(Ok(task)) => Ok(Some(IcebergFileScanTask { task })),
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

// ---------------------------------------------------------------------------
// Incremental split-scan API types
// ---------------------------------------------------------------------------

/// A single appended file scan task returned by `iceberg_incremental_next_append_task`.
pub struct IcebergIncrementalAppendTask {
    pub task: AppendedFileScanTask,
}

unsafe impl Send for IcebergIncrementalAppendTask {}

/// A single positional-delete task: a set of deleted row positions within one data file.
/// Produced by `iceberg_incremental_next_pos_delete_task`; skips DeletedFile/EqualityDeletes.
pub struct IcebergIncrementalPosDeleteTask {
    pub file_path: String,
    pub positions: Vec<u64>,
}

unsafe impl Send for IcebergIncrementalPosDeleteTask {}

/// Buffered stream of `AppendedFileScanTask` items produced by incremental `plan_files`.
pub struct IcebergIncrementalAppendTaskStream {
    pub(crate) receiver:
        AsyncMutex<tokio::sync::mpsc::Receiver<Result<AppendedFileScanTask, iceberg::Error>>>,
    pub(crate) producer_handle: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
}

unsafe impl Send for IcebergIncrementalAppendTaskStream {}

impl IcebergIncrementalAppendTaskStream {
    pub fn new(
        stream: futures::stream::BoxStream<'static, Result<AppendedFileScanTask, iceberg::Error>>,
        prefetch_depth: usize,
    ) -> Self {
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

    pub async fn next(
        &self,
    ) -> Result<Option<IcebergIncrementalAppendTask>, anyhow::Error> {
        let mut rx = self.receiver.lock().await;
        match rx.recv().await {
            Some(Ok(task)) => Ok(Some(IcebergIncrementalAppendTask { task })),
            Some(Err(e)) => Err(anyhow::anyhow!("Append task planning error: {}", e)),
            None => {
                let mut h = self.producer_handle.lock().await;
                if let Some(handle) = h.take() {
                    handle.await.map_err(|e| anyhow::anyhow!("Planner panicked: {}", e))?;
                }
                Ok(None)
            }
        }
    }
}

/// Buffered stream of positional-delete tasks produced by incremental `plan_files`.
/// Only `DeleteScanTask::PositionalDeletes` variants are forwarded; others are dropped.
pub struct IcebergIncrementalPosDeleteTaskStream {
    pub(crate) receiver: AsyncMutex<
        tokio::sync::mpsc::Receiver<Result<IcebergIncrementalPosDeleteTask, anyhow::Error>>,
    >,
    pub(crate) producer_handle: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
}

unsafe impl Send for IcebergIncrementalPosDeleteTaskStream {}

impl IcebergIncrementalPosDeleteTaskStream {
    pub fn new(
        stream: futures::stream::BoxStream<'static, Result<DeleteScanTask, iceberg::Error>>,
        prefetch_depth: usize,
    ) -> Self {
        use futures::StreamExt;
        let (tx, rx) = tokio::sync::mpsc::channel(prefetch_depth);
        let handle = tokio::spawn(async move {
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                match item {
                    Ok(DeleteScanTask::PositionalDeletes(file_path, dv)) => {
                        let positions: Vec<u64> = dv.iter().collect();
                        let task =
                            IcebergIncrementalPosDeleteTask { file_path, positions };
                        if tx.send(Ok(task)).await.is_err() {
                            break;
                        }
                    }
                    Ok(_) => {} // skip DeletedFile and EqualityDeletes
                    Err(e) => {
                        let _ = tx.send(Err(anyhow::anyhow!("Delete task error: {}", e))).await;
                        break;
                    }
                }
            }
        });
        Self {
            receiver: AsyncMutex::new(rx),
            producer_handle: AsyncMutex::new(Some(handle)),
        }
    }

    pub async fn next(
        &self,
    ) -> Result<Option<IcebergIncrementalPosDeleteTask>, anyhow::Error> {
        let mut rx = self.receiver.lock().await;
        match rx.recv().await {
            Some(Ok(task)) => Ok(Some(task)),
            Some(Err(e)) => Err(e),
            None => {
                let mut h = self.producer_handle.lock().await;
                if let Some(handle) = h.take() {
                    handle.await.map_err(|e| anyhow::anyhow!("Planner panicked: {}", e))?;
                }
                Ok(None)
            }
        }
    }
}

/// Read a single `AppendedFileScanTask` into an Arrow record-batch stream.
/// Wraps the task in a one-element stream and calls the iceberg `StreamsInto` machinery.
pub(crate) fn read_incremental_append_task(
    reader: ArrowReader,
    task: AppendedFileScanTask,
) -> iceberg::Result<iceberg::scan::ArrowRecordBatchStream> {
    use futures::{stream, StreamExt};
    let append_stream =
        stream::once(async { Ok(task) }).boxed();
    let delete_stream = stream::empty::<Result<DeleteScanTask, iceberg::Error>>().boxed();
    let streams = (append_stream, delete_stream);
    let (arrow_stream, _): UnzippedIncrementalBatchRecordStream =
        StreamsInto::<ArrowReader, UnzippedIncrementalBatchRecordStream>::stream(streams, reader)?;
    Ok(arrow_stream)
}

/// Type aliases for response types
pub type IcebergTableResponse = IcebergBoxedResponse<IcebergTable>;
pub type IcebergArrowStreamResponse = IcebergBoxedResponse<IcebergArrowStream>;
pub type IcebergArrowReaderContextResponse = IcebergBoxedResponse<IcebergArrowReaderContext>;
pub type IcebergFileScanTaskStreamResponse = IcebergBoxedResponse<IcebergFileScanTaskStream>;
pub type IcebergFileScanTaskResponse = IcebergBoxedResponse<IcebergFileScanTask>;
pub type IcebergIncrementalAppendTaskResponse =
    IcebergBoxedResponse<IcebergIncrementalAppendTask>;
pub type IcebergIncrementalPosDeleteTaskResponse =
    IcebergBoxedResponse<IcebergIncrementalPosDeleteTask>;

/// Response for next_file_scan — null value pointer means end-of-stream.
#[repr(transparent)]
pub struct IcebergNextFileScanTaskResponse(pub IcebergBoxedResponse<IcebergFileScanTask>);

unsafe impl Send for IcebergNextFileScanTaskResponse {}

impl RawResponse for IcebergNextFileScanTaskResponse {
    type Payload = Option<IcebergFileScanTask>;
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
        unsafe { let _ = Box::from_raw(ctx); }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_file_scan_task_stream_free(stream: *mut IcebergFileScanTaskStream) {
    if !stream.is_null() {
        unsafe { let _ = Box::from_raw(stream); }
    }
}

/// Free a file scan task. Do NOT call after passing it to read_file_scan — that consumes it.
#[no_mangle]
pub extern "C" fn iceberg_file_scan_task_free(task: *mut IcebergFileScanTask) {
    if !task.is_null() {
        unsafe { let _ = Box::from_raw(task); }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_incremental_append_task_stream_free(
    stream: *mut IcebergIncrementalAppendTaskStream,
) {
    if !stream.is_null() {
        unsafe { let _ = Box::from_raw(stream); }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_incremental_pos_delete_task_stream_free(
    stream: *mut IcebergIncrementalPosDeleteTaskStream,
) {
    if !stream.is_null() {
        unsafe { let _ = Box::from_raw(stream); }
    }
}

/// Free an append task. Do NOT call after passing it to read_append_task — that consumes it.
#[no_mangle]
pub extern "C" fn iceberg_incremental_append_task_free(
    task: *mut IcebergIncrementalAppendTask,
) {
    if !task.is_null() {
        unsafe { let _ = Box::from_raw(task); }
    }
}

/// Free a positional-delete task. Do NOT call after passing to read_pos_delete_task.
#[no_mangle]
pub extern "C" fn iceberg_incremental_pos_delete_task_free(
    task: *mut IcebergIncrementalPosDeleteTask,
) {
    if !task.is_null() {
        unsafe { let _ = Box::from_raw(task); }
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

// Get next batch from stream
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
        use futures::TryStreamExt;
        let mut stream_guard = stream_ref.stream.lock().await;

        match stream_guard.try_next().await {
            Ok(Some(record_batch)) => {
                Ok(Some(record_batch))
            }
            Ok(None) => {
                // End of stream
                tracing::debug!("End of stream reached");
                Ok(None)
            }
            Err(e) => Err(anyhow::anyhow!("Error reading batch: {}", e)),
        }
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
