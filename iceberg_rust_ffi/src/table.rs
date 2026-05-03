use crate::ordered_file_pipeline::FileScan;
use crate::response::IcebergBoxedResponse;
/// Table and streaming support for iceberg_rust_ffi
use crate::{CResult, Context, RawResponse};
use iceberg::io::{FileIOBuilder, OpenDalRoutingStorageFactory};
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

/// Type aliases for response types
pub type IcebergTableResponse = IcebergBoxedResponse<IcebergTable>;
pub type IcebergArrowStreamResponse = IcebergBoxedResponse<IcebergArrowStream>;

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

/// Outer stream of per-file scans from the nested pipeline.
pub struct IcebergFileScanStream {
    pub stream: AsyncMutex<futures::stream::BoxStream<'static, Result<FileScan, iceberg::Error>>>,
}

unsafe impl Send for IcebergFileScanStream {}

/// C-compatible per-file scan item returned to Julia.
/// Owns `filename` (must be freed via iceberg_file_scan_free) and `stream`.
#[repr(C)]
pub struct IcebergFileScan {
    /// Null-terminated file path. Owned; freed by iceberg_file_scan_free.
    pub filename: *mut c_char,
    pub record_count: i64,
    /// Inner batch stream. Owned; freed by iceberg_file_scan_free.
    /// Callers must NOT call iceberg_arrow_stream_free on this pointer.
    pub stream: *mut IcebergArrowStream,
}

unsafe impl Send for IcebergFileScan {}

pub type IcebergFileScanStreamResponse = IcebergBoxedResponse<IcebergFileScanStream>;

/// Response for iceberg_next_file_scan (mirrors IcebergBatchResponse).
#[repr(transparent)]
pub struct IcebergFileScanResponse(pub IcebergBoxedResponse<IcebergFileScan>);

unsafe impl Send for IcebergFileScanResponse {}

impl RawResponse for IcebergFileScanResponse {
    type Payload = Option<FileScan>;

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
            Some(fs) => {
                let filename = std::ffi::CString::new(fs.filename)
                    .unwrap_or_default()
                    .into_raw();
                let stream = Box::into_raw(Box::new(fs.stream));
                self.0.value = Box::into_raw(Box::new(IcebergFileScan {
                    filename,
                    record_count: fs.record_count,
                    stream,
                }));
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

/// Free a file scan (its owned filename and inner stream).
#[no_mangle]
pub extern "C" fn iceberg_file_scan_free(scan: *mut IcebergFileScan) {
    if scan.is_null() {
        return;
    }
    unsafe {
        let scan = Box::from_raw(scan);
        if !scan.filename.is_null() {
            let _ = std::ffi::CString::from_raw(scan.filename);
        }
        if !scan.stream.is_null() {
            let _ = Box::from_raw(scan.stream);
        }
    }
}

/// Free a file scan stream.
#[no_mangle]
pub extern "C" fn iceberg_file_scan_stream_free(stream: *mut IcebergFileScanStream) {
    if !stream.is_null() {
        unsafe {
            let _ = Box::from_raw(stream);
        }
    }
}

/// Return the record count of a file scan. Returns -1 on null input.
#[no_mangle]
pub extern "C" fn iceberg_file_scan_record_count(scan: *const IcebergFileScan) -> i64 {
    if scan.is_null() {
        return -1;
    }
    unsafe { (*scan).record_count }
}

/// Return a borrowed pointer to the null-terminated filename of a file scan.
/// The pointer is valid for the lifetime of the IcebergFileScan.
/// The caller must NOT free this pointer; iceberg_file_scan_free handles it.
#[no_mangle]
pub extern "C" fn iceberg_file_scan_filename(scan: *const IcebergFileScan) -> *const c_char {
    if scan.is_null() {
        return ptr::null();
    }
    unsafe { (*scan).filename }
}

// Get next file scan from outer stream (async)
export_runtime_op!(
    iceberg_next_file_scan,
    IcebergFileScanResponse,
    || {
        if stream.is_null() {
            return Err(anyhow::anyhow!("Null file scan stream pointer"));
        }
        let stream_ref = unsafe { &*stream };
        Ok(stream_ref)
    },
    stream_ref,
    async {
        use futures::StreamExt;
        let mut guard = stream_ref.stream.lock().await;
        match guard.next().await {
            Some(Ok(fs)) => Ok(Some(fs)),
            Some(Err(e)) => Err(anyhow::anyhow!("Error reading file scan: {}", e)),
            None => Ok(None),
        }
    },
    stream: *mut IcebergFileScanStream
);

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
