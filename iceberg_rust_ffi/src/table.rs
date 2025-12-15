/// Table and streaming support for iceberg_rust_ffi
use crate::{CResult, Context, RawResponse};
use iceberg::io::FileIOBuilder;
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

/// Response type for table operations
#[repr(C)]
pub struct IcebergTableResponse {
    pub result: CResult,
    pub table: *mut IcebergTable,
    pub error_message: *mut c_char,
    pub context: *const Context,
}

unsafe impl Send for IcebergTableResponse {}

impl RawResponse for IcebergTableResponse {
    type Payload = IcebergTable;
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
            Some(table) => {
                let table_ptr = Box::into_raw(Box::new(table));
                self.table = table_ptr;
            }
            None => self.table = ptr::null_mut(),
        }
    }
}

/// Response type for stream operations
#[repr(C)]
pub struct IcebergArrowStreamResponse {
    pub result: CResult,
    pub stream: *mut IcebergArrowStream,
    pub error_message: *mut c_char,
    pub context: *const Context,
}

unsafe impl Send for IcebergArrowStreamResponse {}

impl RawResponse for IcebergArrowStreamResponse {
    type Payload = IcebergArrowStream;

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
            Some(stream) => {
                self.stream = Box::into_raw(Box::new(stream));
            }
            None => self.stream = ptr::null_mut(),
        }
    }
}

/// Response type for batch operations
#[repr(C)]
pub struct IcebergBatchResponse {
    pub result: CResult,
    pub batch: *mut ArrowBatch,
    pub error_message: *mut c_char,
    pub context: *const Context,
}

unsafe impl Send for IcebergBatchResponse {}

impl RawResponse for IcebergBatchResponse {
    type Payload = Option<ArrowBatch>;
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
        match payload.flatten() {
            Some(arrow_batch) => {
                self.batch = Box::into_raw(Box::new(arrow_batch));
            }
            None => self.batch = ptr::null_mut(),
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

// FFI Export functions for table operations
// These functions are exported to be called from Julia via the FFI

// Open a table from metadata file
export_runtime_op!(
    iceberg_table_open,
    IcebergTableResponse,
    || {
        let snapshot_path_str = parse_c_string(snapshot_path, "snapshot_path")?;
        let scheme_str = parse_c_string(scheme, "scheme")?;
        let props = parse_properties(properties, properties_len)?;

        // Convert HashMap to Vec of tuples for compatibility with FileIOBuilder::with_props
        let props_vec: Vec<(String, String)> = props.into_iter().collect();
        Ok((snapshot_path_str, scheme_str, props_vec))
    },
    result_tuple,
    async {
        let (full_metadata_path, scheme_string, props) = result_tuple;

        // Create file IO with the specified scheme
        // Default behavior (when props is empty) uses environment variables for credentials
        let file_io = FileIOBuilder::new(&scheme_string)
            .with_props(props)
            .build()?;

        // Create table identifier
        let table_ident = TableIdent::from_strs(["default", "table"])?;

        // Load the static table
        let static_table =
            StaticTable::from_metadata_file(&full_metadata_path, table_ident, file_io).await?;

        Ok::<IcebergTable, anyhow::Error>(IcebergTable { table: static_table.into_table() })
    },
    snapshot_path: *const c_char,
    scheme: *const c_char,
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
