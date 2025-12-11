/// Table and streaming support for iceberg_rust_ffi
use crate::{CResult, Context, RawResponse};
use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use iceberg::table::Table;
use std::ffi::{c_char, c_void};
use std::ptr;
use tokio::sync::Mutex as AsyncMutex;

/// Direct table structure - no opaque wrapper
#[repr(C)]
pub struct IcebergTable {
    pub table: Table,
}

/// Stream wrapper for FFI - using async mutex to avoid blocking calls
#[repr(C)]
pub struct IcebergArrowStream {
    // TODO: Maybe remove this mutex and let this be handled in Julia?
    pub stream:
        AsyncMutex<futures::stream::BoxStream<'static, Result<RecordBatch, iceberg::Error>>>,
}

unsafe impl Send for IcebergArrowStream {}

/// Arrow batch serialized for FFI
#[repr(C)]
pub struct ArrowBatch {
    pub data: *const u8,
    pub length: usize,
    pub rust_ptr: *mut std::ffi::c_void,
}

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
    type Payload = Option<RecordBatch>;
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
            Some(batch) => {
                // TODO: This is currently a bottleneck, and should be done in parallel.
                let arrow_batch = serialize_record_batch(batch);
                match arrow_batch {
                    Ok(arrow_batch) => {
                        self.batch = Box::into_raw(Box::new(arrow_batch));
                    }
                    Err(_) => {
                        self.batch = ptr::null_mut();
                    }
                }
            }
            None => self.batch = ptr::null_mut(),
        }
    }
}

/// Helper function to serialize RecordBatch to Arrow IPC format
pub fn serialize_record_batch(batch: RecordBatch) -> Result<ArrowBatch> {
    let buffer = Vec::new();
    let mut stream_writer = StreamWriter::try_new(buffer, &batch.schema())?;
    stream_writer.write(&batch)?;
    stream_writer.finish()?;
    let serialized_data = stream_writer.into_inner()?;

    let boxed_data = Box::new(serialized_data);
    let data_ptr = boxed_data.as_ptr();
    let length = boxed_data.len();
    let rust_ptr = Box::into_raw(boxed_data) as *mut c_void;

    Ok(ArrowBatch {
        data: data_ptr,
        length,
        rust_ptr,
    })
}
