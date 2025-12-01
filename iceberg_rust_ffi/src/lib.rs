use futures::StreamExt;
use futures::TryStreamExt;
use std::ffi::{c_char, c_void, CStr};
use std::ptr;
use tokio::sync::Mutex as AsyncMutex;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use iceberg::io::FileIOBuilder;
use iceberg::table::{StaticTable, Table};
use iceberg::TableIdent;

// Import from object_store_ffi
use object_store_ffi::{
    cancel_context, current_metrics, destroy_context, destroy_cstring, export_runtime_op,
    with_cancellation, CResult, Context, NotifyGuard, PanicCallback, RawResponse, ResponseGuard,
    ResultCallback, RESULT_CB, RT,
};

// Modules for scan functionality
mod full;
mod incremental;
mod scan_common;

// Re-export scan types and functions
pub use full::IcebergScan;
pub use incremental::{IcebergIncrementalScan, IcebergUnzippedStreamsResponse};

// We use `jl_adopt_thread` to ensure Rust can call into Julia when notifying
// the Base.Event that is waiting for the Rust result.
// Note that this will be linked in from the Julia process, we do not try
// to link it while building this Rust lib.
#[cfg(feature = "julia")]
extern "C" {
    fn jl_adopt_thread() -> i32;
    fn jl_gc_safe_enter() -> i32;
    fn jl_gc_disable_finalizers_internal() -> c_void;
}

// Simple response type for operations that only need success/failure status
#[repr(C)]
pub struct IcebergResponse {
    result: CResult,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergResponse {}

impl RawResponse for IcebergResponse {
    type Payload = ();

    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }

    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }

    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }

    fn set_payload(&mut self, _payload: Option<Self::Payload>) {
        // No payload for simple response
    }
}

// Simple config for iceberg - only what we need
#[derive(Copy, Clone)]
#[repr(C)]
pub struct IcebergStaticConfig {
    n_threads: usize,
}

impl Default for IcebergStaticConfig {
    fn default() -> Self {
        IcebergStaticConfig {
            n_threads: 0, // 0 means use tokio's default
        }
    }
}

// FFI structure for passing key-value properties
#[repr(C)]
pub struct PropertyEntry {
    pub key: *const c_char,
    pub value: *const c_char,
}

unsafe impl Send for PropertyEntry {}

// Direct structures - no opaque wrappers
#[repr(C)]
pub struct IcebergTable {
    pub table: Table,
}

// Stream wrapper for FFI - using async mutex to avoid blocking calls
#[repr(C)]
pub struct IcebergArrowStream {
    // TODO: Maybe remove this mutex and let this be handled in Julia?
    pub stream: AsyncMutex<futures::stream::BoxStream<'static, Result<ArrowBatch, iceberg::Error>>>,
}

unsafe impl Send for IcebergArrowStream {}

#[repr(C)]
pub struct ArrowBatch {
    pub data: *const u8,
    pub length: usize,
    pub rust_ptr: *mut c_void,
}

// SAFETY: ArrowBatch contains raw pointers that are owned by the FFI layer.
// The pointers are allocated in Rust and deallocated via iceberg_arrow_batch_free,
// making it safe to send between threads.
unsafe impl Send for ArrowBatch {}

// Response types for async operations
#[repr(C)]
pub struct IcebergTableResponse {
    result: CResult,
    table: *mut IcebergTable,
    error_message: *mut c_char,
    context: *const Context,
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

#[repr(C)]
pub struct IcebergArrowStreamResponse {
    result: CResult,
    stream: *mut IcebergArrowStream,
    error_message: *mut c_char,
    context: *const Context,
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

#[repr(C)]
pub struct IcebergBatchResponse {
    result: CResult,
    batch: *mut ArrowBatch,
    error_message: *mut c_char,
    context: *const Context,
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
                // Serialization already done - just transfer ownership
                self.batch = Box::into_raw(Box::new(arrow_batch));
            }
            None => self.batch = ptr::null_mut(),
        }
    }
}

// Helper function to transform a RecordBatch stream into an ArrowBatch stream with parallel serialization
pub fn transform_stream_with_parallel_serialization(
    stream: futures::stream::BoxStream<'static, Result<RecordBatch, iceberg::Error>>,
    concurrency: usize,
) -> futures::stream::BoxStream<'static, Result<ArrowBatch, iceberg::Error>> {
    stream
        .map(|batch_result| async move {
            match batch_result {
                Ok(record_batch) => {
                    // Spawn blocking task and await immediately
                    match tokio::task::spawn_blocking(move || serialize_record_batch(record_batch))
                        .await
                    {
                        Ok(Ok(arrow_batch)) => Ok(arrow_batch),
                        Ok(Err(e)) => Err(iceberg::Error::new(
                            iceberg::ErrorKind::Unexpected,
                            e.to_string(),
                        )),
                        Err(e) => Err(iceberg::Error::new(
                            iceberg::ErrorKind::Unexpected,
                            format!("Serialization task panicked: {}", e),
                        )),
                    }
                }
                Err(e) => Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Stream error: {}", e),
                )),
            }
        })
        .buffer_unordered(concurrency)
        .boxed()
}

// Helper function to create ArrowBatch from RecordBatch
// TODO: Switch to zero-copy once Arrow.jl supports C API.
fn serialize_record_batch(batch: RecordBatch) -> Result<ArrowBatch> {
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

// Initialize runtime - configure RT and RESULT_CB directly
#[no_mangle]
pub extern "C" fn iceberg_init_runtime(
    config: IcebergStaticConfig,
    panic_callback: PanicCallback,
    result_callback: ResultCallback,
) -> CResult {
    // Set the result callback
    if let Err(_) = RESULT_CB.set(result_callback) {
        return CResult::Error; // Already initialized
    }

    // Set up panic hook
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        prev(info);
        unsafe { panic_callback() };
    }));

    // Set up logging if not already configured
    if std::env::var("RUST_LOG").is_err() {
        unsafe { std::env::set_var("RUST_LOG", "iceberg_rust_ffi=warn,iceberg=warn") }
    }

    // Initialize tracing subscriber
    let _ = tracing_subscriber::fmt::try_init();

    // Build tokio runtime
    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();

    // Configure Julia thread adoption for Julia integration
    rt_builder.on_thread_start(|| {
        #[cfg(feature = "julia")]
        {
            unsafe { jl_adopt_thread() };
            unsafe { jl_gc_safe_enter() };
            unsafe { jl_gc_disable_finalizers_internal() };
        }
    });

    if config.n_threads > 0 {
        rt_builder.worker_threads(config.n_threads);
    }

    let runtime = match rt_builder.build() {
        Ok(rt) => rt,
        Err(_) => return CResult::Error,
    };

    if RT.set(runtime).is_err() {
        return CResult::Error;
    }

    CResult::Ok
}

// Use export_runtime_op! macro for table opening
export_runtime_op!(
    iceberg_table_open,
    IcebergTableResponse,
    || {
        let snapshot_path_str = unsafe {
            CStr::from_ptr(snapshot_path).to_str()
                .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in snapshot path: {}", e))?
        };

        let scheme_str = unsafe {
            CStr::from_ptr(scheme).to_str()
                .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in scheme: {}", e))?
        };

        // Convert properties from FFI to Rust Vec
        let mut props = Vec::new();
        if !properties.is_null() && properties_len > 0 {
            let properties_slice = unsafe {
                std::slice::from_raw_parts(properties, properties_len)
            };

            for prop in properties_slice {
                let key = unsafe {
                    CStr::from_ptr(prop.key).to_str()
                        .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in property key: {}", e))?
                };
                let value = unsafe {
                    CStr::from_ptr(prop.value).to_str()
                        .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in property value: {}", e))?
                };
                props.push((key.to_string(), value.to_string()));
            }
        }

        Ok((snapshot_path_str.to_string(), scheme_str.to_string(), props))
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

// Async function to get next batch from existing stream
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

// Synchronous operations
#[no_mangle]
pub extern "C" fn iceberg_table_free(table: *mut IcebergTable) {
    if !table.is_null() {
        unsafe {
            let _ = Box::from_raw(table);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_arrow_stream_free(stream: *mut IcebergArrowStream) {
    if !stream.is_null() {
        unsafe {
            let _ = Box::from_raw(stream);
        }
    }
}

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

// Re-export object_store_ffi utilities
#[no_mangle]
pub extern "C" fn iceberg_destroy_cstring(string: *mut c_char) -> CResult {
    destroy_cstring(string)
}

#[no_mangle]
pub extern "C" fn iceberg_current_metrics() -> *const c_char {
    current_metrics()
}

// Re-export context management functions for cancellation support
#[no_mangle]
pub extern "C" fn iceberg_cancel_context(ctx_ptr: *const Context) -> CResult {
    cancel_context(ctx_ptr)
}

#[no_mangle]
pub extern "C" fn iceberg_destroy_context(ctx_ptr: *const Context) -> CResult {
    destroy_context(ctx_ptr)
}
