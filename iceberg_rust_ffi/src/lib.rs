use futures::StreamExt;
use std::ffi::{c_char, c_void};

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;

// Import from object_store_ffi
use object_store_ffi::{
    cancel_context, current_metrics, destroy_context, destroy_cstring, CResult, Context,
    PanicCallback, RawResponse, ResultCallback, RESULT_CB, RT,
};

// Modules for scan functionality
mod full;
mod incremental;
mod scan_common;

// Catalog module
mod catalog;

// Table and streaming module
mod table;

// Utility module for FFI parsing
mod util;

// Re-export types and functions from submodules
pub use catalog::{
    IcebergBoolResponse, IcebergCatalog, IcebergCatalogResponse, IcebergNestedStringListResponse,
    IcebergStringListResponse,
};
pub use full::IcebergScan;
pub use incremental::{IcebergIncrementalScan, IcebergUnzippedStreamsResponse};
pub use table::{
    ArrowBatch, IcebergArrowStream, IcebergArrowStreamResponse, IcebergBatchResponse, IcebergTable,
    IcebergTableResponse,
};

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

// Table structures are in the table module

/// Helper function to serialize RecordBatch to Arrow IPC format
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

/// Transform a stream of RecordBatches into a stream of serialized ArrowBatches
/// with configurable parallel serialization concurrency
pub(crate) fn transform_stream_with_parallel_serialization(
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
