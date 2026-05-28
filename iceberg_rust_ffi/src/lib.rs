use futures::StreamExt;
use std::ffi::{c_char, c_void};

pub(crate) fn unexpected(msg: impl std::fmt::Display) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, msg.to_string())
}

pub(crate) fn cpu_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

use anyhow::Result;
use arrow_array::{ffi as arrow_ffi, Array, RecordBatch, StructArray};

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

// Transaction module
mod transaction;

// Writer module
mod writer;

// Column-based writer module (zero-copy from Julia)
mod writer_columns;

// Profiling stats for the file-parallel pipeline
mod pipeline_stats;

// Ordered file-parallel pipeline (shared helpers)
mod nested_pipeline;

// Full-scan pipeline entry points (composes `nested_pipeline` helpers
// with an `ArrowReaderBuilder`-based per-file batch stream).
mod full_pipeline;

// Per-file nested pipeline for incremental scans
mod incremental_pipeline;

// Response types module
mod response;

// Utility module for FFI parsing
mod util;

// Stable error codes and classifier
mod error_codes;
pub use error_codes::{classified_error, classify, ClassifiedError};

// Re-export types and functions from submodules
pub use catalog::{IcebergBoolResponse, IcebergCatalog, IcebergCatalogResponse};
pub use full::IcebergScan;
pub use incremental::{IcebergIncrementalScan, IcebergUnzippedStreamsResponse};
pub use response::{
    IcebergBoxedResponse, IcebergNestedStringListResponse, IcebergPropertyResponse,
    IcebergStringListResponse,
};
pub use table::{
    ArrowBatch, IcebergArrowStream, IcebergArrowStreamResponse, IcebergBatchResponse,
    IcebergFileScan, IcebergFileScanResponse, IcebergFileScanStream, IcebergFileScanStreamResponse,
    IcebergTable, IcebergTableResponse,
};
pub use transaction::{
    IcebergDataFiles, IcebergDataFilesResponse, IcebergOverwriteAction, IcebergTransaction,
    IcebergTransactionResponse,
};
pub use writer::{
    IcebergDataFileWriter, IcebergDataFileWriterResponse, IcebergWriterCloseResponse,
};
pub use writer_columns::ColumnDescriptor;

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
#[derive(Copy, Clone, Default)]
#[repr(C)]
pub struct IcebergStaticConfig {
    n_threads: usize,
}

// FFI structure for passing key-value properties
#[repr(C)]
pub struct PropertyEntry {
    pub key: *const c_char,
    pub value: *const c_char,
}

unsafe impl Send for PropertyEntry {}

// Table structures are in the table module

/// Export a RecordBatch via the Arrow C Data Interface (zero-copy).
///
/// The batch is represented as a struct-typed (ArrowSchema, ArrowArray) pair
/// where each column appears as a child.  Julia consumes it with
/// `Arrow.from_c_data`.  `iceberg_arrow_batch_free` drops the inner
/// `ArrowBatchInner`, which calls the arrow-rs `Drop` impls and frees all
/// buffer Arcs.
fn record_batch_to_c_ffi(batch: RecordBatch) -> Result<ArrowBatch> {
    use crate::table::ArrowBatchInner;

    let struct_array = StructArray::from(batch);
    // to_ffi returns (FFI_ArrowArray, FFI_ArrowSchema) — array first, schema second.
    let (ffi_array, ffi_schema) = arrow_ffi::to_ffi(&struct_array.to_data())?;

    let mut inner = Box::new(ArrowBatchInner {
        schema: ffi_schema,
        array: ffi_array,
    });
    let schema_ptr = &mut inner.schema as *mut arrow_ffi::FFI_ArrowSchema;
    let array_ptr = &mut inner.array as *mut arrow_ffi::FFI_ArrowArray;
    let rust_ptr = Box::into_raw(inner) as *mut c_void;

    Ok(ArrowBatch {
        schema: schema_ptr,
        array: array_ptr,
        rust_ptr,
    })
}

/// Transform a stream of RecordBatches into a stream of C Data Interface ArrowBatches.
pub(crate) fn transform_stream_with_parallel_serialization(
    stream: futures::stream::BoxStream<'static, Result<RecordBatch, iceberg::Error>>,
    concurrency: usize,
) -> futures::stream::BoxStream<'static, Result<ArrowBatch, iceberg::Error>> {
    stream
        .map(|batch_result| async move {
            match batch_result {
                Ok(record_batch) => record_batch_to_c_ffi(record_batch).map_err(unexpected),
                Err(e) => Err(unexpected(format!("Stream error: {e}"))),
            }
        })
        .buffered(concurrency)
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
    if RESULT_CB.set(result_callback).is_err() {
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
