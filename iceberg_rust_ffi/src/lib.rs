use futures::StreamExt;
use futures::TryStreamExt;
use std::ffi::{c_char, c_void};

use anyhow::Result;
use iceberg::io::FileIOBuilder;
use iceberg::table::StaticTable;
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

// Use helper functions from other modules
use util::{parse_c_string, parse_properties, parse_string_array};

// Wrapper type for Send-safe raw pointers
struct SendPtr<T>(*mut T);
unsafe impl<T: Send> Send for SendPtr<T> {}
impl<T> SendPtr<T> {
    fn as_ptr(self) -> *mut T {
        self.0
    }
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
pub extern "C" fn iceberg_table_free(table: *mut table::IcebergTable) {
    if !table.is_null() {
        unsafe {
            let _ = Box::from_raw(table);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_arrow_stream_free(stream: *mut table::IcebergArrowStream) {
    if !stream.is_null() {
        unsafe {
            let _ = Box::from_raw(stream);
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_arrow_batch_free(batch: *mut table::ArrowBatch) {
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

// Catalog operations
#[no_mangle]
pub extern "C" fn iceberg_catalog_free(catalog: *mut catalog::IcebergCatalog) {
    if !catalog.is_null() {
        unsafe {
            let _ = Box::from_raw(catalog);
        }
    }
}

// Create a REST catalog
export_runtime_op!(
    iceberg_rest_catalog_create,
    catalog::IcebergCatalogResponse,
    || {
        let uri_str = parse_c_string(uri, "uri")?;
        let props = parse_properties(properties, properties_len)?;
        Ok((uri_str, props))
    },
    result_tuple,
    async {
        let (uri, props) = result_tuple;
        catalog::IcebergCatalog::create_rest(uri, props).await
    },
    uri: *const c_char,
    properties: *const PropertyEntry,
    properties_len: usize
);

// Load a table from the catalog
export_runtime_op!(
    iceberg_catalog_load_table,
    IcebergTableResponse,
    || {
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        let namespace_parts = parse_string_array(namespace_parts_ptr, namespace_parts_len)?;
        let table_name = parse_c_string(table_name, "table_name")?;

        Ok((SendPtr(catalog), namespace_parts, table_name))
    },
    result_tuple,
    async {
        let (catalog_ptr, namespace_parts, table_name) = result_tuple;
        let catalog_ref = unsafe { &*catalog_ptr.as_ptr() };
        catalog_ref.load_table(namespace_parts, table_name).await
    },
    catalog: *mut catalog::IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize,
    table_name: *const c_char
);

// List tables in a namespace
export_runtime_op!(
    iceberg_catalog_list_tables,
    catalog::IcebergStringListResponse,
    || {
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        let namespace_parts = parse_string_array(namespace_parts_ptr, namespace_parts_len)?;
        Ok((SendPtr(catalog), namespace_parts))
    },
    result_tuple,
    async {
        let (catalog_ptr, namespace_parts) = result_tuple;
        let catalog_ref = unsafe { &*catalog_ptr.as_ptr() };
        catalog_ref.list_tables(namespace_parts).await
    },
    catalog: *mut catalog::IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize
);

// List namespaces
export_runtime_op!(
    iceberg_catalog_list_namespaces,
    catalog::IcebergNestedStringListResponse,
    || {
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        let parent_parts = if namespace_parts_len > 0 {
            Some(parse_string_array(namespace_parts_ptr, namespace_parts_len)?)
        } else {
            None
        };

        Ok((SendPtr(catalog), parent_parts))
    },
    result_tuple,
    async {
        let (catalog_ptr, parent_parts) = result_tuple;
        let catalog_ref = unsafe { &*catalog_ptr.as_ptr() };
        catalog_ref.list_namespaces(parent_parts).await
    },
    catalog: *mut catalog::IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize
);

// Check if a table exists
export_runtime_op!(
    iceberg_catalog_table_exists,
    catalog::IcebergBoolResponse,
    || {
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        let namespace_parts = parse_string_array(namespace_parts_ptr, namespace_parts_len)?;
        let table_name = parse_c_string(table_name, "table_name")?;

        Ok((SendPtr(catalog), namespace_parts, table_name))
    },
    result_tuple,
    async {
        let (catalog_ptr, namespace_parts, table_name) = result_tuple;
        let catalog_ref = unsafe { &*catalog_ptr.as_ptr() };
        catalog_ref.table_exists(namespace_parts, table_name).await
    },
    catalog: *mut catalog::IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize,
    table_name: *const c_char
);
