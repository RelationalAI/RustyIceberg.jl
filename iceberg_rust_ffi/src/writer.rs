/// Writer support for iceberg_rust_ffi
///
/// Encoding uses Tokio's blocking thread pool via `spawn_blocking`. A global `Semaphore`
/// with N=available_parallelism permits bounds concurrent Parquet+ZSTD encodes. Per-writer
/// ordering is guaranteed by the per-writer `Arc<Mutex<ConcreteDataFileWriter>>` inside
/// WriterState: only one blocking task encodes a given writer at a time.
use std::ffi::{c_char, c_void};
use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::DataFileFormat;
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

/// Compression codec values (must match Julia's CompressionCodec enum)
const COMPRESSION_UNCOMPRESSED: i32 = 0;
const COMPRESSION_SNAPPY: i32 = 1;
const COMPRESSION_GZIP: i32 = 2;
const COMPRESSION_LZ4: i32 = 3;
const COMPRESSION_ZSTD: i32 = 4;

fn compression_from_code(code: i32) -> Compression {
    match code {
        COMPRESSION_UNCOMPRESSED => Compression::UNCOMPRESSED,
        COMPRESSION_SNAPPY => Compression::SNAPPY,
        COMPRESSION_GZIP => Compression::GZIP(Default::default()),
        COMPRESSION_LZ4 => Compression::LZ4,
        COMPRESSION_ZSTD => Compression::ZSTD(Default::default()),
        _ => Compression::SNAPPY,
    }
}

/// Parquet writer properties passed from Julia (must match Julia's ParquetWriterProperties layout).
/// Fields are ordered largest-to-smallest to avoid gaps in the repr(C) layout.
/// Size fields use 0 to mean "use parquet default".
#[repr(C)]
pub struct ParquetWriterPropertiesFFI {
    /// Maximum number of rows per row group (0 = parquet default: 1 048 576)
    pub max_row_group_size: i64,
    /// Target uncompressed data page size in bytes (0 = parquet default: 1 048 576)
    pub data_page_size: i64,
    /// Number of rows encoded per column chunk within a row group (0 = parquet default: 1024)
    pub write_batch_size: i64,
    /// Compression codec (see COMPRESSION_* constants)
    pub compression_codec: i32,
    /// Whether to enable dictionary encoding globally
    pub dictionary_enabled: bool,
    /// Force PLAIN encoding for all columns, bypassing DELTA_BINARY_PACKED default for INT64/INT32
    pub use_plain_encoding: bool,
    /// Collect per-page and per-row-group min/max statistics (default true; set false to skip)
    pub statistics_enabled: bool,
}

impl ParquetWriterPropertiesFFI {
    fn to_writer_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_compression(compression_from_code(self.compression_codec))
            .set_dictionary_enabled(self.dictionary_enabled);
        if self.max_row_group_size > 0 {
            builder = builder.set_max_row_group_row_count(Some(self.max_row_group_size as usize));
        }
        if self.data_page_size > 0 {
            builder = builder.set_data_page_size_limit(self.data_page_size as usize);
        }
        if self.write_batch_size > 0 {
            builder = builder.set_write_batch_size(self.write_batch_size as usize);
        }
        if self.use_plain_encoding {
            builder = builder.set_encoding(Encoding::PLAIN);
        }
        if !self.statistics_enabled {
            builder = builder.set_statistics_enabled(EnabledStatistics::None);
        }
        builder.build()
    }
}

use crate::batch_builder::ColumnBatchBuilder;
use crate::response::IcebergBoxedResponse;
use crate::table::IcebergTable;
use crate::transaction::IcebergDataFiles;
use crate::util::parse_c_string;
use crate::writer_columns::{ColumnDescriptor, SliceRef};
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};

/// Type alias for the concrete DataFileWriter we use
type ConcreteDataFileWriter =
    DataFileWriter<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

/// Shared mutable state for one IcebergDataFileWriter.
pub(crate) struct WriterState {
    /// The underlying Parquet writer. Protected by a Mutex so at most one blocking task
    /// encodes a given writer at a time. Set to None when the writer is closed or freed.
    writer: Mutex<Option<ConcreteDataFileWriter>>,
    /// Number of encode tasks submitted but not yet completed.
    pending: AtomicUsize,
    /// Notified when `pending` drops to zero so iceberg_writer_close can wait efficiently.
    done_notify: tokio::sync::Notify,
    /// First encode error encountered by any task for this writer, if any.
    error: Mutex<Option<anyhow::Error>>,
}

// Safety: ConcreteDataFileWriter is Send.
unsafe impl Send for WriterState {}
unsafe impl Sync for WriterState {}

/// Global encode state: semaphore + Tokio handle.
/// Captured at first writer creation (inside the Tokio runtime) so submit_batch can
/// spawn tasks even when called from Julia's sync (non-Tokio) context.
struct GlobalEncodeState {
    semaphore: Arc<tokio::sync::Semaphore>,
    handle: tokio::runtime::Handle,
}

static GLOBAL_ENCODE_STATE: OnceLock<GlobalEncodeState> = OnceLock::new();

/// Must be called from within a Tokio runtime (iceberg_writer_new satisfies this).
fn get_or_init_encode_state() -> &'static GlobalEncodeState {
    GLOBAL_ENCODE_STATE.get_or_init(|| {
        let n = thread::available_parallelism().unwrap().get();
        GlobalEncodeState {
            semaphore: Arc::new(tokio::sync::Semaphore::new(n)),
            handle: tokio::runtime::Handle::current(),
        }
    })
}

/// Opaque writer handle for FFI.
///
/// Writing is pipelined: Julia gathers a RecordBatch and submits it directly to the
/// global encode pool, then returns immediately. Pool workers (N = available_parallelism)
/// encode Parquet concurrently across all active writers.
pub struct IcebergDataFileWriter {
    /// Arrow schema for this table, used by write_columns to create RecordBatches.
    pub(crate) arrow_schema: ArrowSchemaRef,
    /// Shared state: owns the ConcreteDataFileWriter, tracks pending count and errors.
    pub(crate) writer_state: Arc<WriterState>,
}

unsafe impl Send for IcebergDataFileWriter {}
unsafe impl Sync for IcebergDataFileWriter {}

/// Type alias for writer response
pub type IcebergDataFileWriterResponse = IcebergBoxedResponse<IcebergDataFileWriter>;

/// Type alias for data files response (returns IcebergDataFiles handle)
pub type IcebergWriterCloseResponse = IcebergBoxedResponse<IcebergDataFiles>;

/// Store an error in the writer state (first error wins).
pub(crate) fn store_writer_error(writer_ref: &IcebergDataFileWriter, e: anyhow::Error) {
    let mut slot = writer_ref
        .writer_state
        .error
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if slot.is_none() {
        *slot = Some(e);
    }
}

/// Submit a `RecordBatch` for async Parquet encoding via `spawn_blocking`.
///
/// A semaphore permit is acquired before the blocking task runs, bounding concurrent
/// encodes to N. The caller returns immediately; errors are surfaced at close time.
pub(crate) fn submit_batch(
    writer_ref: &IcebergDataFileWriter,
    batch: RecordBatch,
) -> Result<(), anyhow::Error> {
    let enc = match GLOBAL_ENCODE_STATE.get() {
        Some(s) => s,
        None => {
            return Err(anyhow::anyhow!(
                "encode state not initialized; call iceberg_writer_new first"
            ))
        }
    };
    let state = writer_ref.writer_state.clone();
    let semaphore = enc.semaphore.clone();
    let handle = enc.handle.clone();
    state.pending.fetch_add(1, Ordering::AcqRel);

    handle.clone().spawn(async move {
        // Acquire a permit before spawning — this is the backpressure mechanism.
        let permit = match semaphore.acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                let mut slot = state.error.lock().unwrap_or_else(|e| e.into_inner());
                if slot.is_none() {
                    *slot = Some(anyhow::anyhow!("encode semaphore closed unexpectedly"));
                }
                drop(slot);
                let prev = state.pending.fetch_sub(1, Ordering::AcqRel);
                if prev == 1 {
                    state.done_notify.notify_one();
                }
                return;
            }
        };

        let state2 = state.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit; // released when blocking work completes
            let mut guard = state2.writer.lock().unwrap_or_else(|e| e.into_inner());
            match guard.as_mut() {
                Some(w) => handle
                    .block_on(w.write(batch))
                    .map_err(|e| anyhow::anyhow!("write batch: {}", e)),
                None => Err(anyhow::anyhow!("writer already closed")),
            }
        })
        .await;

        let err = match result {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e),
            Err(join_err) => Some(anyhow::anyhow!("encode task failed: {}", join_err)),
        };
        if let Some(e) = err {
            let mut slot = state.error.lock().unwrap_or_else(|e| e.into_inner());
            if slot.is_none() {
                *slot = Some(e);
            }
        }
        let prev = state.pending.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            state.done_notify.notify_one();
        }
    });

    Ok(())
}

unsafe fn write_columns_inner(
    writer_ref: &IcebergDataFileWriter,
    arrow_schema: ArrowSchemaRef,
    col_descs: &[ColumnDescriptor],
) -> Result<(), anyhow::Error> {
    if col_descs.len() != arrow_schema.fields().len() {
        return Err(anyhow::anyhow!(
            "Column count mismatch: got {} but schema has {}",
            col_descs.len(),
            arrow_schema.fields().len()
        ));
    }
    let num_rows = col_descs.iter().map(|d| d.num_rows).max().unwrap_or(0);
    let col_types: Vec<i32> = col_descs.iter().map(|d| d.column_type).collect();
    let mut builder = ColumnBatchBuilder::new(arrow_schema.clone(), &col_types, num_rows.max(1))?;
    let slices: Vec<SliceRef> = col_descs
        .iter()
        .map(|d| SliceRef {
            data_ptr: d.data_ptr,
            lengths_ptr: d.lengths_ptr,
            validity_ptr: d.validity_ptr,
            sel_ptr: std::ptr::null(),
            len: d.num_rows,
        })
        .collect();
    unsafe { builder.append_slice(&slices) }?;
    builder.write_and_reset(writer_ref)
}

/// Synchronous write of flat column data: copies each column from Julia memory into
/// Rust-owned Arrow arrays in the calling thread, then submits to the global encode
/// pool asynchronously.
///
/// Each `ColumnDescriptor` is treated as a single sequential slice (no scatter/gather).
/// Returns 0 on success, -1 on error (error stored in writer state, propagated on close).
#[no_mangle]
pub extern "C" fn iceberg_writer_write_columns(
    writer: *mut IcebergDataFileWriter,
    columns: *const ColumnDescriptor,
    num_columns: usize,
) -> i32 {
    if writer.is_null() || columns.is_null() || num_columns == 0 {
        return -1;
    }
    let writer_ref = unsafe { &*writer };
    let arrow_schema = writer_ref.arrow_schema.clone();
    let col_descs = unsafe { std::slice::from_raw_parts(columns, num_columns) };
    if let Err(e) = unsafe { write_columns_inner(writer_ref, arrow_schema, col_descs) } {
        store_writer_error(writer_ref, e);
        return -1;
    }
    0
}

/// Free a writer. Poisons the writer state so any in-flight pool tasks fail gracefully.
#[no_mangle]
pub extern "C" fn iceberg_writer_free(writer: *mut IcebergDataFileWriter) {
    if !writer.is_null() {
        unsafe {
            let boxed = Box::from_raw(writer);
            // Poison the ConcreteDataFileWriter so any in-flight pool tasks return an error
            // rather than writing to a partially-freed writer.
            let _ = boxed.writer_state.writer.lock().unwrap().take();
        }
    }
}

// Create a new DataFileWriter from a table with configuration options.
//
// The global encode pool (N = available_parallelism threads) is initialized on the first call.
export_runtime_op!(
    iceberg_writer_new,
    IcebergDataFileWriterResponse,
    || {
        if table.is_null() {
            return Err(anyhow::anyhow!("Null table pointer provided"));
        }
        if parquet_props.is_null() {
            return Err(anyhow::anyhow!("Null parquet_props pointer provided"));
        }

        let prefix_str = parse_c_string(prefix, "prefix")?;
        let table_ref = unsafe { &*table };
        let props = unsafe { &*parquet_props };
        Ok((table_ref, prefix_str, target_file_size_bytes, props.to_writer_properties()))
    },
    result_tuple,
    async {
        let (table_ref, prefix_str, target_file_size_bytes, writer_props) = result_tuple;
        let table = &table_ref.table;

        // Create LocationGenerator from table metadata
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
            .map_err(|e| anyhow::anyhow!("Failed to create location generator: {}", e))?;

        // Create FileNameGenerator
        let file_name_generator = DefaultFileNameGenerator::new(
            prefix_str,
            None,
            DataFileFormat::Parquet,
        );

        // Create ParquetWriterBuilder with table schema and configured properties
        let parquet_writer_builder = ParquetWriterBuilder::new(
            writer_props,
            table.metadata().current_schema().clone(),
        );

        // Create RollingFileWriterBuilder with configured file size
        let rolling_file_writer_builder = if target_file_size_bytes > 0 {
            RollingFileWriterBuilder::new(
                parquet_writer_builder,
                target_file_size_bytes as usize,
                table.file_io().clone(),
                location_generator,
                file_name_generator,
            )
        } else {
            RollingFileWriterBuilder::new_with_default_file_size(
                parquet_writer_builder,
                table.file_io().clone(),
                location_generator,
                file_name_generator,
            )
        };

        // Build the concrete DataFileWriter
        let concrete_writer = DataFileWriterBuilder::new(rolling_file_writer_builder)
            .build(None)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to build data file writer: {}", e))?;

        // Convert Iceberg schema to Arrow schema for use in write_columns
        let arrow_schema = Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema().as_ref())
                .map_err(|e| anyhow::anyhow!("Failed to convert schema to Arrow: {}", e))?
        );

        // Initialize global encode state (no-op if already done).
        get_or_init_encode_state();

        let writer_state = Arc::new(WriterState {
            writer: Mutex::new(Some(concrete_writer)),
            pending: AtomicUsize::new(0),
            done_notify: tokio::sync::Notify::new(),
            error: Mutex::new(None),
        });

        Ok::<IcebergDataFileWriter, anyhow::Error>(IcebergDataFileWriter {
            arrow_schema,
            writer_state,
        })
    },
    table: *mut IcebergTable,
    prefix: *const c_char,
    target_file_size_bytes: i64,
    parquet_props: *const ParquetWriterPropertiesFFI
);

/// Write Arrow IPC data synchronously: copy IPC bytes from Julia, deserialize the stream,
/// and submit each RecordBatch to the global encode pool.
///
/// Returns 0 on success, -1 on error (error stored in writer state, propagated on close).
#[no_mangle]
pub extern "C" fn iceberg_writer_write(
    writer: *mut IcebergDataFileWriter,
    arrow_ipc_data: *const u8,
    arrow_ipc_len: usize,
) -> i32 {
    if writer.is_null() || arrow_ipc_data.is_null() || arrow_ipc_len == 0 {
        return -1;
    }

    let writer_ref = unsafe { &*writer };
    let ipc_bytes = unsafe { std::slice::from_raw_parts(arrow_ipc_data, arrow_ipc_len).to_vec() };

    let cursor = Cursor::new(ipc_bytes);
    let reader = match StreamReader::try_new(cursor, None) {
        Ok(r) => r,
        Err(e) => {
            store_writer_error(writer_ref, anyhow::anyhow!("IPC reader: {}", e));
            return -1;
        }
    };

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                store_writer_error(writer_ref, anyhow::anyhow!("IPC batch: {}", e));
                return -1;
            }
        };
        if let Err(e) = submit_batch(writer_ref, batch) {
            store_writer_error(writer_ref, e);
            return -1;
        }
    }

    0
}

// Close the writer and return the produced DataFiles.
//
// Waits for all pending pool encodes to complete, then finalizes the Parquet file
// and returns the DataFiles metadata.
export_runtime_op!(
    iceberg_writer_close,
    IcebergWriterCloseResponse,
    || {
        if writer.is_null() {
            return Err(anyhow::anyhow!("Null writer pointer provided"));
        }
        let writer_ref = unsafe { &mut *writer };
        Ok(writer_ref)
    },
    writer_ref,
    async {
        // Wait for all pending pool encodes to complete.
        // Uses a timeout to guard against a dead worker thread (e.g. panic outside
        // catch_unwind) that would otherwise leave pending > 0 forever.
        // Tokio's Notify preserves the notification if notify_one() fired before
        // notified() is polled, so the check-then-wait sequence is race-free.
        let state = &writer_ref.writer_state;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            if state.pending.load(Ordering::Acquire) == 0 {
                break;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(anyhow::anyhow!(
                    "Timed out waiting for {} encode task(s) to complete; \
                     an encode worker may have crashed",
                    state.pending.load(Ordering::Acquire)
                ));
            }
            tokio::select! {
                _ = state.done_notify.notified() => {}
                _ = tokio::time::sleep(remaining) => {}
            }
        }

        // Propagate any encode error
        if let Some(e) = state.error.lock().unwrap().take() {
            return Err(e);
        }

        // Take the concrete writer and finalize the Parquet file
        let mut concrete = state
            .writer
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| anyhow::anyhow!("Writer already closed"))?;

        let data_files = concrete
            .close()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to close writer: {}", e))?;

        Ok::<IcebergDataFiles, anyhow::Error>(IcebergDataFiles { data_files })
    },
    writer: *mut IcebergDataFileWriter
);
