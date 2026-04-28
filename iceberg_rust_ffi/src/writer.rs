/// Writer support for iceberg_rust_ffi
///
/// Encoding is handled by a global pool of N=available_parallelism OS threads shared
/// across all writers. Per-writer ordering is guaranteed by the per-writer
/// `Arc<Mutex<ConcreteDataFileWriter>>` inside WriterState: only one pool thread encodes
/// a given writer at a time, and the FIFO global queue ensures batches are submitted
/// in order. Drain tasks are lightweight async forwarders (no CPU work).
use std::ffi::{c_char, c_void};
use std::io::Cursor;
use std::panic::{catch_unwind, AssertUnwindSafe};
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
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

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

use crate::response::IcebergBoxedResponse;
use crate::table::IcebergTable;
use crate::transaction::IcebergDataFiles;
use crate::util::parse_c_string;
use crate::writer_columns::{
    build_arrow_array_scattered, ColumnDescriptor, GatheredColumnDescriptor, SliceRef,
};
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};

/// Type alias for the concrete DataFileWriter we use
type ConcreteDataFileWriter =
    DataFileWriter<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

/// Batch item sent through the per-writer drain channel.
pub(crate) type BatchItem = RecordBatch;

/// Encode task submitted to the global worker pool.
struct EncodeTask {
    batch: RecordBatch,
    state: Arc<WriterState>,
}

// Safety: RecordBatch is Send; WriterState fields are Send.
unsafe impl Send for EncodeTask {}

/// Shared mutable state for one IcebergDataFileWriter.
/// Owned by the IcebergDataFileWriter and shared with pool workers via Arc.
pub(crate) struct WriterState {
    /// The underlying Parquet writer. Protected by a Mutex so pool workers can access it
    /// concurrently (though at most one worker encodes a given writer at a time due to the
    /// bounded per-writer channel). Set to None when the writer is closed or freed.
    writer: Mutex<Option<ConcreteDataFileWriter>>,
    /// Number of encode tasks submitted to the pool but not yet completed.
    pending: AtomicUsize,
    /// Notified when `pending` drops to zero, so iceberg_writer_close can wait efficiently.
    done_notify: tokio::sync::Notify,
    /// First encode error encountered by a pool worker, if any.
    error: Mutex<Option<anyhow::Error>>,
}

// Safety: ConcreteDataFileWriter is Send (verified by its use in spawn_blocking previously).
unsafe impl Send for WriterState {}
unsafe impl Sync for WriterState {}

/// Global pool of N=available_parallelism encode worker threads shared across all writers.
struct GlobalWorkerPool {
    task_tx: tokio::sync::mpsc::Sender<EncodeTask>,
}

static GLOBAL_ENCODE_POOL: OnceLock<GlobalWorkerPool> = OnceLock::new();

/// Desired encode worker count. 0 means "use available_parallelism".
/// Must be set before the first iceberg_writer_new call.
static ENCODE_WORKERS: AtomicUsize = AtomicUsize::new(0);

/// Set the number of encode worker threads in the global pool.
/// Must be called before creating any writers; has no effect afterwards.
#[no_mangle]
pub extern "C" fn iceberg_set_encode_workers(n: i32) {
    if n > 0 {
        ENCODE_WORKERS.store(n as usize, Ordering::Relaxed);
    }
}

/// Initialize the global encode pool on first call.
/// Must be called from within a Tokio runtime (iceberg_writer_new satisfies this).
fn get_or_init_encode_pool() -> &'static GlobalWorkerPool {
    GLOBAL_ENCODE_POOL.get_or_init(|| {
        let configured = ENCODE_WORKERS.load(Ordering::Relaxed);
        let n = if configured > 0 {
            configured
        } else {
            // available_parallelism() only fails on unusual platforms (embedded, some sandboxes).
            // On Linux/macOS/Windows it always succeeds, so the unwrap never fires in practice.
            thread::available_parallelism().unwrap().get()
        };
        let handle = tokio::runtime::Handle::current();
        // Buffer 2× workers — drain tasks are rarely blocked on submit.
        let (task_tx, task_rx) = tokio::sync::mpsc::channel::<EncodeTask>(n * 2);
        let task_rx = Arc::new(tokio::sync::Mutex::new(task_rx));

        for i in 0..n {
            let task_rx = task_rx.clone();
            let handle = handle.clone();
            thread::Builder::new()
                .name(format!("iceberg-encode-{}", i))
                .spawn(move || {
                    loop {
                        // Acquire the shared receiver lock, then wait for a task.
                        // The lock is released as soon as recv() returns, so workers
                        // are not serialized during encoding — only during task pickup.
                        let task = {
                            let mut rx = handle.block_on(task_rx.lock());
                            match handle.block_on(rx.recv()) {
                                Some(t) => t,
                                None => break, // sender dropped → pool shutting down
                            }
                        };

                        // Clone state before moving task into the closure so we can
                        // always decrement pending even if the closure panics.
                        let state = task.state.clone();

                        let handle_enc = handle.clone();
                        let encode_result = catch_unwind(AssertUnwindSafe(move || {
                            let result: anyhow::Result<()> = {
                                let mut guard =
                                    task.state.writer.lock().unwrap_or_else(|e| e.into_inner());
                                match guard.as_mut() {
                                    Some(w) => handle_enc
                                        .block_on(w.write(task.batch))
                                        .map_err(|e| anyhow::anyhow!("write batch: {}", e)),
                                    None => Err(anyhow::anyhow!("writer already closed")),
                                }
                            };
                            result
                        }));

                        let err = match encode_result {
                            Ok(Ok(())) => None,
                            Ok(Err(e)) => Some(e),
                            Err(_panic) => Some(anyhow::anyhow!("encode worker panicked")),
                        };
                        if let Some(e) = err {
                            let mut slot = state.error.lock().unwrap_or_else(|e| e.into_inner());
                            if slot.is_none() {
                                *slot = Some(e);
                            }
                        }

                        // Always decrement pending; notify close() if this was the last task.
                        let prev = state.pending.fetch_sub(1, Ordering::AcqRel);
                        if prev == 1 {
                            state.done_notify.notify_one();
                        }
                    }
                })
                .expect("failed to spawn iceberg encode worker");
        }

        GlobalWorkerPool { task_tx }
    })
}

/// Opaque writer handle for FFI.
///
/// Writing is pipelined: Julia gathers a RecordBatch, sends it to the bounded per-writer
/// channel (capacity 1), and returns immediately. A lightweight async drain task forwards
/// batches to the global encode pool. Pool workers (N = available_parallelism) encode
/// Parquet concurrently across all active writers.
pub struct IcebergDataFileWriter {
    /// Arrow schema for this table, used by write_columns to create RecordBatches.
    pub(crate) arrow_schema: ArrowSchemaRef,
    /// Sender side of the per-writer batch queue (bounded, capacity 1).
    /// Dropped in iceberg_writer_close to signal EOF to the drain task.
    pub(crate) batch_tx: Option<mpsc::Sender<BatchItem>>,
    /// Lightweight async drain task: forwards batches from batch_tx to the global pool.
    pub(crate) drain_handle: Option<JoinHandle<()>>,
    /// Shared state: owns the ConcreteDataFileWriter, tracks pending count and errors.
    pub(crate) writer_state: Arc<WriterState>,
}

unsafe impl Send for IcebergDataFileWriter {}
unsafe impl Sync for IcebergDataFileWriter {}

/// Type alias for writer response
pub type IcebergDataFileWriterResponse = IcebergBoxedResponse<IcebergDataFileWriter>;

/// Type alias for data files response (returns IcebergDataFiles handle)
pub type IcebergWriterCloseResponse = IcebergBoxedResponse<IcebergDataFiles>;

/// Synchronous scatter-gather write: gathers all column data from Julia memory into
/// Arrow arrays in the calling thread, submits the RecordBatch to the global encode
/// pool, then returns immediately (encode is still async).
///
/// Unlike `iceberg_writer_write_scattered_columns` (which requires a Tokio async context
/// and uses a callback), this function is designed for plain C `ccall` from Julia:
/// - Julia keeps source arrays alive via `GC.@preserve` for the duration of this call.
/// - After this function returns, all Julia pointers have been consumed; Julia may safely
///   release the source data.
/// - Encode is still asynchronous in the global pool; call `iceberg_writer_close` to
///   wait for all pending encodes.
///
/// Returns 0 on success, -1 on error (error stored in writer state, propagated on close).
#[no_mangle]
pub extern "C" fn iceberg_writer_write_scattered_columns_sync(
    writer: *mut IcebergDataFileWriter,
    columns: *const GatheredColumnDescriptor,
    num_columns: usize,
) -> i32 {
    if writer.is_null() || columns.is_null() || num_columns == 0 {
        return -1;
    }

    let writer_ref = unsafe { &*writer };

    let pool = match GLOBAL_ENCODE_POOL.get() {
        Some(p) => p,
        None => {
            eprintln!("[iceberg:sync] encode pool not initialized; call iceberg_writer_new first");
            return -1;
        }
    };

    let arrow_schema = writer_ref.arrow_schema.clone();
    let col_descs = unsafe { std::slice::from_raw_parts(columns, num_columns) };

    if col_descs.len() != arrow_schema.fields().len() {
        let mut slot = writer_ref
            .writer_state
            .error
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if slot.is_none() {
            *slot = Some(anyhow::anyhow!(
                "Column count mismatch: got {} but schema has {}",
                col_descs.len(),
                arrow_schema.fields().len()
            ));
        }
        return -1;
    }

    // Gather: read from Julia memory into Rust-owned Arrow arrays (blocking, in calling thread).
    // After this block, all Julia pointers have been consumed — safe to release.
    let batch = match (|| -> Result<RecordBatch, anyhow::Error> {
        let mut arrays = Vec::with_capacity(num_columns);
        for (i, desc) in col_descs.iter().enumerate() {
            arrays.push(unsafe { build_arrow_array_scattered(desc, arrow_schema.field(i))? });
        }
        RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| anyhow::anyhow!("RecordBatch: {}", e))
    })() {
        Ok(b) => b,
        Err(e) => {
            let mut slot = writer_ref
                .writer_state
                .error
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if slot.is_none() {
                *slot = Some(e);
            }
            return -1;
        }
    };

    // Increment pending before submit so iceberg_writer_close always accounts for this batch.
    writer_ref
        .writer_state
        .pending
        .fetch_add(1, Ordering::AcqRel);
    let task = EncodeTask {
        batch,
        state: writer_ref.writer_state.clone(),
    };

    match pool.task_tx.blocking_send(task) {
        Ok(()) => 0,
        Err(_) => {
            // Pool channel closed — undo the pending increment.
            let prev = writer_ref
                .writer_state
                .pending
                .fetch_sub(1, Ordering::AcqRel);
            if prev == 1 {
                writer_ref.writer_state.done_notify.notify_one();
            }
            eprintln!("[iceberg:sync] pool channel closed");
            -1
        }
    }
}

/// Synchronous write of flat column data: copies each column from Julia memory into
/// Rust-owned Arrow arrays in the calling thread, then submits to the global encode
/// pool asynchronously.
///
/// Each `ColumnDescriptor` is treated as a single sequential slice (no scatter/gather).
/// Returns 0 on success, -1 on error (error stored in writer state, propagated on close).
#[no_mangle]
pub extern "C" fn iceberg_writer_write_columns_sync(
    writer: *mut IcebergDataFileWriter,
    columns: *const ColumnDescriptor,
    num_columns: usize,
) -> i32 {
    if writer.is_null() || columns.is_null() || num_columns == 0 {
        return -1;
    }

    let writer_ref = unsafe { &*writer };

    let pool = match GLOBAL_ENCODE_POOL.get() {
        Some(p) => p,
        None => {
            eprintln!("[iceberg:sync] encode pool not initialized; call iceberg_writer_new first");
            return -1;
        }
    };

    let arrow_schema = writer_ref.arrow_schema.clone();
    let col_descs = unsafe { std::slice::from_raw_parts(columns, num_columns) };

    if col_descs.len() != arrow_schema.fields().len() {
        let mut slot = writer_ref
            .writer_state
            .error
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if slot.is_none() {
            *slot = Some(anyhow::anyhow!(
                "Column count mismatch: got {} but schema has {}",
                col_descs.len(),
                arrow_schema.fields().len()
            ));
        }
        return -1;
    }

    // Wrap each ColumnDescriptor as a single-slice GatheredColumnDescriptor.
    // Sequential access (sel_ptr = null) so data[0..num_rows] is read in order.
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

    let gathered: Vec<GatheredColumnDescriptor> = col_descs
        .iter()
        .zip(slices.iter())
        .map(|(d, s)| GatheredColumnDescriptor {
            slices: s as *const SliceRef,
            num_slices: 1,
            total_rows: d.num_rows,
            column_type: d.column_type,
            is_nullable: d.is_nullable,
        })
        .collect();

    let batch = match (|| -> Result<RecordBatch, anyhow::Error> {
        let mut arrays = Vec::with_capacity(num_columns);
        for (i, desc) in gathered.iter().enumerate() {
            arrays.push(unsafe { build_arrow_array_scattered(desc, arrow_schema.field(i))? });
        }
        RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| anyhow::anyhow!("RecordBatch: {}", e))
    })() {
        Ok(b) => b,
        Err(e) => {
            let mut slot = writer_ref
                .writer_state
                .error
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if slot.is_none() {
                *slot = Some(e);
            }
            return -1;
        }
    };

    writer_ref
        .writer_state
        .pending
        .fetch_add(1, Ordering::AcqRel);
    let task = EncodeTask {
        batch,
        state: writer_ref.writer_state.clone(),
    };

    match pool.task_tx.blocking_send(task) {
        Ok(()) => 0,
        Err(_) => {
            let prev = writer_ref
                .writer_state
                .pending
                .fetch_sub(1, Ordering::AcqRel);
            if prev == 1 {
                writer_ref.writer_state.done_notify.notify_one();
            }
            eprintln!("[iceberg:sync] pool channel closed");
            -1
        }
    }
}

/// Free a writer. Aborts the drain task and poisons the writer state so any in-flight
/// pool tasks fail gracefully (error/cancel path).
#[no_mangle]
pub extern "C" fn iceberg_writer_free(writer: *mut IcebergDataFileWriter) {
    if !writer.is_null() {
        unsafe {
            let boxed = Box::from_raw(writer);
            if let Some(handle) = boxed.drain_handle {
                handle.abort();
            }
            // Poison the ConcreteDataFileWriter so any in-flight pool tasks return an error
            // rather than writing to a partially-freed writer.
            let _ = boxed.writer_state.writer.lock().unwrap().take();
        }
    }
}

// Create a new DataFileWriter from a table with configuration options.
//
// Spawns a lightweight async drain task per writer. The global encode pool
// (N = available_parallelism threads) is initialized on the first call.
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

        // Initialize global pool (no-op if already running).
        let pool = get_or_init_encode_pool();

        let writer_state = Arc::new(WriterState {
            writer: Mutex::new(Some(concrete_writer)),
            pending: AtomicUsize::new(0),
            done_notify: tokio::sync::Notify::new(),
            error: Mutex::new(None),
        });

        // Bounded per-writer channel (capacity 1): Julia can be at most one batch ahead
        // of the pool. This provides backpressure: send blocks if the drain task hasn't
        // forwarded the previous batch yet.
        let (batch_tx, mut batch_rx) = mpsc::channel::<BatchItem>(1);

        // Lightweight drain task: forwards batches to the global pool asynchronously.
        let drain_handle: JoinHandle<()> = {
            let state = writer_state.clone();
            let pool_tx = pool.task_tx.clone();
            tokio::task::spawn(async move {
                while let Some(batch) = batch_rx.recv().await {
                    state.pending.fetch_add(1, Ordering::AcqRel);
                    let task = EncodeTask { batch, state: state.clone() };
                    if pool_tx.send(task).await.is_err() {
                        eprintln!("[iceberg:drain] pool channel closed");
                        break;
                    }
                }
            })
        };

        Ok::<IcebergDataFileWriter, anyhow::Error>(IcebergDataFileWriter {
            arrow_schema,
            batch_tx: Some(batch_tx),
            drain_handle: Some(drain_handle),
            writer_state,
        })
    },
    table: *mut IcebergTable,
    prefix: *const c_char,
    target_file_size_bytes: i64,
    parquet_props: *const ParquetWriterPropertiesFFI
);

// Write Arrow IPC data to the writer.
//
// Deserializes the IPC stream, then sends each RecordBatch to the drain task's channel.
// Returns immediately after queuing — encoding happens asynchronously in the global pool.
export_runtime_op!(
    iceberg_writer_write,
    crate::IcebergResponse,
    || {
        if writer.is_null() {
            return Err(anyhow::anyhow!("Null writer pointer provided"));
        }
        if arrow_ipc_data.is_null() {
            return Err(anyhow::anyhow!("Null arrow_ipc_data pointer provided"));
        }
        if arrow_ipc_len == 0 {
            return Err(anyhow::anyhow!("Arrow IPC data length is zero"));
        }

        // Copy the IPC data into a Vec for safe use across await points
        let ipc_bytes = unsafe {
            std::slice::from_raw_parts(arrow_ipc_data, arrow_ipc_len).to_vec()
        };

        let writer_ref = unsafe { &mut *writer };
        Ok((writer_ref, ipc_bytes))
    },
    result_tuple,
    async {
        let (writer_ref, ipc_bytes) = result_tuple;

        let tx = writer_ref
            .batch_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Writer has been closed"))?;

        // Deserialize Arrow IPC to RecordBatches and enqueue each one
        let cursor = Cursor::new(ipc_bytes);
        let mut reader = StreamReader::try_new(cursor, None)
            .map_err(|e| anyhow::anyhow!("Failed to create Arrow IPC reader: {}", e))?;

        while let Some(batch_result) = reader.next() {
            let batch = batch_result
                .map_err(|e| anyhow::anyhow!("Failed to read Arrow IPC batch: {}", e))?;
            tx.send(batch).await
                .map_err(|_| anyhow::anyhow!("Writer channel closed (drain task may have failed)"))?;
        }

        Ok::<(), anyhow::Error>(())
    },
    writer: *mut IcebergDataFileWriter,
    arrow_ipc_data: *const u8,
    arrow_ipc_len: usize
);

// Close the writer and return the produced DataFiles.
//
// Drops the batch sender (signals EOF to the drain task), waits for the drain task to
// finish forwarding all batches, waits for all pool encodes to complete, then finalizes
// the Parquet file and returns the DataFiles metadata.
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
        // Drop the sender — signals the drain task that no more batches are coming
        drop(writer_ref.batch_tx.take());

        // Wait for the drain task to finish forwarding all batches to the pool
        if let Some(handle) = writer_ref.drain_handle.take() {
            handle.await
                .map_err(|e| anyhow::anyhow!("Drain task panicked: {}", e))?;
        }

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
