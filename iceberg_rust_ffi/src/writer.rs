/// Writer support for iceberg_rust_ffi
///
/// Encoding is handled by a global pool of N async worker tasks (default
/// 2 * available_parallelism, configurable via `iceberg_set_encode_workers`) running
/// on the tokio runtime, shared across all writers. Each writer owns its own FIFO queue
/// of pending batches; workers scan the set of active writers and claim one (via the
/// per-writer `busy` flag) before draining its queue. This avoids the head-of-line
/// blocking that the old single-MPMC design suffered when many workers happened to pull
/// tasks for the same writer. Workers `.await` the I/O inside `w.write()`, so a runtime
/// thread parked on an S3 PUT is free to drive another writer's encode in the meantime.
use std::collections::VecDeque;
use std::ffi::{c_char, c_void};
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
const COMPRESSION_LZ4_RAW: i32 = 5;

fn compression_from_code(code: i32) -> Compression {
    match code {
        COMPRESSION_UNCOMPRESSED => Compression::UNCOMPRESSED,
        COMPRESSION_SNAPPY => Compression::SNAPPY,
        COMPRESSION_GZIP => Compression::GZIP(Default::default()),
        COMPRESSION_LZ4 => Compression::LZ4,
        COMPRESSION_ZSTD => Compression::ZSTD(Default::default()),
        COMPRESSION_LZ4_RAW => Compression::LZ4_RAW,
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
/// Owned by the IcebergDataFileWriter and shared with pool workers via Arc.
///
/// # Invariants
///
/// 1. **Per-writer FIFO ordering.** Batches for a given writer are encoded in submission
///    order. Pushes go to the back of `pending_queue`; pops come from the front; and
///    `busy` ensures at most one worker drains the queue at a time — so the encode order
///    is identical to the push order.
/// 2. **Single-claim.** A worker may only encode for this writer while it has won the
///    `busy.compare_exchange(false → true)`. The claimed worker drains the queue to
///    empty (or until the writer reports an error) before releasing `busy`.
/// 3. **Stranded-task mitigation.** After releasing `busy`, the worker re-checks
///    `queue_len`; if non-zero (a producer pushed a batch between the worker's last
///    pop and the release), it notifies the global pool again. This prevents the
///    classic missed-notification race where a notification is consumed by a worker
///    that arrived between the producer's push and the queue's becoming non-empty.
pub(crate) struct WriterState {
    /// The underlying Parquet writer. Protected by a Mutex so pool workers can access it.
    /// Only the worker that holds the `busy` claim ever locks this — so there's no real
    /// contention here; the Mutex is preserved purely to coordinate with
    /// `iceberg_writer_free`, which may take the writer out from under in-flight work.
    /// Set to None when the writer is closed or freed.
    writer: Mutex<Option<ConcreteDataFileWriter>>,
    /// FIFO queue of batches awaiting encode for this writer.
    pending_queue: Mutex<VecDeque<RecordBatch>>,
    /// Snapshot of `pending_queue.len()` exposed as an atomic so workers can skip writers
    /// with no work without taking the queue lock. Kept in sync with the queue under the
    /// queue lock by `submit_batch` (increments before notifying) and by workers (decrement
    /// after popping).
    queue_len: AtomicUsize,
    /// Set to true by the worker currently encoding for this writer. Other workers skip
    /// this writer while `busy` is true, even if `queue_len > 0`.
    busy: AtomicBool,
    /// True once this writer has been registered in `GlobalWorkerPool::active_writers`.
    /// First submitter wins the CAS and performs the registration.
    registered: AtomicBool,
    /// Number of encode tasks submitted but not yet completed. Includes queued + in-flight.
    pending: AtomicUsize,
    /// Notified when `pending` drops to zero, so iceberg_writer_close can wait efficiently.
    done_notify: tokio::sync::Notify,
    /// First encode error encountered by a pool worker, if any.
    error: Mutex<Option<anyhow::Error>>,
    /// Set by iceberg_writer_free to tell in-flight async encodes to drop the writer
    /// instead of putting it back. Needed because async encode takes the writer out of
    /// the Option for the duration of `w.write(batch).await`, releasing the Mutex so the
    /// runtime thread can drive other tasks while parked on I/O.
    poisoned: AtomicBool,
}

// Safety: ConcreteDataFileWriter is Send (verified by its use in spawn_blocking previously).
unsafe impl Send for WriterState {}
unsafe impl Sync for WriterState {}

impl WriterState {
    fn new(writer: ConcreteDataFileWriter) -> Self {
        WriterState {
            writer: Mutex::new(Some(writer)),
            pending_queue: Mutex::new(VecDeque::new()),
            queue_len: AtomicUsize::new(0),
            busy: AtomicBool::new(false),
            registered: AtomicBool::new(false),
            pending: AtomicUsize::new(0),
            done_notify: tokio::sync::Notify::new(),
            error: Mutex::new(None),
            poisoned: AtomicBool::new(false),
        }
    }
}

/// Global pool of N encode worker tasks shared across all writers (N defaults to
/// 2 * available_parallelism so async workers parked on I/O don't starve cores of
/// encode CPU work; tune via `iceberg_set_encode_workers`).
///
/// Replaces the previous single-MPMC channel design. Each writer owns its own queue;
/// workers scan the active-writer list looking for a writer that (a) has queued work and
/// (b) is not currently claimed by another worker. The first such writer is claimed
/// (`busy = true`), drained, then released.
///
/// # Wakeup discipline
///
/// `wake` is a single shared `Notify` for the whole pool. Both producers (`submit_batch`)
/// and workers (after releasing a writer that still has work) call `wake.notify_one()`.
/// To avoid stranded tasks when multiple producers fire concurrently and only one permit
/// can be stored, a worker that successfully claims a writer cascades the wakeup by
/// calling `wake.notify_one()` before draining — so if more writers have work, another
/// worker is roused to look.
pub(crate) struct GlobalWorkerPool {
    /// Currently-registered writers. Workers iterate this on each pass looking for work.
    /// Locked only briefly to snapshot the list (Arc clones); never held during encode.
    active_writers: Mutex<Vec<Arc<WriterState>>>,
    /// Wakeup channel for idle workers. Producers and finishing workers notify; idle
    /// workers wait. See struct doc for the cascade discipline that prevents lost wakeups.
    wake: tokio::sync::Notify,
    /// Rotating start offset for the per-pass scan, so workers don't all collide on
    /// writer 0 when several writers have work.
    scan_offset: AtomicUsize,
}

pub(crate) static GLOBAL_ENCODE_POOL: OnceLock<GlobalWorkerPool> = OnceLock::new();

impl GlobalWorkerPool {
    /// Add a writer to the active set. Idempotent via the `registered` CAS on WriterState.
    fn register(&self, state: &Arc<WriterState>) {
        if state
            .registered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let mut guard = self.active_writers.lock().unwrap_or_else(|e| e.into_inner());
            guard.push(state.clone());
        }
    }

    /// Remove a writer from the active set. Called on free; idempotent.
    fn unregister(&self, state: &WriterState) {
        if !state.registered.swap(false, Ordering::AcqRel) {
            return;
        }
        let mut guard = self.active_writers.lock().unwrap_or_else(|e| e.into_inner());
        let target = state as *const WriterState;
        guard.retain(|s| Arc::as_ptr(s) != target);
    }

    /// Snapshot the active-writers list. Returns Arc clones so subsequent encoding does
    /// not hold the list lock.
    fn snapshot(&self) -> Vec<Arc<WriterState>> {
        let guard = self.active_writers.lock().unwrap_or_else(|e| e.into_inner());
        guard.clone()
    }
}

/// Try to claim a writer with pending work. Returns the claimed writer (busy=true) or
/// None if no writer has work available right now.
///
/// Scans the active-writers snapshot starting at a rotating offset so workers don't all
/// race for writer 0.
fn try_claim_writer(pool: &GlobalWorkerPool) -> Option<Arc<WriterState>> {
    let writers = pool.snapshot();
    if writers.is_empty() {
        return None;
    }
    let n = writers.len();
    let start = pool.scan_offset.fetch_add(1, Ordering::Relaxed) % n;
    for i in 0..n {
        let w = &writers[(start + i) % n];
        if w.queue_len.load(Ordering::Acquire) == 0 {
            continue;
        }
        if w
            .busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // Re-check after winning the claim: another worker may have drained the
            // queue between our queue_len load and our CAS. If so, release and skip.
            if w.queue_len.load(Ordering::Acquire) == 0 {
                w.busy.store(false, Ordering::Release);
                continue;
            }
            return Some(w.clone());
        }
    }
    None
}

/// Drop guard that decrements `pending` exactly once. Used by the async encode path to
/// guarantee the counter still falls to zero (so `iceberg_writer_close` can return) even
/// if `w.write(...).await` panics and unwinds the task.
struct PendingGuard(Arc<WriterState>);

impl Drop for PendingGuard {
    fn drop(&mut self) {
        let prev = self.0.pending.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            self.0.done_notify.notify_one();
        }
    }
}

/// Encode a single batch for the given (already-claimed) writer.
///
/// **Async**: takes the underlying writer out of `state.writer` under the std Mutex
/// (briefly), awaits `w.write(batch)` without holding any sync lock, then puts the writer
/// back — unless `iceberg_writer_free` has set `poisoned`, in which case the writer is
/// dropped. The take-put pattern is what lets the runtime thread go drive other tasks
/// while this one is parked in an S3 PUT inside `w.write()`.
///
/// Stores any encode error in `state.error` (first-writer-wins). `pending` is decremented
/// exactly once via the `PendingGuard` Drop impl, so close() never hangs even on panic.
async fn encode_one_batch(state: Arc<WriterState>, batch: RecordBatch) {
    let _pending = PendingGuard(state.clone());

    // Test hook: bypass the real Parquet write so we can exercise the dispatch logic in
    // isolation. Enabled only when a test installs a positive delay via `test_hooks`.
    #[cfg(test)]
    {
        let delay_ms = test_hooks::DELAY_MS.load(Ordering::Relaxed);
        if delay_ms > 0 {
            test_hooks::run_hook(&state, &batch);
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            return;
        }
    }

    // Take the writer out under the std Mutex. If the writer was already poisoned by
    // a prior free(), we want to drop any writer that's still in the slot — but the
    // Some/None of the slot itself tells us that.
    let writer_opt = {
        let mut guard = state.writer.lock().unwrap_or_else(|e| e.into_inner());
        guard.take()
    };

    let (mut writer_opt, result) = match writer_opt {
        Some(mut w) => {
            let r = w
                .write(batch)
                .await
                .map_err(|e| anyhow::anyhow!("write batch: {}", e));
            (Some(w), r)
        }
        None => (None, Err(anyhow::anyhow!("writer already closed"))),
    };

    // Put the writer back unless free() ran during our .await — in which case
    // `poisoned` is set and we drop the writer to honor the poison semantic.
    if let Some(w) = writer_opt.take() {
        if state.poisoned.load(Ordering::Acquire) {
            drop(w);
        } else {
            *state.writer.lock().unwrap_or_else(|e| e.into_inner()) = Some(w);
        }
    }

    if let Err(e) = result {
        let mut slot = state.error.lock().unwrap_or_else(|e| e.into_inner());
        if slot.is_none() {
            *slot = Some(e);
        }
    }
}

/// Drain the claimed writer's queue while we hold `busy`. Pops one batch at a time and
/// encodes it. The `busy` flag ensures FIFO per-writer ordering: while we hold it, no
/// other worker can interleave a pop on this writer's queue.
async fn drain_claimed_writer(state: Arc<WriterState>) {
    loop {
        let batch = {
            let mut q = state
                .pending_queue
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match q.pop_front() {
                Some(b) => {
                    state.queue_len.fetch_sub(1, Ordering::AcqRel);
                    b
                }
                None => break,
            }
        };
        encode_one_batch(state.clone(), batch).await;
    }
}

/// Worker thread body: scan for a writer with work, claim it, drain its queue, release,
/// re-check (stranded-task mitigation), and either continue or wait for a wake-up.
///
/// The wake-up protocol uses a single shared `Notify` with a cascade discipline. See the
/// docs on `GlobalWorkerPool` for the full picture; the key races are:
///
/// - **Producer notification lost.** Tokio's `Notify` stores at most one permit, so if
///   two producers fire `notify_one()` while all workers are sleeping, only one worker
///   wakes. To prevent the second producer's work from being stranded, the woken worker
///   calls `wake.notify_one()` *before* it starts draining — cascading the wakeup so
///   another worker checks the remaining writers.
/// - **Push between last-pop and busy-release.** A producer pushes a batch after the
///   drain loop sees an empty queue but before this worker clears `busy`. The producer's
///   `notify_one()` may have been consumed by some other worker that ran an empty scan
///   and went back to sleep. Mitigation: after clearing `busy`, this worker re-reads
///   `queue_len`; if non-zero, it notifies again so someone re-claims the writer.
async fn encode_worker_loop(pool: &'static GlobalWorkerPool) {
    loop {
        // Pre-register interest in the next wake-up. `enable()` guarantees that any
        // `notify_one()` issued from this point on will wake the future even if it
        // hasn't been polled yet — so the check-then-wait below is race-free.
        let notified = pool.wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if let Some(state) = try_claim_writer(pool) {
            // Cascade: another writer may also have work. Wake a peer to look in
            // parallel before we commit to draining this one.
            pool.wake.notify_one();

            drain_claimed_writer(state.clone()).await;

            // Release the claim. After this point another worker is free to claim
            // the writer.
            state.busy.store(false, Ordering::Release);

            // Stranded-task mitigation: a producer may have pushed between our last
            // pop and our release. If so, ensure a worker is woken to handle it.
            if state.queue_len.load(Ordering::Acquire) > 0 {
                pool.wake.notify_one();
            }
            continue;
        }

        // Nothing to claim — go to sleep until notified.
        notified.await;
    }
}

/// Desired encode worker count. 0 means "use 2 * available_parallelism", which
/// oversubscribes the core count on purpose: encode worker tasks are async, so workers
/// parked on S3 PUTs don't cost CPU and we want enough total tasks for the runtime to
/// keep cores busy with CPU encode while others wait on I/O.
/// Must be set before the first iceberg_writer_new call.
static ENCODE_WORKERS: AtomicUsize = AtomicUsize::new(0);

/// Set the number of encode worker tasks in the global pool.
/// Must be called before any writer is created. Returns 0 on success, 1 if the pool is
/// already initialized (call ignored).
#[no_mangle]
pub extern "C" fn iceberg_set_encode_workers(n: i32) -> i32 {
    if GLOBAL_ENCODE_POOL.get().is_some() {
        return 1;
    }
    if n > 0 {
        ENCODE_WORKERS.store(n as usize, Ordering::Relaxed);
    }
    0
}

/// Initialize the global encode pool on first call.
/// Must be called from within a Tokio runtime (iceberg_writer_new satisfies this).
fn get_or_init_encode_pool() -> &'static GlobalWorkerPool {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let configured = ENCODE_WORKERS.load(Ordering::Relaxed);
        let n = if configured > 0 {
            configured
        } else {
            // available_parallelism() only fails on unusual platforms (embedded, some sandboxes).
            // On Linux/macOS/Windows it always succeeds, so the unwrap never fires in practice.
            // 2x: oversubscribe so async workers parked on I/O leave room for other workers
            // to do encode CPU on the freed runtime threads.
            thread::available_parallelism().unwrap().get() * 2
        };
        let handle = tokio::runtime::Handle::current();

        // Install the pool first so workers can reference it as `&'static`.
        GLOBAL_ENCODE_POOL
            .set(GlobalWorkerPool {
                active_writers: Mutex::new(Vec::new()),
                wake: tokio::sync::Notify::new(),
                scan_offset: AtomicUsize::new(0),
            })
            .ok()
            .expect("encode pool initialized twice");
        let pool_ref: &'static GlobalWorkerPool = GLOBAL_ENCODE_POOL
            .get()
            .expect("pool was just installed");

        // Spawn N async worker tasks on the tokio runtime. Each task runs
        // `encode_worker_loop`, which awaits at I/O boundaries inside `w.write()` —
        // freeing the runtime thread to drive other tasks (other writers' encodes)
        // during S3 PUTs. Number of in-flight encodes is no longer bounded by OS
        // thread count, only by core count for actual CPU work.
        for _ in 0..n {
            handle.spawn(encode_worker_loop(pool_ref));
        }
    });
    GLOBAL_ENCODE_POOL
        .get()
        .expect("encode pool not installed by INIT")
}

/// Opaque writer handle for FFI.
///
/// Writing is pipelined: Julia gathers a RecordBatch and submits it directly to the
/// global encode pool, then returns immediately. Pool workers (async tasks; default
/// N = 2 * available_parallelism) encode Parquet concurrently across all active writers.
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
fn store_writer_error(writer_ref: &IcebergDataFileWriter, e: anyhow::Error) {
    let mut slot = writer_ref
        .writer_state
        .error
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if slot.is_none() {
        *slot = Some(e);
    }
}

/// Store an error in the writer state (public for batch_builder module).
pub(crate) fn store_writer_error_pub(writer_ref: &IcebergDataFileWriter, e: anyhow::Error) {
    store_writer_error(writer_ref, e);
}

/// Submit a `RecordBatch` to the writer's queue. Lazily registers the writer with the
/// global pool on first submit, then pushes onto the per-writer FIFO queue and notifies
/// the pool that there is work available somewhere.
///
/// `pending` (queued + in-flight) is incremented under the queue lock so that
/// `iceberg_writer_close` sees a consistent count.
pub(crate) fn submit_batch(
    writer_ref: &IcebergDataFileWriter,
    pool: &GlobalWorkerPool,
    batch: RecordBatch,
) -> Result<(), anyhow::Error> {
    // Idempotent — only the first submit pays the lock to push into active_writers.
    pool.register(&writer_ref.writer_state);

    {
        let mut q = writer_ref
            .writer_state
            .pending_queue
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        q.push_back(batch);
        // Increment counters under the lock so queue and counters stay consistent.
        writer_ref
            .writer_state
            .queue_len
            .fetch_add(1, Ordering::AcqRel);
        writer_ref
            .writer_state
            .pending
            .fetch_add(1, Ordering::AcqRel);
    }

    pool.wake.notify_one();
    Ok(())
}

/// Validates column count, converts each `ColumnDescriptor` into a single-slice `SliceRef`,
/// routes through `ColumnBatchBuilder`, and submits the resulting `RecordBatch` to the
/// encode pool.  Using the builder here keeps all type-conversion and null-bit logic in one
/// place (`batch_builder.rs`) instead of duplicating it.
unsafe fn write_columns_inner(
    writer_ref: &IcebergDataFileWriter,
    pool: &GlobalWorkerPool,
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
    builder.write_and_reset(writer_ref, pool)
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
    let pool = match GLOBAL_ENCODE_POOL.get() {
        Some(p) => p,
        None => {
            eprintln!("[iceberg] encode pool not initialized; call iceberg_writer_new first");
            return -1;
        }
    };
    let arrow_schema = writer_ref.arrow_schema.clone();
    let col_descs = unsafe { std::slice::from_raw_parts(columns, num_columns) };
    if let Err(e) = unsafe { write_columns_inner(writer_ref, pool, arrow_schema, col_descs) } {
        store_writer_error(writer_ref, e);
        return -1;
    }
    0
}

/// Free a writer. Poisons the writer state so any in-flight pool tasks fail gracefully,
/// and unregisters the writer from the global pool's active-writers list so workers stop
/// scanning it.
#[no_mangle]
pub extern "C" fn iceberg_writer_free(writer: *mut IcebergDataFileWriter) {
    if !writer.is_null() {
        unsafe {
            let boxed = Box::from_raw(writer);
            if let Some(pool) = GLOBAL_ENCODE_POOL.get() {
                pool.unregister(&boxed.writer_state);
            }
            // Set the poison flag BEFORE taking the writer out, so any encode task that
            // currently holds the writer outside the Mutex (across its `.await`) will see
            // `poisoned == true` when it goes to put the writer back, and drop it instead.
            boxed
                .writer_state
                .poisoned
                .store(true, Ordering::Release);
            // Poison the ConcreteDataFileWriter so any in-flight pool tasks return an error
            // rather than writing to a partially-freed writer.
            let _ = boxed.writer_state.writer.lock().unwrap().take();
        }
    }
}

// Create a new DataFileWriter from a table with configuration options.
//
// The global encode pool (N async worker tasks, default 2 * available_parallelism) is
// initialized on the first call.
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
        get_or_init_encode_pool();

        let writer_state = Arc::new(WriterState::new(concrete_writer));

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

    let pool = match GLOBAL_ENCODE_POOL.get() {
        Some(p) => p,
        None => {
            eprintln!("[iceberg:sync] encode pool not initialized; call iceberg_writer_new first");
            return -1;
        }
    };

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
        if let Err(e) = submit_batch(writer_ref, pool, batch) {
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

// ─────────────────────────────────────────────────────────────────────────────
// Test hooks + dispatch tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::atomic::AtomicU64;
    use std::sync::Mutex;

    use arrow_array::{Array, Int64Array, RecordBatch};

    use super::WriterState;
    use std::sync::Arc;

    /// When non-zero, `encode_one_batch` skips the real Parquet write, sleeps this many
    /// milliseconds, and records the completion. Used by dispatch-logic tests.
    pub(crate) static DELAY_MS: AtomicU64 = AtomicU64::new(0);

    /// Recorded `(writer_id, batch_id)` for each completed encode while `DELAY_MS > 0`.
    /// `writer_id` is the `Arc<WriterState>` pointer cast to usize. `batch_id` is read
    /// from the batch's first column (assumed to be an Int64Array of length 1).
    pub(crate) static COMPLETIONS: Mutex<Vec<(usize, i64)>> = Mutex::new(Vec::new());

    pub(crate) fn run_hook(state: &Arc<WriterState>, batch: &RecordBatch) {
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(0))
            .unwrap_or(-1);
        let writer_id = Arc::as_ptr(state) as usize;
        COMPLETIONS.lock().unwrap().push((writer_id, id));
    }

    pub(crate) fn reset() {
        DELAY_MS.store(0, std::sync::atomic::Ordering::Relaxed);
        COMPLETIONS.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};

    use super::*;

    /// Serializes all dispatch tests so they don't trample the shared global pool
    /// state (`DELAY_MS`, `COMPLETIONS`, `active_writers`).
    static TEST_SERIAL: Mutex<()> = Mutex::new(());

    /// A long-lived multi-threaded runtime that the global encode pool can pin its
    /// `Handle` to across tests. `#[tokio::test]` builds a fresh runtime per test and
    /// drops it at end, which would invalidate the workers' handles.
    fn pinned_runtime() -> &'static tokio::runtime::Runtime {
        static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        RT.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
        })
    }

    fn batch_with_id(id: i64) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).unwrap()
    }

    /// Constructs a WriterState with no underlying Parquet writer. Safe because the
    /// test hook bypasses the real `w.write(batch)` path.
    fn mock_writer_state() -> Arc<WriterState> {
        Arc::new(WriterState {
            writer: Mutex::new(None),
            pending_queue: Mutex::new(std::collections::VecDeque::new()),
            queue_len: std::sync::atomic::AtomicUsize::new(0),
            busy: std::sync::atomic::AtomicBool::new(false),
            registered: std::sync::atomic::AtomicBool::new(false),
            pending: std::sync::atomic::AtomicUsize::new(0),
            done_notify: tokio::sync::Notify::new(),
            error: Mutex::new(None),
            poisoned: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Push a batch onto a WriterState's queue, registering with the pool and waking it.
    /// Mirrors `submit_batch` but takes a bare WriterState (so tests can use mock states
    /// without going through IcebergDataFileWriter).
    fn push(pool: &GlobalWorkerPool, state: &Arc<WriterState>, batch: RecordBatch) {
        pool.register(state);
        {
            let mut q = state.pending_queue.lock().unwrap();
            q.push_back(batch);
            state.queue_len.fetch_add(1, Ordering::AcqRel);
            state.pending.fetch_add(1, Ordering::AcqRel);
        }
        pool.wake.notify_one();
    }

    fn wait_for_pending_zero(state: &WriterState, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while state.pending.load(Ordering::Acquire) > 0 {
            if start.elapsed() > timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        true
    }

    /// Initializes the global encode pool from inside the pinned runtime. Safe to call
    /// many times — only the first call does any work.
    fn ensure_pool() -> &'static GlobalWorkerPool {
        let _g = pinned_runtime().enter();
        get_or_init_encode_pool()
    }

    /// Detach any mock writers we registered with the pool so a subsequent test starts
    /// from a clean active-writer list. Doesn't shut down the workers (they're shared).
    fn cleanup_writers(pool: &GlobalWorkerPool, states: &[Arc<WriterState>]) {
        for s in states {
            pool.unregister(s);
        }
    }

    /// Fairness: with 4 writers each holding 8 queued batches and N>=4 workers, the new
    /// dispatch should drain all 4 writers in parallel rather than serializing one
    /// writer at a time.
    ///
    /// We assert two properties:
    ///   1. Per-writer FIFO: each writer's batches complete in submission order.
    ///   2. Parallelism: within any group of 4 consecutive completions, all 4 writers
    ///      appear — i.e., a round-robin pattern emerges naturally because each writer
    ///      is being drained by its own worker, all sleeping for the same delay.
    #[test]
    fn fairness_drains_writers_in_parallel() {
        let _serial = TEST_SERIAL.lock().unwrap();
        let pool = ensure_pool();

        test_hooks::reset();
        test_hooks::DELAY_MS.store(20, Ordering::Relaxed);

        let writers: Vec<Arc<WriterState>> = (0..4).map(|_| mock_writer_state()).collect();
        let writer_ids: HashMap<usize, usize> = writers
            .iter()
            .enumerate()
            .map(|(i, s)| (Arc::as_ptr(s) as usize, i))
            .collect();

        // Submit interleaved: round 0 of every writer, then round 1, etc.
        for round in 0..8i64 {
            for (i, w) in writers.iter().enumerate() {
                let batch_id = (i as i64) * 100 + round;
                push(pool, w, batch_with_id(batch_id));
            }
        }

        for w in &writers {
            assert!(
                wait_for_pending_zero(w, Duration::from_secs(10)),
                "writer did not drain in time"
            );
        }

        let completions = test_hooks::COMPLETIONS.lock().unwrap().clone();
        // 4 writers × 8 batches = 32 completions.
        assert_eq!(completions.len(), 32);

        // (1) FIFO per writer: filter completions by writer and check batch IDs ascend.
        for (i, w) in writers.iter().enumerate() {
            let id = Arc::as_ptr(w) as usize;
            let ids: Vec<i64> = completions
                .iter()
                .filter(|(wid, _)| *wid == id)
                .map(|(_, bid)| *bid)
                .collect();
            assert_eq!(ids.len(), 8, "writer {} missing batches", i);
            for j in 0..8 {
                assert_eq!(
                    ids[j],
                    (i as i64) * 100 + j as i64,
                    "writer {} batch {} out of order: {:?}",
                    i,
                    j,
                    ids
                );
            }
        }

        // (2) Parallelism: each group of 4 consecutive completions should contain 4
        // distinct writers. With <4 workers in the pool this would fail; on any modern
        // dev machine `available_parallelism() >= 4`.
        for chunk in completions.chunks(4) {
            let distinct: std::collections::HashSet<usize> = chunk
                .iter()
                .map(|(wid, _)| writer_ids[wid])
                .collect();
            assert_eq!(
                distinct.len(),
                4,
                "expected 4 distinct writers per round, got {:?}",
                chunk
            );
        }

        cleanup_writers(pool, &writers);
        test_hooks::reset();
    }

    /// Stranded-task race: hammer the pool with many submits across many writers and
    /// verify that every submitted batch is eventually drained — i.e., `pending` always
    /// converges to zero, no batch sits forever in a per-writer queue because of a
    /// missed wake-up.
    #[test]
    fn no_stranded_tasks_under_load() {
        let _serial = TEST_SERIAL.lock().unwrap();
        let pool = ensure_pool();

        test_hooks::reset();
        // Tiny delay (1ms) so a) the test runs fast, b) producers and drains
        // genuinely race rather than one always preceding the other.
        test_hooks::DELAY_MS.store(1, Ordering::Relaxed);

        const WRITERS: usize = 8;
        const BATCHES_PER_WRITER: usize = 200;
        let writers: Vec<Arc<WriterState>> = (0..WRITERS).map(|_| mock_writer_state()).collect();

        // Drive submissions from several threads to maximize interleaving.
        let mut handles = Vec::new();
        for tid in 0..4 {
            let writers = writers.clone();
            let pool: &'static GlobalWorkerPool = pool;
            handles.push(std::thread::spawn(move || {
                for batch_idx in 0..(BATCHES_PER_WRITER / 4) {
                    for (wi, w) in writers.iter().enumerate() {
                        let id = (tid as i64) * 1_000_000
                            + (wi as i64) * 10_000
                            + batch_idx as i64;
                        push(pool, w, batch_with_id(id));
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Wait for every writer's pending to drop to zero. If any single writer's queue
        // is stranded, this would time out.
        for (i, w) in writers.iter().enumerate() {
            assert!(
                wait_for_pending_zero(w, Duration::from_secs(30)),
                "writer {} did not drain; pending={} queue_len={}",
                i,
                w.pending.load(Ordering::Acquire),
                w.queue_len.load(Ordering::Acquire),
            );
            assert_eq!(w.queue_len.load(Ordering::Acquire), 0);
        }

        let total = test_hooks::COMPLETIONS.lock().unwrap().len();
        assert_eq!(total, WRITERS * BATCHES_PER_WRITER);

        cleanup_writers(pool, &writers);
        test_hooks::reset();
    }
}
