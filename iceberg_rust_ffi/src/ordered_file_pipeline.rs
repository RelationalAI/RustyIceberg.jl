//! File-parallel pipeline for reading Iceberg tables with strict ordering.
//!
//! # Problem
//! iceberg-rs's `to_arrow()` uses `try_for_each_concurrent` which interleaves
//! batches from different files in arbitrary order. We need strict
//! file-then-row ordering.
//!
//! # Approach
//! We call `plan_files()` to get an ordered list of FileScanTasks, then
//! process N files concurrently while yielding batches in strict file order.
//!
//! Each file task is a background tokio task that:
//!   1. Builds a per-file ArrowReader (same iceberg-rs code path as to_arrow)
//!   2. Reads row groups sequentially → RecordBatch stream
//!   3. Serializes each batch to Arrow IPC (via spawn_blocking)
//!   4. Sends serialized batches into a per-file mpsc channel
//!
//! A consumer (`run_nested`) uses FuturesUnordered to maintain N tasks in
//! flight and yield per-file FileScan values as they become ready. Each
//! FileScan wraps the file's batch channel as an IcebergArrowStream with
//! integrated semaphore-permit release.
//!
//! # Memory bounding
//! Each file task has its own Semaphore(MAX_BUFFERED_BYTES_PER_TASK). After
//! serializing a batch, the task acquires byte_len permits. If the budget is
//! exhausted, the task yields (async, not blocking) until the consumer drains
//! batches and releases permits. This caps each file's buffered output to
//! ~100MB independently.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use tokio::sync::{mpsc, Mutex as AsyncMutex, Semaphore};

use crate::table::{ArrowBatch, IcebergArrowStream, IcebergFileScanStream};
use crate::unexpected;

/// Per-file cap on serialized bytes buffered ahead of the consumer.
const MAX_BUFFERED_BYTES_PER_TASK: usize = 100 * 1024 * 1024;

/// Hard cap on file-level concurrency to keep total memory bounded.
const MAX_FILE_CONCURRENCY: usize = 16;

// ===========================================================================
// Temporary profiling — will be removed before merging to production.
//
// Global atomic counters accumulate timing/size data across all file tasks.
// Called from Julia via `@ccall iceberg_print_pipeline_stats()`.
// ===========================================================================

/// Accumulates profiling data across all file tasks in a pipeline run.
/// All fields are atomics so concurrent file tasks can update without locks.
struct PipelineStats {
    // ── Overall ──
    pipeline_wall_ns: AtomicU64,
    files_completed: AtomicUsize,
    batches_produced: AtomicUsize,
    bytes_produced: AtomicU64,

    // ── Concurrency ──
    peak_concurrency: AtomicUsize,
    active_tasks: AtomicUsize,

    // ── Per-phase cumulative time (ns), summed across all file tasks ──
    /// Time to build ArrowReader and call reader.read() (opens parquet,
    /// loads metadata, resolves schema, loads delete files, builds filters).
    reader_setup_ns: AtomicU64,
    /// Time inside batch_stream.next() — fetches compressed pages from
    /// storage, decompresses (ZSTD), decodes pages, assembles columns.
    fetch_decode_ns: AtomicU64,
    /// Time in spawn_blocking(serialize_record_batch) — writes RecordBatch
    /// to Arrow IPC wire format for transfer to Julia.
    serialize_ns: AtomicU64,
    /// Time blocked on per-file semaphore (backpressure from consumer).
    semaphore_wait_ns: AtomicU64,

    // ── Consumer ──
    /// Time the flat consumer (`run_flat`) spends waiting for the next file.
    consumer_wait_ns: AtomicU64,

    // ── Memory ──
    /// Live counter of serialized bytes buffered across all file tasks.
    buffered_bytes: AtomicU64,
    /// High-water mark of buffered_bytes.
    peak_buffered_bytes: AtomicU64,
}

impl PipelineStats {
    const fn new() -> Self {
        Self {
            pipeline_wall_ns: AtomicU64::new(0),
            files_completed: AtomicUsize::new(0),
            batches_produced: AtomicUsize::new(0),
            bytes_produced: AtomicU64::new(0),
            peak_concurrency: AtomicUsize::new(0),
            active_tasks: AtomicUsize::new(0),
            reader_setup_ns: AtomicU64::new(0),
            fetch_decode_ns: AtomicU64::new(0),
            serialize_ns: AtomicU64::new(0),
            semaphore_wait_ns: AtomicU64::new(0),
            consumer_wait_ns: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            peak_buffered_bytes: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        self.pipeline_wall_ns.store(0, Ordering::Relaxed);
        self.files_completed.store(0, Ordering::Relaxed);
        self.batches_produced.store(0, Ordering::Relaxed);
        self.bytes_produced.store(0, Ordering::Relaxed);
        self.peak_concurrency.store(0, Ordering::Relaxed);
        self.active_tasks.store(0, Ordering::Relaxed);
        self.reader_setup_ns.store(0, Ordering::Relaxed);
        self.fetch_decode_ns.store(0, Ordering::Relaxed);
        self.serialize_ns.store(0, Ordering::Relaxed);
        self.semaphore_wait_ns.store(0, Ordering::Relaxed);
        self.consumer_wait_ns.store(0, Ordering::Relaxed);
        self.buffered_bytes.store(0, Ordering::Relaxed);
        self.peak_buffered_bytes.store(0, Ordering::Relaxed);
    }

    fn track_task_start(&self) {
        let prev = self.active_tasks.fetch_add(1, Ordering::Relaxed);
        self.peak_concurrency.fetch_max(prev + 1, Ordering::Relaxed);
    }

    fn track_task_end(&self) {
        self.active_tasks.fetch_sub(1, Ordering::Relaxed);
    }

    fn track_buffer_add(&self, bytes: u64) {
        let prev = self.buffered_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.peak_buffered_bytes
            .fetch_max(prev + bytes, Ordering::Relaxed);
    }

    fn track_buffer_release(&self, bytes: u64) {
        self.buffered_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    fn print_summary(&self) {
        let wall_ms = self.pipeline_wall_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let files = self.files_completed.load(Ordering::Relaxed);
        let batches = self.batches_produced.load(Ordering::Relaxed);
        let bytes = self.bytes_produced.load(Ordering::Relaxed);
        let peak = self.peak_concurrency.load(Ordering::Relaxed);
        let setup_ms = self.reader_setup_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let fd_ms = self.fetch_decode_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let ser_ms = self.serialize_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let sem_ms = self.semaphore_wait_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let con_ms = self.consumer_wait_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let peak_buf = self.peak_buffered_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);

        let bytes_mb = bytes as f64 / (1024.0 * 1024.0);
        let throughput = if wall_ms > 0.0 {
            bytes_mb / (wall_ms / 1000.0)
        } else {
            0.0
        };
        let file_wall_ms = setup_ms + fd_ms + ser_ms + sem_ms;
        let parallelism = if wall_ms > 0.0 {
            file_wall_ms / wall_ms
        } else {
            0.0
        };
        let limit_mb = MAX_BUFFERED_BYTES_PER_TASK / (1024 * 1024);

        // Box layout: "│  " + content + padding + "  │", total = BOX chars.
        // All content uses ASCII only so byte len = display width.
        const BOX: usize = 68; // total width including borders

        let row = |content: &str| {
            // "│  " = 3, "  │" = 3 => content area = BOX - 6
            let pad = (BOX - 6).saturating_sub(content.len());
            println!("│  {}{:pad$}  │", content, "", pad = pad);
        };
        let dashes = |n: usize| -> String { "─".repeat(n) };
        let sep = |label: &str| {
            // "│  ── label ────  │" — label is ASCII, dashes are multi-byte.
            // Display width: 3 + 3 + label.len() + 1 + fill + 1 = BOX
            // => fill = BOX - 3 - 3 - label.len() - 3 - 1 = BOX - 10 - label.len()
            let fill = (BOX - 10).saturating_sub(label.len());
            println!("│  {} {} {}  │", dashes(2), label, dashes(fill));
        };
        let border = |left: char, right: char| {
            println!("{}{}{}", left, dashes(BOX - 2), right);
        };

        border('┌', '┐');
        row(&format!("Pipeline Stats"));
        row("");
        row(&format!("wall time:       {:>9.1} ms", wall_ms));
        row(&format!("files:           {:>9}       peak concurrency: {}", files, peak));
        row(&format!("batches:         {:>9}       serialized: {:.1} MB", batches, bytes_mb));
        row(&format!("throughput:      {:>9.1} MB/s  parallelism: {:.1}x", throughput, parallelism));
        sep("time across all file tasks (sum)");
        row(&format!("reader setup:    {:>9.1} ms    (open, metadata, deletes)", setup_ms));
        row(&format!("fetch+decode:    {:>9.1} ms    (I/O + ZSTD + decode)", fd_ms));
        row(&format!("serialize IPC:   {:>9.1} ms    (RecordBatch -> Arrow IPC)", ser_ms));
        row(&format!("semaphore wait:  {:>9.1} ms    (backpressure)", sem_ms));
        sep("consumer");
        row(&format!("ordering stall:  {:>9.1} ms    (waiting for head file)", con_ms));
        sep("memory");
        row(&format!("peak buffered:   {:>9.1} MB    (limit: {} MB/task)", peak_buf, limit_mb));
        border('└', '┘');
    }
}

static STATS: PipelineStats = PipelineStats::new();

// ── FFI exports for profiling (called from Julia benchmark teardown) ─────

#[no_mangle]
pub extern "C" fn iceberg_print_pipeline_stats() {
    STATS.print_summary();
}

#[no_mangle]
pub extern "C" fn iceberg_reset_pipeline_stats() {
    STATS.reset();
}

// ===========================================================================
// Pipeline implementation
// ===========================================================================

/// A serialized Arrow IPC batch bundled with its byte size and a reference
/// to the originating file task's semaphore. The consumer releases permits
/// after forwarding the batch to Julia, unblocking the producer.
struct BufferedBatch {
    batch: ArrowBatch,
    byte_len: usize,
    semaphore: Arc<Semaphore>,
}

/// Internal per-file scan result: filename, record count, and a prefetched
/// inner batch stream. Used as the item type of the nested pipeline channel.
pub struct FileScan {
    pub filename: String,
    pub record_count: i64,
    pub stream: IcebergArrowStream,
}

/// Convert a per-file mpsc receiver into an IcebergArrowStream.
/// Semaphore permits are released and STATS updated as each batch is yielded.
fn make_file_stream(
    file_rx: mpsc::Receiver<Result<BufferedBatch, iceberg::Error>>,
) -> IcebergArrowStream {
    let stream = futures::stream::unfold(file_rx, |mut rx| async move {
        rx.recv().await.map(|item| {
            let result = item.map(|buf| {
                buf.semaphore.add_permits(buf.byte_len);
                STATS.track_buffer_release(buf.byte_len as u64);
                buf.batch
            });
            (result, rx)
        })
    })
    .boxed();
    IcebergArrowStream {
        stream: AsyncMutex::new(stream),
    }
}

/// Create the nested file-parallel pipeline and return it as an
/// IcebergFileScanStream. Each item in the outer stream is a FileScan
/// carrying the filename, record count, and a prefetched inner batch stream.
pub async fn create_nested_pipeline(
    tasks: Vec<FileScanTask>,
    file_io: FileIO,
    batch_size: Option<usize>,
    concurrency: usize,
) -> anyhow::Result<IcebergFileScanStream> {
    assert!(
        concurrency <= MAX_FILE_CONCURRENCY,
        "file concurrency {concurrency} exceeds hard cap {MAX_FILE_CONCURRENCY}"
    );

    STATS.reset();

    let (tx, rx) = mpsc::channel::<Result<FileScan, iceberg::Error>>(concurrency);

    tokio::spawn(run_nested(tasks, file_io, batch_size, concurrency, tx));

    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
    .boxed();

    Ok(IcebergFileScanStream {
        stream: AsyncMutex::new(stream),
    })
}

/// Create the flat file-parallel pipeline and return it as an IcebergArrowStream.
///
/// Implemented by creating the nested pipeline and flattening it, so all
/// ordering, backpressure, and stats logic lives in one place.
pub async fn create_pipeline(
    tasks: Vec<FileScanTask>,
    file_io: FileIO,
    batch_size: Option<usize>,
    concurrency: usize,
) -> anyhow::Result<IcebergArrowStream> {
    assert!(
        concurrency <= MAX_FILE_CONCURRENCY,
        "file concurrency {concurrency} exceeds hard cap {MAX_FILE_CONCURRENCY}"
    );

    // Outer channel: flatten task → Julia (via IcebergArrowStream).
    let (tx, rx) = mpsc::channel(concurrency * 2);

    let nested = create_nested_pipeline(tasks, file_io, batch_size, concurrency).await?;

    tokio::spawn(run_flat(nested, tx));

    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
    .boxed();

    Ok(IcebergArrowStream {
        stream: AsyncMutex::new(stream),
    })
}

/// Flatten the nested pipeline into a single batch channel (backwards-compat).
async fn run_flat(
    nested: IcebergFileScanStream,
    tx: mpsc::Sender<Result<ArrowBatch, iceberg::Error>>,
) {
    let pipeline_start = Instant::now();

    let mut outer = nested.stream.lock().await;
    while let Some(item) = outer.next().await {
        match item {
            Ok(file_scan) => {
                let mut inner = file_scan.stream.stream.lock().await;
                while let Some(batch_result) = inner.next().await {
                    if tx.send(batch_result).await.is_err() {
                        return;
                    }
                }
            }
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        }
    }

    STATS
        .pipeline_wall_ns
        .store(pipeline_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
}

/// Main nested consumer loop. Orchestrates file-level parallelism while
/// maintaining strict file ordering.
///
/// Uses FuturesOrdered to poll N file tasks concurrently but yield their
/// (filename, record_count, receiver) tuples in push order. Wraps each
/// receiver as an IcebergArrowStream and sends a FileScan to the outer channel.
async fn run_nested(
    tasks: Vec<FileScanTask>,
    file_io: FileIO,
    batch_size: Option<usize>,
    concurrency: usize,
    tx: mpsc::Sender<Result<FileScan, iceberg::Error>>,
) {
    use futures::stream::FuturesUnordered;

    let mut in_flight = FuturesUnordered::new();
    let mut task_iter = tasks.into_iter();

    // Seed the first N file tasks.
    for _ in 0..concurrency {
        if let Some(task) = task_iter.next() {
            in_flight.push(spawn_file_task_with_meta(task, file_io.clone(), batch_size));
        }
    }

    while let Some(file_result) = in_flight.next().await {
        // Eagerly start the next file to keep N tasks in flight.
        if let Some(task) = task_iter.next() {
            in_flight.push(spawn_file_task_with_meta(task, file_io.clone(), batch_size));
        }

        match file_result {
            Ok((filename, record_count, file_rx)) => {
                let stream = make_file_stream(file_rx);
                let file_scan = FileScan {
                    filename,
                    record_count,
                    stream,
                };
                if tx.send(Ok(file_scan)).await.is_err() {
                    return; // outer consumer dropped the stream
                }
            }
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        }
    }
}

/// Spawn a single file task. Returns a future that resolves immediately to
/// the file's metadata and the receiving end of its batch channel.
///
/// The actual work (parquet I/O, decode, serialize) happens in the background
/// tokio task. The future resolves to (filename, record_count, Receiver) so
/// that FuturesOrdered can yield them in file order.
fn spawn_file_task_with_meta(
    task: FileScanTask,
    file_io: FileIO,
    batch_size: Option<usize>,
) -> impl std::future::Future<
    Output = Result<
        (String, i64, mpsc::Receiver<Result<BufferedBatch, iceberg::Error>>),
        iceberg::Error,
    >,
> {
    let filename = task.data_file_path().to_string();
    let record_count = task.record_count.unwrap_or(0) as i64;

    let sem = Arc::new(Semaphore::new(MAX_BUFFERED_BYTES_PER_TASK));
    let (file_tx, file_rx) = mpsc::channel(8);

    tokio::spawn(process_file(task, file_io, batch_size, sem, file_tx));

    async move { Ok((filename, record_count, file_rx)) }
}

/// Wrapper around process_file_inner that ensures errors are sent to the
/// channel and stats are updated even on failure. When this function
/// returns, `tx` is dropped, causing the consumer's `file_rx.recv()` to
/// return None — signaling that this file is done.
async fn process_file(
    task: FileScanTask,
    file_io: FileIO,
    batch_size: Option<usize>,
    semaphore: Arc<Semaphore>,
    tx: mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) {
    STATS.track_task_start();
    let result = process_file_inner(task, file_io, batch_size, semaphore, &tx).await;
    if let Err(e) = result {
        let _ = tx.send(Err(e)).await;
    }
    STATS.track_task_end();
}

/// Process a single parquet file through four timed phases:
///   1. Reader setup — build ArrowReader, open file, resolve schema
///   2. Fetch + decode — I/O, ZSTD decompression, column assembly
///   3. Serialize — RecordBatch → Arrow IPC for transfer to Julia
///   4. Backpressure — wait on semaphore if buffered too far ahead
///
/// Each phase's cumulative time is recorded in STATS for the summary.
async fn process_file_inner(
    task: FileScanTask,
    file_io: FileIO,
    batch_size: Option<usize>,
    semaphore: Arc<Semaphore>,
    tx: &mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) -> Result<(), iceberg::Error> {

    // ── Phase 1: Reader setup ───────────────────────────────────────────
    // Builds a per-file ArrowReader using the same iceberg-rs code path as
    // to_arrow(): opens parquet metadata, resolves schema, loads delete
    // files, builds row filters. with_data_file_concurrency_limit(1) means
    // this reader processes one file (the one we give it).
    let setup_start = Instant::now();
    let mut builder = ArrowReaderBuilder::new(file_io).with_data_file_concurrency_limit(1);
    if let Some(bs) = batch_size {
        builder = builder.with_batch_size(bs);
    }
    let reader = builder.build();
    let task_stream = Box::pin(futures::stream::once(async { Ok(task) }));
    let batch_stream = reader
        .read(task_stream)
        .map_err(|e| unexpected(e))?;
    STATS
        .reader_setup_ns
        .fetch_add(setup_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

    tokio::pin!(batch_stream);

    loop {
        // ── Phase 2: Fetch + decode ─────────────────────────────────────
        // Each .next() call fetches compressed parquet pages from storage,
        // decompresses (ZSTD), decodes column encodings, and assembles a
        // RecordBatch. These are inseparable without forking parquet-rs.
        let fd_start = Instant::now();
        let batch_opt = batch_stream.next().await;
        STATS
            .fetch_decode_ns
            .fetch_add(fd_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let batch = match batch_opt {
            Some(Ok(b)) => b,
            Some(Err(e)) => return Err(e),
            None => break, // end of file
        };

        // ── Phase 3: Serialize to Arrow IPC ─────────────────────────────
        // CPU-bound work, offloaded to the blocking thread pool to avoid
        // starving the tokio executor.
        let ser_start = Instant::now();
        let serialized = tokio::task::spawn_blocking(move || crate::serialize_record_batch(batch))
            .await
            .map_err(|e| unexpected(format!("serialize panicked: {e}")))?
            .map_err(|e| unexpected(e))?;
        STATS
            .serialize_ns
            .fetch_add(ser_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let byte_len = serialized.length;
        STATS.batches_produced.fetch_add(1, Ordering::Relaxed);
        STATS
            .bytes_produced
            .fetch_add(byte_len as u64, Ordering::Relaxed);

        // ── Phase 4: Backpressure ───────────────────────────────────────
        // Acquire permits equal to the serialized size. If this file task
        // has produced > MAX_BUFFERED_BYTES_PER_TASK ahead of the consumer,
        // this yields (async, not thread-blocking) until permits are freed.
        let sem_start = Instant::now();
        let _permit = semaphore
            .acquire_many(byte_len as u32)
            .await
            .map_err(|e| unexpected(format!("semaphore: {e}")))?;
        STATS
            .semaphore_wait_ns
            .fetch_add(sem_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        // Detach the permit — the consumer releases it via add_permits().
        std::mem::forget(_permit);

        STATS.track_buffer_add(byte_len as u64);

        // Send the batch to this file's channel.
        if tx
            .send(Ok(BufferedBatch {
                batch: serialized,
                byte_len,
                semaphore: semaphore.clone(),
            }))
            .await
            .is_err()
        {
            return Ok(()); // consumer dropped the receiver
        }
    }

    STATS.files_completed.fetch_add(1, Ordering::Relaxed);
    Ok(())
}
