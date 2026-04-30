//! Warm file stream: plan N files concurrently, yield one per-file batch stream
//! at a time in manifest order.
//!
//! Modelled on `ordered_file_pipeline.rs` but instead of a single flat batch
//! stream, returns a stream of `IcebergWarmFileScan` items — each containing
//! file metadata and an already-running per-file batch channel.
//!
//! # Memory bounding
//! Each file task owns a `Semaphore(MAX_BUFFERED_BYTES_PER_FILE)`. The producer
//! acquires `byte_len` permits before sending each `BufferedBatch`; the consumer
//! releases them as Julia drains batches from the per-file `IcebergArrowStream`.

use std::ffi::{c_char, CString};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use iceberg::arrow::ArrowReader;
use iceberg::scan::incremental::AppendedFileScanTask;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use tokio::sync::{mpsc, Mutex as AsyncMutex, Semaphore};

use crate::response::IcebergBoxedResponse;
use crate::table::{ArrowBatch, IcebergArrowStream};
use crate::{CResult, Context, RawResponse};

/// Per-file cap on serialized bytes buffered ahead of the consumer.
pub(crate) const MAX_BUFFERED_BYTES_PER_FILE: usize = 100 * 1024 * 1024;

// ===========================================================================
// Stats
// ===========================================================================

pub(crate) struct WarmScanStats {
    /// Wall time from run_warm start to end (ns).
    pub(crate) pipeline_wall_ns: AtomicU64,
    pub(crate) files_completed: AtomicUsize,
    pub(crate) batches_produced: AtomicUsize,
    pub(crate) bytes_produced: AtomicU64,
    pub(crate) peak_concurrency: AtomicUsize,
    active_tasks: AtomicUsize,
    /// Phase 1: build reader, open parquet footer, resolve schema, load deletes.
    pub(crate) reader_setup_ns: AtomicU64,
    /// Phase 2: fetch + decompress + decode row group.
    pub(crate) fetch_decode_ns: AtomicU64,
    /// Phase 3: RecordBatch → Arrow IPC (spawn_blocking).
    pub(crate) serialize_ns: AtomicU64,
    /// Phase 4: semaphore acquire — producer blocked waiting for Julia to drain.
    pub(crate) semaphore_wait_ns: AtomicU64,
    /// Time blocked on per-file batch channel send (channel full = Julia draining slowly).
    pub(crate) batch_send_ns: AtomicU64,
    /// Time blocked on outer file channel send (task_prefetch_depth slots full).
    pub(crate) file_send_ns: AtomicU64,
    /// Time Julia waits in iceberg_next_warm_file (outer channel recv latency).
    pub(crate) consumer_wait_ns: AtomicU64,
    buffered_bytes: AtomicU64,
    pub(crate) peak_buffered_bytes: AtomicU64,
}

impl WarmScanStats {
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
            batch_send_ns: AtomicU64::new(0),
            file_send_ns: AtomicU64::new(0),
            consumer_wait_ns: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            peak_buffered_bytes: AtomicU64::new(0),
        }
    }

    pub(crate) fn reset(&self) {
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
        self.batch_send_ns.store(0, Ordering::Relaxed);
        self.file_send_ns.store(0, Ordering::Relaxed);
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

    pub(crate) fn track_buffer_release(&self, bytes: u64) {
        self.buffered_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub(crate) fn print_summary(&self) {
        let wall_ms = self.pipeline_wall_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let files = self.files_completed.load(Ordering::Relaxed);
        let batches = self.batches_produced.load(Ordering::Relaxed);
        let bytes = self.bytes_produced.load(Ordering::Relaxed);
        let peak = self.peak_concurrency.load(Ordering::Relaxed);
        let setup_ms = self.reader_setup_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let fd_ms = self.fetch_decode_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let ser_ms = self.serialize_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let sem_ms = self.semaphore_wait_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let send_ms = self.batch_send_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let fsend_ms = self.file_send_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let con_ms = self.consumer_wait_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let peak_buf = self.peak_buffered_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);
        let bytes_mb = bytes as f64 / (1024.0 * 1024.0);
        let limit_mb = MAX_BUFFERED_BYTES_PER_FILE / (1024 * 1024);
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

        const BOX: usize = 68;
        let row = |content: &str| {
            let pad = (BOX - 6).saturating_sub(content.len());
            println!("│  {}{:pad$}  │", content, "", pad = pad);
        };
        let dashes = |n: usize| -> String { "─".repeat(n) };
        let sep = |label: &str| {
            let fill = (BOX - 10).saturating_sub(label.len());
            println!("│  {} {} {}  │", dashes(2), label, dashes(fill));
        };
        let border = |left: char, right: char| {
            println!("{}{}{}", left, dashes(BOX - 2), right);
        };

        border('┌', '┐');
        row("Warm-Scan Stats");
        row("");
        row(&format!("wall time:       {:>9.1} ms", wall_ms));
        row(&format!(
            "files:           {:>9}       peak concurrency: {}",
            files, peak
        ));
        row(&format!(
            "batches:         {:>9}       serialized: {:.1} MB",
            batches, bytes_mb
        ));
        row(&format!(
            "throughput:      {:>9.1} MB/s  parallelism: {:.1}x",
            throughput, parallelism
        ));
        sep("time across all file tasks (sum)");
        row(&format!(
            "reader setup:    {:>9.1} ms    (open, metadata, deletes)",
            setup_ms
        ));
        row(&format!(
            "fetch+decode:    {:>9.1} ms    (I/O + ZSTD + decode)",
            fd_ms
        ));
        row(&format!(
            "serialize IPC:   {:>9.1} ms    (RecordBatch -> Arrow IPC)",
            ser_ms
        ));
        row(&format!(
            "semaphore wait:  {:>9.1} ms    (backpressure)",
            sem_ms
        ));
        row(&format!(
            "batch send:      {:>9.1} ms    (channel send to Julia)",
            send_ms
        ));
        row(&format!(
            "file send:       {:>9.1} ms    (outer channel send, depth full)",
            fsend_ms
        ));
        sep("consumer");
        row(&format!(
            "next_file wait:  {:>9.1} ms    (Julia waiting for next file)",
            con_ms
        ));
        sep("memory");
        row(&format!(
            "peak buffered:   {:>9.1} MB    (limit: {} MB/file)",
            peak_buf, limit_mb
        ));
        border('└', '┘');
    }
}

pub(crate) static WARM_SCAN_STATS: WarmScanStats = WarmScanStats::new();

#[no_mangle]
pub extern "C" fn iceberg_print_warm_scan_stats() {
    WARM_SCAN_STATS.print_summary();
}

#[no_mangle]
pub extern "C" fn iceberg_reset_warm_scan_stats() {
    WARM_SCAN_STATS.reset();
}

// ===========================================================================
// Core types
// ===========================================================================

/// A serialized batch bundled with its size and the file's semaphore so the
/// consumer can release permits when the batch is drained.
pub(crate) struct BufferedBatch {
    pub(crate) batch: ArrowBatch,
    pub(crate) byte_len: usize,
    pub(crate) semaphore: Arc<Semaphore>,
}

/// One file's worth of metadata + an already-running batch stream.
/// The stream's unfold releases semaphore permits as Julia drains batches.
pub struct IcebergWarmFileScan {
    pub file_path: CString,
    pub record_count: Option<u64>,
    pub stream: IcebergArrowStream,
}

unsafe impl Send for IcebergWarmFileScan {}

/// Stream of warm files. A background producer runs `run_warm`, which uses
/// `FuturesOrdered` to keep `task_prefetch_depth` file tasks in flight and
/// sends ready `IcebergWarmFileScan` items to the channel in manifest order.
pub struct IcebergWarmFileScanStream {
    pub(crate) receiver: AsyncMutex<mpsc::Receiver<Result<IcebergWarmFileScan, anyhow::Error>>>,
    pub(crate) producer_handle: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
}

unsafe impl Send for IcebergWarmFileScanStream {}

impl IcebergWarmFileScanStream {
    pub async fn next(&self) -> Result<Option<IcebergWarmFileScan>, anyhow::Error> {
        let mut rx = self.receiver.lock().await;
        let t = Instant::now();
        let result = rx.recv().await;
        WARM_SCAN_STATS
            .consumer_wait_ns
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        match result {
            Some(Ok(item)) => Ok(Some(item)),
            Some(Err(e)) => Err(e),
            None => {
                let mut handle_guard = self.producer_handle.lock().await;
                if let Some(handle) = handle_guard.take() {
                    match handle.await {
                        Ok(()) => Ok(None),
                        Err(e) => Err(anyhow::anyhow!("Warm scan producer panicked: {}", e)),
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }
}

// ===========================================================================
// Response types
// ===========================================================================

pub type IcebergWarmFileScanStreamResponse = IcebergBoxedResponse<IcebergWarmFileScanStream>;

/// Response for iceberg_plan_incremental_warm — returns warm append stream +
/// delete stream in one FFI call so plan_files() is only called once.
#[repr(C)]
pub struct IcebergWarmIncrementalStreamsResponse {
    result: CResult,
    pub append_warm_stream: *mut IcebergWarmFileScanStream,
    pub delete_stream: *mut crate::table::IcebergIncrementalPosDeleteFileStream,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for IcebergWarmIncrementalStreamsResponse {}

impl RawResponse for IcebergWarmIncrementalStreamsResponse {
    type Payload = (
        IcebergWarmFileScanStream,
        crate::table::IcebergIncrementalPosDeleteFileStream,
    );
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
            Some((append, delete)) => {
                self.append_warm_stream = Box::into_raw(Box::new(append));
                self.delete_stream = Box::into_raw(Box::new(delete));
            }
            None => {
                self.append_warm_stream = std::ptr::null_mut();
                self.delete_stream = std::ptr::null_mut();
            }
        }
    }
}

#[repr(transparent)]
pub struct IcebergNextWarmFileScanResponse(pub IcebergBoxedResponse<IcebergWarmFileScan>);

unsafe impl Send for IcebergNextWarmFileScanResponse {}

impl RawResponse for IcebergNextWarmFileScanResponse {
    type Payload = Option<IcebergWarmFileScan>;
    fn result_mut(&mut self) -> &mut CResult {
        &mut self.0.result
    }
    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.0.context
    }
    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.0.error_message
    }
    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        use std::ptr;
        match payload.flatten() {
            Some(item) => self.0.value = Box::into_raw(Box::new(item)),
            None => self.0.value = ptr::null_mut(),
        }
    }
}

// ===========================================================================
// Pipeline implementation
// ===========================================================================

type SpawnFileTaskResult = Result<
    (
        CString,
        Option<u64>,
        mpsc::Receiver<Result<BufferedBatch, iceberg::Error>>,
    ),
    anyhow::Error,
>;

/// Spawn a single file task. Captures metadata, creates per-file channel and
/// semaphore, spawns `process_file` in background, and returns immediately.
/// The returned future resolves instantly to the receiver so FuturesOrdered
/// yields them in push order.
pub(crate) fn spawn_file_task(
    task: FileScanTask,
    reader: ArrowReader,
    batch_prefetch_depth: usize,
) -> BoxFuture<'static, SpawnFileTaskResult> {
    let file_path =
        CString::new(task.data_file_path.as_str()).unwrap_or_else(|_| CString::new("").unwrap());
    let record_count = task.record_count;
    let sem = Arc::new(Semaphore::new(MAX_BUFFERED_BYTES_PER_FILE));
    let (file_tx, file_rx) = mpsc::channel(batch_prefetch_depth);

    tokio::spawn(process_file(task, reader, sem, file_tx));

    Box::pin(async move { Ok((file_path, record_count, file_rx)) })
}

/// Wrapper: sends error to channel and updates stats on failure.
/// Dropping `tx` signals the consumer that this file is done.
/// Generic wrapper: runs an async producer, forwards any error, updates stats.
async fn run_file_task<Fut>(
    producer: impl FnOnce() -> Fut,
    tx: mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) where
    Fut: std::future::Future<Output = Result<(), iceberg::Error>>,
{
    let result = producer().await;
    if let Err(e) = result {
        let _ = tx.send(Err(e)).await;
    }
    WARM_SCAN_STATS.track_task_end();
}

async fn process_file(
    task: FileScanTask,
    reader: ArrowReader,
    semaphore: Arc<Semaphore>,
    tx: mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) {
    run_file_task(
        || process_file_inner(task, reader, semaphore, &tx),
        tx.clone(),
    )
    .await;
}

/// Shared phases 2-4: drain a RecordBatch stream into the file channel.
/// Called by both process_file_inner (full scan) and
/// process_incremental_append_file_inner (incremental).
async fn drain_batch_stream(
    mut batch_stream: impl futures::Stream<Item = Result<RecordBatch, iceberg::Error>> + Unpin,
    semaphore: Arc<Semaphore>,
    tx: &mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) -> Result<(), iceberg::Error> {
    loop {
        // ── Phase 2: Fetch + decode ─────────────────────────────────────
        let fd_start = Instant::now();
        let batch_opt = batch_stream.next().await;
        WARM_SCAN_STATS
            .fetch_decode_ns
            .fetch_add(fd_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let batch = match batch_opt {
            Some(Ok(b)) => b,
            Some(Err(e)) => return Err(e),
            None => break,
        };

        // ── Phase 3: Serialize to Arrow IPC ─────────────────────────────
        let ser_start = Instant::now();
        let serialized = tokio::task::spawn_blocking(move || crate::serialize_record_batch(batch))
            .await
            .map_err(|e| {
                iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("serialize panicked: {e}"),
                )
            })?
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;
        WARM_SCAN_STATS
            .serialize_ns
            .fetch_add(ser_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let byte_len = serialized.length;
        WARM_SCAN_STATS
            .batches_produced
            .fetch_add(1, Ordering::Relaxed);
        WARM_SCAN_STATS
            .bytes_produced
            .fetch_add(byte_len as u64, Ordering::Relaxed);

        // ── Phase 4: Backpressure ───────────────────────────────────────
        let sem_start = Instant::now();
        let _permit = semaphore.acquire_many(byte_len as u32).await.map_err(|e| {
            iceberg::Error::new(iceberg::ErrorKind::Unexpected, format!("semaphore: {e}"))
        })?;
        WARM_SCAN_STATS
            .semaphore_wait_ns
            .fetch_add(sem_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        std::mem::forget(_permit);

        WARM_SCAN_STATS.track_buffer_add(byte_len as u64);

        let send_start = Instant::now();
        let send_err = tx
            .send(Ok(BufferedBatch {
                batch: serialized,
                byte_len,
                semaphore: semaphore.clone(),
            }))
            .await
            .is_err();
        WARM_SCAN_STATS
            .batch_send_ns
            .fetch_add(send_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if send_err {
            return Ok(());
        }
    }
    WARM_SCAN_STATS
        .files_completed
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}

async fn process_file_inner(
    task: FileScanTask,
    reader: ArrowReader,
    semaphore: Arc<Semaphore>,
    tx: &mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) -> Result<(), iceberg::Error> {
    WARM_SCAN_STATS.track_task_start();
    let setup_start = Instant::now();
    let task_stream = Box::pin(futures::stream::once(async { Ok(task) }));
    let batch_stream = reader
        .read(task_stream)
        .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;
    WARM_SCAN_STATS
        .reader_setup_ns
        .fetch_add(setup_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    tokio::pin!(batch_stream);
    drain_batch_stream(batch_stream, semaphore, tx).await
}

// ===========================================================================
// Incremental append file support
// ===========================================================================

pub(crate) fn spawn_incremental_append_task(
    task: AppendedFileScanTask,
    reader: ArrowReader,
    batch_prefetch_depth: usize,
) -> BoxFuture<'static, SpawnFileTaskResult> {
    let file_path = CString::new(task.base.data_file_path.as_str())
        .unwrap_or_else(|_| CString::new("").unwrap());
    let record_count = task.base.record_count;
    let sem = Arc::new(Semaphore::new(MAX_BUFFERED_BYTES_PER_FILE));
    let (file_tx, file_rx) = mpsc::channel(batch_prefetch_depth);
    tokio::spawn(process_incremental_append_file(task, reader, sem, file_tx));
    Box::pin(async move { Ok((file_path, record_count, file_rx)) })
}

async fn process_incremental_append_file(
    task: AppendedFileScanTask,
    reader: ArrowReader,
    semaphore: Arc<Semaphore>,
    tx: mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) {
    run_file_task(
        || process_incremental_append_file_inner(task, reader, semaphore, &tx),
        tx.clone(),
    )
    .await;
}

async fn process_incremental_append_file_inner(
    task: AppendedFileScanTask,
    reader: ArrowReader,
    semaphore: Arc<Semaphore>,
    tx: &mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) -> Result<(), iceberg::Error> {
    WARM_SCAN_STATS.track_task_start();
    let setup_start = Instant::now();
    let batch_stream = crate::table::read_incremental_append_file(reader, task)
        .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;
    WARM_SCAN_STATS
        .reader_setup_ns
        .fetch_add(setup_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    tokio::pin!(batch_stream);
    drain_batch_stream(batch_stream, semaphore, tx).await
}

/// Shared FuturesOrdered orchestrator. Drives any planning stream, keeps
/// `task_prefetch_depth` file tasks in flight, and forwards
/// `IcebergWarmFileScan` items to `outer_tx` in order.
///
/// `spawn_fn` maps a task + reader + batch_prefetch_depth to a boxed future
/// that resolves immediately to the file's batch-channel receiver.
async fn run_warm_orchestrator<T, S, F>(
    task_stream: S,
    reader: ArrowReader,
    task_prefetch_depth: usize,
    batch_prefetch_depth: usize,
    outer_tx: mpsc::Sender<Result<IcebergWarmFileScan, anyhow::Error>>,
    spawn_fn: F,
) where
    T: Send + 'static,
    S: futures::Stream<Item = Result<T, iceberg::Error>> + Unpin + Send,
    F: Fn(T, ArrowReader, usize) -> BoxFuture<'static, SpawnFileTaskResult>,
{
    let pipeline_start = Instant::now();
    let mut in_flight: FuturesOrdered<BoxFuture<'static, SpawnFileTaskResult>> =
        FuturesOrdered::new();

    futures::pin_mut!(task_stream);
    let mut stream_done = false;

    for _ in 0..task_prefetch_depth {
        match task_stream.next().await {
            Some(Ok(task)) => {
                in_flight.push_back(spawn_fn(task, reader.clone(), batch_prefetch_depth));
            }
            Some(Err(e)) => {
                let _ = outer_tx
                    .send(Err(anyhow::anyhow!("Planning error: {}", e)))
                    .await;
                return;
            }
            None => {
                stream_done = true;
                break;
            }
        }
    }

    while let Some(file_result) = in_flight.next().await {
        if !stream_done {
            match task_stream.next().await {
                Some(Ok(task)) => {
                    in_flight.push_back(spawn_fn(task, reader.clone(), batch_prefetch_depth));
                }
                Some(Err(e)) => {
                    let _ = outer_tx
                        .send(Err(anyhow::anyhow!("Planning error: {}", e)))
                        .await;
                    return;
                }
                None => stream_done = true,
            }
        }

        let (file_path, record_count, file_rx) = match file_result {
            Ok(tuple) => tuple,
            Err(e) => {
                let _ = outer_tx.send(Err(e)).await;
                return;
            }
        };

        let stream = futures::stream::unfold(
            file_rx,
            |mut rx: mpsc::Receiver<Result<BufferedBatch, iceberg::Error>>| async move {
                rx.recv().await.map(|result| {
                    let item = result.map(|buf| {
                        buf.semaphore.add_permits(buf.byte_len);
                        WARM_SCAN_STATS.track_buffer_release(buf.byte_len as u64);
                        buf.batch
                    });
                    (item, rx)
                })
            },
        )
        .boxed();

        let warm = IcebergWarmFileScan {
            file_path,
            record_count,
            stream: IcebergArrowStream {
                stream: AsyncMutex::new(stream),
            },
        };

        let fsend_start = Instant::now();
        let fsend_err = outer_tx.send(Ok(warm)).await.is_err();
        WARM_SCAN_STATS
            .file_send_ns
            .fetch_add(fsend_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if fsend_err {
            return;
        }
    }

    WARM_SCAN_STATS.pipeline_wall_ns.store(
        pipeline_start.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );
}

pub(crate) async fn run_warm(
    task_stream: FileScanTaskStream,
    reader: ArrowReader,
    task_prefetch_depth: usize,
    batch_prefetch_depth: usize,
    outer_tx: mpsc::Sender<Result<IcebergWarmFileScan, anyhow::Error>>,
) {
    run_warm_orchestrator(
        task_stream,
        reader,
        task_prefetch_depth,
        batch_prefetch_depth,
        outer_tx,
        spawn_file_task,
    )
    .await;
}

pub(crate) async fn run_warm_append(
    task_stream: futures::stream::BoxStream<'static, Result<AppendedFileScanTask, iceberg::Error>>,
    reader: ArrowReader,
    task_prefetch_depth: usize,
    batch_prefetch_depth: usize,
    outer_tx: mpsc::Sender<Result<IcebergWarmFileScan, anyhow::Error>>,
) {
    run_warm_orchestrator(
        task_stream,
        reader,
        task_prefetch_depth,
        batch_prefetch_depth,
        outer_tx,
        spawn_incremental_append_task,
    )
    .await;
}
