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
//!   3. Serializes each batch to Arrow IPC (via a shared rayon thread pool)
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

use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use futures::StreamExt;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use tokio::sync::{mpsc, Mutex as AsyncMutex, Semaphore};

use crate::pipeline_stats::{MAX_BUFFERED_BYTES_PER_TASK, STATS};
use crate::table::{ArrowBatch, IcebergArrowStream, IcebergFileScanStream};
use crate::unexpected;

/// Process-global rayon pool for Arrow IPC serialization.
static SERIALIZE_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    let n = std::thread::available_parallelism().unwrap().get();
    rayon::ThreadPoolBuilder::new()
        .num_threads(n)
        .build()
        .expect("failed to build serialization thread pool")
});

/// Time an async expression and record its duration into a STATS field.
///
/// ```ignore
/// let result = timed!(serialize_ns, { rayon_dispatch(&pool, batch) })
///     .map_err(...)?;
/// ```
macro_rules! timed {
    ($field:ident, $fut:expr) => {{
        let _start = Instant::now();
        let _result = $fut.await;
        STATS.add_elapsed(&STATS.$field, _start);
        _result
    }};
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
    prefetch_depth: usize,
) -> anyhow::Result<IcebergFileScanStream> {
    STATS.reset();

    let (tx, rx) = mpsc::channel::<Result<FileScan, iceberg::Error>>(prefetch_depth);

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
    prefetch_depth: usize,
) -> anyhow::Result<IcebergArrowStream> {
    // Outer channel: flatten task → Julia (via IcebergArrowStream).
    let (tx, rx) = mpsc::channel(concurrency * 2);

    let nested =
        create_nested_pipeline(tasks, file_io, batch_size, concurrency, prefetch_depth).await?;

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

    STATS.store_elapsed(&STATS.pipeline_wall_ns, pipeline_start);
}

/// Main nested consumer loop. Keeps `concurrency` file tasks in flight concurrently using
/// FuturesUnordered and wraps each completed task's receiver as an
/// IcebergArrowStream before sending a FileScan to the outer channel.
async fn run_nested(
    tasks: Vec<FileScanTask>,
    file_io: FileIO,
    batch_size: Option<usize>,
    concurrency: usize,
    tx: mpsc::Sender<Result<FileScan, iceberg::Error>>,
) {
    use futures::stream::FuturesUnordered;

    // For the nested path this records "time until last FileScan was handed to
    // the consumer". For the flat path run_flat overwrites it with the full
    // end-to-end time once all batches are drained, so the flat metric is
    // unaffected.
    let pipeline_start = Instant::now();

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
                if timed!(file_dispatch_wait_ns, tx.send(Ok(file_scan))).is_err() {
                    return; // outer consumer dropped the stream
                }
            }
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        }
    }

    STATS.store_elapsed(&STATS.pipeline_wall_ns, pipeline_start);
}

/// Spawn a single file task. Returns a future that resolves immediately to
/// the file's metadata and the receiving end of its batch channel.
///
/// The actual work (parquet I/O, decode, serialize) happens in the background
/// tokio task. The future resolves immediately to (filename, record_count,
/// Receiver) so that FuturesUnordered can poll it alongside other tasks.
fn spawn_file_task_with_meta(
    task: FileScanTask,
    file_io: FileIO,
    batch_size: Option<usize>,
) -> impl std::future::Future<
    Output = Result<
        (
            String,
            i64,
            mpsc::Receiver<Result<BufferedBatch, iceberg::Error>>,
        ),
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
    let batch_stream = reader.read(task_stream).map_err(|e| unexpected(e))?;
    STATS.add_elapsed(&STATS.reader_setup_ns, setup_start);

    tokio::pin!(batch_stream);

    loop {
        // ── Phase 2: Fetch + decode ─────────────────────────────────────
        // Each .next() call fetches compressed parquet pages from storage,
        // decompresses (ZSTD), decodes column encodings, and assembles a
        // RecordBatch. These are inseparable without forking parquet-rs.
        let batch_opt = timed!(fetch_decode_ns, batch_stream.next());

        let batch = match batch_opt {
            Some(Ok(b)) => b,
            Some(Err(e)) => return Err(e),
            None => break, // end of file
        };

        // ── Phase 3: Serialize to Arrow IPC ─────────────────────────────
        // Dispatched to the shared rayon pool so N idle workers handle all
        // file tasks; the oneshot channel bridges rayon back to async.
        let serialized = timed!(serialize_ns, {
            let (stx, srx) = tokio::sync::oneshot::channel();
            SERIALIZE_POOL.spawn(move || {
                let _ = stx.send(crate::serialize_record_batch(batch));
            });
            srx
        })
        .map_err(|e| unexpected(format!("serialize panicked: {e}")))?
        .map_err(|e| unexpected(e))?;

        let byte_len = serialized.length;
        STATS.batches_produced.fetch_add(1, Ordering::Relaxed);
        STATS
            .bytes_produced
            .fetch_add(byte_len as u64, Ordering::Relaxed);

        // ── Phase 4: Backpressure ───────────────────────────────────────
        // Acquire permits equal to the serialized size. If this file task
        // has produced > MAX_BUFFERED_BYTES_PER_TASK ahead of the consumer,
        // this yields (async, not thread-blocking) until permits are freed.
        let _permit = timed!(semaphore_wait_ns, semaphore.acquire_many(byte_len as u32))
            .map_err(|e| unexpected(format!("semaphore: {e}")))?;
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
