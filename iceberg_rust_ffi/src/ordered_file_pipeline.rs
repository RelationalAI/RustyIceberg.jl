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

use arrow_array::RecordBatch;
use futures::{Stream, StreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use tokio::sync::{mpsc, Mutex as AsyncMutex, Semaphore};

use crate::pipeline_stats::{MAX_BUFFERED_BYTES_PER_TASK, STATS};
use crate::table::{ArrowBatch, IcebergArrowStream, IcebergFileScanStream};
use crate::unexpected;

/// Process-global rayon pool for Arrow IPC serialization.
pub(crate) static SERIALIZE_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(crate::cpu_count())
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
pub(crate) struct BufferedBatch {
    pub(crate) batch: ArrowBatch,
    pub(crate) byte_len: usize,
    pub(crate) semaphore: Arc<Semaphore>,
}

/// Internal per-file scan result: filename, record count, and a prefetched
/// inner batch stream. Used as the item type of the nested pipeline channel.
pub struct FileScan {
    pub filename: String,
    pub record_count: i64,
    pub stream: IcebergArrowStream,
}

/// Convert a per-file mpsc receiver into an `IcebergArrowStream`.
/// Releases semaphore permits as each batch is yielded and calls `on_release`
/// with the byte count (use it to update stats or pass `|_| {}` to skip).
pub(crate) fn make_file_stream<F>(
    file_rx: mpsc::Receiver<Result<BufferedBatch, iceberg::Error>>,
    on_release: F,
) -> IcebergArrowStream
where
    F: Fn(usize) + Send + 'static,
{
    let stream = futures::stream::unfold((file_rx, on_release), |(mut rx, cb)| async move {
        rx.recv().await.map(|item| {
            let result = item.map(|buf| {
                buf.semaphore.add_permits(buf.byte_len);
                cb(buf.byte_len);
                buf.batch
            });
            (result, (rx, cb))
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

/// Generic `FuturesUnordered` orchestrator shared by the full scan and
/// incremental append pipelines.
///
/// Keeps `concurrency` file tasks in flight from `task_stream`, wraps each
/// completed receiver as an `IcebergArrowStream` via `make_file_stream`, and
/// forwards `FileScan` items to `tx` in source order.
///
/// `spawn_task` converts a task into a future that resolves immediately to
/// `(filename, record_count, Receiver)` — the actual I/O runs in the
/// background tokio task spawned inside `spawn_task`.
///
/// `on_release` is forwarded to `make_file_stream`; pass
/// `|n| STATS.track_buffer_release(n as u64)` for the full scan or
/// `|_| {}` when no stats tracking is needed.
pub(crate) async fn run_nested_pipeline<T, S, SpawnFut, F>(
    mut task_stream: S,
    concurrency: usize,
    tx: mpsc::Sender<Result<FileScan, iceberg::Error>>,
    spawn_task: impl Fn(T) -> SpawnFut,
    on_release: F,
) where
    S: futures::Stream<Item = iceberg::Result<T>> + Unpin,
    SpawnFut: std::future::Future<
            Output = iceberg::Result<(
                String,
                i64,
                mpsc::Receiver<Result<BufferedBatch, iceberg::Error>>,
            )>,
        > + Send
        + 'static,
    F: Fn(usize) + Send + Clone + 'static,
{
    use futures::{future::BoxFuture, stream::FuturesUnordered};

    type SpawnResult<E> = Result<(String, i64, mpsc::Receiver<Result<BufferedBatch, E>>), E>;
    let mut in_flight: FuturesUnordered<BoxFuture<'static, SpawnResult<iceberg::Error>>> =
        FuturesUnordered::new();
    let mut stream_done = false;

    for _ in 0..concurrency {
        match task_stream.next().await {
            Some(Ok(task)) => in_flight.push(Box::pin(spawn_task(task))),
            Some(Err(e)) => {
                let _ = tx.send(Err(e)).await;
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
            // Eagerly start the next file to keep N tasks in flight.
            match task_stream.next().await {
                Some(Ok(task)) => in_flight.push(Box::pin(spawn_task(task))),
                Some(Err(e)) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                None => stream_done = true,
            }
        }

        match file_result {
            Ok((filename, record_count, file_rx)) => {
                let file_scan = FileScan {
                    filename,
                    record_count,
                    stream: make_file_stream(file_rx, on_release.clone()),
                };
                if timed!(file_dispatch_wait_ns, tx.send(Ok(file_scan))).is_err() {
                    return;
                }
            }
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        }
    }
}

/// Full-scan nested consumer. Wraps `run_nested_pipeline` and records
/// wall-clock time around it for `STATS.pipeline_wall_ns`.
async fn run_nested(
    tasks: Vec<FileScanTask>,
    file_io: FileIO,
    batch_size: Option<usize>,
    concurrency: usize,
    tx: mpsc::Sender<Result<FileScan, iceberg::Error>>,
) {
    // For the nested path this records "time until last FileScan was handed to
    // the consumer". For the flat path run_flat overwrites it with the full
    // end-to-end time once all batches are drained, so the flat metric is
    // unaffected.
    let pipeline_start = Instant::now();

    let task_stream =
        futures::stream::iter(tasks.into_iter().map(Ok::<FileScanTask, iceberg::Error>));

    run_nested_pipeline(
        task_stream,
        concurrency,
        tx,
        move |task| spawn_file_task_with_meta(task, file_io.clone(), batch_size),
        |n| STATS.track_buffer_release(n as u64),
    )
    .await;

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

    drain_batch_stream(batch_stream, &semaphore, tx).await
}

/// Drain a batch stream through phases 2-4 of the file pipeline, recording
/// timing and throughput into STATS. Shared by both the full-scan and
/// incremental-scan pipelines — the only difference between the two is how
/// the stream is obtained before calling this function.
///
///   2. Fetch + decode — timed; each `.next()` does I/O + decompression
///   3. Serialize      — timed; dispatched to SERIALIZE_POOL via oneshot
///   4. Backpressure   — timed; semaphore blocks if producer is too far ahead
pub(crate) async fn drain_batch_stream(
    mut batch_stream: impl Stream<Item = iceberg::Result<RecordBatch>> + Unpin,
    semaphore: &Arc<Semaphore>,
    tx: &mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) -> Result<(), iceberg::Error> {
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

#[cfg(test)]
mod tests {
    use std::future::Future;

    use super::*;

    // ── Helpers ──────────────────────────────────────────────────────────────

    type SpawnOutput = iceberg::Result<(
        String,
        i64,
        mpsc::Receiver<Result<BufferedBatch, iceberg::Error>>,
    )>;

    /// Spawn function that resolves immediately with an empty inner channel
    /// (simulates a file with no batches). No background task is spawned.
    fn spawn_empty(
        (name, rc): (String, i64),
    ) -> impl Future<Output = SpawnOutput> + Send + 'static {
        let (_, rx) = mpsc::channel(1); // tx dropped → channel immediately closed
        async move { Ok((name, rc, rx)) }
    }

    /// Spawn function that always returns an error.
    fn spawn_failing(
        (_name, _rc): (String, i64),
    ) -> impl Future<Output = SpawnOutput> + Send + 'static {
        async move {
            Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "spawn failed",
            ))
        }
    }

    /// Run `run_nested_pipeline` to completion and collect all output items.
    async fn collect<F, Fut>(
        tasks: Vec<(String, i64)>,
        concurrency: usize,
        spawn: F,
    ) -> Vec<iceberg::Result<(String, i64)>>
    where
        F: Fn((String, i64)) -> Fut,
        Fut: Future<Output = SpawnOutput> + Send + 'static,
    {
        let stream = futures::stream::iter(tasks.into_iter().map(Ok::<_, iceberg::Error>));
        let (tx, mut rx) = mpsc::channel(128);
        run_nested_pipeline(stream, concurrency, tx, spawn, |_| {}).await;
        let mut out = vec![];
        while let Some(item) = rx.recv().await {
            out.push(item.map(|fs| (fs.filename, fs.record_count)));
        }
        out
    }

    // ── Orchestration tests (spawn_empty — no actual I/O) ────────────────────
    //
    // These tests use spawn_empty, which drops the inner sender immediately so
    // no batches flow and no file I/O happens.  They only exercise the
    // FuturesUnordered seeding/draining logic inside run_nested_pipeline.

    #[tokio::test]
    async fn empty_stream_yields_nothing() {
        // Baseline: a pipeline with zero input tasks produces zero output.
        assert!(collect(vec![], 4, spawn_empty).await.is_empty());
    }

    #[tokio::test]
    async fn single_file_forwarded() {
        // A single task produces exactly one FileScan with matching metadata.
        let got = collect(vec![("a.parquet".into(), 42)], 1, spawn_empty).await;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].as_ref().unwrap(), &("a.parquet".to_string(), 42));
    }

    #[tokio::test]
    async fn files_yielded_in_source_order() {
        // With concurrency=1 only one future is in-flight at a time, so
        // FuturesUnordered resolves them strictly in push order.
        let tasks = vec![
            ("a.parquet".into(), 10),
            ("b.parquet".into(), 20),
            ("c.parquet".into(), 30),
        ];
        let pairs: Vec<(String, i64)> = collect(tasks, 1, spawn_empty)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            pairs,
            vec![
                ("a.parquet".to_string(), 10),
                ("b.parquet".to_string(), 20),
                ("c.parquet".to_string(), 30),
            ]
        );
    }

    #[tokio::test]
    async fn all_files_present_with_high_concurrency() {
        // When concurrency > task count all tasks are seeded before any complete.
        // Order is non-deterministic, so we only check all five are present.
        let tasks: Vec<(String, i64)> = (0..5)
            .map(|i| (format!("f{i}.parquet"), i as i64))
            .collect();
        let got = collect(tasks, 8, spawn_empty).await;
        assert_eq!(got.len(), 5);
        assert!(got.iter().all(|r| r.is_ok()));
        let names: std::collections::HashSet<String> =
            got.into_iter().map(|r| r.unwrap().0).collect();
        for i in 0..5usize {
            assert!(names.contains(&format!("f{i}.parquet")));
        }
    }

    #[tokio::test]
    async fn source_stream_error_at_seeding_propagates() {
        // An error as the very first item is caught during the seeding loop and
        // forwarded to the output channel; no FileScan is produced.
        let stream = futures::stream::iter(vec![Err::<(String, i64), _>(iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "seed error",
        ))]);
        let (tx, mut rx) = mpsc::channel(128);
        run_nested_pipeline(stream, 1, tx, spawn_empty, |_| {}).await;

        let item = rx.recv().await.unwrap();
        assert!(item.is_err());
        assert!(item.err().unwrap().to_string().contains("seed error"));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn source_stream_error_during_drain_propagates() {
        // An error on the second pull hits during the "eagerly fetch next" step
        // inside the main drain loop.  The pipeline sends the error immediately
        // and stops; the in-flight FileScan for the first task is dropped.
        let stream = futures::stream::iter(vec![
            Ok::<(String, i64), iceberg::Error>(("a.parquet".into(), 1)),
            Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "drain error",
            )),
        ]);
        let (tx, mut rx) = mpsc::channel(128);
        run_nested_pipeline(stream, 1, tx, spawn_empty, |_| {}).await;

        let item = rx.recv().await.unwrap();
        assert!(item.is_err());
        assert!(item.err().unwrap().to_string().contains("drain error"));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn spawn_error_propagates() {
        // If the spawn_task future itself resolves to Err, the error is forwarded
        // to the outer channel and the pipeline stops.
        let got = collect(vec![("bad.parquet".into(), 0)], 1, spawn_failing).await;
        assert_eq!(got.len(), 1);
        assert!(got[0].is_err());
    }

    #[tokio::test]
    async fn consumer_dropping_rx_does_not_panic() {
        // If the consumer drops its receiver before the pipeline finishes, the
        // pipeline must exit cleanly (the channel send returns Err, which is
        // treated as a stop signal, not a panic).
        let tasks: Vec<(String, i64)> = (0..20)
            .map(|i| (format!("f{i}.parquet"), i as i64))
            .collect();
        let stream = futures::stream::iter(tasks.into_iter().map(Ok::<_, iceberg::Error>));
        let (tx, rx) = mpsc::channel(1); // tiny buffer so the pipeline blocks quickly
        drop(rx); // consumer disappears immediately
        run_nested_pipeline(stream, 4, tx, spawn_empty, |_| {}).await;
    }

    // ── End-to-end test: exercises the full per-file processing path ──────
    //
    // Unlike the orchestration tests above, this test runs real parquet I/O
    // through the pipeline: reader setup, fetch/decode, Arrow IPC serialization,
    // semaphore backpressure, and make_file_stream permit release.

    #[tokio::test]
    async fn full_pipeline_reads_parquet_file() {
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_ipc::reader::StreamReader;
        use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use bytes::Bytes;
        use iceberg::io::FileIO;
        use iceberg::scan::FileScanTask;
        use iceberg::spec::{
            DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type,
        };
        use parquet::arrow::ArrowWriter;
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        use std::collections::HashMap;

        // ── 1. Arrow schema with embedded field ID ─────────────────────────
        let arrow_field = ArrowField::new("id", DataType::Int32, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
        );
        let arrow_schema = std::sync::Arc::new(ArrowSchema::new(vec![arrow_field]));

        // ── 2. Write a 3-row RecordBatch to parquet bytes ──────────────────
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![std::sync::Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let parquet_bytes = Bytes::from(buf);
        let file_size = parquet_bytes.len() as u64;

        // ── 3. Store in in-memory FileIO ───────────────────────────────────
        let file_io = FileIO::new_with_memory();
        let path = "memory:///pipeline_test/data.parquet";
        file_io
            .new_output(path)
            .unwrap()
            .write(parquet_bytes)
            .await
            .unwrap();

        // ── 4. Matching iceberg schema + FileScanTask ──────────────────────
        let iceberg_schema = std::sync::Arc::new(
            IcebergSchema::builder()
                .with_fields(vec![std::sync::Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .unwrap(),
        );
        let task = FileScanTask {
            data_file_path: path.to_string(),
            file_size_in_bytes: file_size,
            start: 0,
            length: file_size,
            record_count: Some(3),
            data_file_format: DataFileFormat::Parquet,
            schema: iceberg_schema,
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        };

        // ── 5. Run the full pipeline ───────────────────────────────────────
        // Exercises: run_nested → run_nested_pipeline → spawn_file_task_with_meta
        //   → process_file_inner (reader setup, fetch/decode, serialize IPC,
        //     semaphore acquire) → make_file_stream (semaphore release)
        let nested = create_nested_pipeline(vec![task], file_io, None, 1, 1)
            .await
            .unwrap();

        // ── 6. Drain outer FileScanStream ──────────────────────────────────
        let mut outer = nested.stream.lock().await;
        let file_scan = outer.next().await.unwrap().unwrap();
        assert_eq!(file_scan.filename, path);
        assert_eq!(file_scan.record_count, 3);
        assert!(outer.next().await.is_none(), "expected exactly one file");

        // ── 7. Drain inner IcebergArrowStream → decode Arrow IPC ──────────
        let mut inner = file_scan.stream.stream.lock().await;
        let arrow_batch = inner.next().await.unwrap().unwrap();

        let data = unsafe { std::slice::from_raw_parts(arrow_batch.data, arrow_batch.length) };
        let mut reader = StreamReader::try_new(std::io::Cursor::new(data), None).unwrap();
        let decoded = reader.next().unwrap().unwrap();
        assert_eq!(decoded.num_rows(), 3);
        let id_col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.values(), &[10, 20, 30]);

        // Release the Vec<u8> allocated by serialize_record_batch.
        unsafe { drop(Box::from_raw(arrow_batch.rust_ptr as *mut Vec<u8>)) };

        assert!(inner.next().await.is_none(), "expected exactly one batch");
    }
}
