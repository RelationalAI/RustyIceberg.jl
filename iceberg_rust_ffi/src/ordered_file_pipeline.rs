//! File-parallel pipeline for reading Iceberg tables with strict ordering.
//!
//! # Problem
//! iceberg-rs's `to_arrow()` uses `try_for_each_concurrent` which interleaves
//! batches from different files in arbitrary order. We need strict
//! file-then-row ordering.
//!
//! # Approach
//! We call `plan_files()` to get an ordered list of FileScanTasks, then feed
//! them to the file-parallel pipeline.
//!
//! Each file task is a background tokio task that:
//!   1. Builds a per-file ArrowReader (same iceberg-rs code path as to_arrow)
//!   2. Reads row groups sequentially → RecordBatch stream
//!   3. Serializes each batch to Arrow IPC (via a shared rayon thread pool)
//!   4. Sends serialized batches into a per-file mpsc channel
//!
//! For full scans, `create_full_scan_stream` drives the spawn rate via
//! `futures::stream::iter(tasks).map(start_file_task).buffered(prefetch_depth)`.
//! Each `FileScan` wraps its file's batch channel as an `IcebergArrowStream`
//! with integrated semaphore-permit release.
//!
//! For incremental scans, the shared `run_nested_pipeline` orchestrator below
//! uses `FuturesUnordered` over a streaming source of `FileScanTask`s.
//!
//! # Concurrency invariant — alive `process_file` tasks
//! `Stream::buffered(N)` keeps at most `N` inner futures actively polled. Each
//! inner future trivially resolves to `Ok(FileScan)` after `tokio::spawn` —
//! but the spawned `process_file` keeps running independently afterwards,
//! throttled by per-file `slot_sem`/`byte_sem` and woken by consumer pulls.
//! So the cap on alive `process_file` tasks is:
//!
//!     prefetch_depth + (number of FileScans handed out but not yet drained)
//!
//! In the simple case where one consumer drains files serially, the
//! parenthetical is `1`, i.e. `prefetch_depth + 1`. With M parallel consumers
//! (e.g. Julia's `ICEBERG_FILE_TASK_GROUP` with M workers), the cap is
//! `prefetch_depth + M`. The unit test pulls exactly one FileScan and so
//! checks the `prefetch_depth + 1` lower bound directly.
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

/// A serialized Arrow IPC batch bundled with its byte size and references
/// to the originating file task's two semaphores. The consumer releases
/// permits after forwarding the batch to Julia, unblocking the producer:
/// `byte_sem` adds back `byte_len` bytes; `slot_sem` adds back 1 batch slot.
pub(crate) struct BufferedBatch {
    pub(crate) batch: ArrowBatch,
    pub(crate) byte_len: usize,
    pub(crate) byte_sem: Arc<Semaphore>,
    pub(crate) slot_sem: Arc<Semaphore>,
}

/// Initial batch-slot budget for an unattached file (i.e. not yet picked
/// up by `iceberg_next_file_scan`). The producer can fill at most this
/// many batches before parking on `slot_sem.acquire()`. Promoted to
/// `ATTACHED_SLOTS` at the FFI handoff site (see `IcebergFileScanResponse::set_payload`).
pub(crate) const UNATTACHED_SLOTS: usize = 1;
/// Full batch-slot budget for an attached file. Matches the per-file mpsc
/// capacity, so once attached the slot semaphore stops being the binding
/// constraint and the existing 100 MB byte budget is what throttles.
pub(crate) const ATTACHED_SLOTS: usize = 8;

/// Internal per-file scan result: filename, record count, prefetched inner
/// batch stream, and the per-file slot semaphore. The slot semaphore is
/// carried through the outer channel so the FFI handoff site can promote
/// it from `UNATTACHED_SLOTS` to `ATTACHED_SLOTS` with one `add_permits`
/// call — no separate plumbing through the orchestrator.
pub struct FileScan {
    pub filename: String,
    pub record_count: i64,
    pub stream: IcebergArrowStream,
    pub(crate) slot_sem: Arc<Semaphore>,
}

/// Convert a per-file mpsc receiver into an `IcebergArrowStream`.
/// Releases semaphore permits as each batch is yielded and calls `on_release`
/// with the byte count (use it to update stats or pass `|_| {}` to skip).
///
/// Also records two consumer-side metrics (separate from `on_release`):
/// - `consumer_batch_wait_ns`: time the consumer spent inside `rx.recv().await`.
///   This is the symmetric counterpart of `producer_stall_ns` (the producer
///   waiting on the *same* per-file mpsc when it's full): high ⇒ producer-
///   bound, low ⇒ Julia/consumer-bound at the per-batch level.
/// - `pipeline_end_ns`: stamped via `record_file_drained()` when `recv()`
///   returns `None`, i.e. when this file's `process_file` task dropped its
///   `tx`. Used to compute the true pipeline wall clock in `print_summary`.
pub(crate) fn make_file_stream<F>(
    file_rx: mpsc::Receiver<Result<BufferedBatch, iceberg::Error>>,
    on_release: F,
) -> IcebergArrowStream
where
    F: Fn(usize) + Send + 'static,
{
    let stream = futures::stream::unfold((file_rx, on_release), |(mut rx, cb)| async move {
        let recv_start = Instant::now();
        let recvd = rx.recv().await;
        STATS.add_elapsed(&STATS.consumer_batch_wait_ns, recv_start);
        match recvd {
            None => {
                // Producer dropped tx → this file is done draining.
                STATS.record_file_drained();
                None
            }
            Some(item) => {
                let result = item.map(|buf| {
                    buf.byte_sem.add_permits(buf.byte_len);
                    buf.slot_sem.add_permits(1);
                    cb(buf.byte_len);
                    buf.batch
                });
                Some((result, (rx, cb)))
            }
        }
    })
    .boxed();
    IcebergArrowStream {
        stream: AsyncMutex::new(stream),
    }
}

/// Build the full-scan pipeline as a `Stream<FileScan>` driven by
/// `Stream::buffered(prefetch_depth)` over a synchronous spawn factory.
/// See the module-level "Concurrency invariant" doc for the bound on alive
/// `process_file` tasks.
pub async fn create_full_scan_stream(
    tasks: Vec<FileScanTask>,
    file_io: FileIO,
    batch_size: Option<usize>,
    prefetch_depth: usize,
) -> IcebergFileScanStream {
    STATS.reset();
    let prefetch_depth = prefetch_depth.max(1);

    let buffered = futures::stream::iter(tasks)
        .map(move |task| {
            let fs = start_file_task(task, file_io.clone(), batch_size);
            async move { Ok::<FileScan, iceberg::Error>(fs) }
        })
        .buffered(prefetch_depth);

    // Time how long Julia (the consumer) waits when no FileScan is ready
    // yet — symmetric to the per-batch `consumer_batch_wait_ns` stat.
    let timed = futures::stream::unfold(Box::pin(buffered), |mut s| async move {
        let recv_start = Instant::now();
        let item = s.next().await;
        STATS.add_elapsed(&STATS.consumer_file_wait_ns, recv_start);
        item.map(|i| (i, s))
    })
    .boxed();

    IcebergFileScanStream {
        stream: AsyncMutex::new(timed),
    }
}

/// Sync per-file setup + spawn. Allocates the per-file mpsc and both
/// semaphores, kicks off `process_file` on the Tokio runtime, and returns
/// the `FileScan` handle. Errors during setup are infallible (allocations
/// only); real I/O failures surface inside the spawned task and are
/// forwarded as `Err(...)` items on the per-file mpsc.
fn start_file_task(task: FileScanTask, file_io: FileIO, batch_size: Option<usize>) -> FileScan {
    let filename = task.data_file_path().to_string();
    let record_count = task.record_count.unwrap_or(0) as i64;

    let byte_sem = Arc::new(Semaphore::new(MAX_BUFFERED_BYTES_PER_TASK));
    // Start at the unattached prefetch floor; the FFI handoff site
    // (`IcebergFileScanResponse::set_payload`) tops up to ATTACHED_SLOTS.
    let slot_sem = Arc::new(Semaphore::new(UNATTACHED_SLOTS));
    let (file_tx, file_rx) = mpsc::channel(ATTACHED_SLOTS);

    tokio::spawn(process_file(
        task,
        file_io,
        batch_size,
        byte_sem,
        slot_sem.clone(),
        file_tx,
    ));

    FileScan {
        filename,
        record_count,
        stream: make_file_stream(file_rx, |n| STATS.track_buffer_release(n as u64)),
        slot_sem,
    }
}

/// Generic `FuturesUnordered` orchestrator used by the incremental append
/// pipeline (the full-scan path uses `create_full_scan_stream` directly).
///
/// Keeps `concurrency` file tasks in flight from `task_stream`, wraps each
/// completed receiver as an `IcebergArrowStream` via `make_file_stream`, and
/// forwards `FileScan` items to `tx` in source order.
///
/// `spawn_task` converts a task into a future that resolves immediately to
/// `(filename, record_count, Receiver, slot_sem)` — the actual I/O runs in
/// the background tokio task spawned inside `spawn_task`. `slot_sem` rides
/// along on the resulting `FileScan` so the FFI handoff site can promote it
/// from `UNATTACHED_SLOTS` to `ATTACHED_SLOTS` without any extra plumbing.
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
                Arc<Semaphore>,
            )>,
        > + Send
        + 'static,
    F: Fn(usize) + Send + Clone + 'static,
{
    use futures::{future::BoxFuture, stream::FuturesUnordered};

    type SpawnResult<E> = Result<
        (
            String,
            i64,
            mpsc::Receiver<Result<BufferedBatch, E>>,
            Arc<Semaphore>,
        ),
        E,
    >;
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
            Ok((filename, record_count, file_rx, slot_sem)) => {
                let file_scan = FileScan {
                    filename,
                    record_count,
                    stream: make_file_stream(file_rx, on_release.clone()),
                    slot_sem,
                };
                if timed!(outer_queue_full_wait_ns, tx.send(Ok(file_scan))).is_err() {
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

/// Process a single parquet file through four timed phases:
///   1. Reader setup — build ArrowReader, open file, resolve schema
///   2. Fetch + decode — I/O, ZSTD decompression, column assembly
///   3. Serialize — RecordBatch → Arrow IPC for transfer to Julia
///   4. Backpressure — wait on semaphore if buffered too far ahead
///
/// Each phase's cumulative time is recorded in STATS for the summary. When
/// this function returns, `tx` is dropped, causing the consumer's
/// `file_rx.recv()` to return None — signaling that this file is done.
async fn process_file(
    task: FileScanTask,
    file_io: FileIO,
    batch_size: Option<usize>,
    byte_sem: Arc<Semaphore>,
    slot_sem: Arc<Semaphore>,
    tx: mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) {
    STATS.track_task_start();
    let result: Result<(), iceberg::Error> = async {
        // ── Phase 1: Reader setup ───────────────────────────────────────
        // Builds a per-file ArrowReader using the same iceberg-rs code path
        // as to_arrow(): opens parquet metadata, resolves schema, loads
        // delete files, builds row filters. with_data_file_concurrency_limit(1)
        // means this reader processes one file (the one we give it).
        let setup_start = Instant::now();
        let mut builder = ArrowReaderBuilder::new(file_io).with_data_file_concurrency_limit(1);
        if let Some(bs) = batch_size {
            builder = builder.with_batch_size(bs);
        }
        let reader = builder.build();
        let task_stream = Box::pin(futures::stream::once(async { Ok(task) }));
        let batch_stream = reader.read(task_stream).map_err(|e| unexpected(e))?;
        STATS.add_elapsed(&STATS.reader_setup_ns, setup_start);

        drain_batch_stream(batch_stream, &byte_sem, &slot_sem, &tx).await
    }
    .await;
    if let Err(e) = result {
        let _ = tx.send(Err(e)).await;
    }
    STATS.track_task_end();
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
    byte_sem: &Arc<Semaphore>,
    slot_sem: &Arc<Semaphore>,
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
        // Producer is throttled by three per-file bounds, all funneling
        // into `producer_stall_ns` because they share one root cause
        // ("consumer hasn't drained enough"):
        //   (a) byte budget — `byte_sem.acquire_many(byte_len)` blocks if
        //       this file's outstanding buffered bytes exceed
        //       MAX_BUFFERED_BYTES_PER_TASK (100 MB).
        //   (b) batch-slot budget — `slot_sem.acquire()` blocks if the
        //       file has hit its current cap (UNATTACHED_SLOTS before
        //       Julia attaches; ATTACHED_SLOTS after).
        //   (c) channel-capacity safety — `tx.send(...)` blocks if the
        //       mpsc is full. With slot_sem ≤ ATTACHED_SLOTS = mpsc
        //       capacity, this is a redundant safety net.
        //
        // Each acquire races against `tx.closed()` so that if the consumer
        // drops the receiver, parked producers wake immediately and exit
        // cleanly. Without this, an unattached producer parked on
        // `slot_sem.acquire()` would never be woken (its permits are only
        // released by consumer `recv()`) and the spawned task would leak.
        let byte_acquire_start = Instant::now();
        let _byte_permit = tokio::select! {
            permit = byte_sem.acquire_many(byte_len as u32) => {
                STATS.add_elapsed(&STATS.producer_stall_ns, byte_acquire_start);
                permit.map_err(|e| unexpected(format!("byte_sem: {e}")))?
            }
            _ = tx.closed() => return Ok(()),
        };
        std::mem::forget(_byte_permit);
        let slot_acquire_start = Instant::now();
        let _slot_permit = tokio::select! {
            permit = slot_sem.acquire() => {
                STATS.add_elapsed(&STATS.producer_stall_ns, slot_acquire_start);
                permit.map_err(|e| unexpected(format!("slot_sem: {e}")))?
            }
            _ = tx.closed() => return Ok(()),
        };
        std::mem::forget(_slot_permit);

        STATS.track_buffer_add(byte_len as u64);

        // Send the batch to this file's channel.
        let send_result = timed!(
            producer_stall_ns,
            tx.send(Ok(BufferedBatch {
                batch: serialized,
                byte_len,
                byte_sem: byte_sem.clone(),
                slot_sem: slot_sem.clone(),
            }))
        );
        if send_result.is_err() {
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
        Arc<Semaphore>,
    )>;

    /// Spawn function that resolves immediately with an empty inner channel
    /// (simulates a file with no batches). No background task is spawned.
    fn spawn_empty(
        (name, rc): (String, i64),
    ) -> impl Future<Output = SpawnOutput> + Send + 'static {
        let (_, rx) = mpsc::channel(1); // tx dropped → channel immediately closed
        let slot_sem = Arc::new(Semaphore::new(UNATTACHED_SLOTS));
        async move { Ok((name, rc, rx, slot_sem)) }
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

    #[tokio::test]
    async fn drain_batch_stream_wakes_when_consumer_drops_receiver() {
        // A producer parked on `slot_sem.acquire()` must wake when the
        // consumer drops the receiver, otherwise the spawned task leaks.
        //
        // Force the producer to actually park on slot_sem before we drop rx:
        //   1. slot_sem starts at 1 permit. Batch 1 consumes it (1 → 0).
        //   2. Batch 1 sends successfully into the mpsc (rx still alive).
        //   3. Producer asks for batch 2 → blocks on slot_sem.acquire() since
        //      no consumer has released a permit (we never drain rx).
        //   4. We drop rx, firing tx.closed().
        //   5. Without the `tokio::select! { _ = tx.closed() => ... }` arm,
        //      the producer hangs forever. With it, drain returns Ok(()).
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use std::sync::Arc;

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let make_batch = || {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .map_err(|e| unexpected(e.to_string()))
        };

        // Two batches; the second will block the producer on slot_sem.
        let batches = futures::stream::iter(vec![make_batch(), make_batch()]);

        let byte_sem = Arc::new(Semaphore::new(MAX_BUFFERED_BYTES_PER_TASK));
        let slot_sem = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel::<Result<BufferedBatch, iceberg::Error>>(8);

        // Spawn the drain. The byte_sem and slot_sem must be moved in (not
        // borrowed) since the JoinHandle outlives this stack frame.
        let byte_sem_for_drain = byte_sem.clone();
        let slot_sem_for_drain = slot_sem.clone();
        let drain = tokio::spawn(async move {
            drain_batch_stream(batches, &byte_sem_for_drain, &slot_sem_for_drain, &tx).await
        });

        // Wait for batch 1 to reach the mpsc (proves the producer ran past
        // its first slot_sem.acquire and is now parked on the second).
        let _batch1 = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .expect("producer did not deliver batch 1 within timeout")
            .expect("mpsc closed unexpectedly")
            .expect("batch 1 was an Err");
        assert_eq!(slot_sem.available_permits(), 0, "slot_sem should be drained");

        // Drop rx → tx.closed() fires → the cancellation arm in
        // drain_batch_stream wakes the parked producer.
        drop(rx);

        // Without the cancellation fix, this would never complete.
        let result = tokio::time::timeout(std::time::Duration::from_millis(500), drain)
            .await
            .expect("drain_batch_stream did not exit after consumer dropped rx (parked on slot_sem)")
            .expect("drain task panicked");
        assert!(result.is_ok());
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
        // Exercises: create_full_scan_stream → start_file_task → process_file
        //   (reader setup, fetch/decode, serialize IPC, semaphore acquire)
        //   → make_file_stream (semaphore release)
        let nested = create_full_scan_stream(vec![task], file_io, None, 1).await;

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

    #[tokio::test]
    async fn create_full_scan_stream_caps_in_flight_at_prefetch_depth_plus_one() {
        // Verify the single-consumer slice of the concurrency invariant
        // (see module-level "Concurrency invariant" doc): with one FileScan
        // pulled out and held, alive `process_file` tasks ≤ prefetch_depth + 1.
        //
        // The prior pipeline accepted a separate `data_file_concurrency_limit`
        // that did not actually cap alive tasks; setting it to 1 still let
        // peak in-flight reach ~8. This test pins down that under the new
        // single-knob shape, with prefetch_depth=2 and 10 input files, peak
        // is ≤ 3 (2 buffered + 1 we pulled).
        //
        // To force producers to *stay alive concurrently* (and not silently
        // pass via fast complete-and-exit timing), we use multi-batch files:
        // batch_size=1 turns each 3-row file into 3 batches, so after batch 1
        // each unattached producer parks on `slot_sem.acquire()` (UNATTACHED_SLOTS=1).
        // We pull the first FileScan to attach it but never drain its inner
        // stream, so the attached file's producer also stays alive. With all
        // producers parked, the peak reflects the actual structural cap.
        use arrow_array::{Int32Array, RecordBatch};
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

        let arrow_field = ArrowField::new("id", DataType::Int32, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
        );
        let arrow_schema = std::sync::Arc::new(ArrowSchema::new(vec![arrow_field]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![std::sync::Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let parquet_bytes = Bytes::from(buf);
        let file_size = parquet_bytes.len() as u64;

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

        let file_io = FileIO::new_with_memory();
        let mut tasks = Vec::with_capacity(10);
        for i in 0..10 {
            let path = format!("memory:///prefetch_test/data-{i}.parquet");
            file_io
                .new_output(&path)
                .unwrap()
                .write(parquet_bytes.clone())
                .await
                .unwrap();
            tasks.push(FileScanTask {
                data_file_path: path,
                file_size_in_bytes: file_size,
                start: 0,
                length: file_size,
                record_count: Some(3),
                data_file_format: DataFileFormat::Parquet,
                schema: iceberg_schema.clone(),
                project_field_ids: vec![1],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            });
        }

        let prefetch_depth = 2;
        // batch_size=1 → 3 batches/file → producers actually park on slot_sem
        // (UNATTACHED_SLOTS=1) after batch 1 instead of finishing instantly.
        let nested = create_full_scan_stream(tasks, file_io, Some(1), prefetch_depth).await;

        // Pull the first FileScan (the "+1"). Don't drain its inner stream —
        // its producer stays alive parked on slot_sem after one ATTACHED_SLOTS
        // batch (8 batches > 3-row file, but the consumer never recv()s, so the
        // producer stays alive until the file's batches all queue or ATTACHED_SLOTS
        // is hit; for our 3-batch file it queues all then hits batch_stream.next()
        // returning None and exits — which is acceptable: peak is observed during
        // the stable window before that producer exits). Subsequent files'
        // producers stay parked at UNATTACHED_SLOTS=1 → 0 after batch 1.
        let mut outer = nested.stream.lock().await;
        let _first_file = outer.next().await.unwrap().unwrap();

        // Wait for peak_concurrency to stabilize (two consecutive yields with no
        // change). `buffered` polls eagerly so the count climbs quickly; bail
        // after a hard cap to avoid hangs.
        let mut last_peak = 0usize;
        let mut stable_ticks = 0usize;
        for _ in 0..200 {
            tokio::task::yield_now().await;
            let p = STATS.peak_concurrency.load(Ordering::Relaxed);
            if p == last_peak && p > 0 {
                stable_ticks += 1;
                if stable_ticks >= 3 {
                    break;
                }
            } else {
                stable_ticks = 0;
                last_peak = p;
            }
        }

        let peak = STATS.peak_concurrency.load(Ordering::Relaxed);
        assert!(
            peak >= 1,
            "expected at least 1 process_file alive (the attached file)"
        );
        assert!(
            peak <= prefetch_depth + 1,
            "peak in-flight {} exceeds prefetch_depth+1 = {}; the new single-knob \
             buffered({}) bound is not being enforced",
            peak,
            prefetch_depth + 1,
            prefetch_depth
        );

        // Drop the stream so spawned tasks wind down via the cancellation path
        // (slot_sem.acquire() racing tx.closed()) before the test exits.
        drop(_first_file);
        drop(outer);
        drop(nested);
        tokio::task::yield_now().await;
    }
}
