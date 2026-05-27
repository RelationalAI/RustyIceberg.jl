//! Nested per-file pipeline shared by the full-scan and
//! incremental-append entry points.
//!
//! # Problem
//! iceberg-rs's `to_arrow()` uses `try_for_each_concurrent` which
//! interleaves batches from different files in arbitrary order. We need
//! strict file-then-row ordering.
//!
//! # Shape
//! Both pipelines reduce to the same skeleton: a source `Stream` of
//! per-file tasks (`FileScanTask` for full-scan / `AppendedFileScanTask`
//! for incremental-append) is fed through `create_nested_pipeline`,
//! which uses `futures::stream::Stream::buffered(prefetch_depth)` to
//! keep at most `prefetch_depth` per-file `spawn_file_task` calls in
//! flight. Each `spawn_file_task` returns a `FileScan` synchronously
//! while `tokio::spawn`-ing a background `process_file` task that:
//!
//!   1. Builds a per-file batch stream (the only pipeline-specific
//!      step — an `ArrowReaderBuilder` for full-scan, a `StreamsInto`
//!      call for incremental — passed in as a `build_batch_stream`
//!      closure).
//!   2. Reads row groups sequentially → `RecordBatch` stream.
//!   3. Serializes each batch to Arrow IPC on Tokio's blocking thread
//!      pool.
//!   4. Sends serialized batches into the per-file mpsc channel,
//!      throttled by `byte_sem` (100 MB cap) and `slot_sem` (1 batch while
//!      the file is *waiting* → 8 once it becomes *active* via the FFI
//!      handoff in `IcebergFileScanResponse::set_payload`).
//!
//! This file hosts only the shared helpers (`create_nested_pipeline`,
//! `spawn_file_task`, `process_file`, `serialize_and_forward_batches`,
//! `make_file_stream`, `BufferedBatch`, `FileScan`). The per-pipeline
//! entry points live in sibling files:
//!
//!   * `full_pipeline.rs` — `create_full_scan_pipeline` (nested FFI) and
//!     `create_pipeline` (legacy flat-FFI wrapper that flattens the
//!     nested stream).
//!   * `incremental_pipeline.rs` — `create_incremental_nested_pipeline`,
//!     which composes the per-file `build_batch_stream` closure with
//!     glue for the separate delete-file stream.
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
//! serializing a batch, the task acquires clamped_byte_len permits. If the budget is
//! exhausted, the task yields (async, not blocking) until the consumer drains
//! batches and releases permits. This caps each file's buffered output to
//! ~100MB independently.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use futures::{Stream, StreamExt};
use iceberg::arrow::{ArrowReader, ArrowReaderBuilder};
use iceberg::io::FileIO;
use tokio::sync::{mpsc, Mutex as AsyncMutex, Semaphore};

use crate::pipeline_stats::{MAX_BUFFERED_BYTES_PER_TASK, STATS};
use crate::table::{ArrowBatch, IcebergArrowStream, IcebergFileScanStream};
use crate::unexpected;

/// Build an `ArrowReader` from a `FileIO` and batch size, with per-file
/// concurrency pinned to 1 (each reader handles one file). Shared by the
/// full-scan and incremental-append pipelines.
pub(crate) fn build_reader(file_io: FileIO, batch_size: usize) -> ArrowReader {
    ArrowReaderBuilder::new(file_io)
        .with_data_file_concurrency_limit(1)
        .with_batch_size(batch_size)
        .build()
}

/// Time an async expression and record its duration into a STATS field.
///
/// ```ignore
/// let result = timed!(serialize_ns, tokio::task::spawn_blocking(move || work()))
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

/// An Arrow C Data Interface batch bundled with the *clamped* in-memory byte
/// size used for backpressure accounting and references to the originating
/// file task's two semaphores. The consumer releases permits after forwarding
/// the batch to Julia, unblocking the producer: `byte_sem` adds back
/// `clamped_byte_len` bytes; `slot_sem` adds back 1 batch slot.
pub(crate) struct BufferedBatch {
    pub(crate) batch: ArrowBatch,
    pub(crate) clamped_byte_len: usize,
    pub(crate) byte_len: usize,
    pub(crate) byte_sem: Arc<Semaphore>,
    pub(crate) slot_sem: Arc<Semaphore>,
}

/// Initial batch-slot budget for a *waiting* file — one whose `FileScan`
/// has been produced by the outer pipeline but not yet picked up by
/// `iceberg_next_file_scan`. The producer can fill at most this many
/// batches before parking on `slot_sem.acquire()`. Promoted to
/// `MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE` at the FFI handoff site (see
/// `IcebergFileScanResponse::set_payload`).
pub(crate) const MAX_PREFETCH_BUFFERS_OF_WAITING_FILE: usize = 1;
/// Full batch-slot budget for an *active* file — one whose `FileScan` has
/// been handed off to a Julia consumer. Matches the per-file mpsc capacity,
/// so once active the slot semaphore stops being the binding constraint
/// and the existing 100 MB byte budget is what throttles.
pub(crate) const MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE: usize = 8;

/// Internal per-file scan result: filename, record count, prefetched inner
/// batch stream, and the per-file slot semaphore. The slot semaphore is
/// carried through the outer channel so the FFI handoff site can promote
/// it from `MAX_PREFETCH_BUFFERS_OF_WAITING_FILE` to
/// `MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE` with one `add_permits` call —
/// no separate plumbing through the orchestrator.
pub struct FileScan {
    pub filename: String,
    pub record_count: i64,
    pub stream: IcebergArrowStream,
    pub(crate) slot_sem: Arc<Semaphore>,
}

/// Handoff shape between a per-pipeline planner adapter and the orchestrator.
/// Carries the three pieces `spawn_file_task` needs: the filename to surface
/// at the FFI, the record-count hint, and the deferred batch-stream builder
/// (called on the spawned `process_file` task). Generic over `S` because the
/// full-scan and incremental-append pipelines return different concrete
/// `Stream<Item = iceberg::Result<RecordBatch>>` types.
pub(crate) struct FileToScan<S> {
    pub(crate) filename: String,
    pub(crate) record_count: i64,
    pub(crate) build_batch_stream: Box<dyn FnOnce() -> iceberg::Result<S> + Send>,
}

/// Convert a per-file mpsc receiver into an `IcebergArrowStream`. On each
/// batch pull it releases the originating producer's `byte_sem` and
/// `slot_sem` permits (unblocking the producer) and updates the
/// `buffered_bytes` running total in `STATS`.
///
/// Also records two consumer-side metrics:
/// - `consumer_batch_wait_ns`: time the consumer spent inside `rx.recv().await`.
///   This is the symmetric counterpart of `producer_stall_ns` (the producer
///   waiting on the *same* per-file mpsc when it's full): high ⇒ producer-
///   bound, low ⇒ Julia/consumer-bound at the per-batch level.
/// - `pipeline_end_ns`: stamped via `record_file_drained()` when `recv()`
///   returns `None`, i.e. when this file's `process_file` task dropped its
///   `tx`. Used to compute the true pipeline wall clock in `print_summary`.
pub(crate) fn make_file_stream(
    file_rx: mpsc::Receiver<Result<BufferedBatch, iceberg::Error>>,
) -> IcebergArrowStream {
    let stream = futures::stream::unfold(file_rx, |mut rx| async move {
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
                    // Release `clamped_byte_len` permits back to `byte_sem`
                    // (must match what the producer acquired) but report the
                    // *unclamped* batch size to `track_buffer_release` —
                    // `STATS.buffered_bytes` measures real in-flight RAM,
                    // not semaphore permits.
                    buf.byte_sem.add_permits(buf.clamped_byte_len);
                    buf.slot_sem.add_permits(1);
                    STATS.track_buffer_release(buf.byte_len as u64);
                    buf.batch
                });
                Some((result, rx))
            }
        }
    })
    .boxed();
    IcebergArrowStream {
        stream: AsyncMutex::new(stream),
    }
}

/// Build a nested per-file pipeline as a `Stream<FileScan>` driven by
/// `Stream::buffered(prefetch_depth)` over a stream of `FileToScan` handoff
/// values. Used by both the full-scan and the incremental-append entry
/// points; the only difference between the two is how their planner stream
/// is adapted into `FileToScan` (and what concrete batch-stream type `S`
/// the `build_batch_stream` closure returns).
///
/// See the module-level "Concurrency invariant" doc for the bound on alive
/// `process_file` tasks.
pub(crate) async fn create_nested_pipeline<S, Src>(
    source: Src,
    prefetch_depth: usize,
) -> IcebergFileScanStream
where
    S: Stream<Item = iceberg::Result<RecordBatch>> + Send + Unpin + 'static,
    Src: Stream<Item = iceberg::Result<FileToScan<S>>> + Send + 'static,
{
    let prefetch_depth = prefetch_depth.max(1);

    // Single reset site for both pipelines. `pipeline_start_ns` is captured
    // inside `reset()` so wall-time accounting starts here.
    STATS.reset();

    // Each inner future does the sync `spawn_file_task` (which itself
    // `tokio::spawn`s the `process_file` task) *when polled*, so `buffered`'s
    // cap (prefetch_depth) actually bounds how many `process_file` tasks are
    // kicked off ahead of the consumer. Doing the spawn in `.map` directly
    // would fire it as soon as the source is polled, bypassing the bound.
    let buffered = source
        .map(|res| async move {
            res.map(|f| spawn_file_task(f.filename, f.record_count, f.build_batch_stream))
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
/// only); real I/O failures surface inside `process_file` and are forwarded
/// as `Err(...)` items on the per-file mpsc.
///
/// `build_batch_stream` is invoked once on the spawned task and is the
/// only per-pipeline difference between full and incremental:
///   * full (`full_pipeline.rs`) wraps `read_one_full_scan_file(reader, task)`.
///   * incremental (`incremental_pipeline.rs`) wraps
///     `read_one_append_file(reader, task)`.
///
/// The reader-setup time is timed centrally inside `process_file`.
pub(crate) fn spawn_file_task<S, BSF>(
    filename: String,
    record_count: i64,
    build_batch_stream: BSF,
) -> FileScan
where
    BSF: FnOnce() -> iceberg::Result<S> + Send + 'static,
    S: Stream<Item = iceberg::Result<RecordBatch>> + Send + Unpin + 'static,
{
    let byte_sem = Arc::new(Semaphore::new(MAX_BUFFERED_BYTES_PER_TASK));
    // Start at the waiting-file prefetch floor; the FFI handoff site
    // (`IcebergFileScanResponse::set_payload`) tops up to the active-file
    // budget.
    let slot_sem = Arc::new(Semaphore::new(MAX_PREFETCH_BUFFERS_OF_WAITING_FILE));
    let (file_tx, file_rx) = mpsc::channel(MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE);

    tokio::spawn(process_file(
        build_batch_stream,
        byte_sem,
        MAX_BUFFERED_BYTES_PER_TASK,
        slot_sem.clone(),
        file_tx,
    ));

    FileScan {
        filename,
        record_count,
        stream: make_file_stream(file_rx),
        slot_sem,
    }
}

/// Wrapper around `process_file_inner` that ensures errors are sent to the
/// channel and stats are updated even on failure. When this function
/// returns, `tx` is dropped, causing the consumer's `file_rx.recv()` to
/// return None — signaling that this file is done.
async fn process_file<S, BSF>(
    build_batch_stream: BSF,
    byte_sem: Arc<Semaphore>,
    byte_budget: usize,
    slot_sem: Arc<Semaphore>,
    tx: mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) where
    BSF: FnOnce() -> iceberg::Result<S>,
    S: Stream<Item = iceberg::Result<RecordBatch>> + Unpin,
{
    STATS.track_task_start();
    let result =
        process_file_inner(build_batch_stream, &byte_sem, byte_budget, &slot_sem, &tx).await;
    if let Err(e) = result {
        let _ = tx.send(Err(e)).await;
    }
    STATS.track_task_end();
}

/// Process a single parquet file through four timed phases:
///   1. Reader setup — invoke `build_batch_stream`, which opens the file
///      and returns a stream of `RecordBatch`es.
///   2. Fetch + decode — I/O, ZSTD decompression, column assembly.
///   3. Export — RecordBatch → Arrow C Data Interface for transfer to Julia.
///   4. Backpressure — wait on byte_sem / slot_sem if buffered too far
///      ahead.
///
/// Each phase's cumulative time is recorded in `STATS` for the summary.
/// The `build_batch_stream` closure is the only per-pipeline difference
/// between full-scan and incremental-append; see `spawn_file_task` for
/// the two flavours.
async fn process_file_inner<S, BSF>(
    build_batch_stream: BSF,
    byte_sem: &Arc<Semaphore>,
    byte_budget: usize,
    slot_sem: &Arc<Semaphore>,
    tx: &mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) -> Result<(), iceberg::Error>
where
    BSF: FnOnce() -> iceberg::Result<S>,
    S: Stream<Item = iceberg::Result<RecordBatch>> + Unpin,
{
    let setup_start = Instant::now();
    let batch_stream = build_batch_stream()?;
    STATS.add_elapsed(&STATS.reader_setup_ns, setup_start);

    serialize_and_forward_batches(batch_stream, byte_sem, slot_sem, byte_budget, tx).await
}

/// Take a per-file `RecordBatch` stream and forward each batch — exported
/// via Arrow C Data Interface — into the file's mpsc, throttled by `byte_sem` / `slot_sem`.
/// Phases 2-4 of the file pipeline; recording timing and throughput into STATS.
/// Shared by both the full-scan and incremental-scan pipelines — the only
/// difference between the two is how the input stream is obtained before
/// calling this function.
///
/// `byte_budget` is the total number of permits initially in `byte_sem`. Each
/// per-batch byte cost is clamped to this value before acquiring, so a single
/// batch larger than the budget does not deadlock waiting for permits that
/// will never exist.
///
/// Batches within a single file are decoded strictly sequentially (one
/// `RecordBatch` per `batch_stream.next().await`); concurrency lives at the
/// file level via `Stream::buffered(prefetch_depth)` in `create_nested_pipeline`.
///
///   2. Fetch + decode — timed; each `.next()` does I/O + decompression
///   3. C FFI export   — timed; inline O(columns) Arrow C Data Interface wrap
///   4. Backpressure   — timed; semaphore blocks if producer is too far ahead
pub(crate) async fn serialize_and_forward_batches(
    mut batch_stream: impl Stream<Item = iceberg::Result<RecordBatch>> + Unpin,
    byte_sem: &Arc<Semaphore>,
    slot_sem: &Arc<Semaphore>,
    byte_budget: usize,
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

        // ── Phase 3: Export to Arrow C Data Interface ───────────────────
        // C Data Interface export is O(num_columns): just Arc-clones and a
        // few small heap allocations. No serialization or blocking thread
        // needed — runs inline on the Tokio worker.
        let byte_len = batch.get_array_memory_size();
        let export_start = std::time::Instant::now();
        let serialized = crate::record_batch_to_c_ffi(batch).map_err(unexpected)?;
        STATS.add_elapsed(&STATS.serialize_ns, export_start);

        STATS.batches_produced.fetch_add(1, Ordering::Relaxed);
        STATS
            .bytes_produced
            .fetch_add(byte_len as u64, Ordering::Relaxed);
        // Clamp the per-batch cost used for backpressure to the byte budget.
        // `acquire_many` for a >budget batch would never resolve (the
        // semaphore only has `byte_budget` permits), and clamping also
        // protects the `as u32` cast below from wrapping at ≥ 4 GiB.
        // Real throughput accounting (`bytes_produced`, above) sees the
        // unclamped value.
        let clamped_byte_len = byte_len.min(byte_budget);

        // ── Phase 4: Backpressure ───────────────────────────────────────
        // Producer is throttled by three per-file bounds, all funneling
        // into `producer_stall_ns` because they share one root cause
        // ("consumer hasn't drained enough"):
        //   (a) byte budget — `byte_sem.acquire_many(clamped_byte_len)`
        //       blocks if this file's outstanding buffered bytes exceed
        //       MAX_BUFFERED_BYTES_PER_TASK (100 MB).
        //   (b) batch-slot budget — `slot_sem.acquire()` blocks if the file
        //       has hit its current batch-count cap. Needed *in addition to*
        //       the byte budget so a still-waiting file (Julia hasn't picked
        //       it up yet) doesn't fetch too far into the future (we want to
        //       prioritize prefetching of buffers on active files!):
        //       cap is MAX_PREFETCH_BUFFERS_OF_WAITING_FILE while waiting,
        //       MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE once active.
        //   (c) channel-capacity safety — `tx.send(...)` blocks if the
        //       mpsc is full. With slot_sem ≤ MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE
        //       = mpsc capacity, this is a redundant safety net.
        //
        // Each `.acquire()` is wrapped in a `tokio::select!` with `tx.closed()`,
        // so a producer parked on a semaphore wakes immediately when the
        // consumer drops the receiver and exits cleanly. Without this, a
        // waiting-file producer parked on `slot_sem.acquire()` would hang
        // forever — its permits are only released by consumer `recv()`,
        // which won't happen anymore — and the spawned task would leak.
        let byte_acquire_start = Instant::now();
        let _byte_permit = tokio::select! {
            permit = byte_sem.acquire_many(clamped_byte_len as u32) => {
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

        // Track the *actual* in-memory byte size, not the clamped permit count.
        // The clamp only affects what we acquire from `byte_sem`.
        STATS.track_buffer_add(byte_len as u64);

        // Send the batch to this file's channel.
        let send_result = timed!(
            producer_stall_ns,
            tx.send(Ok(BufferedBatch {
                batch: serialized,
                clamped_byte_len,
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

/// Serialise pipeline-level tests so they don't race on the process-global
/// `STATS` (e.g. `peak_concurrency`). Tests that drive a real pipeline
/// (in this file or in `full_pipeline.rs`) acquire this lock for their
/// duration.
#[cfg(test)]
pub(crate) static PIPELINE_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    // ── Orchestration tests (empty-batch-stream builder — no real I/O) ──
    //
    // These tests exercise `create_nested_pipeline`'s plumbing
    // (`Stream::buffered` semantics, error propagation, source order) by
    // feeding a synthetic source of `FileToScan` values whose
    // `build_batch_stream` returns an empty Arrow stream. Each spawned
    // `process_file` task drops its `tx` immediately, so the per-file
    // inner stream closes without any I/O — what we're checking is the
    // outer-stream plumbing.

    /// Build a `FileToScan` whose batch-stream builder yields an empty
    /// stream — `process_file` drops its `tx` immediately, closing the
    /// per-file inner stream without any real I/O.
    fn fake_file_to_scan(
        filename: String,
        record_count: i64,
    ) -> FileToScan<futures::stream::Empty<iceberg::Result<RecordBatch>>> {
        FileToScan {
            filename,
            record_count,
            build_batch_stream: Box::new(|| Ok(futures::stream::empty())),
        }
    }

    /// Drive `create_nested_pipeline` over a synthetic source and collect
    /// the yielded `Result<(filename, record_count), _>` pairs. Holds
    /// `PIPELINE_TEST_LOCK` for the duration so concurrent test workers
    /// don't race on the global `STATS` (which `create_nested_pipeline`
    /// resets at start).
    async fn collect_filescans(
        items: Vec<iceberg::Result<(String, i64)>>,
        prefetch_depth: usize,
    ) -> Vec<iceberg::Result<(String, i64)>> {
        let _guard = PIPELINE_TEST_LOCK.lock().await;
        let source = futures::stream::iter(items)
            .map_ok(|(name, rc): (String, i64)| fake_file_to_scan(name, rc));
        let nested = create_nested_pipeline(source, prefetch_depth).await;
        let mut out = vec![];
        let mut stream = nested.stream.lock().await;
        while let Some(item) = stream.next().await {
            out.push(item.map(|fs| (fs.filename, fs.record_count)));
        }
        out
    }

    #[tokio::test]
    async fn empty_source_yields_nothing() {
        assert!(collect_filescans(vec![], 4).await.is_empty());
    }

    #[tokio::test]
    async fn single_file_forwarded() {
        let got = collect_filescans(vec![Ok(("a.parquet".into(), 42))], 1).await;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].as_ref().unwrap(), &("a.parquet".to_string(), 42));
    }

    #[tokio::test]
    async fn files_yielded_in_source_order() {
        // `Stream::buffered` preserves source order even at high
        // prefetch_depth (cf. `buffer_unordered`).
        let items = vec![
            Ok(("a.parquet".into(), 10)),
            Ok(("b.parquet".into(), 20)),
            Ok(("c.parquet".into(), 30)),
        ];
        let pairs: Vec<(String, i64)> = collect_filescans(items, 8)
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
    async fn all_files_present_with_high_prefetch_depth() {
        let items: Vec<iceberg::Result<(String, i64)>> = (0..5)
            .map(|i| Ok((format!("f{i}.parquet"), i as i64)))
            .collect();
        let got = collect_filescans(items, 8).await;
        assert_eq!(got.len(), 5);
        for (i, item) in got.iter().enumerate() {
            assert_eq!(item.as_ref().unwrap(), &(format!("f{i}.parquet"), i as i64));
        }
    }

    #[tokio::test]
    async fn source_error_propagates_in_order() {
        // An `Err` item from the source is forwarded at the position it
        // appears, with no special seeding-vs-drain distinction.
        let items = vec![Err(iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "source error",
        ))];
        let got = collect_filescans(items, 1).await;
        assert_eq!(got.len(), 1);
        let err = got.into_iter().next().unwrap().err().unwrap();
        assert!(err.to_string().contains("source error"));
    }

    #[tokio::test]
    async fn source_error_after_ok_yields_ok_then_err() {
        let items = vec![
            Ok(("a.parquet".into(), 1)),
            Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "drain error",
            )),
        ];
        let got = collect_filescans(items, 1).await;
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].as_ref().unwrap(), &("a.parquet".to_string(), 1));
        assert!(got[1]
            .as_ref()
            .err()
            .unwrap()
            .to_string()
            .contains("drain error"));
    }

    /// If a per-pipeline `build_batch_stream` closure returns `Err` (e.g.
    /// the underlying reader failed to open the file), `process_file`
    /// must surface that error on the per-file inner stream rather than
    /// panicking or hanging. Then the inner stream must close cleanly.
    #[tokio::test]
    async fn build_batch_stream_err_propagates_to_inner_stream() {
        let _guard = PIPELINE_TEST_LOCK.lock().await;
        // Use spawn_file_task directly so we can supply a build_batch_stream
        // closure that fails. The closure has the same signature as the one
        // full_pipeline / incremental_pipeline pass at the real call sites.
        let fs = spawn_file_task::<futures::stream::Empty<iceberg::Result<RecordBatch>>, _>(
            "bad.parquet".into(),
            0,
            || {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "reader setup failed",
                ))
            },
        );
        let mut inner = fs.stream.stream.lock().await;
        // First item: the error from the builder closure, forwarded by
        // process_file as Err(...) on the per-file mpsc.
        let first = inner.next().await.expect("expected one error item");
        let err = first.err().expect("expected Err");
        assert!(
            err.to_string().contains("reader setup failed"),
            "unexpected error message: {err}"
        );
        // Then the inner stream closes (process_file's tx is dropped).
        assert!(
            inner.next().await.is_none(),
            "stream did not close after Err"
        );
    }

    #[tokio::test]
    async fn consumer_dropping_stream_does_not_panic() {
        let _guard = PIPELINE_TEST_LOCK.lock().await;
        // If the consumer drops the outer stream before draining, the
        // pipeline must unwind cleanly. With `Stream::buffered`, dropping
        // the outer stream drops the inner-future queue, which in turn
        // drops each (already-resolved) FileScan and closes its inner mpsc.
        let items: Vec<iceberg::Result<(String, i64)>> = (0..20)
            .map(|i| Ok((format!("f{i}.parquet"), i as i64)))
            .collect();
        let source = futures::stream::iter(items)
            .map_ok(|(name, rc): (String, i64)| fake_file_to_scan(name, rc));
        let nested = create_nested_pipeline(source, 4).await;
        drop(nested);
    }

    #[tokio::test]
    async fn serialize_and_forward_batches_wakes_when_consumer_drops_receiver() {
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
            serialize_and_forward_batches(
                batches,
                &byte_sem_for_drain,
                &slot_sem_for_drain,
                MAX_BUFFERED_BYTES_PER_TASK,
                &tx,
            )
            .await
        });

        // Wait for batch 1 to reach the mpsc (proves the producer ran past
        // its first slot_sem.acquire and is now parked on the second).
        let _batch1 = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .expect("producer did not deliver batch 1 within timeout")
            .expect("mpsc closed unexpectedly")
            .expect("batch 1 was an Err");
        assert_eq!(
            slot_sem.available_permits(),
            0,
            "slot_sem should be drained"
        );

        // Drop rx → tx.closed() fires → the cancellation arm in
        // serialize_and_forward_batches wakes the parked producer.
        drop(rx);

        // Without the cancellation fix, this would never complete.
        let result = tokio::time::timeout(std::time::Duration::from_millis(500), drain)
            .await
            .expect(
                "serialize_and_forward_batches did not exit after consumer dropped rx (parked on slot_sem)",
            )
            .expect("drain task panicked");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn oversized_batch_does_not_deadlock_byte_sem() {
        // Regression test: a single batch whose serialized size exceeds the
        // per-task byte budget must not deadlock on `byte_sem.acquire_many`.
        // Pre-fix, `acquire_many(byte_len)` for a >budget batch could never
        // resolve (the semaphore only has `byte_budget` permits) and the
        // producer hung. The clamp `clamped_byte_len = serialized.length.min(byte_budget)`
        // makes the acquire fit, and the symmetric `add_permits` in
        // `make_file_stream` keeps the running budget consistent.
        //
        // We use a tiny byte_budget (smaller than the IPC overhead of a
        // 3-row batch, ~~ a few hundred bytes) so we don't have to allocate
        // anything large in CI.
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use std::sync::Arc;

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .map_err(|e| unexpected(e.to_string()));
        let batches = futures::stream::iter(vec![batch]);

        let byte_budget = 8usize; // far smaller than the IPC-serialized 3-row batch
        let byte_sem = Arc::new(Semaphore::new(byte_budget));
        let slot_sem = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel::<Result<BufferedBatch, iceberg::Error>>(8);

        let byte_sem_for_drain = byte_sem.clone();
        let slot_sem_for_drain = slot_sem.clone();
        let drain = tokio::spawn(async move {
            serialize_and_forward_batches(
                batches,
                &byte_sem_for_drain,
                &slot_sem_for_drain,
                byte_budget,
                &tx,
            )
            .await
        });

        // Pre-fix this would hang forever inside `byte_sem.acquire_many`.
        let batch = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .expect("producer hung on byte_sem.acquire_many for oversized batch")
            .expect("mpsc closed unexpectedly")
            .expect("batch was an Err");
        assert_eq!(
            batch.clamped_byte_len, byte_budget,
            "byte_len should be clamped to the budget"
        );

        // Releasing permits via the consumer path returns exactly the clamped
        // amount; the byte_sem must end up back at full capacity.
        batch.byte_sem.add_permits(batch.clamped_byte_len);
        assert_eq!(byte_sem.available_permits(), byte_budget);

        // Free the IPC payload allocated by serialize_record_batch.
        unsafe { drop(Box::from_raw(batch.batch.rust_ptr as *mut Vec<u8>)) };

        // Drain finishes cleanly once the stream is exhausted.
        drop(rx);
        let result = tokio::time::timeout(std::time::Duration::from_millis(500), drain)
            .await
            .expect("drain did not exit")
            .expect("drain task panicked");
        assert!(result.is_ok());
    }
}
