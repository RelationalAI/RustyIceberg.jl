// ===========================================================================
// Pipeline statistics — timing and throughput counters for the file-parallel
// scan pipeline. Global atomics accumulate data across all file tasks and are
// readable from Julia via `@ccall iceberg_print_pipeline_stats()`.
// ===========================================================================

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::LazyLock;
use std::time::Instant;

pub(crate) const MAX_BUFFERED_BYTES_PER_TASK: usize = 100 * 1024 * 1024;

/// Process-monotonic origin used as a common reference point for the
/// `pipeline_start_ns` / `pipeline_end_ns` timestamps. We can't store
/// `Instant` directly in atomics, so each timestamp is encoded as
/// nanoseconds elapsed since this origin.
pub(crate) static PROCESS_START: LazyLock<Instant> = LazyLock::new(Instant::now);

#[inline]
pub(crate) fn nanos_since_process_start() -> u64 {
    PROCESS_START.elapsed().as_nanos() as u64
}

/// Accumulates profiling data across all file tasks in a pipeline run.
/// All fields are atomics so concurrent file tasks can update without locks.
pub(crate) struct PipelineStats {
    // ── Overall ──
    /// Nanos-since-`PROCESS_START` when `reset()` was called (= pipeline kicked
    /// off). 0 means "never started since last reset".
    pub pipeline_start_ns: AtomicU64,
    /// Nanos-since-`PROCESS_START` of the most recent per-file stream close
    /// (i.e. moment a `process_file` task's tx was dropped and the consumer's
    /// `recv()` returned `None`). The `print_summary` wall-clock formula is
    /// `pipeline_end_ns - pipeline_start_ns`. We monotonically `fetch_max` so
    /// the field always holds the latest end-of-file event.
    pub pipeline_end_ns: AtomicU64,
    pub files_completed: AtomicUsize,
    pub batches_produced: AtomicUsize,
    pub bytes_produced: AtomicU64,

    // ── Concurrency ──
    pub peak_concurrency: AtomicUsize,
    pub active_tasks: AtomicUsize,

    // ── Per-phase cumulative time (ns), summed across all file tasks ──
    /// Time to build ArrowReader and call reader.read() (opens parquet,
    /// loads metadata, resolves schema, loads delete files, builds filters).
    pub reader_setup_ns: AtomicU64,
    /// Time inside batch_stream.next() — fetches compressed pages from
    /// storage, decompresses (ZSTD), decodes pages, assembles columns.
    pub fetch_decode_ns: AtomicU64,
    /// Time in spawn_blocking(serialize_record_batch) — writes RecordBatch
    /// to Arrow IPC wire format for transfer to Julia.
    pub serialize_ns: AtomicU64,
    /// Time a producer (`drain_batch_stream`) is suspended on per-file
    /// backpressure — either the per-file 100 MB byte semaphore
    /// (`semaphore.acquire_many`) or the per-file mpsc(8) being full
    /// (`tx.send`). Both stall the same producer for the same reason
    /// ("consumer hasn't drained enough"), so they're combined.
    pub producer_stall_ns: AtomicU64,

    // ── Producer waits on consumer (Rust ahead, Julia behind) ──
    /// Time `run_nested` spends in `tx.send(FileScan)` to the outer
    /// mpsc(prefetch_depth) when it's already full — i.e. Rust has pre-built
    /// FileScans faster than Julia is calling `next_file_scan` to consume
    /// them. Producer-side stall at the file-handoff level. Distinct from
    /// `producer_stall_ns`: that one is per-file (per-batch buffer full
    /// inside one file's mpsc(8)/100MB); this is per-table (the global
    /// outer queue connecting Rust to Julia).
    pub outer_queue_full_wait_ns: AtomicU64,

    // ── Consumer waits on producer (Julia ahead, Rust behind) ──
    /// Time spent inside `make_file_stream`'s unfold on `rx.recv().await`
    /// for the per-file mpsc — Julia asked for the next batch but the
    /// producer hasn't produced one yet. Summed across all per-file streams.
    /// One counterpart of `producer_stall_ns` viewed from the consumer.
    pub consumer_batch_wait_ns: AtomicU64,
    /// Time spent in the outer `IcebergFileScanStream`'s unfold on
    /// `rx.recv().await` — Julia called `next_file_scan` but the outer
    /// mpsc is empty (Rust hasn't pushed the next FileScan yet). One
    /// counterpart of `outer_queue_full_wait_ns` viewed from the consumer.
    pub consumer_file_wait_ns: AtomicU64,

    // ── Memory ──
    /// Live counter of serialized bytes buffered across all file tasks.
    pub buffered_bytes: AtomicU64,
    /// High-water mark of buffered_bytes.
    pub peak_buffered_bytes: AtomicU64,
}

impl PipelineStats {
    pub(crate) const fn new() -> Self {
        Self {
            pipeline_start_ns: AtomicU64::new(0),
            pipeline_end_ns: AtomicU64::new(0),
            files_completed: AtomicUsize::new(0),
            batches_produced: AtomicUsize::new(0),
            bytes_produced: AtomicU64::new(0),
            peak_concurrency: AtomicUsize::new(0),
            active_tasks: AtomicUsize::new(0),
            reader_setup_ns: AtomicU64::new(0),
            fetch_decode_ns: AtomicU64::new(0),
            serialize_ns: AtomicU64::new(0),
            producer_stall_ns: AtomicU64::new(0),
            outer_queue_full_wait_ns: AtomicU64::new(0),
            consumer_batch_wait_ns: AtomicU64::new(0),
            consumer_file_wait_ns: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            peak_buffered_bytes: AtomicU64::new(0),
        }
    }

    /// Reset all counters and stamp `pipeline_start_ns` with "now". Called
    /// from `create_nested_pipeline` (and the flat wrapper) before the
    /// pipeline kicks off.
    pub(crate) fn reset(&self) {
        self.pipeline_start_ns
            .store(nanos_since_process_start(), Ordering::Relaxed);
        self.pipeline_end_ns.store(0, Ordering::Relaxed);
        self.files_completed.store(0, Ordering::Relaxed);
        self.batches_produced.store(0, Ordering::Relaxed);
        self.bytes_produced.store(0, Ordering::Relaxed);
        self.peak_concurrency.store(0, Ordering::Relaxed);
        self.active_tasks.store(0, Ordering::Relaxed);
        self.reader_setup_ns.store(0, Ordering::Relaxed);
        self.fetch_decode_ns.store(0, Ordering::Relaxed);
        self.serialize_ns.store(0, Ordering::Relaxed);
        self.producer_stall_ns.store(0, Ordering::Relaxed);
        self.outer_queue_full_wait_ns.store(0, Ordering::Relaxed);
        self.consumer_batch_wait_ns.store(0, Ordering::Relaxed);
        self.consumer_file_wait_ns.store(0, Ordering::Relaxed);
        self.buffered_bytes.store(0, Ordering::Relaxed);
        self.peak_buffered_bytes.store(0, Ordering::Relaxed);
    }

    /// Mark the pipeline-end timestamp. Called from `make_file_stream` when a
    /// per-file `recv()` returns `None` — i.e. that file's producer has
    /// dropped its `tx`. We `fetch_max` so the field ends up holding the
    /// timestamp of the last file to drain.
    pub(crate) fn record_file_drained(&self) {
        let now = nanos_since_process_start();
        self.pipeline_end_ns.fetch_max(now, Ordering::Relaxed);
    }

    pub(crate) fn track_task_start(&self) {
        let prev = self.active_tasks.fetch_add(1, Ordering::Relaxed);
        self.peak_concurrency.fetch_max(prev + 1, Ordering::Relaxed);
    }

    pub(crate) fn track_task_end(&self) {
        self.active_tasks.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn add_elapsed(&self, field: &AtomicU64, start: Instant) {
        field.fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    pub(crate) fn track_buffer_add(&self, bytes: u64) {
        let prev = self.buffered_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.peak_buffered_bytes
            .fetch_max(prev + bytes, Ordering::Relaxed);
    }

    /// Release buffered bytes. Uses saturating_sub to guard against underflow
    /// caused by lingering tasks from a previous scan releasing after a reset().
    pub(crate) fn track_buffer_release(&self, bytes: u64) {
        self.buffered_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(bytes))
            })
            .ok();
    }

    pub(crate) fn print_summary(&self) {
        // Just delegate to format_summary so we can unit-test the rendering.
        print!("{}", self.format_summary());
    }

    pub(crate) fn format_summary(&self) -> String {
        // ── Wall time ────────────────────────────────────────────────────
        // `start_ns` is set in reset() at pipeline kickoff. `end_ns` is set
        // by `record_file_drained` each time a per-file stream observes
        // `recv() == None` (i.e. its producer dropped tx). We take the max
        // of those file-end timestamps; the pipeline is done when the last
        // file is done.
        //
        // Fallback: if `end_ns` is 0 (e.g. print called before any file
        // drained, or stats reset but pipeline never ran), fall back to
        // "now" so the wall is at least monotone-non-negative.
        let start_ns = self.pipeline_start_ns.load(Ordering::Relaxed);
        let end_ns_raw = self.pipeline_end_ns.load(Ordering::Relaxed);
        let end_ns = if end_ns_raw == 0 {
            nanos_since_process_start()
        } else {
            end_ns_raw
        };
        let wall_ns = end_ns.saturating_sub(start_ns);
        let wall_ms = wall_ns as f64 / 1e6;

        let files = self.files_completed.load(Ordering::Relaxed);
        let batches = self.batches_produced.load(Ordering::Relaxed);
        let bytes = self.bytes_produced.load(Ordering::Relaxed);
        let peak = self.peak_concurrency.load(Ordering::Relaxed);
        let setup_ms = self.reader_setup_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let fd_ms = self.fetch_decode_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let ser_ms = self.serialize_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let producer_stall_ms = self.producer_stall_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let outer_full_ms = self.outer_queue_full_wait_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let consumer_batch_ms = self.consumer_batch_wait_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let consumer_file_ms = self.consumer_file_wait_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let peak_buf = self.peak_buffered_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);

        let bytes_mb = bytes as f64 / (1024.0 * 1024.0);
        let throughput = if wall_ms > 0.0 {
            bytes_mb / (wall_ms / 1000.0)
        } else {
            0.0
        };
        // Effective parallelism = (CPU work summed across producers) / wall.
        // We exclude the wait-times (producer_stall, consumer_wait) because
        // those don't represent CPU consumption — a stalled producer isn't
        // holding a Tokio thread, and consumer_wait is a consumer-side
        // metric. Including them would conflate "we did CPU work in
        // parallel" with "we sat idle in parallel".
        let cpu_work_ms = setup_ms + fd_ms + ser_ms;
        let parallelism = if wall_ms > 0.0 { cpu_work_ms / wall_ms } else { 0.0 };
        let limit_mb = MAX_BUFFERED_BYTES_PER_TASK / (1024 * 1024);

        // Per-batch averages help distinguish "more parallel producers" from
        // "each batch got slower". Sum-across-producers lines double when
        // concurrency doubles even if per-batch wall stays flat; the avg
        // controls for that. Standard 4-space gap before the parenthetical
        // matches the rest of the rows (e.g. "reader setup:").
        let avg_per_batch = |sum_ms: f64| -> Option<f64> {
            if batches == 0 {
                None
            } else {
                Some(sum_ms / batches as f64)
            }
        };
        let per_batch_only = |sum_ms: f64| -> String {
            avg_per_batch(sum_ms)
                .map(|avg| format!("    (avg {:.2} ms/batch)", avg))
                .unwrap_or_default()
        };
        let per_batch_with_note = |sum_ms: f64, note: &str| -> String {
            match avg_per_batch(sum_ms) {
                Some(avg) => format!("    ({}, avg {:.2} ms/batch)", note, avg),
                None => format!("    ({})", note),
            }
        };
        let per_file_with_note = |sum_ms: f64, note: &str| -> String {
            if files == 0 {
                format!("    ({})", note)
            } else {
                format!(
                    "    ({}, avg {:.2} ms/file)",
                    note,
                    sum_ms / files as f64
                )
            }
        };

        // Build the box content as a list of (kind, text) where kind ∈
        // {Row, Sep, Blank}. We compute the box width from the widest line
        // *after* formatting, so adding new lines (or longer numbers) can't
        // misalign the borders. All text is ASCII, so byte len = display width.
        enum Line {
            Row(String),
            Sep(String),
            Blank,
        }
        let lines: Vec<Line> = vec![
            Line::Row("Pipeline Stats".to_string()),
            Line::Blank,
            Line::Row(format!("wall time:       {:>9.1} ms", wall_ms)),
            Line::Row(format!(
                "files:           {:>9}       peak in-flight: {}",
                files, peak
            )),
            Line::Row(format!(
                "batches:         {:>9}       shipped Arrow bytes: {:.1} MB",
                batches, bytes_mb
            )),
            Line::Row(format!(
                "throughput:      {:>9.1} MB/s  CPU-parallelism: {:.1}x",
                throughput, parallelism
            )),
            Line::Sep("producer-side time (sum across all file tasks)".to_string()),
            Line::Row(format!(
                "reader setup:    {:>9.1} ms    (open parquet, schema, deletes)",
                setup_ms
            )),
            Line::Row(format!("fetch+decode:    {:>9.1} ms{}", fd_ms, per_batch_only(fd_ms))),
            Line::Row(format!(
                "serialize IPC:   {:>9.1} ms{}",
                ser_ms,
                per_batch_only(ser_ms)
            )),
            Line::Row(format!(
                "batch send wait: {:>9.1} ms    (per-file buffer full: 100 MB or 8 batches)",
                producer_stall_ms
            )),
            Line::Row(format!(
                "file send wait:  {:>9.1} ms    (outer queue full, prefetched ahead of Julia)",
                outer_full_ms
            )),
            Line::Sep("consumer-side wait (Julia ahead, waiting on Rust)".to_string()),
            Line::Row(format!(
                "next batch wait: {:>9.1} ms{}",
                consumer_batch_ms,
                per_batch_with_note(consumer_batch_ms, "per-file mpsc empty")
            )),
            Line::Row(format!(
                "next file wait:  {:>9.1} ms{}",
                consumer_file_ms,
                per_file_with_note(consumer_file_ms, "outer mpsc empty")
            )),
            Line::Sep("memory".to_string()),
            Line::Row(format!(
                "peak buffered:   {:>9.1} MB    (sum over {} tasks, {} MB/task cap)",
                peak_buf, peak, limit_mb
            )),
        ];

        // Inner width = max content length.
        // - Row line uses "│  {content}  │" → needs `inner` chars between borders.
        // - Sep line uses "│  ── {label} {fill}  │" → needs at least
        //   `4 (── plus space) + label.len() + 1 (space) + 3 (min fill)` = label.len() + 8
        //   to look reasonable, plus the same 4 chars of side-padding.
        let row_w = lines.iter().filter_map(|l| match l {
            Line::Row(s) => Some(s.len()),
            _ => None,
        });
        let sep_w = lines.iter().filter_map(|l| match l {
            Line::Sep(s) => Some(s.len() + 8), // "── " + label + " ──"
            _ => None,
        });
        let inner: usize = row_w.chain(sep_w).max().unwrap_or(20);
        // Add a 4-char side padding ("│  " on the left, "  │" on the right).
        let total = inner + 6;

        let dashes = |n: usize| -> String { "─".repeat(n) };
        use std::fmt::Write as _;
        let mut out = String::new();
        let _ = writeln!(out, "┌{}┐", dashes(total - 2));
        for line in &lines {
            let content: &str = match line {
                Line::Row(s) => s,
                Line::Blank => "",
                Line::Sep(s) => {
                    // Layout: "│  ── {label} {fill}  │"; fill = inner - 4 - label.len().
                    let fill = inner.saturating_sub(s.len() + 4);
                    let _ = writeln!(
                        out,
                        "│  {} {} {}  │",
                        dashes(2),
                        s,
                        dashes(fill)
                    );
                    continue;
                }
            };
            let pad = inner.saturating_sub(content.len());
            let _ = writeln!(out, "│  {}{:pad$}  │", content, "", pad = pad);
        }
        let _ = writeln!(out, "└{}┘", dashes(total - 2));
        out
    }
}

pub(crate) static STATS: PipelineStats = PipelineStats::new();

// ── FFI exports (called from Julia benchmark teardown) ────────────────────

#[no_mangle]
pub extern "C" fn iceberg_print_pipeline_stats() {
    STATS.print_summary();
}

#[no_mangle]
pub extern "C" fn iceberg_reset_pipeline_stats() {
    STATS.reset();
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn fresh() -> PipelineStats {
        PipelineStats::new()
    }

    // ── reset ─────────────────────────────────────────────────────────────────

    #[test]
    fn reset_clears_every_field() {
        let s = fresh();
        // pipeline_start_ns is set by reset() (not zeroed), so we don't seed it.
        s.pipeline_end_ns.store(1, Ordering::Relaxed);
        s.files_completed.store(2, Ordering::Relaxed);
        s.batches_produced.store(3, Ordering::Relaxed);
        s.bytes_produced.store(4, Ordering::Relaxed);
        s.peak_concurrency.store(5, Ordering::Relaxed);
        s.active_tasks.store(6, Ordering::Relaxed);
        s.reader_setup_ns.store(7, Ordering::Relaxed);
        s.fetch_decode_ns.store(8, Ordering::Relaxed);
        s.serialize_ns.store(9, Ordering::Relaxed);
        s.producer_stall_ns.store(10, Ordering::Relaxed);
        s.outer_queue_full_wait_ns.store(11, Ordering::Relaxed);
        s.consumer_batch_wait_ns.store(12, Ordering::Relaxed);
        s.consumer_file_wait_ns.store(13, Ordering::Relaxed);
        s.buffered_bytes.store(14, Ordering::Relaxed);
        s.peak_buffered_bytes.store(15, Ordering::Relaxed);

        s.reset();

        // reset() stamps pipeline_start_ns with a non-zero value (now); we
        // only assert it's been touched.
        assert!(s.pipeline_start_ns.load(Ordering::Relaxed) > 0);
        assert_eq!(s.pipeline_end_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.files_completed.load(Ordering::Relaxed), 0);
        assert_eq!(s.batches_produced.load(Ordering::Relaxed), 0);
        assert_eq!(s.bytes_produced.load(Ordering::Relaxed), 0);
        assert_eq!(s.peak_concurrency.load(Ordering::Relaxed), 0);
        assert_eq!(s.active_tasks.load(Ordering::Relaxed), 0);
        assert_eq!(s.reader_setup_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.fetch_decode_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.serialize_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.producer_stall_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.outer_queue_full_wait_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.consumer_batch_wait_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.consumer_file_wait_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.buffered_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(s.peak_buffered_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn record_file_drained_advances_end_monotonically() {
        let s = fresh();
        // Force PROCESS_START to materialize so the timestamps are nonzero.
        let _ = nanos_since_process_start();
        s.record_file_drained();
        let first = s.pipeline_end_ns.load(Ordering::Relaxed);
        assert!(first > 0);
        std::thread::sleep(Duration::from_millis(2));
        s.record_file_drained();
        let second = s.pipeline_end_ns.load(Ordering::Relaxed);
        assert!(second > first);
    }

    #[test]
    fn record_file_drained_takes_max_not_last() {
        let s = fresh();
        let _ = nanos_since_process_start();
        // Pretend a "later" timestamp was already stored.
        let later = nanos_since_process_start() + 1_000_000_000; // +1s
        s.pipeline_end_ns.store(later, Ordering::Relaxed);
        s.record_file_drained(); // "now" is well before `later`
        // fetch_max preserves `later`.
        assert_eq!(s.pipeline_end_ns.load(Ordering::Relaxed), later);
    }

    // ── track_task_start / track_task_end ─────────────────────────────────────

    #[test]
    fn task_start_increments_active_and_updates_peak() {
        let s = fresh();
        s.track_task_start();
        assert_eq!(s.active_tasks.load(Ordering::Relaxed), 1);
        assert_eq!(s.peak_concurrency.load(Ordering::Relaxed), 1);

        s.track_task_start();
        assert_eq!(s.active_tasks.load(Ordering::Relaxed), 2);
        assert_eq!(s.peak_concurrency.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn task_end_decrements_active_without_touching_peak() {
        let s = fresh();
        s.track_task_start();
        s.track_task_start();
        s.track_task_end();
        assert_eq!(s.active_tasks.load(Ordering::Relaxed), 1);
        assert_eq!(s.peak_concurrency.load(Ordering::Relaxed), 2); // high-water mark unchanged
    }

    #[test]
    fn peak_concurrency_is_high_water_mark() {
        let s = fresh();
        s.track_task_start(); // active 1, peak 1
        s.track_task_start(); // active 2, peak 2
        s.track_task_start(); // active 3, peak 3
        s.track_task_end(); // active 2, peak 3
        s.track_task_end(); // active 1, peak 3
        s.track_task_end(); // active 0, peak 3
        s.track_task_start(); // active 1 — below previous peak, peak stays 3
        assert_eq!(s.active_tasks.load(Ordering::Relaxed), 1);
        assert_eq!(s.peak_concurrency.load(Ordering::Relaxed), 3);
    }

    // ── track_buffer_add / track_buffer_release ───────────────────────────────

    #[test]
    fn buffer_add_increments_bytes_and_peak() {
        let s = fresh();
        s.track_buffer_add(100);
        assert_eq!(s.buffered_bytes.load(Ordering::Relaxed), 100);
        assert_eq!(s.peak_buffered_bytes.load(Ordering::Relaxed), 100);

        s.track_buffer_add(50);
        assert_eq!(s.buffered_bytes.load(Ordering::Relaxed), 150);
        assert_eq!(s.peak_buffered_bytes.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn buffer_release_decrements_bytes_without_touching_peak() {
        let s = fresh();
        s.track_buffer_add(200);
        s.track_buffer_release(80);
        assert_eq!(s.buffered_bytes.load(Ordering::Relaxed), 120);
        assert_eq!(s.peak_buffered_bytes.load(Ordering::Relaxed), 200); // peak unchanged
    }

    #[test]
    fn buffer_peak_does_not_decrease_after_release_and_smaller_add() {
        let s = fresh();
        s.track_buffer_add(500); // peak → 500
        s.track_buffer_release(500); // current → 0
        s.track_buffer_add(10); // current → 10, peak stays 500
        assert_eq!(s.buffered_bytes.load(Ordering::Relaxed), 10);
        assert_eq!(s.peak_buffered_bytes.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn buffer_release_saturates_at_zero_instead_of_wrapping() {
        let s = fresh();
        // Simulate a release arriving after a stats reset (lingering task).
        s.track_buffer_release(100); // would have wrapped to u64::MAX without saturating_sub
        assert_eq!(s.buffered_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(s.peak_buffered_bytes.load(Ordering::Relaxed), 0);
    }

    // ── add_elapsed ───────────────────────────────────────────────────────────

    #[test]
    fn add_elapsed_accumulates_into_field() {
        let s = fresh();
        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(2));
        s.add_elapsed(&s.reader_setup_ns, start);
        let after_first = s.reader_setup_ns.load(Ordering::Relaxed);
        assert!(after_first > 0, "first add_elapsed should record > 0 ns");

        let start2 = Instant::now();
        std::thread::sleep(Duration::from_millis(2));
        s.add_elapsed(&s.reader_setup_ns, start2);
        let after_second = s.reader_setup_ns.load(Ordering::Relaxed);
        assert!(
            after_second > after_first,
            "second add_elapsed should accumulate"
        );
    }

    #[test]
    fn add_elapsed_is_independent_per_field() {
        let s = fresh();
        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(2));
        s.add_elapsed(&s.fetch_decode_ns, start);

        // Unrelated fields stay at 0.
        assert_eq!(s.serialize_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.producer_stall_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.reader_setup_ns.load(Ordering::Relaxed), 0);
        assert!(s.fetch_decode_ns.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn outer_queue_full_wait_accumulates_via_add_elapsed() {
        let s = fresh();
        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(2));
        s.add_elapsed(&s.outer_queue_full_wait_ns, start);
        assert!(s.outer_queue_full_wait_ns.load(Ordering::Relaxed) > 0);
        // Other fields are unaffected.
        assert_eq!(s.producer_stall_ns.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn consumer_batch_wait_accumulates_via_add_elapsed() {
        let s = fresh();
        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(2));
        s.add_elapsed(&s.consumer_batch_wait_ns, start);
        assert!(s.consumer_batch_wait_ns.load(Ordering::Relaxed) > 0);
        assert_eq!(s.consumer_file_wait_ns.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn consumer_file_wait_accumulates_via_add_elapsed() {
        let s = fresh();
        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(2));
        s.add_elapsed(&s.consumer_file_wait_ns, start);
        assert!(s.consumer_file_wait_ns.load(Ordering::Relaxed) > 0);
        assert_eq!(s.consumer_batch_wait_ns.load(Ordering::Relaxed), 0);
    }

    // ── format_summary alignment ──────────────────────────────────────────────

    /// Returns the on-screen width (in chars/code points) of `s`. All box
    /// content is ASCII, but borders and dashes are multi-byte UTF-8, so we
    /// count chars rather than bytes for alignment checks.
    fn display_width(s: &str) -> usize {
        s.chars().count()
    }

    /// Helper: populate stats with realistic-looking numbers and produce the
    /// rendered summary.
    fn populated_summary() -> String {
        let s = fresh();
        // Keep PROCESS_START hot.
        let _ = nanos_since_process_start();
        s.pipeline_start_ns.store(1_000, Ordering::Relaxed);
        s.pipeline_end_ns
            .store(1_000 + 645_000_000, Ordering::Relaxed); // 645 ms wall
        s.files_completed.store(10, Ordering::Relaxed);
        s.batches_produced.store(40, Ordering::Relaxed);
        // 468.8 MB
        s.bytes_produced
            .store((468.8 * 1024.0 * 1024.0) as u64, Ordering::Relaxed);
        s.peak_concurrency.store(10, Ordering::Relaxed);
        s.reader_setup_ns.store(100_000, Ordering::Relaxed); // 0.1 ms
        s.fetch_decode_ns
            .store(1_691_200_000, Ordering::Relaxed); // 1691.2 ms
        s.serialize_ns.store(1_430_000_000, Ordering::Relaxed); // 1430.0 ms
        s.producer_stall_ns.store(100_000, Ordering::Relaxed); // 0.1 ms
        s.outer_queue_full_wait_ns
            .store(54_000_000, Ordering::Relaxed); // 54.0 ms
        s.consumer_batch_wait_ns
            .store(597_000_000, Ordering::Relaxed); // 597.0 ms (over 40 batches)
        s.consumer_file_wait_ns
            .store(7_500_000, Ordering::Relaxed); // 7.5 ms (over 10 files)
        s.peak_buffered_bytes
            .store((345.9 * 1024.0 * 1024.0) as u64, Ordering::Relaxed);
        s.format_summary()
    }

    #[test]
    fn summary_box_borders_align() {
        let out = populated_summary();
        let lines: Vec<&str> = out.lines().collect();
        assert!(lines.len() > 5, "summary should have several lines");

        // Every line must have the same on-screen width.
        let widths: Vec<usize> = lines.iter().map(|l| display_width(l)).collect();
        let first = widths[0];
        for (i, w) in widths.iter().enumerate() {
            assert_eq!(
                *w, first,
                "line {} width {} != first line width {}; offending line: {:?}",
                i, w, first, lines[i]
            );
        }
    }

    #[test]
    fn summary_box_left_and_right_borders_present() {
        let out = populated_summary();
        let lines: Vec<&str> = out.lines().collect();
        // First and last line are pure border. Middle lines are bordered.
        assert!(lines.first().unwrap().starts_with('┌'));
        assert!(lines.first().unwrap().ends_with('┐'));
        assert!(lines.last().unwrap().starts_with('└'));
        assert!(lines.last().unwrap().ends_with('┘'));
        for (i, line) in lines.iter().enumerate().skip(1).take(lines.len() - 2) {
            assert!(
                line.starts_with('│') && line.ends_with('│'),
                "interior line {} missing │ borders: {:?}",
                i,
                line
            );
        }
    }

    #[test]
    fn summary_visual_dump() {
        // Always prints (visible with `cargo test ... -- --nocapture`).
        // Useful to eyeball border alignment when iterating on labels.
        let out = populated_summary();
        eprintln!("\n--- rendered pipeline-stats box ---\n{}---\n", out);
    }

    #[test]
    fn summary_parenthetical_column_is_consistent() {
        // Every row that has a "(...)" parenthetical should place its '('
        // at the same column. Catches the regression where per-batch
        // averages used " (avg ...)" (1 space) while peer rows used
        // "    (...)" (4 spaces).
        let out = populated_summary();
        let paren_cols: Vec<usize> = out
            .lines()
            .filter_map(|l| {
                // Skip section separators (which contain "── ... ──") and
                // border lines. We only want value rows with a parenthetical.
                if l.contains("──") {
                    return None;
                }
                l.find('(').map(|byte_idx| {
                    // byte_idx is fine: every char before '(' is ASCII (the
                    // border '│' is multi-byte, but we measure as char count
                    // for display alignment).
                    l[..byte_idx].chars().count()
                })
            })
            .collect();
        assert!(
            paren_cols.len() >= 4,
            "expected at least 4 rows with parentheticals; got {}",
            paren_cols.len()
        );
        let first = paren_cols[0];
        for (i, &col) in paren_cols.iter().enumerate() {
            assert_eq!(
                col, first,
                "row #{} has '(' at column {}, expected {} (all parentheticals \
                 should align). Full summary:\n{}",
                i, col, first, out
            );
        }
    }

    #[test]
    fn summary_includes_all_metric_labels() {
        let out = populated_summary();
        for needle in [
            "wall time:",
            "files:",
            "batches:",
            "throughput:",
            "CPU-parallelism:",
            "reader setup:",
            "fetch+decode:",
            "serialize IPC:",
            "batch send wait:",
            "file send wait:",
            "next batch wait:",
            "next file wait:",
            "peak buffered:",
            "ms/batch",
            "ms/file",
        ] {
            assert!(
                out.contains(needle),
                "summary missing label {:?}; full summary:\n{}",
                needle,
                out
            );
        }
    }
}
