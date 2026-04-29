// ===========================================================================
// Temporary profiling — will be removed before merging to production.
//
// Global atomic counters accumulate timing/size data across all file tasks.
// Called from Julia via `@ccall iceberg_print_pipeline_stats()`.
// ===========================================================================

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

pub(crate) const MAX_BUFFERED_BYTES_PER_TASK: usize = 100 * 1024 * 1024;

/// Accumulates profiling data across all file tasks in a pipeline run.
/// All fields are atomics so concurrent file tasks can update without locks.
pub(crate) struct PipelineStats {
    // ── Overall ──
    pub pipeline_wall_ns: AtomicU64,
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
    /// Time blocked on per-file semaphore (backpressure from consumer).
    pub semaphore_wait_ns: AtomicU64,

    // ── Dispatch ──
    /// Time `run_nested` blocks on the outer channel waiting for the consumer
    /// to call `next_file_scan` (backpressure from a slow consumer).
    pub file_dispatch_wait_ns: AtomicU64,

    // ── Memory ──
    /// Live counter of serialized bytes buffered across all file tasks.
    pub buffered_bytes: AtomicU64,
    /// High-water mark of buffered_bytes.
    pub peak_buffered_bytes: AtomicU64,
}

impl PipelineStats {
    pub(crate) const fn new() -> Self {
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
            file_dispatch_wait_ns: AtomicU64::new(0),
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
        self.file_dispatch_wait_ns.store(0, Ordering::Relaxed);
        self.buffered_bytes.store(0, Ordering::Relaxed);
        self.peak_buffered_bytes.store(0, Ordering::Relaxed);
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

    pub(crate) fn store_elapsed(&self, field: &AtomicU64, start: Instant) {
        field.store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
        let wall_ms = self.pipeline_wall_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let files = self.files_completed.load(Ordering::Relaxed);
        let batches = self.batches_produced.load(Ordering::Relaxed);
        let bytes = self.bytes_produced.load(Ordering::Relaxed);
        let peak = self.peak_concurrency.load(Ordering::Relaxed);
        let setup_ms = self.reader_setup_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let fd_ms = self.fetch_decode_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let ser_ms = self.serialize_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let sem_ms = self.semaphore_wait_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let dispatch_ms = self.file_dispatch_wait_ns.load(Ordering::Relaxed) as f64 / 1e6;
        let peak_buf =
            self.peak_buffered_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);

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
        row("Pipeline Stats");
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
            "batch push wait: {:>9.1} ms    (backpressure)",
            sem_ms
        ));
        sep("dispatch");
        row(&format!(
            "file push wait:  {:>9.1} ms    (backpressure)",
            dispatch_ms
        ));
        sep("memory");
        row(&format!(
            "peak buffered:   {:>9.1} MB    (limit: {} MB/task)",
            peak_buf, limit_mb
        ));
        border('└', '┘');
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
        s.pipeline_wall_ns.store(1, Ordering::Relaxed);
        s.files_completed.store(2, Ordering::Relaxed);
        s.batches_produced.store(3, Ordering::Relaxed);
        s.bytes_produced.store(4, Ordering::Relaxed);
        s.peak_concurrency.store(5, Ordering::Relaxed);
        s.active_tasks.store(6, Ordering::Relaxed);
        s.reader_setup_ns.store(7, Ordering::Relaxed);
        s.fetch_decode_ns.store(8, Ordering::Relaxed);
        s.serialize_ns.store(9, Ordering::Relaxed);
        s.semaphore_wait_ns.store(10, Ordering::Relaxed);
        s.file_dispatch_wait_ns.store(11, Ordering::Relaxed);
        s.buffered_bytes.store(12, Ordering::Relaxed);
        s.peak_buffered_bytes.store(13, Ordering::Relaxed);

        s.reset();

        assert_eq!(s.pipeline_wall_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.files_completed.load(Ordering::Relaxed), 0);
        assert_eq!(s.batches_produced.load(Ordering::Relaxed), 0);
        assert_eq!(s.bytes_produced.load(Ordering::Relaxed), 0);
        assert_eq!(s.peak_concurrency.load(Ordering::Relaxed), 0);
        assert_eq!(s.active_tasks.load(Ordering::Relaxed), 0);
        assert_eq!(s.reader_setup_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.fetch_decode_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.serialize_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.semaphore_wait_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.file_dispatch_wait_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.buffered_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(s.peak_buffered_bytes.load(Ordering::Relaxed), 0);
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
        assert!(after_second > after_first, "second add_elapsed should accumulate");
    }

    #[test]
    fn add_elapsed_is_independent_per_field() {
        let s = fresh();
        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(2));
        s.add_elapsed(&s.fetch_decode_ns, start);

        // Unrelated fields stay at 0.
        assert_eq!(s.serialize_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.semaphore_wait_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.reader_setup_ns.load(Ordering::Relaxed), 0);
        assert!(s.fetch_decode_ns.load(Ordering::Relaxed) > 0);
    }

    // ── store_elapsed ─────────────────────────────────────────────────────────

    #[test]
    fn store_elapsed_overwrites_previous_value() {
        let s = fresh();
        s.pipeline_wall_ns.store(999_999_999, Ordering::Relaxed); // some large value

        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(2));
        s.store_elapsed(&s.pipeline_wall_ns, start);
        let stored = s.pipeline_wall_ns.load(Ordering::Relaxed);

        // Stored value is the elapsed time, which is much less than 999_999_999 ns (1 s).
        assert!(stored > 0);
        assert!(stored < 999_999_999, "store_elapsed should overwrite, not accumulate");
    }

    #[test]
    fn file_dispatch_wait_accumulates_via_add_elapsed() {
        let s = fresh();
        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(2));
        s.add_elapsed(&s.file_dispatch_wait_ns, start);
        assert!(s.file_dispatch_wait_ns.load(Ordering::Relaxed) > 0);
        // Other fields are unaffected.
        assert_eq!(s.semaphore_wait_ns.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn store_elapsed_does_not_affect_other_fields() {
        let s = fresh();
        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(2));
        s.store_elapsed(&s.pipeline_wall_ns, start);

        assert_eq!(s.reader_setup_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.fetch_decode_ns.load(Ordering::Relaxed), 0);
        assert_eq!(s.serialize_ns.load(Ordering::Relaxed), 0);
    }
}
