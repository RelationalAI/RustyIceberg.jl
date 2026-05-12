# Iceberg file-scan pipelines: full + incremental, unified on a shared core

This doc describes the **net state** of the Iceberg file-scan pipelines on
this branch, compared against `origin/main` (`f4425f4 Add nested incremental
file-scan pipeline (#97)`). It is the architecture you should read into when
reviewing — not a change log.

## TL;DR

Two FFI-facing pipelines — **full-scan** (`full_pipeline.rs`) and
**incremental-append** (`incremental_pipeline.rs`) — sit on top of one shared
per-file machine in `nested_pipeline.rs`. Each pipeline adapts its
planner stream into a stream of `FileToScan` handoff values and hands
that to the shared orchestrator. A `FileToScan` carries the three things
the orchestrator needs to spawn a per-file task: `filename`,
`record_count`, and a deferred `build_batch_stream` closure that produces
a `Stream<Item = iceberg::Result<RecordBatch>>` when polled.

The two pipelines therefore differ in exactly two places:

1. The **source `Stream<Item = FileToScan<S>>`** they construct from their
   planner output (`FileScanTask` for full / `AppendedFileScanTask` for
   incremental).
2. The **`build_batch_stream` body** inside each `FileToScan`
   (`read_one_full_scan_file` for full / `read_one_append_file` for
   incremental).

Everything else — orchestration, prefetch bound, byte/slot semaphores,
serialize spawn, cancellation, stats accounting — is one implementation in
the shared file. The orchestrator is `futures::stream::Stream::buffered`;
there is no more `FuturesUnordered` loop, no separate
`run_nested_pipeline` / `run_incremental_nested` / `run_flat`, no
`data_file_concurrency_limit` knob (the FFI setter survives as a
validation-only stub for ABI compat). `prefetch_depth` is the single
concurrency knob, and the cap on alive `process_file` tasks is exactly
`prefetch_depth + (files handed out but not drained)`.

`batch_size` is required: the FFI stream ops error out (rather than
silently using the parquet crate's internal default) if Julia hasn't
called `with_batch_size!` before scanning.

## File layout

| File | Role |
|---|---|
| `src/nested_pipeline.rs` | Shared core: `create_nested_pipeline<S, Src>`, `spawn_file_task<S, BSF>`, `process_file`, `serialize_and_forward_batches`, `make_file_stream`, `build_reader`, `BufferedBatch`, `FileScan`, the `FileToScan<S>` handoff struct, constants (`MAX_PREFETCH_BUFFERS_OF_WAITING_FILE=1`, `MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE=8`), and orchestration tests (which feed `FileToScan` values with an empty batch-stream builder). |
| `src/full_pipeline.rs` | Full-scan entries: `create_full_scan_pipeline` (nested FFI) and `create_pipeline` (legacy flat-FFI wrapper = nested + `try_flatten` + inline `slot_sem` promotion). Per-file helper `read_one_full_scan_file`. Real-parquet end-to-end tests live here. |
| `src/incremental_pipeline.rs` | Incremental-append entry: `create_incremental_nested_pipeline`. Pulls the same shared helpers, adds `read_one_append_file` + delete-stream glue. Real-parquet tests live here. |

`src/pipeline_stats.rs` hosts the global `STATS` counters used by both
pipelines. Reset is done once inside `create_nested_pipeline` (after
`plan_files()` has been awaited and the per-file pipeline is about to
start), so each scan's wall-clock counts only per-file execution, not
manifest planning.

## Before (master)

```
   ┌────────────────────────────┐         ┌────────────────────────────┐
   │     FULL-SCAN PATH         │         │   INCREMENTAL-APPEND PATH  │
   │                            │         │                            │
   │   run_nested  ←─┐          │         │  run_incremental_nested    │
   │   run_flat    ──┤          │         │                            │
   │                 │          │         │                            │
   │                 ▼          │         │                            │
   │   FuturesUnordered         │         │   FuturesUnordered         │
   │   (cap: data_file_         │         │   (cap: data_file_         │
   │    concurrency_limit)      │         │    concurrency_limit)      │
   │                 │          │         │                 │          │
   │                 ▼          │         │                 ▼          │
   │   spawn_file_task_with_    │         │   spawn_incremental_       │
   │   meta                     │         │   file_task                │
   │                 │          │         │                 │          │
   │                 ▼          │         │                 ▼          │
   │   process_file             │         │   process_incremental_file │
   │                 │          │         │                 │          │
   │                 ▼          │         │                 ▼          │
   │   outer mpsc(prefetch_     │         │   outer mpsc(prefetch_     │
   │   depth)                   │         │   depth)                   │
   └────────────────────────────┘         └────────────────────────────┘

   Two concurrency knobs (per pipeline!):
     • data_file_concurrency_limit  — bounds FuturesUnordered
     • file_prefetch_depth          — bounds the outer mpsc
     Interaction is non-obvious. Neither knob actually caps alive
     `process_file` tasks directly: the futures inside FuturesUnordered
     resolve immediately after `tokio::spawn(process_file(...))`, so the
     spawned tasks live outside the set. The only real bound is outer-
     mpsc back-pressure on the orchestrator's `tx.send`. See the
     dedicated "What the old data_file_concurrency_limit knob actually
     did" section below for a full breakdown.

   Truly shared:
     • run_nested_pipeline<T,…>        (generic FuturesUnordered driver)
     • drain_batch_stream              (per-batch consumer drain)
     • make_file_stream                (receiver → Stream adapter)

   Looks-the-same but separate implementations:
     • spawn_file_task_with_meta  vs  spawn_incremental_file_task
     • process_file               vs  process_incremental_file
     • run_nested / run_flat      vs  run_incremental_nested

   Result: several per-file stats counters are broken on the
   incremental path because its parallel implementation doesn't go
   through the same call sites as the full-scan one. `reader_setup_ns`
   and `peak_concurrency` stay 0; `buffered_bytes` accumulates
   monotonically to the total bytes produced (no decrement on consumer
   pull). Wall-clock is broken on both paths but in different ways —
   see the "Stats accounting" section.
```

## After (this branch)

```
   ┌────────────────────────────┐    ┌────────────────────────────┐
   │     FULL-SCAN PATH         │    │   INCREMENTAL-APPEND PATH  │
   │                            │    │                            │
   │  create_full_scan_pipeline │    │ create_incremental_nested_ │
   │  create_pipeline (flat)    │    │ pipeline                   │
   │                            │    │                            │
   │  .map_ok the planner       │    │  .map_ok the planner       │
   │  stream into FileToScan:   │    │  stream into FileToScan:   │
   │    FileScanTaskStream      │    │    BoxStream of            │
   │      from plan_files()     │    │      AppendedFileScanTask  │
   │    build_batch_stream =    │    │    build_batch_stream =    │
   │      read_one_full_scan_   │    │      read_one_append_file  │
   │      file(reader, task)    │    │      (reader, task)        │
   └─────────────┬──────────────┘    └─────────────┬──────────────┘
                 │                                 │
                 │   Stream<Item = iceberg::Result<FileToScan<S>>>
                 │                                 │
                 └────────────────┬────────────────┘
                                  ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                  SHARED CORE (nested_pipeline.rs)           │
   │                                                             │
   │   create_nested_pipeline<S, Src>(source, prefetch_depth)    │
   │     │                                                       │
   │     │  source.map(|res| async move {                        │
   │     │      res.map(|f| spawn_file_task(                     │
   │     │          f.filename, f.record_count,                  │
   │     │          f.build_batch_stream))                       │
   │     │  }).buffered(prefetch_depth)                          │
   │     ▼                                                       │
   │   spawn_file_task<S, BSF>(filename, rc, build_batch_stream) │
   │     │                                                       │
   │     ▼                                                       │
   │   process_file<S, BSF>(build_batch_stream, sems,            │
   │                        byte_budget, tx)                     │
   │     ├─ reader setup, decode, serialize, send                │
   │     └─ serialize_and_forward_batches + make_file_stream     │
   │                                                             │
   │   Stats: written once → both pipelines populated correctly  │
   └─────────────────────────────────────────────────────────────┘

   One concurrency knob:
     • file_prefetch_depth  — `Stream::buffered(N)` literally caps the
       number of in-flight per-file futures. Invariant:
         alive process_file tasks ≤ prefetch_depth + (files held by FFI)

   Truly shared (one impl, called by both):
     • create_nested_pipeline<S,Src>     (orchestrator, generic)
     • FileToScan<S>                     (planner→orchestrator handoff)
     • spawn_file_task<S,BSF>            (per-file setup + spawn)
     • process_file<S,BSF>               (decode + serialize + send loop)
     • serialize_and_forward_batches     (phases 2-4 of the file pipeline)
     • make_file_stream                  (receiver → Stream adapter)
     • build_reader                      (ArrowReader with batch_size,
                                          per-file concurrency = 1)
     • Stats accounting                  (one set of writes → both pipelines)

   Per-pipeline (necessarily different — one .map_ok adapter per pipeline):
     • planner stream:    FileScanTaskStream    vs incremental append stream
     • per-file reader:   read_one_full_scan_file vs read_one_append_file
```

## What is *shared* vs *merely similar in shape*

Word choice matters here, because "the incremental pipeline mirrors the
full pipeline" was already true at master. The change is moving from
**shape-mirroring** (two parallel implementations that happen to look
alike) to **single implementation** (one function reached from both
entry points).

Now genuinely *shared* (one body, called by both):

* `create_nested_pipeline<S, Src>` — the orchestrator. Generic over the
  per-file batch-stream type `S` and the source stream `Src`.
* `FileToScan<S>` — the planner-to-orchestrator handoff struct. Carries
  `filename`, `record_count`, and a deferred `build_batch_stream:
  Box<dyn FnOnce() -> iceberg::Result<S> + Send>`.
* `spawn_file_task<S, BSF>` — per-file setup + `tokio::spawn(process_file)`.
  Generic over the batch-stream type and the build closure.
* `process_file<S, BSF>` and `process_file_inner<S, BSF>` — the per-file
  decode + serialize + send loop, including the timed call to
  `build_batch_stream()` for reader setup. Take a `byte_budget` parameter
  so the per-batch byte cost can be clamped to the semaphore size (see
  the "Backpressure clamp" subsection below).
* `serialize_and_forward_batches` — RecordBatch → Arrow-IPC → mpsc, with
  byte/slot semaphore acquires and `tokio::select!`-based cancellation
  on `tx.closed()`.
* `make_file_stream` — receiver → `Stream<ArrowBatch>` adapter with
  per-batch permit release.
* `build_reader` — `ArrowReaderBuilder` with the configured `batch_size`
  and per-file concurrency pinned to 1.
* `BufferedBatch`, `FileScan` — the in-flight types.
* `MAX_PREFETCH_BUFFERS_OF_WAITING_FILE` / `MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE`
  — the two-tier slot cap, including the FFI handoff promotion at
  `IcebergFileScanResponse::set_payload`.
* `MAX_BUFFERED_BYTES_PER_TASK` (in `pipeline_stats.rs`) — the byte_sem cap.
* Stats counters: every `STATS.*` field is written from the shared code, so
  the incremental pipeline now correctly populates `reader_setup_ns`,
  `peak_concurrency`, `buffered_bytes`, etc. (At master, three of these
  were silently zero on the incremental path because the parallel
  implementation didn't update them.)

Per-pipeline (not shared, by necessity). Each entry point `.map_ok`s its
planner stream into `Stream<Item = iceberg::Result<FileToScan<S>>>` and
hands that to `create_nested_pipeline`:

| Spot | Full-scan | Incremental-append |
|---|---|---|
| **Planner stream** | `FileScanTaskStream` from `scan.plan_files()` | `BoxStream<AppendedFileScanTask>` from iceberg-rs incremental scan |
| **Per-file reader call** | `read_one_full_scan_file(reader, task)` (= `ArrowReader::read(once(task))`) | `read_one_append_file(reader, task)` (= `StreamsInto::stream((once(task), empty()), reader)`) |

Everything else — orchestration, prefetch bound, semaphores, serialize
spawn, cancellation, stats — is one body in `nested_pipeline.rs`. The
delete-files side of incremental is a separate stream (`StreamsInto`-based,
not file-grouped) and is unaffected by this unification. The flat-FFI
wrapper `create_pipeline` is full-only — flat output is not part of the
incremental API.

## Concurrency invariant (single knob)

`Stream::buffered(prefetch_depth)` keeps at most `prefetch_depth` inner
futures actively polled. Each inner future resolves trivially with a
`FileScan` after `tokio::spawn`, **but the spawned `process_file` runs
independently in the background, gated by per-file slot/byte semaphores**.
The cap on alive `process_file` tasks is therefore:

```
alive process_file tasks  ≤  prefetch_depth + M
```

where `M` is the number of `FileScan`s currently handed to FFI consumers
and not yet drained. With one serial consumer, `M = 1` and the cap is
`prefetch_depth + 1` (the unit test
`create_full_scan_pipeline_caps_in_flight_at_prefetch_depth_plus_one`
pins this down). Julia's `ICEBERG_FILE_TASK_GROUP` uses `M = nthreads()`.

Memory per file is capped at ~100 MB by `byte_sem`. Slot count per file is
capped at `MAX_PREFETCH_BUFFERS_OF_WAITING_FILE = 1` while waiting in the
outer buffer (only one batch ahead before the FFI consumer picks the file
up), promoted to `MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE = 8` once the FFI
consumer has called `iceberg_next_file_scan`. The promotion prevents
serialized bytes from accumulating in front of files that may never get
drained (e.g. cancelled scans, slow consumers).

### Backpressure clamp for oversized batches

The byte semaphore has `MAX_BUFFERED_BYTES_PER_TASK` permits. A single
serialized batch larger than that would deadlock `byte_sem.acquire_many`,
since the requested permit count can never be reached. To prevent that,
`serialize_and_forward_batches` clamps the per-batch backpressure cost
once at the `BufferedBatch` construction site:

```rust
let clamped_byte_len = serialized.length.min(byte_budget);
```

The clamp lives on the `BufferedBatch.clamped_byte_len` field, so the
`byte_sem.acquire_many(clamped_byte_len)` on the producer side and the
matching `byte_sem.add_permits(buf.clamped_byte_len)` on the consumer
side stay automatically symmetric. Real in-flight RAM accounting
(`STATS.bytes_produced`, `STATS.buffered_bytes` via `track_buffer_add` /
`track_buffer_release`) uses the unclamped `serialized.length` /
`buf.batch.length` — these counters measure actual bytes, not semaphore
permits. As a side benefit the clamp also prevents the
`byte_len as u32` cast from wrapping at ≥ 4 GiB batches.

## What the old `data_file_concurrency_limit` knob actually did

The pre-unification API exposed two concurrency knobs:
`data_file_concurrency_limit` and `file_prefetch_depth`. The first was
advertised as "how many files are read concurrently". Reading the master
orchestrator (`ordered_file_pipeline.rs` `run_nested_pipeline`,
`spawn_file_task_with_meta`, `spawn_incremental_file_task`) reveals that
this is not what it did.

`run_nested_pipeline` kept `concurrency` items in a `FuturesUnordered`
set, refilling on each `next()`. But each item in the set was a future
returned by `spawn_file_task_with_meta`:

```rust
fn spawn_file_task_with_meta(...) -> impl Future<Output = ...> {
    let (file_tx, file_rx) = mpsc::channel(8);
    tokio::spawn(process_file(task, file_io, batch_size, sem, file_tx)); // detach
    async move { Ok((filename, record_count, file_rx)) }                  // resolve now
}
```

The future is trivially resolved — its first poll returns
`Ok((filename, record_count, file_rx))` after the `tokio::spawn` has
already kicked off the real work in a detached task. So
`FuturesUnordered` is not bounding **anything** that does I/O; it is
bounding the *rate* at which `spawn_file_task_with_meta` is called.
Every alive `process_file` task runs entirely outside the `FuturesUnordered`
set, throttled only by its own per-file 100 MB byte semaphore and its
own 8-slot mpsc.

The only thing that actually slowed the spawning loop down was outer-
mpsc back-pressure: `run_nested_pipeline` pushes each `FileScan` into
`mpsc::channel(prefetch_depth)`, and when that fills, the `tx.send`
inside the orchestrator stops the loop. So the effective cap on alive
`process_file` tasks was roughly:

```
concurrency + prefetch_depth + 1
```

dominated by `prefetch_depth` in practice (master's defaults were
`concurrency = 2 × nthreads`, `prefetch_depth = nthreads`). Setting
`data_file_concurrency_limit = 1` from Julia did not give you "one file
at a time" — it gave you roughly `1 + prefetch_depth + 1 ≈ nthreads + 2`
alive readers, each holding an open Parquet file and up to ~100 MB of
serialized Arrow IPC in its per-file mpsc.

Net consequences:

* **Memory was much higher than the knob suggested.** Each alive
  `process_file` task can buffer up to `MAX_BUFFERED_BYTES_PER_TASK`
  (100 MB) of serialized IPC. With the master defaults, peak in-flight
  bytes were on the order of `(concurrency + prefetch_depth) × 100 MB`
  ≈ ~1.2 GB on a 4-thread default — not the `concurrency × 100 MB` a
  reader would infer.
* **The setter lied to callers.** The Julia binding (and its raicode-side
  use in `IcebergPerfConfig`) treated `data_file_concurrency_limit` as a
  meaningful tuning lever; in practice the only knob that influenced
  alive-task count was `prefetch_depth`, and it was the one users
  generally didn't touch.

This branch collapses to one knob. `create_nested_pipeline` uses
`futures::stream::Stream::buffered(prefetch_depth)` to drive a closure
that ends in `tokio::spawn(process_file(...))`. `Stream::buffered(N)`
keeps exactly `N` inner futures actively polled — and because each one
ends with a `tokio::spawn` followed by `FileScan` return, advancing past
`N` requires the consumer to first pull a `FileScan` out, which only
happens after the previous file has been handed off. The cap on alive
`process_file` tasks therefore matches the documented invariant
(`prefetch_depth + M`, where `M` is the number of `FileScan`s held by
FFI consumers) and the test
`create_full_scan_pipeline_caps_in_flight_at_prefetch_depth_plus_one`
locks it in.

## Stats accounting

Stats on this branch are written from a single set of call sites
(`nested_pipeline.rs::process_file`, `process_file_inner`,
`serialize_and_forward_batches`, `make_file_stream`, plus the
orchestrator's outer unfold), and that single set of writes feeds both
pipelines correctly.
Compared to master, two classes of bugs are fixed:

### (1) Incremental path was missing most stats

Master's incremental pipeline (`incremental_pipeline.rs` at `origin/main`)
defined its own `process_incremental_file` /
`process_incremental_file_inner` / `spawn_incremental_file_task` — siblings
of the full-scan equivalents that shared only `drain_batch_stream`. Direct
consequences in the master code:

* **`peak_concurrency` / `active_tasks` stayed at 0** — `track_task_start()`
  / `track_task_end()` only existed on the full-scan `process_file`
  wrapper, not on `process_incremental_file`.
* **`reader_setup_ns` stayed at 0** — the timing wrapper around the
  reader-setup phase only existed in the full-scan `process_file_inner`.
* **`buffered_bytes` never decremented** —
  `spawn_incremental_file_task` passed `|_| {}` as the
  `on_release` callback for `make_file_stream`, so consumer-side batch
  pulls never called `track_buffer_release`. After a run, `buffered_bytes`
  ended up equal to total `bytes_produced`, telling you nothing about
  the peak in-flight situation. (`peak_buffered_bytes` itself was still
  correct, but the running gauge was useless.)

On this branch all of these counters are written once, in the shared
`process_file` / `make_file_stream` paths, so the incremental pipeline
gets the same numbers as the full-scan pipeline.

Note that the byte-throughput counters (`bytes_produced`,
`buffered_bytes` / `peak_buffered_bytes` via `track_buffer_add` /
`track_buffer_release`) deliberately use the *unclamped* serialized
batch size, not `clamped_byte_len`. The clamp from the "Backpressure
clamp" subsection above only governs semaphore permits; STATS counters
report real in-flight RAM. See `make_file_stream` for the symmetric
unclamped read of `buf.batch.length` on the consumer side.

### (2) Wall-clock metric was inconsistent / misleading

Master had one `pipeline_wall_ns` field with two writers using
`store_elapsed` (= "store, overwriting"):

* `run_flat` wrote it on the legacy flat-FFI path, measured from start
  of the flat orchestrator to the moment the consumer fully drained the
  flattened batch stream.
* `run_nested` wrote it on the nested FFI path, measured from start of
  the nested orchestrator to the moment the **last `FileScan`** was
  pushed into the outer mpsc.

The nested writer fires when each per-file `process_file` task has
**spawned**, not when each file's batches have been drained. Once raicode
moved to consuming files in parallel via `ICEBERG_FILE_TASK_GROUP`, the
spawn-completion timestamp had essentially nothing to do with end-to-end
data flow — the actual data was still streaming through Julia for tens
to hundreds of milliseconds after `run_nested` had already written its
"wall" timestamp and returned. The reported number was effectively the
manifest-planning + spawn-loop latency, not a wall clock; the printed
summary's `parallelism: NNNx` ratio (computed from `wall_ns`) became
nonsense.

The flat writer's number was correct for its own path, but it was
written with `store_elapsed`, racing the nested writer; whichever
finished last won. The flat path was effectively dead in raicode (the
nested API had replaced it), so in practice the misleading nested value
was the one that printed.

The fix on this branch:

* `pipeline_wall_ns` is replaced by two timestamps,
  `pipeline_start_ns` and `pipeline_end_ns`, both expressed as nanos
  since a `LazyLock<Instant>` `PROCESS_START`. The summary computes
  wall as `pipeline_end_ns - pipeline_start_ns`.
* `pipeline_start_ns` is stamped in `STATS.reset()`, which is called
  once inside `create_nested_pipeline` (after `plan_files()` has
  resolved). So manifest planning is excluded; per-file pipeline
  execution is what's measured.
* `pipeline_end_ns` is updated via `fetch_max` in `make_file_stream`
  whenever a per-file `recv()` returns `None` (i.e. that file's
  `process_file` task has dropped its `tx`). Since the consumer is the
  one driving the `recv()`, this fires after the file's last batch has
  actually been pulled, not when the file was scheduled. With
  `fetch_max`, the field ends up holding the timestamp of whichever
  file finished latest — which is exactly the wall-clock-end of the
  whole pipeline run regardless of file ordering or consumer
  parallelism.

Both pipelines now produce identical, correctly-meaning wall stats
under either FFI flavour, including the multi-file-at-a-time consumer
shape that raicode uses today.

### Smaller related changes

* **Producer-side waits collapsed into one field.** Master had separate
  `semaphore_wait_ns` (byte_sem) and `file_dispatch_wait_ns` (outer
  mpsc `tx.send` block). The latter was only ever written from the
  master orchestrator's send-FileScan-to-outer-channel call, which no
  longer exists. The current code has three producer-side back-pressure
  points (byte_sem, slot_sem, per-file `tx.send`), all of which share
  one root cause ("consumer behind"), so they all feed into a single
  `producer_stall_ns`. Useful one-glance signal for "is the consumer
  draining fast enough?"
* **Consumer-side waits added.** Two new counters time consumer-side
  blocking on the inner per-file mpsc (`consumer_batch_wait_ns`) and on
  the outer FileScan stream (`consumer_file_wait_ns`). Useful one-glance
  signal for "is Julia ahead of Rust?" Master had no consumer-side
  timing at all.
* **New atomic helpers.** `pipeline_stats::PROCESS_START` /
  `nanos_since_process_start` are the building blocks the new wall
  metric is built on (and are also used by the wall-clock-invariant
  test in `full_pipeline.rs::tests`).

## FFI surface

ABI-unchanged. The dead `file_concurrency` field is kept on both
`IcebergScan` and `IcebergIncrementalScan` for shape symmetry with the
`scan_common.rs` macros (which copy whole structs across builder/scan
states). The FFI setters
`iceberg_scan_with_data_file_concurrency_limit` and
`iceberg_incremental_scan_with_data_file_concurrency_limit` are
validation-only stubs: they check the pointer and return `Ok` without
modifying state. Julia callers calling them are unaffected.

**Behaviour change**: `batch_size` is now required at scan-start time.
`IcebergScan.batch_size` / `IcebergIncrementalScan.batch_size` are still
typed `Option<usize>` and still initialize to `None`, but the FFI stream
ops (`iceberg_arrow_stream`, `iceberg_file_scan_stream`,
`iceberg_incremental_file_scan_stream`) now error out with `batch_size
not set; call iceberg_*_with_batch_size before scan` if Julia hasn't
called the setter. Previously `None` silently fell through to the
parquet crate's internal default. The Julia bindings' `with_batch_size!`
docstrings have been updated to mark it required.

## Tests

Three test "tiers":

1. **Pipeline-level orchestration tests** (`nested_pipeline.rs::tests`):
   feed `create_nested_pipeline` a synthetic source of `FileToScan` values
   whose `build_batch_stream` returns an empty Arrow stream. Verify
   `Stream::buffered` shape, error propagation, source order, drop-safety,
   `serialize_and_forward_batches` cancellation, and the
   oversized-batch backpressure clamp
   (`oversized_batch_does_not_deadlock_byte_sem`). No parquet I/O.
2. **Real-parquet end-to-end tests** (`full_pipeline.rs::tests` and
   `incremental_pipeline.rs::tests`): generate in-memory parquet files,
   run them through `create_full_scan_pipeline` /
   `create_pipeline` / `create_incremental_nested_pipeline`, decode the
   IPC bytes, and assert on row contents. The multi-file flat test
   (`flat_pipeline_reads_multiple_parquet_files`) exercises 10 files × 3
   rows × `batch_size=1` to cover both the buffered-prefetch path and
   per-batch attach in the flat wrapper.
3. **Stats-only unit tests** (`pipeline_stats.rs::tests`): cover the new
   wall-clock fields, `peak_concurrency` mechanics, summary formatting,
   and reset clearing every field.

Tests that drive a real pipeline acquire `PIPELINE_TEST_LOCK` (a process-
global `tokio::sync::Mutex<()>` exposed `pub(crate)` from
`nested_pipeline.rs`) so concurrent `cargo test` workers don't race on the
global `STATS`. The lock is `#[cfg(test)]`-gated and not in the release
build.

## Possible follow-ups

These are not done on this branch; flagging them for future work.

* **Drop dead `file_concurrency` field.** Requires reworking the
  `scan_common.rs` macros (`impl_scan_build!` etc.) to either functional-
  record-update or to know which fields apply to which struct. Currently
  not worth the macro complexity for one dead field.
* **Drop the legacy flat FFI `iceberg_arrow_stream`.** All known Julia
  callers use the nested FFI (`iceberg_file_scan_stream`). Removing the
  flat wrapper would simplify `full_pipeline.rs` by deleting
  `create_pipeline` and the inline `slot_sem` promotion. Blocked on
  confirming no out-of-tree consumers.
* **Expose `byte_budget` and the slot constants as FFI-tunable knobs.**
  `byte_budget` is already a parameter on `serialize_and_forward_batches`
  / `process_file` / `process_file_inner` (so the oversized-batch
  regression test can use a tiny budget), but `spawn_file_task` still
  hardcodes it to `MAX_BUFFERED_BYTES_PER_TASK`. The slot constants are
  still compile-time. Some Julia workloads (very wide schemas, small
  batch sizes) might benefit from a different mix.
* **Per-pipeline stats namespaces.** Right now `STATS` is process-global
  and reset inside `create_nested_pipeline`. If two scans run concurrently
  in the same process the counters interleave. Currently irrelevant
  (Julia serializes scan starts) but would matter if the FFI ever
  becomes re-entrant.
