# Iceberg file-scan pipelines: full + incremental, unified on a shared core

This doc describes the **net state** of the Iceberg file-scan pipelines on
this branch, compared against `origin/main` (`f4425f4 Add nested incremental
file-scan pipeline (#97)`). It is the architecture you should read into when
reviewing — not a change log.

## TL;DR

Two FFI-facing pipelines — **full-scan** (`full_pipeline.rs`) and
**incremental-append** (`incremental_pipeline.rs`) — sit on top of one shared
per-file machine in `nested_pipeline.rs`. They differ in exactly two places:

1. The **source `Stream<Item = T>`** of per-file tasks
   (`FileScanTask` for full / `AppendedFileScanTask` for incremental).
2. The **`build_batch_stream` closure**, run on the spawned per-file task,
   which produces a `Stream<Item = iceberg::Result<RecordBatch>>` from `T`.

Everything else — orchestration, prefetch bound, byte/slot semaphores,
serialize spawn, cancellation, stats accounting — is one implementation in
the shared file. The orchestrator is `futures::stream::Stream::buffered`;
there is no more `FuturesUnordered` loop, no separate
`run_nested_pipeline` / `run_incremental_nested` / `run_flat`, no
`data_file_concurrency_limit` knob. `prefetch_depth` is the single
concurrency knob, and the cap on alive `process_file` tasks is exactly
`prefetch_depth + (files handed out but not drained)`.

## File layout

| File | Lines | Role |
|---|---:|---|
| `src/nested_pipeline.rs` | 640 | Shared core: `create_nested_pipeline<T, Src, F>`, `spawn_file_task<S, BSF>`, `process_file`, `drain_batch_stream`, `make_file_stream`, `BufferedBatch`, `FileScan`, constants (`UNATTACHED_SLOTS=1`, `ATTACHED_SLOTS=8`), and orchestration tests (with a stub `start_one` closure). |
| `src/full_pipeline.rs` | 457 | Full-scan entries: `create_full_scan_pipeline` (nested FFI) and `create_pipeline` (legacy flat-FFI wrapper = nested + `try_flatten` + inline `slot_sem` promotion). Real-parquet end-to-end tests live here. |
| `src/incremental_pipeline.rs` | 347 | Incremental-append entry: `create_incremental_nested_pipeline`. Pulls the same shared helpers, adds `read_one_append_file` + delete-stream glue. Real-parquet tests live here. |

`src/pipeline_stats.rs` hosts the global `STATS` counters used by both
pipelines. Reset is done at FFI entry (`iceberg_arrow_stream`,
`iceberg_file_scan_stream`, `iceberg_incremental_file_scan_stream`) so each
top-level scan starts from zero.

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
     Interaction is non-obvious; `prefetch_depth` doesn't actually cap
     alive `process_file` tasks (FuturesUnordered does).

   Truly shared:
     • run_nested_pipeline<T,…>        (generic FuturesUnordered driver)
     • drain_batch_stream              (per-batch consumer drain)
     • make_file_stream                (receiver → Stream adapter)

   Looks-the-same but separate implementations:
     • spawn_file_task_with_meta  vs  spawn_incremental_file_task
     • process_file               vs  process_incremental_file
     • run_nested / run_flat      vs  run_incremental_nested

   Result: ~3 of the per-file stats counters (reader_setup_ns,
   peak_concurrency, buffered_bytes) silently stay zero on the
   incremental path, because the parallel implementation doesn't write
   them.
```

## After (this branch)

```
   ┌────────────────────────────┐    ┌────────────────────────────┐
   │     FULL-SCAN PATH         │    │   INCREMENTAL-APPEND PATH  │
   │                            │    │                            │
   │  create_full_scan_pipeline │    │ create_incremental_nested_ │
   │  create_pipeline (flat)    │    │ pipeline                   │
   │                            │    │                            │
   │  passes 2 closures:        │    │  passes 2 closures:        │
   │    source = iter(Vec<      │    │    source = append stream  │
   │             FileScanTask>) │    │             from iceberg-rs│
   │    build_batch =           │    │    build_batch =           │
   │      ArrowReader.read(task)│    │      read_one_append_file  │
   └─────────────┬──────────────┘    └─────────────┬──────────────┘
                 │                                 │
                 └────────────────┬────────────────┘
                                  ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                  SHARED CORE (nested_pipeline.rs)           │
   │                                                             │
   │   create_nested_pipeline<T, Src, F>(source, start_one,      │
   │                                     prefetch_depth)         │
   │     │                                                       │
   │     │  source.map_ok(start_one).buffered(prefetch_depth)    │
   │     ▼                                                       │
   │   spawn_file_task<S, BSF>(filename, rc, build_batch_stream) │
   │     │                                                       │
   │     ▼                                                       │
   │   process_file<S, BSF>(build_batch_stream, sems, tx)        │
   │     ├─ reader setup, decode, serialize, send                │
   │     └─ drain_batch_stream + make_file_stream                │
   │                                                             │
   │   Stats: written once → both pipelines populated correctly  │
   └─────────────────────────────────────────────────────────────┘

   One concurrency knob:
     • file_prefetch_depth  — `Stream::buffered(N)` literally caps the
       number of in-flight per-file futures. Invariant:
         alive process_file tasks ≤ prefetch_depth + (files held by FFI)

   Truly shared (one impl, called by both):
     • create_nested_pipeline<T,Src,F>   (orchestrator, generic)
     • spawn_file_task<S,BSF>            (per-file setup + spawn)
     • process_file<S,BSF>               (decode + serialize + send loop)
     • drain_batch_stream                (consumer-side drain)
     • make_file_stream                  (receiver → Stream adapter)
     • Stats accounting                  (one set of writes → both pipelines)

   Per-pipeline (necessarily different — 2 closures per pipeline):
     • source stream: `iter(Vec<…>)`           vs incremental scan stream
     • build_batch_stream closure: ArrowReader vs read_one_append_file
```

## What is *shared* vs *merely similar in shape*

Word choice matters here, because at master the two pipelines already
"mirrored" each other in shape. The difference is moving from two parallel
implementations that happen to look alike to **one function reached from
both entry points**.

Each pipeline now supplies exactly two things:

| Spot | Full-scan | Incremental-append |
|---|---|---|
| **Source `Stream<T>`** | `iter(Vec<FileScanTask>).map(Ok)` | iceberg-rs incremental scan stream of `AppendedFileScanTask` |
| **`build_batch_stream` closure** | `ArrowReaderBuilder…build().read(once(task))` | `read_one_append_file(reader, task)` |

Everything else — orchestration, prefetch bound, semaphores, serialize
spawn, cancellation, stats — is one body in `nested_pipeline.rs`. The
delete-files side of incremental is a separate stream and is unaffected
by this unification.

## What is *shared* vs *merely similar in shape*

Word choice matters here, because "the incremental pipeline mirrors the
full pipeline" was already true at master. The change is moving from
**shape-mirroring** (two parallel implementations that happen to look
alike) to **single implementation** (one function reached from both
entry points).

Now genuinely *shared* (one body, called by both):

* `create_nested_pipeline<T, Src, F>` — the orchestrator. Generic over the
  task type and the source stream.
* `spawn_file_task<S, BSF>` — per-file setup + `tokio::spawn(process_file)`.
  Generic over the batch-stream type and the build closure.
* `process_file<S, BSF>` and `process_file_inner<S, BSF>` — the per-file
  decode + serialize + send loop, including the timed call to
  `build_batch_stream()` for reader setup.
* `drain_batch_stream` — the consumer-side semaphore-aware drain.
* `make_file_stream` — receiver → `Stream<ArrowBatch>` adapter with
  per-batch permit release.
* `BufferedBatch`, `FileScan` — the in-flight types.
* `UNATTACHED_SLOTS` / `ATTACHED_SLOTS` — the two-tier slot cap, including
  the FFI handoff promotion at `IcebergFileScanResponse::set_payload`.
* `MAX_BUFFERED_BYTES_PER_TASK` (in `pipeline_stats.rs`) — the byte_sem cap.
* Stats counters: every `STATS.*` field is written from the shared code, so
  the incremental pipeline now correctly populates `reader_setup_ns`,
  `peak_concurrency`, `buffered_bytes`, etc. (At master, three of these
  were silently zero on the incremental path because the parallel
  implementation didn't update them.)

Per-pipeline (not shared, by necessity):

* The **source stream** (entry point chooses `iter(Vec<FileScanTask>)` or
  the iceberg-rs incremental-append stream).
* The **`build_batch_stream` closure** (full builds an
  `ArrowReaderBuilder`-based reader; incremental builds a per-append-file
  reader via `read_one_append_file`).
* The **delete-file stream** (incremental only).

The flat-FFI wrapper `create_pipeline` is also full-only — flat output is
not part of the incremental API.

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
capped at `UNATTACHED_SLOTS = 1` while waiting in the outer buffer (only
one batch ahead before consumer attach), promoted to `ATTACHED_SLOTS = 8`
once the FFI consumer has called `iceberg_next_file_scan`. The promotion
prevents serialized bytes from accumulating in front of files that may
never get drained (e.g. cancelled scans, slow consumers).

## Stats accounting

`STATS.reset()` is called once per top-level FFI scan
(`iceberg_arrow_stream`, `iceberg_file_scan_stream`,
`iceberg_incremental_file_scan_stream`) before invoking the pipeline. Wall
time is timestamp-based: `pipeline_start_ns` is captured at reset;
`pipeline_end_ns` is updated by `fetch_max` from each per-file
`make_file_stream` close (so the latest closing file wins). The summary
formula is `pipeline_end_ns - pipeline_start_ns`. This works identically
for nested and flat FFI paths, replacing the previous shape where the flat
path computed its own `wall_ns` and the nested path's `wall_ns` was
misleading.

Producer-side waits collapse into one counter, `producer_stall_ns`, which
covers byte_sem, slot_sem, and `tx.send` (all three stall the same
producer for the same reason — consumer behind). The previous separate
`semaphore_wait_ns` + `file_dispatch_wait_ns` + `outer_queue_full_wait_ns`
fields are gone.

Consumer-side waits are split as before:
`consumer_batch_wait_ns` (Julia waiting on the per-file batch channel) and
`consumer_file_wait_ns` (Julia waiting on the outer FileScan channel).

## FFI surface

ABI-unchanged. The dead `file_concurrency` field is kept on both
`IcebergScan` and `IcebergIncrementalScan` for shape symmetry with the
`scan_common.rs` macros (which copy whole structs across builder/scan
states). The FFI setters
`iceberg_scan_with_data_file_concurrency_limit` and
`iceberg_incremental_scan_with_data_file_concurrency_limit` are
validation-only stubs: they check the pointer and return `Ok` without
modifying state. Julia callers calling them are unaffected.

## Tests

Three test "tiers":

1. **Pipeline-level orchestration tests** (`nested_pipeline.rs::tests`):
   feed `create_nested_pipeline` a synthetic source and a stub `start_one`
   closure that returns an already-closed `FileScan`. Verify
   `Stream::buffered` shape, error propagation, source order, drop-safety,
   and `drain_batch_stream` cancellation. No parquet I/O.
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
* **Hoist `MAX_BUFFERED_BYTES_PER_TASK` and the slot constants to FFI-
  tunable knobs.** Currently hardcoded; some Julia workloads (very wide
  schemas, small batch sizes) might benefit from a different mix.
* **Per-pipeline stats namespaces.** Right now `STATS` is process-global
  and reset at FFI entry. If two scans run concurrently in the same
  process the counters interleave. Currently irrelevant (Julia serializes
  scan starts) but would matter if the FFI ever becomes re-entrant.
