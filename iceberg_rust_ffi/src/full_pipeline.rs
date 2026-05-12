//! Full-scan entry points on top of the shared `nested_pipeline` helpers.
//!
//! Mirrors `incremental_pipeline.rs` for the full-scan path. The per-file
//! machinery (`create_nested_pipeline`, `spawn_file_task`, `process_file`,
//! `serialize_and_forward_batches`, `make_file_stream`, `BufferedBatch`, `FileScan`)
//! lives in `nested_pipeline` and is shared verbatim with the
//! incremental-append entry point. The only full-scan-specific bits in
//! this file are:
//!
//!   * `create_full_scan_pipeline` — wraps the shared helper with a closure
//!     that builds an `ArrowReaderBuilder`-based per-file batch stream.
//!   * `create_pipeline` — legacy flat-FFI wrapper: `create_full_scan_pipeline +
//!     try_flatten`, with inline `slot_sem` promotion (the flat path
//!     bypasses `iceberg_next_file_scan`, which would otherwise be the
//!     promotion site).

use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReader;
use iceberg::io::FileIO;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use tokio::sync::Mutex as AsyncMutex;

use crate::nested_pipeline::{
    build_reader, create_nested_pipeline, FileToScan, MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE,
    MAX_PREFETCH_BUFFERS_OF_WAITING_FILE,
};
use crate::table::{IcebergArrowStream, IcebergFileScanStream};
use crate::unexpected;

/// Read one `FileScanTask` as an Arrow record-batch stream. Wraps the task
/// in a one-element stream and calls `ArrowReader::read`, mirroring
/// `incremental_pipeline::read_one_append_file`.
fn read_one_full_scan_file(
    reader: ArrowReader,
    task: FileScanTask,
) -> iceberg::Result<iceberg::scan::ArrowRecordBatchStream> {
    let task_stream = Box::pin(futures::stream::once(async { Ok(task) }));
    reader.read(task_stream)
}

/// Full-scan entry point — wraps `create_nested_pipeline` with a closure
/// that builds an `ArrowReaderBuilder`-based per-file batch stream. Takes
/// the planner's `FileScanTaskStream` directly so the caller doesn't have
/// to `try_collect` it into a `Vec` first.
pub async fn create_full_scan_pipeline(
    tasks: FileScanTaskStream,
    file_io: FileIO,
    batch_size: usize,
    prefetch_depth: usize,
) -> IcebergFileScanStream {
    let files = tasks.map_ok(move |task: FileScanTask| {
        let filename = task.data_file_path().to_string();
        let record_count = task.record_count.unwrap_or(0) as i64;
        let reader = build_reader(file_io.clone(), batch_size);
        FileToScan {
            filename,
            record_count,
            build_batch_stream: Box::new(move || {
                read_one_full_scan_file(reader, task).map_err(unexpected)
            }),
        }
    });
    create_nested_pipeline(files, prefetch_depth).await
}

/// Build the flat (concatenated) per-batch stream used by the legacy
/// `iceberg_arrow_stream` FFI. Implemented as `create_full_scan_pipeline +
/// try_flatten` so the nested pipeline's ordering and backpressure logic
/// applies unchanged.
///
/// The flat path bypasses `iceberg_next_file_scan`, which is the FFI
/// handoff that promotes `slot_sem` from `MAX_PREFETCH_BUFFERS_OF_WAITING_FILE`
/// to `MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE` (see
/// `IcebergFileScanResponse::set_payload` in `table.rs`). Without an explicit
/// promotion here every flat-path producer would stay capped at the
/// waiting-file budget — correct but slow. Since the flat path always
/// drains each file immediately (i.e. each is active by construction),
/// promote inline.
pub async fn create_pipeline(
    tasks: FileScanTaskStream,
    file_io: FileIO,
    batch_size: usize,
    prefetch_depth: usize,
) -> IcebergArrowStream {
    let nested = create_full_scan_pipeline(tasks, file_io, batch_size, prefetch_depth).await;
    let flat = nested
        .stream
        .into_inner()
        .map_ok(|fs| {
            fs.slot_sem
                .add_permits(MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE - MAX_PREFETCH_BUFFERS_OF_WAITING_FILE);
            fs.stream.stream.into_inner()
        })
        .try_flatten()
        .boxed();
    IcebergArrowStream {
        stream: AsyncMutex::new(flat),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nested_pipeline::PIPELINE_TEST_LOCK;
    use crate::pipeline_stats::{nanos_since_process_start, STATS};
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn full_pipeline_reads_parquet_file() {
        let _guard = PIPELINE_TEST_LOCK.lock().await;
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
        // Exercises: create_full_scan_pipeline → spawn_file_task → process_file
        //   (reader setup, fetch/decode, serialize IPC, semaphore acquire)
        //   → make_file_stream (semaphore release)
        let tasks = Box::pin(futures::stream::iter(vec![Ok::<_, iceberg::Error>(task)]));
        let nested = create_full_scan_pipeline(tasks, file_io, 1024, 1).await;

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

        // Wall-clock invariant: `pipeline_start_ns` was set by `reset()`
        // inside `create_nested_pipeline`; `pipeline_end_ns` is bumped by
        // each `make_file_stream` close. Both timestamps are "nanos since
        // process start", so they may be 0 if the test runs early enough
        // — we assert order, not magnitude.
        let now_after = nanos_since_process_start();
        let start = STATS.pipeline_start_ns.load(Ordering::Relaxed);
        let end = STATS.pipeline_end_ns.load(Ordering::Relaxed);
        assert!(
            end >= start,
            "pipeline_end_ns ({end}) < pipeline_start_ns ({start})"
        );
        assert!(
            start <= now_after,
            "pipeline_start_ns ({start}) > now ({now_after})"
        );
        assert!(
            end <= now_after,
            "pipeline_end_ns ({end}) > now ({now_after})"
        );
    }

    #[tokio::test]
    async fn flat_pipeline_reads_multiple_parquet_files() {
        let _guard = PIPELINE_TEST_LOCK.lock().await;
        // Exercises the legacy flat FFI path (`create_pipeline` →
        // `try_flatten` over the nested stream) across 10 in-memory parquet
        // files. This is the multi-file analogue of `full_pipeline_reads_parquet_file`
        // (which only covers the nested path with one file). batch_size=1
        // turns each 3-row file into 3 batches so we also exercise the
        // per-batch backpressure cycle on the flat path (the inline
        // `slot_sem.add_permits` promotion in `create_pipeline` plus the
        // per-batch permit release in `make_file_stream`). Assert all 30
        // rows arrive in 30 batches.
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

        let arrow_field = ArrowField::new("id", DataType::Int32, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
        );
        let arrow_schema = std::sync::Arc::new(ArrowSchema::new(vec![arrow_field]));
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
            let path = format!("memory:///flat_test/data-{i}.parquet");
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

        // Run the flat pipeline. batch_size=1 forces 3 batches per file so
        // each waiting-file producer parks on slot_sem.acquire() after batch 1;
        // the test then verifies that the inline `slot_sem.add_permits`
        // promotion in `create_pipeline` lets all 30 rows drain. Without it
        // each producer would stall after one batch and the test would hang.
        let tasks = Box::pin(futures::stream::iter(
            tasks.into_iter().map(Ok::<_, iceberg::Error>),
        ));
        let flat = create_pipeline(tasks, file_io, 1, 2).await;

        // Drain all batches and verify total row count + per-batch payload.
        let mut stream = flat.stream.lock().await;
        let mut total_rows = 0usize;
        let mut batches_seen = 0usize;
        while let Some(item) = stream.next().await {
            let arrow_batch = item.unwrap();
            let data = unsafe { std::slice::from_raw_parts(arrow_batch.data, arrow_batch.length) };
            let mut reader = StreamReader::try_new(std::io::Cursor::new(data), None).unwrap();
            let decoded = reader.next().unwrap().unwrap();
            total_rows += decoded.num_rows();
            batches_seen += 1;
            // Release the Vec<u8> allocated by serialize_record_batch.
            unsafe { drop(Box::from_raw(arrow_batch.rust_ptr as *mut Vec<u8>)) };
        }
        assert_eq!(batches_seen, 30, "expected 3 batches per file × 10 files");
        assert_eq!(total_rows, 30, "expected 30 rows total (10 files × 3 rows)");
    }

    #[tokio::test]
    async fn create_full_scan_pipeline_caps_in_flight_at_prefetch_depth_plus_one() {
        let _guard = PIPELINE_TEST_LOCK.lock().await;
        // Verify the single-consumer slice of the concurrency invariant
        // (see nested_pipeline's module-level "Concurrency invariant" doc):
        // with one FileScan pulled out and held, alive `process_file` tasks
        // ≤ prefetch_depth + 1.
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
        // each waiting-file producer parks on `slot_sem.acquire()`
        // (MAX_PREFETCH_BUFFERS_OF_WAITING_FILE=1).
        // We pull the first FileScan to activate it but never drain its inner
        // stream, so the active file's producer also stays alive. With all
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
        // (MAX_PREFETCH_BUFFERS_OF_WAITING_FILE=1) after batch 1 instead of
        // finishing instantly.
        let tasks = Box::pin(futures::stream::iter(
            tasks.into_iter().map(Ok::<_, iceberg::Error>),
        ));
        let nested = create_full_scan_pipeline(tasks, file_io, 1, prefetch_depth).await;

        // Pull the first FileScan (the "+1"). Don't drain its inner stream —
        // its producer stays alive parked on slot_sem after one
        // MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE batch (8 batches > 3-row file,
        // but the consumer never recv()s, so the producer stays alive until
        // the file's batches all queue or MAX_PREFETCH_BUFFERS_OF_ACTIVE_FILE
        // is hit; for our 3-batch file it queues all then hits
        // batch_stream.next() returning None and exits — which is acceptable:
        // peak is observed during the stable window before that producer
        // exits). Subsequent files' producers stay parked at
        // MAX_PREFETCH_BUFFERS_OF_WAITING_FILE=1 → 0 after batch 1.
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
            "expected at least 1 process_file alive (the active file)"
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
