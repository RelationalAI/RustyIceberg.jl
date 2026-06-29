//! Incremental-append entry point on top of the shared `nested_pipeline`
//! helpers. The per-file machinery (`create_nested_pipeline`,
//! `spawn_file_task`, `process_file`, `serialize_and_forward_batches`,
//! `make_file_stream`, `BufferedBatch`, `FileScan`) lives in `nested_pipeline`
//! and is shared verbatim with the full-scan entry point. The only
//! incremental-specific bits in this file are:
//!
//!   * `read_one_append_file` — per-task helper that builds an Arrow batch
//!     stream via iceberg-rs's `StreamsInto` machinery. The full-scan
//!     equivalent is `full_pipeline::read_one_full_scan_file`, which calls
//!     `ArrowReader::read` directly.
//!   * `create_incremental_nested_pipeline` — wraps the shared helper for
//!     the append source and pairs it with a (flat) delete stream.
//!
//! Returns `(IcebergFileScanStream, IcebergArrowStream)`:
//!   - append stream: one `FileScan` per parquet file, in manifest order
//!   - delete stream: flat Arrow stream of delete records

use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::{ArrowReader, StreamsInto, UnzippedIncrementalScanResult};
use iceberg::io::FileIO;
use iceberg::scan::incremental::{AppendedFileScanTask, DeleteScanTask};
use tokio::sync::Mutex as AsyncMutex;

use crate::nested_pipeline::{build_reader, create_nested_pipeline, BufferLimits, FileToScan};
use crate::table::{IcebergArrowStream, IcebergFileScanStream};
use crate::unexpected;

/// Read one `AppendedFileScanTask` as an Arrow record-batch stream.
/// Wraps the task in a one-element stream + empty delete stream and calls the
/// `StreamsInto` machinery (same path `to_unzipped_arrow` uses internally).
fn read_one_append_file(
    reader: ArrowReader,
    task: AppendedFileScanTask,
) -> iceberg::Result<iceberg::scan::ArrowRecordBatchStream> {
    let append = futures::stream::once(async { Ok(task) }).boxed();
    let delete = futures::stream::empty::<iceberg::Result<DeleteScanTask>>().boxed();
    let UnzippedIncrementalScanResult { appends, .. } = StreamsInto::<
        ArrowReader,
        UnzippedIncrementalScanResult,
    >::stream((append, delete), reader)?;
    Ok(appends)
}

/// Build the nested incremental pipeline.
///
/// Returns `(append_stream, delete_stream)`:
/// - `append_stream`: `IcebergFileScanStream` — one `FileScan` per appended
///   parquet file, in manifest order, with a prefetched inner batch stream.
///   Built via the shared `create_nested_pipeline` helper using
///   `read_one_append_file` as the per-task batch-stream builder.
/// - `delete_stream`: flat `IcebergArrowStream` of delete records. Unaffected
///   by the nested-pipeline plumbing — kept as a flat stream because
///   position-delete records are already keyed by `(file_path, pos)` and
///   don't need file-level grouping.
pub async fn create_incremental_nested_pipeline(
    append_tasks: futures::stream::BoxStream<'static, iceberg::Result<AppendedFileScanTask>>,
    delete_tasks: futures::stream::BoxStream<'static, iceberg::Result<DeleteScanTask>>,
    file_io: FileIO,
    batch_size: usize,
    prefetch_depth: usize,
    serialization_concurrency: usize,
    buffer_limits: BufferLimits,
) -> anyhow::Result<(IcebergFileScanStream, IcebergArrowStream)> {
    let reader = build_reader(file_io.clone(), batch_size);

    let files = append_tasks.map_ok(move |task: AppendedFileScanTask| {
        let filename = task.base.data_file_path.clone();
        let record_count = task.base.record_count.unwrap_or(0) as i64;
        let reader = reader.clone();
        FileToScan {
            filename,
            record_count,
            build_batch_stream: Box::new(move || {
                read_one_append_file(reader, task)
                    .map_err(|e| unexpected(format!("reader setup: {e}")))
            }),
        }
    });
    let append_stream = create_nested_pipeline(files, prefetch_depth, buffer_limits).await;

    // Delete stream: StreamsInto with empty append stream routes all delete
    // tasks through the iceberg reader machinery.
    let UnzippedIncrementalScanResult {
        deletes: delete_arrow,
        ..
    } = StreamsInto::<ArrowReader, UnzippedIncrementalScanResult>::stream(
        (
            futures::stream::empty::<iceberg::Result<AppendedFileScanTask>>().boxed(),
            delete_tasks,
        ),
        build_reader(file_io, batch_size),
    )
    .map_err(|e| anyhow::anyhow!("Failed to create delete stream: {e}"))?;

    let delete_stream = IcebergArrowStream {
        stream: AsyncMutex::new(crate::transform_stream_with_parallel_serialization(
            delete_arrow,
            serialization_concurrency,
        )),
    };

    Ok((append_stream, delete_stream))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use bytes::Bytes;
    use futures::StreamExt;
    use iceberg::io::FileIO;
    use iceberg::scan::incremental::{
        AppendedFileScanTask, BaseIncrementalFileScanTask, DeleteScanTask, DeletedFileScanTask,
    };
    use iceberg::spec::{
        DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;
    use crate::nested_pipeline::PIPELINE_TEST_LOCK;

    /// `BufferLimits` built from the production-default consts, so these tests
    /// keep their previous behavior.
    // Mirrors the production defaults in RustyIceberg.jl `IcebergPerfConfig`
    // (100 MiB / 1 / 8); kept inline so tests do not depend on named consts.
    const TEST_LIMITS: BufferLimits = BufferLimits {
        max_buffered_bytes_per_task: 100 * 1024 * 1024,
        max_prefetch_buffers_of_waiting_file: 1,
        max_prefetch_buffers_of_active_file: 8,
    };

    // ── Shared setup helpers ──────────────────────────────────────────────

    /// Write a 3-row parquet file (`id` INT32: [10, 20, 30]) to in-memory storage.
    /// Returns `(FileIO, path, file_size, iceberg_schema)`.
    async fn write_test_parquet() -> (FileIO, &'static str, u64, Arc<IcebergSchema>) {
        let arrow_field = ArrowField::new("id", DataType::Int32, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )
        .unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let parquet_bytes = Bytes::from(buf);
        let file_size = parquet_bytes.len() as u64;

        let file_io = FileIO::new_with_memory();
        let path = "memory:///incremental_test/data.parquet";
        file_io
            .new_output(path)
            .unwrap()
            .write(parquet_bytes)
            .await
            .unwrap();

        let iceberg_schema = Arc::new(
            IcebergSchema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .unwrap(),
        );

        (file_io, path, file_size, iceberg_schema)
    }

    /// Build an `AppendedFileScanTask` pointing at the given in-memory parquet file.
    fn make_append_task(
        path: &str,
        file_size: u64,
        schema: Arc<IcebergSchema>,
    ) -> AppendedFileScanTask {
        AppendedFileScanTask {
            base: BaseIncrementalFileScanTask {
                data_file_path: path.to_string(),
                file_size_in_bytes: file_size,
                start: 0,
                length: file_size,
                record_count: Some(3),
                data_file_format: DataFileFormat::Parquet,
                schema,
                project_field_ids: vec![1],
                partition: None,
                partition_spec: None,
                case_sensitive: false,
            },
            positional_deletes: None,
            equality_delete_predicate: None,
        }
    }

    /// Read row count from an `ArrowBatch` (Arrow C Data Interface) and free it.
    fn decode_batch_row_count(batch: crate::table::ArrowBatch) -> usize {
        assert!(!batch.array.is_null());
        // The first i64 field of FFI_ArrowArray is the row count.
        let n_rows = unsafe { *(batch.array as *const i64) } as usize;
        unsafe {
            drop(Box::from_raw(
                batch.rust_ptr as *mut crate::table::ArrowBatchInner,
            ))
        };
        n_rows
    }

    // ── Tests ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn empty_streams_succeed() {
        let _guard = PIPELINE_TEST_LOCK.lock().await;
        // Baseline: both append and delete streams empty → pipeline returns Ok
        // with two empty streams; no I/O or background tasks are spawned.
        let result = create_incremental_nested_pipeline(
            futures::stream::empty::<iceberg::Result<AppendedFileScanTask>>().boxed(),
            futures::stream::empty::<iceberg::Result<DeleteScanTask>>().boxed(),
            FileIO::new_with_memory(),
            1024,
            1,
            1,
            TEST_LIMITS,
        )
        .await;
        assert!(result.is_ok());
    }

    /// Full end-to-end test for the append path.
    ///
    /// Exercises: `create_incremental_nested_pipeline` → `create_nested_pipeline`
    /// → `spawn_file_task` → `process_file` → `read_one_append_file` (wraps
    /// the task in a one-element stream and calls the `StreamsInto`
    /// machinery) → Arrow IPC serialization (spawn_blocking) → semaphore
    /// backpressure → `make_file_stream` (semaphore release).
    ///
    /// The delete stream is verified to be empty (no deletes were provided).
    #[tokio::test]
    async fn append_stream_reads_parquet_file() {
        let _guard = PIPELINE_TEST_LOCK.lock().await;
        let (file_io, path, file_size, schema) = write_test_parquet().await;
        let task = make_append_task(path, file_size, schema);

        let (append_stream, delete_stream_lock) = create_incremental_nested_pipeline(
            futures::stream::once(async { Ok(task) }).boxed(),
            futures::stream::empty::<iceberg::Result<DeleteScanTask>>().boxed(),
            file_io,
            1024,
            1,
            1,
            TEST_LIMITS,
        )
        .await
        .unwrap();

        // ── Append side: one FileScan with 3 rows ─────────────────────────
        let mut outer = append_stream.stream.lock().await;
        let file_scan = outer.next().await.unwrap().unwrap();
        assert_eq!(file_scan.filename, path);
        assert_eq!(file_scan.record_count, 3);
        assert!(outer.next().await.is_none(), "expected one file");

        let mut inner = file_scan.stream.stream.lock().await;
        let arrow_batch = inner.next().await.unwrap().unwrap();

        // Verify via Arrow C Data Interface pointers.
        assert!(!arrow_batch.schema.is_null());
        assert!(!arrow_batch.array.is_null());
        let n_rows = unsafe { *(arrow_batch.array as *const i64) } as usize;
        assert_eq!(n_rows, 3);
        unsafe {
            drop(Box::from_raw(
                arrow_batch.rust_ptr as *mut crate::table::ArrowBatchInner,
            ))
        };
        assert!(inner.next().await.is_none(), "expected one batch");

        // ── Delete side: empty (no deletes) ──────────────────────────────
        let mut delete_inner = delete_stream_lock.stream.lock().await;
        assert!(delete_inner.next().await.is_none(), "expected no deletes");
    }

    /// Full end-to-end test for the delete path.
    ///
    /// Uses the `DeletedFile` variant of `DeleteScanTask`.  Iceberg handles this
    /// variant by generating `(file_path, pos)` rows synthetically from
    /// `record_count` — it does NOT read the parquet file, so no file I/O is
    /// needed.  With `record_count=3` the delete stream emits three rows with
    /// positions 0, 1, 2.
    ///
    /// The append stream is verified to be empty (no appended files were provided).
    #[tokio::test]
    async fn delete_stream_yields_deleted_file_positions() {
        let _guard = PIPELINE_TEST_LOCK.lock().await;
        let (_, _, _, schema) = write_test_parquet().await;

        let delete_task = DeleteScanTask::DeletedFile(DeletedFileScanTask {
            base: BaseIncrementalFileScanTask {
                data_file_path: "memory:///some/data.parquet".to_string(),
                file_size_in_bytes: 0,
                start: 0,
                length: 0,
                record_count: Some(3), // → 3 (file, pos) rows: pos 0, 1, 2
                data_file_format: DataFileFormat::Parquet,
                schema,
                project_field_ids: vec![],
                partition: None,
                partition_spec: None,
                case_sensitive: false,
            },
        });

        let (append_stream, delete_arrow_stream) = create_incremental_nested_pipeline(
            futures::stream::empty::<iceberg::Result<AppendedFileScanTask>>().boxed(),
            futures::stream::once(async { Ok(delete_task) }).boxed(),
            FileIO::new_with_memory(),
            1024,
            1,
            1,
            TEST_LIMITS,
        )
        .await
        .unwrap();

        // ── Append side: empty ─────────────────────────────────────────────
        let mut outer = append_stream.stream.lock().await;
        assert!(outer.next().await.is_none(), "expected no appended files");

        // ── Delete side: 3 rows (one per position 0..2) ────────────────────
        let mut del = delete_arrow_stream.stream.lock().await;
        let mut total_rows = 0usize;
        while let Some(item) = del.next().await {
            total_rows += decode_batch_row_count(item.unwrap());
        }
        assert_eq!(total_rows, 3, "expected one row per deleted position");
    }
}
