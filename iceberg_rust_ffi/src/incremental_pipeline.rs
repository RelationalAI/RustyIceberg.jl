//! Per-file nested pipeline for incremental append tasks.
//!
//! Mirrors `ordered_file_pipeline` but consumes a streaming
//! `BoxStream<AppendedFileScanTask>` (from `plan_files()`) instead of a
//! pre-collected `Vec<FileScanTask>`, and uses the `StreamsInto` machinery to
//! open each append file individually.
//!
//! Returns `(IcebergFileScanStream, IcebergArrowStream)`:
//!   - append stream: one `FileScan` per parquet file, in manifest order
//!   - delete stream: flat Arrow stream of delete records

use std::sync::Arc;

use futures::StreamExt;
use iceberg::arrow::{
    ArrowReader, ArrowReaderBuilder, StreamsInto, UnzippedIncrementalBatchRecordStream,
};
use iceberg::io::FileIO;
use iceberg::scan::incremental::{AppendedFileScanTask, DeleteScanTask};
use tokio::sync::{mpsc, Mutex as AsyncMutex, Semaphore};

use crate::ordered_file_pipeline::{run_nested_pipeline, BufferedBatch, FileScan};
use crate::pipeline_stats::MAX_BUFFERED_BYTES_PER_TASK;
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
    let (arrow_stream, _): UnzippedIncrementalBatchRecordStream =
        StreamsInto::<ArrowReader, UnzippedIncrementalBatchRecordStream>::stream(
            (append, delete),
            reader,
        )?;
    Ok(arrow_stream)
}

/// Build an `ArrowReader` from a `FileIO` and optional batch size, with
/// per-file concurrency pinned to 1 (each reader handles one file).
fn build_reader(file_io: FileIO, batch_size: Option<usize>) -> ArrowReader {
    let mut b = ArrowReaderBuilder::new(file_io).with_data_file_concurrency_limit(1);
    if let Some(bs) = batch_size {
        b = b.with_batch_size(bs);
    }
    b.build()
}

/// Resolve and read a single append file into the per-file channel.
/// Dropping `tx` signals the consumer that this file is done.
async fn process_incremental_file(
    task: AppendedFileScanTask,
    reader: ArrowReader,
    semaphore: Arc<Semaphore>,
    tx: mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) {
    let result = process_incremental_file_inner(task, reader, &semaphore, &tx).await;
    if let Err(e) = result {
        let _ = tx.send(Err(e)).await;
    }
}

async fn process_incremental_file_inner(
    task: AppendedFileScanTask,
    reader: ArrowReader,
    semaphore: &Arc<Semaphore>,
    tx: &mpsc::Sender<Result<BufferedBatch, iceberg::Error>>,
) -> Result<(), iceberg::Error> {
    let batch_stream =
        read_one_append_file(reader, task).map_err(|e| unexpected(format!("reader setup: {e}")))?;
    tokio::pin!(batch_stream);

    loop {
        let batch = match batch_stream.next().await {
            Some(Ok(b)) => b,
            Some(Err(e)) => return Err(e),
            None => break,
        };

        let serialized = {
            let (stx, srx) = tokio::sync::oneshot::channel();
            crate::ordered_file_pipeline::SERIALIZE_POOL.spawn(move || {
                let _ = stx.send(crate::serialize_record_batch(batch));
            });
            srx
        }
        .await
        .map_err(|e| unexpected(format!("serialize panicked: {e}")))?
        .map_err(unexpected)?;

        let byte_len = serialized.length;

        let _permit = semaphore
            .acquire_many(byte_len as u32)
            .await
            .map_err(|e| unexpected(format!("semaphore: {e}")))?;
        std::mem::forget(_permit);

        if tx
            .send(Ok(BufferedBatch {
                batch: serialized,
                byte_len,
                semaphore: semaphore.clone(),
            }))
            .await
            .is_err()
        {
            return Ok(());
        }
    }
    Ok(())
}

/// Spawn one file task. Returns a future that resolves immediately to
/// `(filename, record_count, file_rx)` so `FuturesUnordered` can poll it
/// alongside other tasks while the actual I/O runs in the background.
fn spawn_incremental_file_task(
    task: AppendedFileScanTask,
    reader: ArrowReader,
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
    let filename = task.base.data_file_path.clone();
    let record_count = task.base.record_count.unwrap_or(0) as i64;
    let sem = Arc::new(Semaphore::new(MAX_BUFFERED_BYTES_PER_TASK));
    let (file_tx, file_rx) = mpsc::channel(8);
    tokio::spawn(process_incremental_file(task, reader, sem, file_tx));
    async move { Ok((filename, record_count, file_rx)) }
}

/// Keep `concurrency` append file tasks in flight, yielding each as a
/// `FileScan` to `tx` in manifest order. Delegates to the shared
/// `run_nested_pipeline` orchestrator.
async fn run_incremental_nested(
    append_tasks: futures::stream::BoxStream<'static, iceberg::Result<AppendedFileScanTask>>,
    reader: ArrowReader,
    concurrency: usize,
    tx: mpsc::Sender<Result<FileScan, iceberg::Error>>,
) {
    run_nested_pipeline(
        append_tasks,
        concurrency,
        tx,
        move |task| spawn_incremental_file_task(task, reader.clone()),
        |_| {},
    )
    .await;
}

/// Build the nested incremental pipeline.
///
/// Returns `(append_stream, delete_stream)`:
/// - `append_stream`: `IcebergFileScanStream` — one `FileScan` per appended
///   parquet file, in manifest order, with a prefetched inner batch stream.
/// - `delete_stream`: flat `IcebergArrowStream` of delete records.
pub async fn create_incremental_nested_pipeline(
    append_tasks: futures::stream::BoxStream<'static, iceberg::Result<AppendedFileScanTask>>,
    delete_tasks: futures::stream::BoxStream<'static, iceberg::Result<DeleteScanTask>>,
    file_io: FileIO,
    batch_size: Option<usize>,
    concurrency: usize,
    prefetch_depth: usize,
    serialization_concurrency: usize,
) -> anyhow::Result<(IcebergFileScanStream, IcebergArrowStream)> {
    let reader = build_reader(file_io.clone(), batch_size);

    let (tx, rx) = mpsc::channel::<Result<FileScan, iceberg::Error>>(prefetch_depth);
    tokio::spawn(run_incremental_nested(
        append_tasks,
        reader,
        concurrency,
        tx,
    ));

    let append_stream = IcebergFileScanStream {
        stream: AsyncMutex::new(
            futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            })
            .boxed(),
        ),
    };

    // Delete stream: StreamsInto with empty append stream routes all delete
    // tasks through the iceberg reader machinery.
    let (_, delete_arrow): UnzippedIncrementalBatchRecordStream =
        StreamsInto::<ArrowReader, UnzippedIncrementalBatchRecordStream>::stream(
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
    use arrow_ipc::reader::StreamReader;
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

    /// Decode an `ArrowBatch` (Arrow IPC bytes) and return the row count.
    /// Frees the underlying Vec<u8> allocation before returning.
    fn decode_batch_row_count(batch: crate::table::ArrowBatch) -> usize {
        let data = unsafe { std::slice::from_raw_parts(batch.data, batch.length) };
        let total = StreamReader::try_new(std::io::Cursor::new(data), None)
            .unwrap()
            .filter_map(|r| r.ok())
            .map(|b| b.num_rows())
            .sum();
        unsafe { drop(Box::from_raw(batch.rust_ptr as *mut Vec<u8>)) };
        total
    }

    // ── Tests ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn empty_streams_succeed() {
        // Baseline: both append and delete streams empty → pipeline returns Ok
        // with two empty streams; no I/O or background tasks are spawned.
        let result = create_incremental_nested_pipeline(
            futures::stream::empty::<iceberg::Result<AppendedFileScanTask>>().boxed(),
            futures::stream::empty::<iceberg::Result<DeleteScanTask>>().boxed(),
            FileIO::new_with_memory(),
            None,
            1,
            1,
            1,
        )
        .await;
        assert!(result.is_ok());
    }

    /// Full end-to-end test for the append path.
    ///
    /// Exercises: `run_incremental_nested` → `spawn_incremental_file_task` →
    /// `process_incremental_file_inner` → `read_one_append_file` (wraps the
    /// task in a one-element stream and calls the `StreamsInto` machinery) →
    /// Arrow IPC serialization (spawn_blocking) → semaphore backpressure →
    /// `make_file_stream` semaphore release.
    ///
    /// The delete stream is verified to be empty (no deletes were provided).
    #[tokio::test]
    async fn append_stream_reads_parquet_file() {
        let (file_io, path, file_size, schema) = write_test_parquet().await;
        let task = make_append_task(path, file_size, schema);

        let (append_stream, delete_stream_lock) = create_incremental_nested_pipeline(
            futures::stream::once(async { Ok(task) }).boxed(),
            futures::stream::empty::<iceberg::Result<DeleteScanTask>>().boxed(),
            file_io,
            None,
            1,
            1,
            1,
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

        // Decode and verify the row values.
        let data = unsafe { std::slice::from_raw_parts(arrow_batch.data, arrow_batch.length) };
        let decoded = StreamReader::try_new(std::io::Cursor::new(data), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(decoded.num_rows(), 3);
        let id_col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.values(), &[10, 20, 30]);
        unsafe { drop(Box::from_raw(arrow_batch.rust_ptr as *mut Vec<u8>)) };
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
            None,
            1,
            1,
            1,
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
