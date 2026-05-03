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

use crate::ordered_file_pipeline::{
    BufferedBatch, FileScan, MAX_FILE_CONCURRENCY, run_nested_pipeline,
};
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
    let batch_stream = read_one_append_file(reader, task)
        .map_err(|e| unexpected(format!("reader setup: {e}")))?;
    tokio::pin!(batch_stream);

    loop {
        let batch = match batch_stream.next().await {
            Some(Ok(b)) => b,
            Some(Err(e)) => return Err(e),
            None => break,
        };

        let serialized =
            tokio::task::spawn_blocking(move || crate::serialize_record_batch(batch))
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
        (String, i64, mpsc::Receiver<Result<BufferedBatch, iceberg::Error>>),
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
    if concurrency > MAX_FILE_CONCURRENCY {
        anyhow::bail!("file concurrency {concurrency} exceeds hard cap {MAX_FILE_CONCURRENCY}");
    }

    let reader = build_reader(file_io.clone(), batch_size);

    let (tx, rx) = mpsc::channel::<Result<FileScan, iceberg::Error>>(prefetch_depth);
    tokio::spawn(run_incremental_nested(append_tasks, reader, concurrency, tx));

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
