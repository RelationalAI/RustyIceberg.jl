/// Writer support for iceberg_rust_ffi
///
/// This module provides FFI bindings for the iceberg-rust Writer API,
/// enabling Julia to write Arrow data to Parquet files and produce DataFiles
/// for use with the Transaction API.
use std::ffi::{c_char, c_void};
use std::io::Cursor;

use arrow_ipc::reader::StreamReader;
use iceberg::spec::DataFileFormat;
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// Compression codec values (must match Julia's CompressionCodec enum)
const COMPRESSION_UNCOMPRESSED: i32 = 0;
const COMPRESSION_SNAPPY: i32 = 1;
const COMPRESSION_GZIP: i32 = 2;
const COMPRESSION_LZ4: i32 = 3;
const COMPRESSION_ZSTD: i32 = 4;

/// Convert FFI compression code to parquet Compression
fn compression_from_code(code: i32) -> Compression {
    match code {
        COMPRESSION_UNCOMPRESSED => Compression::UNCOMPRESSED,
        COMPRESSION_SNAPPY => Compression::SNAPPY,
        COMPRESSION_GZIP => Compression::GZIP(Default::default()),
        COMPRESSION_LZ4 => Compression::LZ4,
        COMPRESSION_ZSTD => Compression::ZSTD(Default::default()),
        _ => Compression::SNAPPY, // Default to SNAPPY for unknown values
    }
}

use crate::response::IcebergBoxedResponse;
use crate::table::IcebergTable;
use crate::transaction::IcebergDataFiles;
use crate::util::parse_c_string;
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};

/// Type alias for the concrete DataFileWriter we use
type ConcreteDataFileWriter =
    DataFileWriter<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

/// Opaque writer handle for FFI
/// Holds the DataFileWriter which can write RecordBatches and produce DataFiles
pub struct IcebergDataFileWriter {
    writer: Option<ConcreteDataFileWriter>,
}

unsafe impl Send for IcebergDataFileWriter {}
unsafe impl Sync for IcebergDataFileWriter {}

/// Type alias for writer response
pub type IcebergDataFileWriterResponse = IcebergBoxedResponse<IcebergDataFileWriter>;

/// Type alias for data files response (returns IcebergDataFiles handle)
pub type IcebergWriterCloseResponse = IcebergBoxedResponse<IcebergDataFiles>;

/// Free a writer
#[no_mangle]
pub extern "C" fn iceberg_writer_free(writer: *mut IcebergDataFileWriter) {
    if !writer.is_null() {
        unsafe {
            let _ = Box::from_raw(writer);
        }
    }
}

// Create a new DataFileWriter from a table with configuration options
//
// This creates the full writer chain:
// ParquetWriterBuilder -> RollingFileWriterBuilder -> DataFileWriterBuilder -> build()
//
// Configuration options:
// - `prefix`: File name prefix (e.g., "data" produces files like "data-xxx.parquet")
// - `target_file_size_bytes`: Target size for rolling to a new file (0 = use default 512 MB)
// - `compression_codec`: Compression codec (0=UNCOMPRESSED, 1=SNAPPY, 2=GZIP, 3=LZ4, 4=ZSTD)
export_runtime_op!(
    iceberg_writer_new,
    IcebergDataFileWriterResponse,
    || {
        if table.is_null() {
            return Err(anyhow::anyhow!("Null table pointer provided"));
        }

        let prefix_str = parse_c_string(prefix, "prefix")?;
        let table_ref = unsafe { &*table };
        Ok((table_ref, prefix_str, target_file_size_bytes, compression_codec))
    },
    result_tuple,
    async {
        let (table_ref, prefix_str, target_file_size_bytes, compression_codec) = result_tuple;
        let table = &table_ref.table;

        // Create LocationGenerator from table metadata
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
            .map_err(|e| anyhow::anyhow!("Failed to create location generator: {}", e))?;

        // Create FileNameGenerator
        let file_name_generator = DefaultFileNameGenerator::new(
            prefix_str,
            None,
            DataFileFormat::Parquet,
        );

        // Create WriterProperties with compression
        let compression = compression_from_code(compression_codec);
        let writer_props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        // Create ParquetWriterBuilder with table schema and configured properties
        let parquet_writer_builder = ParquetWriterBuilder::new(
            writer_props,
            table.metadata().current_schema().clone(),
        );

        // Create RollingFileWriterBuilder with configured file size
        let rolling_file_writer_builder = if target_file_size_bytes > 0 {
            RollingFileWriterBuilder::new(
                parquet_writer_builder,
                target_file_size_bytes as usize,
                table.file_io().clone(),
                location_generator,
                file_name_generator,
            )
        } else {
            RollingFileWriterBuilder::new_with_default_file_size(
                parquet_writer_builder,
                table.file_io().clone(),
                location_generator,
                file_name_generator,
            )
        };

        // Create DataFileWriterBuilder
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);

        // Build the writer (no partition key for basic writes)
        let writer = data_file_writer_builder
            .build(None)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to build data file writer: {}", e))?;

        Ok::<IcebergDataFileWriter, anyhow::Error>(IcebergDataFileWriter {
            writer: Some(writer),
        })
    },
    table: *mut IcebergTable,
    prefix: *const c_char,
    target_file_size_bytes: i64,
    compression_codec: i32
);

// Write Arrow IPC data to the writer
//
// The data must be in Arrow IPC stream format (as produced by Arrow.tobuffer in Julia).
// This deserializes the IPC data to RecordBatch and writes it to the Parquet file.
export_runtime_op!(
    iceberg_writer_write,
    crate::IcebergResponse,
    || {
        if writer.is_null() {
            return Err(anyhow::anyhow!("Null writer pointer provided"));
        }
        if arrow_ipc_data.is_null() {
            return Err(anyhow::anyhow!("Null arrow_ipc_data pointer provided"));
        }
        if arrow_ipc_len == 0 {
            return Err(anyhow::anyhow!("Arrow IPC data length is zero"));
        }

        // Copy the IPC data into a Vec for safe use across await points
        let ipc_bytes = unsafe {
            std::slice::from_raw_parts(arrow_ipc_data, arrow_ipc_len).to_vec()
        };

        let writer_ref = unsafe { &mut *writer };
        Ok((writer_ref, ipc_bytes))
    },
    result_tuple,
    async {
        let (writer_ref, ipc_bytes) = result_tuple;

        // Deserialize Arrow IPC to RecordBatch
        let cursor = Cursor::new(ipc_bytes);
        let mut reader = StreamReader::try_new(cursor, None)
            .map_err(|e| anyhow::anyhow!("Failed to create Arrow IPC reader: {}", e))?;

        // Get the writer
        let writer = writer_ref
            .writer
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Writer has been closed"))?;

        // Read and write all batches from the IPC stream
        while let Some(batch_result) = reader.next() {
            let batch = batch_result
                .map_err(|e| anyhow::anyhow!("Failed to read Arrow IPC batch: {}", e))?;
            writer
                .write(batch)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to write batch: {}", e))?;
        }

        Ok::<(), anyhow::Error>(())
    },
    writer: *mut IcebergDataFileWriter,
    arrow_ipc_data: *const u8,
    arrow_ipc_len: usize
);

// Close the writer and return the produced DataFiles
//
// This flushes any remaining data, closes the Parquet file(s), and returns
// the DataFiles metadata that can be used with fast_append! in a Transaction.
//
// After calling this, the writer cannot be used again.
export_runtime_op!(
    iceberg_writer_close,
    IcebergWriterCloseResponse,
    || {
        if writer.is_null() {
            return Err(anyhow::anyhow!("Null writer pointer provided"));
        }
        let writer_ref = unsafe { &mut *writer };
        Ok(writer_ref)
    },
    writer_ref,
    async {
        // Take the writer out
        let mut writer = writer_ref
            .writer
            .take()
            .ok_or_else(|| anyhow::anyhow!("Writer has already been closed"))?;

        // Close the writer to get the DataFiles
        let data_files = writer
            .close()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to close writer: {}", e))?;

        Ok::<IcebergDataFiles, anyhow::Error>(IcebergDataFiles { data_files })
    },
    writer: *mut IcebergDataFileWriter
);
