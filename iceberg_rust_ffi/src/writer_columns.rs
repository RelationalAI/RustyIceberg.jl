/// Column-based writer support for iceberg_rust_ffi
///
/// This module provides FFI bindings for writing raw column data directly to Parquet,
/// avoiding the overhead of Arrow IPC serialization. Julia passes raw column pointers
/// and metadata, and Rust builds Arrow arrays directly from them.
use std::ffi::c_void;
use std::sync::Arc;

use arrow_array::{
    types::{Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType},
    ArrayRef, BooleanArray, PrimitiveArray, RecordBatch, StringArray,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};

use crate::writer::IcebergDataFileWriter;
use iceberg::writer::IcebergWriter;
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};

/// Column type codes (must match Julia's ColumnType enum)
pub const COLUMN_TYPE_INT32: i32 = 0;
pub const COLUMN_TYPE_INT64: i32 = 1;
pub const COLUMN_TYPE_FLOAT32: i32 = 2;
pub const COLUMN_TYPE_FLOAT64: i32 = 3;
pub const COLUMN_TYPE_STRING: i32 = 4;
pub const COLUMN_TYPE_DATE: i32 = 5;
pub const COLUMN_TYPE_TIMESTAMP: i32 = 6;
pub const COLUMN_TYPE_BOOLEAN: i32 = 7;
pub const COLUMN_TYPE_UUID: i32 = 8;

/// Descriptor for a single column passed from Julia
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ColumnDescriptor {
    /// Pointer to the raw data (interpretation depends on column_type)
    pub data_ptr: *const c_void,
    /// For string columns: pointer to offsets array (Int64)
    pub offsets_ptr: *const i64,
    /// Number of rows
    pub num_rows: usize,
    /// Column type (see COLUMN_TYPE_* constants)
    pub column_type: i32,
    /// Whether this column is nullable
    pub is_nullable: bool,
    /// Pointer to validity bitmap (only if is_nullable is true)
    /// Each byte is 0 (null) or 1 (valid) - we convert to Arrow's bit-packed format
    pub validity_ptr: *const u8,
}

unsafe impl Send for ColumnDescriptor {}
unsafe impl Sync for ColumnDescriptor {}

/// Build an Arrow array from a ColumnDescriptor
unsafe fn build_arrow_array(desc: &ColumnDescriptor) -> Result<ArrayRef, anyhow::Error> {
    let null_buffer = if desc.is_nullable && !desc.validity_ptr.is_null() {
        // Convert Julia's Bool vector to Arrow's null buffer
        // Julia stores bools as bytes (0 or 1), Arrow expects a bit-packed buffer
        let validity_slice = std::slice::from_raw_parts(desc.validity_ptr, desc.num_rows);
        let mut bits = vec![0u8; (desc.num_rows + 7) / 8];
        for (i, &valid) in validity_slice.iter().enumerate() {
            if valid != 0 {
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        Some(NullBuffer::new(BooleanBuffer::new(
            Buffer::from(bits),
            0,
            desc.num_rows,
        )))
    } else {
        None
    };

    let array: ArrayRef = match desc.column_type {
        COLUMN_TYPE_INT32 => {
            let data = std::slice::from_raw_parts(desc.data_ptr as *const i32, desc.num_rows);
            let buffer = ScalarBuffer::from(data.to_vec());
            Arc::new(PrimitiveArray::<Int32Type>::new(buffer, null_buffer))
        }
        COLUMN_TYPE_INT64 => {
            let data = std::slice::from_raw_parts(desc.data_ptr as *const i64, desc.num_rows);
            let buffer = ScalarBuffer::from(data.to_vec());
            Arc::new(PrimitiveArray::<Int64Type>::new(buffer, null_buffer))
        }
        COLUMN_TYPE_FLOAT32 => {
            let data = std::slice::from_raw_parts(desc.data_ptr as *const f32, desc.num_rows);
            let buffer = ScalarBuffer::from(data.to_vec());
            Arc::new(PrimitiveArray::<Float32Type>::new(buffer, null_buffer))
        }
        COLUMN_TYPE_FLOAT64 => {
            let data = std::slice::from_raw_parts(desc.data_ptr as *const f64, desc.num_rows);
            let buffer = ScalarBuffer::from(data.to_vec());
            Arc::new(PrimitiveArray::<Float64Type>::new(buffer, null_buffer))
        }
        COLUMN_TYPE_DATE => {
            // Date is stored as Int32 (days since epoch)
            let data = std::slice::from_raw_parts(desc.data_ptr as *const i32, desc.num_rows);
            let buffer = ScalarBuffer::from(data.to_vec());
            Arc::new(PrimitiveArray::<Date32Type>::new(buffer, null_buffer))
        }
        COLUMN_TYPE_TIMESTAMP => {
            // Timestamp is stored as Int64 (microseconds since epoch)
            let data = std::slice::from_raw_parts(desc.data_ptr as *const i64, desc.num_rows);
            let buffer = ScalarBuffer::from(data.to_vec());
            Arc::new(
                PrimitiveArray::<TimestampMicrosecondType>::new(buffer, null_buffer)
                    .with_timezone("UTC"),
            )
        }
        COLUMN_TYPE_BOOLEAN => {
            let data = std::slice::from_raw_parts(desc.data_ptr as *const u8, desc.num_rows);
            // Convert bytes to boolean buffer
            let mut bits = vec![0u8; (desc.num_rows + 7) / 8];
            for (i, &val) in data.iter().enumerate() {
                if val != 0 {
                    bits[i / 8] |= 1 << (i % 8);
                }
            }
            let values = BooleanBuffer::new(Buffer::from(bits), 0, desc.num_rows);
            Arc::new(BooleanArray::new(values, null_buffer))
        }
        COLUMN_TYPE_STRING => {
            // String data is passed as:
            // - data_ptr: pointer to concatenated UTF-8 bytes
            // - offsets_ptr: pointer to Int64 offsets array (length = num_rows + 1)
            if desc.offsets_ptr.is_null() {
                return Err(anyhow::anyhow!("String column requires offsets"));
            }
            let offsets = std::slice::from_raw_parts(desc.offsets_ptr, desc.num_rows + 1);
            let total_bytes = offsets[desc.num_rows] as usize;
            let bytes = std::slice::from_raw_parts(desc.data_ptr as *const u8, total_bytes);

            // Build strings from offsets
            let mut strings: Vec<Option<&str>> = Vec::with_capacity(desc.num_rows);
            for i in 0..desc.num_rows {
                let is_null: bool = if let Some(ref nb) = null_buffer {
                    nb.is_null(i)
                } else {
                    false
                };
                if is_null {
                    strings.push(None);
                } else {
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    let s = std::str::from_utf8(&bytes[start..end])
                        .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in string column: {}", e))?;
                    strings.push(Some(s));
                }
            }
            Arc::new(StringArray::from(strings))
        }
        COLUMN_TYPE_UUID => {
            // UUID is stored as 16 bytes (UInt128 in Julia)
            // Store as fixed-size binary (16 bytes per value)
            let data = std::slice::from_raw_parts(desc.data_ptr as *const u8, desc.num_rows * 16);

            // Build the array using the builder or from_iter_values
            let values: Vec<&[u8]> = data.chunks(16).collect();
            Arc::new(
                arrow_array::FixedSizeBinaryArray::try_from_iter(values.into_iter())
                    .map_err(|e| anyhow::anyhow!("Failed to create UUID array: {}", e))?,
            )
        }
        _ => {
            return Err(anyhow::anyhow!("Unknown column type: {}", desc.column_type));
        }
    };

    Ok(array)
}

// Write columns directly to the Parquet writer.
// Accepts an array of ColumnDescriptors and builds a RecordBatch from them,
// then writes to the underlying Parquet writer.
// The caller must ensure all pointers are valid and point to appropriately sized data.
export_runtime_op!(
    iceberg_writer_write_columns,
    crate::IcebergResponse,
    || {
        if writer.is_null() {
            return Err(anyhow::anyhow!("Null writer pointer provided"));
        }
        if columns.is_null() || num_columns == 0 {
            return Err(anyhow::anyhow!("No columns provided"));
        }

        // Copy column descriptors for safe use across await
        let cols: Vec<ColumnDescriptor> = unsafe {
            std::slice::from_raw_parts(columns, num_columns).to_vec()
        };

        let writer_ref = unsafe { &mut *writer };
        Ok((writer_ref, cols))
    },
    result_tuple,
    async {
        let (writer_ref, cols) = result_tuple;

        // Get the writer's schema (stored when writer was created)
        let arrow_schema = writer_ref.arrow_schema.clone();

        // Validate column count matches schema
        if cols.len() != arrow_schema.fields().len() {
            return Err(anyhow::anyhow!(
                "Column count mismatch: got {} columns but schema has {} fields",
                cols.len(),
                arrow_schema.fields().len()
            ));
        }

        // Get the writer
        let iceberg_writer = writer_ref
            .writer
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Writer has been closed"))?;

        // Build Arrow arrays from column descriptors
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());

        for desc in cols.iter() {
            let array = unsafe { build_arrow_array(desc)? };
            arrays.push(array);
        }

        // Create record batch using the table's Arrow schema (with proper field IDs)
        let batch = RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))?;

        // Write the batch
        iceberg_writer
            .write(batch)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write batch: {}", e))?;

        Ok::<(), anyhow::Error>(())
    },
    writer: *mut IcebergDataFileWriter,
    columns: *const ColumnDescriptor,
    num_columns: usize
);
