/// Column-based writer support for iceberg_rust_ffi
///
/// This module provides FFI bindings for writing raw column data directly to Parquet,
/// avoiding the overhead of Arrow IPC serialization. Julia passes raw column pointers
/// and metadata, and Rust builds Arrow arrays directly from them.
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow_array::{
    types::{
        Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type,
        TimestampMicrosecondType,
    },
    ArrayRef, BooleanArray, PrimitiveArray, RecordBatch, StringArray,
};
use arrow_buffer::{ArrowNativeType, BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};

/// No-op allocator: Arrow buffers created from Julia memory must not be freed by Rust.
/// Julia owns the memory until the per-batch oneshot ACK is received (write_columns path).
struct NoopAllocation;

/// Build a zero-copy `ScalarBuffer<T>` pointing directly into Julia-owned memory.
/// Safety: `ptr` must be non-null, valid, and alive until the buffer is dropped.
unsafe fn zero_copy_scalar_buffer<T: ArrowNativeType>(
    ptr: *const T,
    num_rows: usize,
) -> ScalarBuffer<T> {
    let byte_len = num_rows * std::mem::size_of::<T>();
    let nn_ptr = NonNull::new_unchecked(ptr as *mut u8);
    let buf = Buffer::from_custom_allocation(nn_ptr, byte_len, Arc::new(NoopAllocation));
    ScalarBuffer::new(buf, 0, num_rows)
}
use arrow_schema::DataType;

use crate::writer::IcebergDataFileWriter;
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
pub const COLUMN_TYPE_TIMESTAMPTZ: i32 = 9;
/// Decimal backed by Int32 (precision ≤ 9): data is i32[] scaled integers
pub const COLUMN_TYPE_DECIMAL_INT32: i32 = 10;
/// Decimal backed by Int64 (precision ≤ 18): data is i64[] scaled integers
pub const COLUMN_TYPE_DECIMAL_INT64: i32 = 11;
/// Decimal backed by Int128 (precision > 18): data is i128[] scaled integers
pub const COLUMN_TYPE_DECIMAL_INT128: i32 = 12;

/// Descriptor for a single column passed from Julia
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ColumnDescriptor {
    /// Pointer to the raw data (interpretation depends on column_type)
    /// For strings: pointer to array of string pointers (Ptr{UInt8}[])
    pub data_ptr: *const c_void,
    /// For string columns: pointer to lengths array (Int64[])
    /// For other types: unused (C_NULL)
    pub lengths_ptr: *const i64,
    /// Pointer to validity bitmap (only if is_nullable is true)
    /// Points to bit-packed data from Julia's BitVector.chunks (UInt64 array)
    /// Bit i is 1 if row i is valid, 0 if null
    pub validity_ptr: *const u8,
    /// Number of rows
    pub num_rows: usize,
    /// Column type (see COLUMN_TYPE_* constants)
    pub column_type: i32,
    /// Whether this column is nullable
    pub is_nullable: bool,
}

unsafe impl Send for ColumnDescriptor {}
unsafe impl Sync for ColumnDescriptor {}

/// Build an Arrow array from a ColumnDescriptor and its corresponding schema field.
/// The schema field is used to extract precision/scale for decimal columns.
unsafe fn build_arrow_array(
    desc: &ColumnDescriptor,
    schema_field: &arrow_schema::Field,
) -> Result<ArrayRef, anyhow::Error> {
    let null_buffer = if desc.is_nullable && !desc.validity_ptr.is_null() {
        // Julia's BitVector uses the same little-endian bit-packed layout as Arrow.
        // Point directly into Julia's buffer — no copy needed.
        let num_bytes = (desc.num_rows + 7) / 8;
        let nn_ptr = NonNull::new_unchecked(desc.validity_ptr as *mut u8);
        let buf = Buffer::from_custom_allocation(nn_ptr, num_bytes, Arc::new(NoopAllocation));
        Some(NullBuffer::new(BooleanBuffer::new(buf, 0, desc.num_rows)))
    } else {
        None
    };

    let array: ArrayRef = match desc.column_type {
        COLUMN_TYPE_INT32 => {
            let buffer = zero_copy_scalar_buffer(desc.data_ptr as *const i32, desc.num_rows);
            Arc::new(PrimitiveArray::<Int32Type>::new(buffer, null_buffer))
        }
        COLUMN_TYPE_INT64 => {
            let buffer = zero_copy_scalar_buffer(desc.data_ptr as *const i64, desc.num_rows);
            Arc::new(PrimitiveArray::<Int64Type>::new(buffer, null_buffer))
        }
        COLUMN_TYPE_FLOAT32 => {
            let buffer = zero_copy_scalar_buffer(desc.data_ptr as *const f32, desc.num_rows);
            Arc::new(PrimitiveArray::<Float32Type>::new(buffer, null_buffer))
        }
        COLUMN_TYPE_FLOAT64 => {
            let buffer = zero_copy_scalar_buffer(desc.data_ptr as *const f64, desc.num_rows);
            Arc::new(PrimitiveArray::<Float64Type>::new(buffer, null_buffer))
        }
        COLUMN_TYPE_DATE => {
            // Date is stored as Int32 (days since epoch)
            let buffer = zero_copy_scalar_buffer(desc.data_ptr as *const i32, desc.num_rows);
            Arc::new(PrimitiveArray::<Date32Type>::new(buffer, null_buffer))
        }
        COLUMN_TYPE_TIMESTAMP => {
            // Timestamp without timezone (Iceberg `timestamp`)
            // Stored as Int64 microseconds since epoch
            let buffer = zero_copy_scalar_buffer(desc.data_ptr as *const i64, desc.num_rows);
            Arc::new(PrimitiveArray::<TimestampMicrosecondType>::new(
                buffer,
                null_buffer,
            ))
        }
        COLUMN_TYPE_TIMESTAMPTZ => {
            // Timestamp with UTC timezone (Iceberg `timestamptz`)
            // Stored as Int64 microseconds since epoch, with timezone metadata
            let buffer = zero_copy_scalar_buffer(desc.data_ptr as *const i64, desc.num_rows);
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
            // String data passed from Julia:
            // - data_ptr: pointer to array of string pointers (each pointing to UTF-8 bytes)
            // - lengths_ptr: pointer to array of string lengths (Int64)
            // Note: While we avoid copying on the Julia side (just passing pointers),
            // Arrow's StringArray copies the data into its own contiguous buffer below.
            if desc.lengths_ptr.is_null() {
                return Err(anyhow::anyhow!("String column requires lengths"));
            }
            let str_ptrs =
                std::slice::from_raw_parts(desc.data_ptr as *const *const u8, desc.num_rows);
            let str_lens = std::slice::from_raw_parts(desc.lengths_ptr, desc.num_rows);

            // Build string references, then Arrow copies them into its internal buffer
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
                    let ptr = str_ptrs[i];
                    let len = str_lens[i] as usize;
                    let bytes = std::slice::from_raw_parts(ptr, len);
                    let s = std::str::from_utf8(bytes)
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
        COLUMN_TYPE_DECIMAL_INT32 | COLUMN_TYPE_DECIMAL_INT64 | COLUMN_TYPE_DECIMAL_INT128 => {
            // All decimal variants map to Arrow Decimal128. Precision and scale come from
            // the schema field (set when the Iceberg table was created).
            let (precision, scale) = match schema_field.data_type() {
                DataType::Decimal128(p, s) => (*p, *s),
                dt => {
                    return Err(anyhow::anyhow!(
                        "Expected Decimal128 schema field for decimal column, got {:?}",
                        dt
                    ))
                }
            };

            // All decimal variants produce an Arrow Decimal128 (i128 backing).
            // INT32/INT64 variants need widening — materialize to avoid a cast loop
            // in the zero-copy path; INT128 shares the same layout and is zero-copy.
            let buffer: ScalarBuffer<i128> = match desc.column_type {
                COLUMN_TYPE_DECIMAL_INT32 => {
                    let data =
                        std::slice::from_raw_parts(desc.data_ptr as *const i32, desc.num_rows);
                    ScalarBuffer::from(data.iter().map(|&v| v as i128).collect::<Vec<_>>())
                }
                COLUMN_TYPE_DECIMAL_INT64 => {
                    let data =
                        std::slice::from_raw_parts(desc.data_ptr as *const i64, desc.num_rows);
                    ScalarBuffer::from(data.iter().map(|&v| v as i128).collect::<Vec<_>>())
                }
                _ => {
                    // COLUMN_TYPE_DECIMAL_INT128: Julia Int128 and Rust i128 share the same
                    // 16-byte little-endian layout on all supported platforms — zero-copy.
                    zero_copy_scalar_buffer(desc.data_ptr as *const i128, desc.num_rows)
                }
            };
            Arc::new(
                PrimitiveArray::<Decimal128Type>::new(buffer, null_buffer)
                    .with_precision_and_scale(precision, scale)
                    .map_err(|e| anyhow::anyhow!("Failed to set decimal precision/scale: {}", e))?,
            )
        }
        _ => {
            return Err(anyhow::anyhow!("Unknown column type: {}", desc.column_type));
        }
    };

    Ok(array)
}

// =============================================================================
// Scattered-gather writer: pass raw source pointers + selection indices to Rust,
// which gathers the data directly into Arrow arrays — eliminating the Julia-side
// staging copy for non-converting numeric column types.
// =============================================================================

/// A reference to one slice of source column data.
/// `sel_ptr = null`  → sequential (identity) access: read data[0..len].
/// `sel_ptr != null` → scattered access: read data[sel[i]-1] for i in 0..len (1-based Julia indices).
/// `validity_ptr = null` → all rows valid (non-nullable or known all-valid slice).
/// `lengths_ptr != null` → string column: data_ptr is Ptr{UInt8}[], lengths_ptr is Int64[].
/// Fields are all 8 bytes — no padding, total 40 bytes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SliceRef {
    pub data_ptr: *const c_void,
    pub lengths_ptr: *const i64,
    pub validity_ptr: *const u8,
    pub sel_ptr: *const i64,
    pub len: usize,
}

unsafe impl Send for SliceRef {}
unsafe impl Sync for SliceRef {}

/// Gathered column descriptor: gather `num_slices` SliceRefs into one Arrow column.
/// `total_rows` must equal the sum of all `slice.len` values.
/// Fields ordered largest-to-smallest; 3 bytes trailing padding → 32 bytes total.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GatheredColumnDescriptor {
    pub slices: *const SliceRef,
    pub num_slices: usize,
    pub total_rows: usize,
    pub column_type: i32,
    pub is_nullable: bool,
}

unsafe impl Send for GatheredColumnDescriptor {}
unsafe impl Sync for GatheredColumnDescriptor {}

/// Build the Arrow null buffer by scanning validity bits across all slices.
/// Returns `None` if every slice has a null `validity_ptr` (all rows valid).
unsafe fn build_null_buffer_scattered(slices: &[SliceRef], total_rows: usize) -> Option<NullBuffer> {
    if !slices.iter().any(|s| !s.validity_ptr.is_null()) {
        return None;
    }
    let mut bits = vec![0u8; (total_rows + 7) / 8];
    let mut out = 0usize;
    for slice in slices {
        if slice.validity_ptr.is_null() {
            // All valid — set bits 1
            for i in 0..slice.len {
                bits[(out + i) / 8] |= 1u8 << ((out + i) % 8);
            }
        } else if slice.sel_ptr.is_null() {
            // Sequential: copy bits with possible alignment shift
            for i in 0..slice.len {
                let b = (*slice.validity_ptr.add(i / 8) >> (i % 8)) & 1;
                bits[(out + i) / 8] |= b << ((out + i) % 8);
            }
        } else {
            // Scattered: gather validity bits via selection indices (1-based)
            let sel = std::slice::from_raw_parts(slice.sel_ptr, slice.len);
            for (i, &idx) in sel.iter().enumerate() {
                let src = (idx - 1) as usize;
                let b = (*slice.validity_ptr.add(src / 8) >> (src % 8)) & 1;
                bits[(out + i) / 8] |= b << ((out + i) % 8);
            }
        }
        out += slice.len;
    }
    Some(NullBuffer::new(BooleanBuffer::new(Buffer::from(bits), 0, total_rows)))
}

/// Gather all slices for a column into an Arrow array.
pub(crate) unsafe fn build_arrow_array_scattered(
    desc: &GatheredColumnDescriptor,
    schema_field: &arrow_schema::Field,
) -> Result<ArrayRef, anyhow::Error> {
    let slices = std::slice::from_raw_parts(desc.slices, desc.num_slices);
    let total = desc.total_rows;
    let null_buf = if desc.is_nullable {
        build_null_buffer_scattered(slices, total)
    } else {
        None
    };

    // Macro gathers a primitive numeric type from all slices.
    // sel_ptr=null → sequential copy; sel_ptr!=null → indirect gather (1-based indices).
    macro_rules! gather_primitive {
        ($T:ty, $ArrowType:ty) => {{
            let mut values = Vec::<$T>::with_capacity(total);
            for slice in slices {
                let src = slice.data_ptr as *const $T;
                if slice.sel_ptr.is_null() {
                    values.extend_from_slice(std::slice::from_raw_parts(src, slice.len));
                } else {
                    for &idx in std::slice::from_raw_parts(slice.sel_ptr, slice.len) {
                        values.push(*src.add((idx - 1) as usize));
                    }
                }
            }
            Arc::new(PrimitiveArray::<$ArrowType>::new(ScalarBuffer::from(values), null_buf))
                as ArrayRef
        }};
    }

    let array: ArrayRef = match desc.column_type {
        COLUMN_TYPE_INT32 => gather_primitive!(i32, Int32Type),
        COLUMN_TYPE_INT64 => gather_primitive!(i64, Int64Type),
        COLUMN_TYPE_FLOAT32 => gather_primitive!(f32, Float32Type),
        COLUMN_TYPE_FLOAT64 => gather_primitive!(f64, Float64Type),
        COLUMN_TYPE_DATE => gather_primitive!(i32, Date32Type),
        COLUMN_TYPE_TIMESTAMP => {
            let mut values = Vec::<i64>::with_capacity(total);
            for slice in slices {
                let src = slice.data_ptr as *const i64;
                if slice.sel_ptr.is_null() {
                    values.extend_from_slice(std::slice::from_raw_parts(src, slice.len));
                } else {
                    for &idx in std::slice::from_raw_parts(slice.sel_ptr, slice.len) {
                        values.push(*src.add((idx - 1) as usize));
                    }
                }
            }
            Arc::new(PrimitiveArray::<TimestampMicrosecondType>::new(
                ScalarBuffer::from(values),
                null_buf,
            ))
        }
        COLUMN_TYPE_TIMESTAMPTZ => {
            let mut values = Vec::<i64>::with_capacity(total);
            for slice in slices {
                let src = slice.data_ptr as *const i64;
                if slice.sel_ptr.is_null() {
                    values.extend_from_slice(std::slice::from_raw_parts(src, slice.len));
                } else {
                    for &idx in std::slice::from_raw_parts(slice.sel_ptr, slice.len) {
                        values.push(*src.add((idx - 1) as usize));
                    }
                }
            }
            Arc::new(
                PrimitiveArray::<TimestampMicrosecondType>::new(ScalarBuffer::from(values), null_buf)
                    .with_timezone("UTC"),
            )
        }
        COLUMN_TYPE_BOOLEAN => {
            let mut bits = vec![0u8; (total + 7) / 8];
            let mut out = 0usize;
            for slice in slices {
                let src = slice.data_ptr as *const u8;
                if slice.sel_ptr.is_null() {
                    let data = std::slice::from_raw_parts(src, slice.len);
                    for (i, &v) in data.iter().enumerate() {
                        if v != 0 { bits[(out + i) / 8] |= 1 << ((out + i) % 8); }
                    }
                } else {
                    for (i, &idx) in std::slice::from_raw_parts(slice.sel_ptr, slice.len).iter().enumerate() {
                        if *src.add((idx - 1) as usize) != 0 {
                            bits[(out + i) / 8] |= 1 << ((out + i) % 8);
                        }
                    }
                }
                out += slice.len;
            }
            Arc::new(BooleanArray::new(BooleanBuffer::new(Buffer::from(bits), 0, total), null_buf))
        }
        COLUMN_TYPE_STRING => {
            // Strings are always pre-staged in Julia; data_ptr = *const *const u8,
            // lengths_ptr = *const i64, sel_ptr is always null (identity).
            let mut all_strings: Vec<Option<&str>> = Vec::with_capacity(total);
            for slice in slices {
                if slice.lengths_ptr.is_null() {
                    return Err(anyhow::anyhow!("String column requires lengths_ptr"));
                }
                let ptrs = std::slice::from_raw_parts(slice.data_ptr as *const *const u8, slice.len);
                let lens = std::slice::from_raw_parts(slice.lengths_ptr, slice.len);
                for i in 0..slice.len {
                    let is_null = if !slice.validity_ptr.is_null() {
                        (*slice.validity_ptr.add(i / 8) >> (i % 8)) & 1 == 0
                    } else {
                        false
                    };
                    if is_null {
                        all_strings.push(None);
                    } else {
                        let s = std::str::from_utf8(std::slice::from_raw_parts(ptrs[i], lens[i] as usize))
                            .map_err(|e| anyhow::anyhow!("Invalid UTF-8: {}", e))?;
                        all_strings.push(Some(s));
                    }
                }
            }
            Arc::new(StringArray::from(all_strings))
        }
        COLUMN_TYPE_UUID => {
            let mut data: Vec<u8> = Vec::with_capacity(total * 16);
            for slice in slices {
                let src = slice.data_ptr as *const u8;
                if slice.sel_ptr.is_null() {
                    data.extend_from_slice(std::slice::from_raw_parts(src, slice.len * 16));
                } else {
                    for &idx in std::slice::from_raw_parts(slice.sel_ptr, slice.len) {
                        data.extend_from_slice(std::slice::from_raw_parts(src.add((idx - 1) as usize * 16), 16));
                    }
                }
            }
            let chunks: Vec<&[u8]> = data.chunks(16).collect();
            Arc::new(
                arrow_array::FixedSizeBinaryArray::try_from_iter(chunks.into_iter())
                    .map_err(|e| anyhow::anyhow!("Failed to build UUID array: {}", e))?,
            )
        }
        COLUMN_TYPE_DECIMAL_INT32 | COLUMN_TYPE_DECIMAL_INT64 | COLUMN_TYPE_DECIMAL_INT128 => {
            let (precision, scale) = match schema_field.data_type() {
                arrow_schema::DataType::Decimal128(p, s) => (*p, *s),
                dt => return Err(anyhow::anyhow!("Expected Decimal128, got {:?}", dt)),
            };
            let mut values = Vec::<i128>::with_capacity(total);
            for slice in slices {
                match desc.column_type {
                    COLUMN_TYPE_DECIMAL_INT32 => {
                        let src = slice.data_ptr as *const i32;
                        if slice.sel_ptr.is_null() {
                            values.extend(std::slice::from_raw_parts(src, slice.len).iter().map(|&v| v as i128));
                        } else {
                            for &idx in std::slice::from_raw_parts(slice.sel_ptr, slice.len) {
                                values.push(*src.add((idx - 1) as usize) as i128);
                            }
                        }
                    }
                    COLUMN_TYPE_DECIMAL_INT64 => {
                        let src = slice.data_ptr as *const i64;
                        if slice.sel_ptr.is_null() {
                            values.extend(std::slice::from_raw_parts(src, slice.len).iter().map(|&v| v as i128));
                        } else {
                            for &idx in std::slice::from_raw_parts(slice.sel_ptr, slice.len) {
                                values.push(*src.add((idx - 1) as usize) as i128);
                            }
                        }
                    }
                    _ => {
                        // DECIMAL_INT128: i128 layout matches Julia Int128
                        let src = slice.data_ptr as *const i128;
                        if slice.sel_ptr.is_null() {
                            values.extend_from_slice(std::slice::from_raw_parts(src, slice.len));
                        } else {
                            for &idx in std::slice::from_raw_parts(slice.sel_ptr, slice.len) {
                                values.push(*src.add((idx - 1) as usize));
                            }
                        }
                    }
                }
            }
            Arc::new(
                PrimitiveArray::<Decimal128Type>::new(ScalarBuffer::from(values), null_buf)
                    .with_precision_and_scale(precision, scale)
                    .map_err(|e| anyhow::anyhow!("Decimal precision/scale: {}", e))?,
            )
        }
        _ => return Err(anyhow::anyhow!("Unknown column type: {}", desc.column_type)),
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

        let arrow_schema = writer_ref.arrow_schema.clone();

        // Validate column count matches schema
        if cols.len() != arrow_schema.fields().len() {
            return Err(anyhow::anyhow!(
                "Column count mismatch: got {} columns but schema has {} fields",
                cols.len(),
                arrow_schema.fields().len()
            ));
        }

        // Build Arrow arrays from column descriptors.
        // These arrays are zero-copy: they hold raw pointers into Julia-owned buffers.
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
        for (i, desc) in cols.iter().enumerate() {
            let schema_field = arrow_schema.field(i);
            let array = unsafe { build_arrow_array(desc, schema_field)? };
            arrays.push(array);
        }

        let batch = RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))?;

        // Arrow arrays are zero-copy (pointing into Julia buffers) — we must not return
        // until Rust has released them. Use a per-batch oneshot: the drain task signals
        // after encoding, at which point the Arrow arrays have been dropped.
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<()>();
        writer_ref
            .batch_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Writer has been closed"))?
            .send((batch, Some(ack_tx)))
            .await
            .map_err(|_| anyhow::anyhow!("Writer channel closed (drain task may have failed)"))?;
        ack_rx
            .await
            .map_err(|_| anyhow::anyhow!("Ack channel closed (drain task may have failed)"))?;

        Ok::<(), anyhow::Error>(())
    },
    writer: *mut IcebergDataFileWriter,
    columns: *const ColumnDescriptor,
    num_columns: usize
);
