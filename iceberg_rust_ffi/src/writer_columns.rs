/// Column-based writer support for iceberg_rust_ffi
///
/// This module provides FFI bindings for writing raw column data directly to Parquet,
/// avoiding the overhead of Arrow IPC serialization. Julia passes raw column pointers
/// and metadata, and Rust builds Arrow arrays directly from them.
use std::ffi::c_void;
use std::sync::Arc;

use arrow_array::{
    types::{
        Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type,
        TimestampMicrosecondType,
    },
    ArrayRef, BooleanArray, PrimitiveArray, StringArray,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};

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

// =============================================================================
// Scattered-gather writer: pass raw source pointers + selection indices to Rust,
// which gathers the data directly into Arrow arrays — eliminating the Julia-side
// staging copy for non-converting numeric column types.
// =============================================================================

/// A reference to one slice of source column data.
/// `sel_ptr = null`  → sequential (identity) access: read data[0..len].
/// `sel_ptr != null` → scattered access: read data[sel[i]-1] for i in 0..len (1-based Julia indices).
/// `validity_ptr = null` → all rows valid (non-nullable or known all-valid slice).
/// `lengths_ptr != null` → string column: data_ptr is Ptr{UInt8}[], lengths_ptr is Int64[] of byte lengths per string.
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

/// Merges the per-slice validity bitmaps from all slices into a single output bitmap.
///
/// Each slice contributes `slice.len` output rows. Slices with a null `validity_ptr` are
/// all-valid. Slices with a bitmap may be misaligned relative to the output (each slice
/// starts at a different `out` offset), so bits are copied one at a time with a shift.
/// The selection vector (`sel_ptr`) governs which *source data* elements to read; the
/// validity bitmap is always indexed by output row position, so sequential and scattered
/// slices are treated identically here.
///
/// Returns `None` if every slice is all-valid (no null buffer needed).
unsafe fn build_null_buffer_scattered(
    slices: &[SliceRef],
    total_rows: usize,
) -> Option<NullBuffer> {
    if !slices.iter().any(|s| !s.validity_ptr.is_null()) {
        return None;
    }
    let mut bits = vec![0u8; (total_rows + 7) / 8];
    let mut out = 0usize;
    for slice in slices {
        if slice.validity_ptr.is_null() {
            // All rows in this slice are valid — set one bit per output row.
            for i in 0..slice.len {
                bits[(out + i) / 8] |= 1u8 << ((out + i) % 8);
            }
        } else {
            // Copy validity bits from the slice's bitmap into the output bitmap,
            // re-aligning from source bit position i to output bit position (out + i).
            for i in 0..slice.len {
                let b = (*slice.validity_ptr.add(i / 8) >> (i % 8)) & 1;
                bits[(out + i) / 8] |= b << ((out + i) % 8);
            }
        }
        out += slice.len;
    }
    Some(NullBuffer::new(BooleanBuffer::new(
        Buffer::from(bits),
        0,
        total_rows,
    )))
}

/// Gather all slices for a column into an Arrow array.
pub(crate) unsafe fn build_arrow_array_gathered(
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
            Arc::new(PrimitiveArray::<$ArrowType>::new(
                ScalarBuffer::from(values),
                null_buf,
            )) as ArrayRef
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
                PrimitiveArray::<TimestampMicrosecondType>::new(
                    ScalarBuffer::from(values),
                    null_buf,
                )
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
                        if v != 0 {
                            bits[(out + i) / 8] |= 1 << ((out + i) % 8);
                        }
                    }
                } else {
                    for (i, &idx) in std::slice::from_raw_parts(slice.sel_ptr, slice.len)
                        .iter()
                        .enumerate()
                    {
                        if *src.add((idx - 1) as usize) != 0 {
                            bits[(out + i) / 8] |= 1 << ((out + i) % 8);
                        }
                    }
                }
                out += slice.len;
            }
            Arc::new(BooleanArray::new(
                BooleanBuffer::new(Buffer::from(bits), 0, total),
                null_buf,
            ))
        }
        COLUMN_TYPE_STRING => {
            // String columns do not support selection vectors. Julia strings are
            // heap-allocated with non-contiguous addresses, so the caller must build
            // str_ptrs/str_lens arrays up-front — any row selection is already applied
            // before add_string_slice! is called. sel_ptr is therefore always null here.
            // data_ptr = *const *const u8, lengths_ptr = *const i64.
            //
            // Build the Arrow StringArray directly: one pass copies string bytes into a
            // contiguous values buffer and tracks cumulative offsets. This avoids the
            // intermediate Vec<Option<&str>> and skips UTF-8 validation — Julia strings
            // are guaranteed valid UTF-8.
            let null_buf = if desc.is_nullable {
                build_null_buffer_scattered(slices, total)
            } else {
                None
            };
            let mut offsets = Vec::<i32>::with_capacity(total + 1);
            offsets.push(0i32);
            let mut values = Vec::<u8>::new();
            for slice in slices {
                if slice.lengths_ptr.is_null() {
                    return Err(anyhow::anyhow!("String column requires lengths_ptr"));
                }
                let ptrs =
                    std::slice::from_raw_parts(slice.data_ptr as *const *const u8, slice.len);
                let lens = std::slice::from_raw_parts(slice.lengths_ptr, slice.len);
                for i in 0..slice.len {
                    let is_null = !slice.validity_ptr.is_null()
                        && ((*slice.validity_ptr.add(i / 8) >> (i % 8)) & 1) == 0;
                    if !is_null {
                        values.extend_from_slice(std::slice::from_raw_parts(
                            ptrs[i],
                            lens[i] as usize,
                        ));
                    }
                    offsets.push(values.len() as i32);
                }
            }
            // SAFETY: offsets are monotonically non-decreasing by construction; values
            // bytes come from Julia String objects (valid UTF-8) kept alive in col.preserve.
            Arc::new(unsafe {
                StringArray::new_unchecked(
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    Buffer::from_vec(values),
                    null_buf,
                )
            })
        }
        COLUMN_TYPE_UUID => {
            let mut data: Vec<u8> = Vec::with_capacity(total * 16);
            for slice in slices {
                let src = slice.data_ptr as *const u8;
                if slice.sel_ptr.is_null() {
                    data.extend_from_slice(std::slice::from_raw_parts(src, slice.len * 16));
                } else {
                    for &idx in std::slice::from_raw_parts(slice.sel_ptr, slice.len) {
                        data.extend_from_slice(std::slice::from_raw_parts(
                            src.add((idx - 1) as usize * 16),
                            16,
                        ));
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
                            values.extend(
                                std::slice::from_raw_parts(src, slice.len)
                                    .iter()
                                    .map(|&v| v as i128),
                            );
                        } else {
                            for &idx in std::slice::from_raw_parts(slice.sel_ptr, slice.len) {
                                values.push(*src.add((idx - 1) as usize) as i128);
                            }
                        }
                    }
                    COLUMN_TYPE_DECIMAL_INT64 => {
                        let src = slice.data_ptr as *const i64;
                        if slice.sel_ptr.is_null() {
                            values.extend(
                                std::slice::from_raw_parts(src, slice.len)
                                    .iter()
                                    .map(|&v| v as i128),
                            );
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
