/// Incremental column batch builder for zero-copy-from-Julia writes.
///
/// Julia calls `iceberg_batch_builder_append_slice` once per operator slice, passing
/// one `SliceRef` per column. Each slice's data is appended directly into a pre-allocated
/// `MutableBuffer` that becomes the Arrow column buffer at finalize time — no intermediate
/// Vec or second copy is needed. Julia can reuse source memory as soon as the call returns.
///
/// Null bits are populated lazily: all-valid slices are skipped entirely. The first null
/// slice triggers a one-time backfill of all prior rows as valid, then subsequent slices
/// are processed normally. If no null slice ever arrives, no NullBuffer is emitted.
///
/// When a coalesce window is full, Julia calls `iceberg_batch_builder_write` which
/// finalises all per-column buffers into Arrow arrays, assembles a `RecordBatch`, and
/// submits it to the async encode pool — then resets the builder in-place for reuse.
/// Reset swaps in a fresh pre-allocated `MutableBuffer` (same capacity) so the next
/// window never reallocates.
use std::sync::Arc;

use arrow_array::{
    types::*, ArrayRef, BooleanArray, FixedSizeBinaryArray, PrimitiveArray, StringArray,
};
use arrow_buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::SchemaRef as ArrowSchemaRef;

use crate::writer::{submit_batch, IcebergDataFileWriter, GLOBAL_ENCODE_POOL};
use crate::writer_columns::{
    SliceRef, COLUMN_TYPE_BOOLEAN, COLUMN_TYPE_DATE, COLUMN_TYPE_DECIMAL_INT128,
    COLUMN_TYPE_DECIMAL_INT32, COLUMN_TYPE_DECIMAL_INT64, COLUMN_TYPE_FLOAT32, COLUMN_TYPE_FLOAT64,
    COLUMN_TYPE_INT32, COLUMN_TYPE_INT64, COLUMN_TYPE_JULIA_DATE, COLUMN_TYPE_JULIA_TIMESTAMP,
    COLUMN_TYPE_JULIA_TIMESTAMPTZ, COLUMN_TYPE_JULIA_TIMESTAMPTZ_NS,
    COLUMN_TYPE_JULIA_TIMESTAMP_NS, COLUMN_TYPE_STRING, COLUMN_TYPE_TIMESTAMP,
    COLUMN_TYPE_TIMESTAMPTZ, COLUMN_TYPE_UUID,
};

/// Days from Julia Date epoch (0001-01-01, Rata Die day 1) to Unix epoch (1970-01-01).
/// Julia Date stores days using 1-based Rata Die: Dates.value(Date(1970,1,1)) == 719163.
const JULIA_DATE_OFFSET: i64 = 719_163;
/// Milliseconds from Julia DateTime epoch (0001-01-01) to Unix epoch (1970-01-01).
const JULIA_TIMESTAMP_OFFSET_MS: i64 = 719_163 * 86_400_000;

// Default coalesce_rows — must match Julia's DEFAULT_COALESCE_ROWS.
const DEFAULT_COALESCE_ROWS: usize = 1_048_576;

/// Bytes per row for numeric column types (0 for Bool/Str which are not Numeric).
fn column_bytes_per_row(column_type: i32) -> usize {
    match column_type {
        COLUMN_TYPE_INT32
        | COLUMN_TYPE_DATE
        | COLUMN_TYPE_FLOAT32
        | COLUMN_TYPE_DECIMAL_INT32
        | COLUMN_TYPE_JULIA_DATE => 4,
        COLUMN_TYPE_INT64
        | COLUMN_TYPE_TIMESTAMP
        | COLUMN_TYPE_TIMESTAMPTZ
        | COLUMN_TYPE_FLOAT64
        | COLUMN_TYPE_DECIMAL_INT64
        | COLUMN_TYPE_JULIA_TIMESTAMP
        | COLUMN_TYPE_JULIA_TIMESTAMPTZ
        | COLUMN_TYPE_JULIA_TIMESTAMP_NS
        | COLUMN_TYPE_JULIA_TIMESTAMPTZ_NS => 8,
        COLUMN_TYPE_DECIMAL_INT128 | COLUMN_TYPE_UUID => 16,
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Per-column value buffer
//
// All numeric and UUID variants use `MutableBuffer` (byte-oriented, Arrow-native).
// On append we write typed bytes directly into it; on finalize we call `.into()` to
// get an Arrow `Buffer` — ownership transfer, no copy. The buffer is then swapped out
// for a fresh pre-allocated one so the next window never reallocates.

enum ColumnValues {
    Numeric(MutableBuffer), // I32/I64/F32/F64/I128/UUID — bytes per row varies by type
    Bool(Vec<u8>),          // BOOLEAN — 1 byte per row; bit-packed at finalize
    Str {
        bytes: Vec<u8>,
        offsets: Vec<i32>, // Arrow offset buffer; offsets[0] = 0 always
    },
}

impl ColumnValues {
    fn new(column_type: i32, coalesce_rows: usize) -> Self {
        match column_type {
            COLUMN_TYPE_BOOLEAN => ColumnValues::Bool(Vec::with_capacity(coalesce_rows)),
            COLUMN_TYPE_STRING => ColumnValues::Str {
                bytes: Vec::new(),
                offsets: {
                    let mut v = Vec::with_capacity(coalesce_rows + 1);
                    v.push(0i32);
                    v
                },
            },
            _ => {
                let bpr = column_bytes_per_row(column_type).max(8); // fallback 8 for unknown
                ColumnValues::Numeric(MutableBuffer::with_capacity(coalesce_rows * bpr))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Per-column builder state

struct ColumnBuilderState {
    column_type: i32,
    bytes_per_row: usize, // for Numeric variant: bytes per element
    is_nullable: bool,
    values: ColumnValues,
    /// Lazily-populated validity bitmap. Empty until the first null slice.
    null_bits: Vec<u8>,
    rows: usize,
    has_nulls: bool,
}

impl ColumnBuilderState {
    fn new(column_type: i32, is_nullable: bool, coalesce_rows: usize) -> Self {
        Self {
            column_type,
            bytes_per_row: column_bytes_per_row(column_type),
            is_nullable,
            values: ColumnValues::new(column_type, coalesce_rows),
            null_bits: Vec::new(),
            rows: 0,
            has_nulls: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Public builder type

pub struct ColumnBatchBuilder {
    columns: Vec<ColumnBuilderState>,
    arrow_schema: ArrowSchemaRef,
    coalesce_rows: usize,
}

impl ColumnBatchBuilder {
    pub(crate) fn new(
        arrow_schema: ArrowSchemaRef,
        col_types: &[i32],
        coalesce_rows: usize,
    ) -> Result<Self, anyhow::Error> {
        if col_types.len() != arrow_schema.fields().len() {
            return Err(anyhow::anyhow!(
                "col_types length {} != schema field count {}",
                col_types.len(),
                arrow_schema.fields().len()
            ));
        }
        let columns = col_types
            .iter()
            .zip(arrow_schema.fields().iter())
            .map(|(&ct, field)| ColumnBuilderState::new(ct, field.is_nullable(), coalesce_rows))
            .collect();
        Ok(Self {
            columns,
            arrow_schema,
            coalesce_rows,
        })
    }

    pub(crate) unsafe fn append_slice(&mut self, slices: &[SliceRef]) -> Result<(), anyhow::Error> {
        if slices.len() != self.columns.len() {
            return Err(anyhow::anyhow!(
                "slice count {} != column count {}",
                slices.len(),
                self.columns.len()
            ));
        }
        for (state, slice) in self.columns.iter_mut().zip(slices.iter()) {
            unsafe { append_to_state(state, slice) }?;
        }
        Ok(())
    }

    pub(crate) fn write_and_reset(
        &mut self,
        writer_ref: &IcebergDataFileWriter,
        pool: &crate::writer::GlobalWorkerPool,
    ) -> Result<(), anyhow::Error> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for (i, state) in self.columns.iter_mut().enumerate() {
            let field = self.arrow_schema.field(i);
            arrays.push(finalize_and_reset(state, field, self.coalesce_rows)?);
        }
        let batch = arrow_array::RecordBatch::try_new(self.arrow_schema.clone(), arrays)
            .map_err(|e| anyhow::anyhow!("RecordBatch: {}", e))?;
        submit_batch(writer_ref, pool, batch)
    }
}

// ---------------------------------------------------------------------------
// Append logic

unsafe fn append_to_state(
    state: &mut ColumnBuilderState,
    slice: &SliceRef,
) -> Result<(), anyhow::Error> {
    let len = slice.len;

    // ---- null bits (lazy) -------------------------------------------------
    // Skip entirely for all-valid slices when no nulls have been seen yet.
    // On the first null slice, backfill all prior rows as valid, then copy bits.
    // For all-valid slices after nulls have been seen, extend the bitmap with 1s.
    if state.is_nullable {
        if !slice.validity_ptr.is_null() {
            let out_start = state.rows;
            let needed = (out_start + len + 7) / 8;
            if !state.has_nulls {
                // First null slice: backfill all prior rows as valid.
                state.null_bits.resize(needed, 0u8);
                set_bits_range(&mut state.null_bits, 0, out_start);
                state.has_nulls = true;
            } else {
                if state.null_bits.len() < needed {
                    state.null_bits.resize(needed, 0u8);
                }
            }
            for i in 0..len {
                let b = unsafe { (*slice.validity_ptr.add(i / 8) >> (i % 8)) & 1 };
                let pos = out_start + i;
                state.null_bits[pos / 8] |= b << (pos % 8);
            }
        } else if state.has_nulls {
            // All-valid slice but nulls seen earlier — extend bitmap with 1s.
            let out_start = state.rows;
            let needed = (out_start + len + 7) / 8;
            if state.null_bits.len() < needed {
                state.null_bits.resize(needed, 0u8);
            }
            set_bits_range(&mut state.null_bits, out_start, out_start + len);
        }
        // else: all-valid slice, no nulls yet — nothing to do.
    }

    // ---- values -----------------------------------------------------------
    match &mut state.values {
        ColumnValues::Numeric(buf) => {
            append_numeric(buf, slice, state.column_type, len)?;
        }
        ColumnValues::Bool(v) => {
            let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const u8, len) };
            if slice.sel_ptr.is_null() {
                v.extend_from_slice(src);
            } else {
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    v.push(src[(idx - 1) as usize]);
                }
            }
        }
        ColumnValues::Str { bytes, offsets } => {
            if slice.lengths_ptr.is_null() {
                return Err(anyhow::anyhow!("String column: lengths_ptr is null"));
            }
            let ptrs =
                unsafe { std::slice::from_raw_parts(slice.data_ptr as *const *const u8, len) };
            let lens = unsafe { std::slice::from_raw_parts(slice.lengths_ptr, len) };
            let out_start = state.rows;
            for i in 0..len {
                let is_null = state.is_nullable && state.has_nulls && {
                    let pos = out_start + i;
                    (state.null_bits[pos / 8] >> (pos % 8)) & 1 == 0
                };
                if !is_null && !ptrs[i].is_null() {
                    bytes.extend_from_slice(unsafe {
                        std::slice::from_raw_parts(ptrs[i], lens[i] as usize)
                    });
                }
                offsets.push(bytes.len() as i32);
            }
        }
    }

    state.rows += len;
    Ok(())
}

/// Append numeric slice data directly into a `MutableBuffer`.
/// Identity (sequential) slices use a bulk byte copy; scattered slices loop element-wise.
unsafe fn append_numeric(
    buf: &mut MutableBuffer,
    slice: &SliceRef,
    column_type: i32,
    len: usize,
) -> Result<(), anyhow::Error> {
    match column_type {
        COLUMN_TYPE_INT32 | COLUMN_TYPE_DATE => {
            let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const i32, len) };
            if slice.sel_ptr.is_null() {
                buf.extend_from_slice(as_bytes(src));
            } else {
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    buf.extend_from_slice(&src[(idx - 1) as usize].to_ne_bytes());
                }
            }
        }
        COLUMN_TYPE_INT64 | COLUMN_TYPE_TIMESTAMP | COLUMN_TYPE_TIMESTAMPTZ => {
            let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const i64, len) };
            if slice.sel_ptr.is_null() {
                buf.extend_from_slice(as_bytes(src));
            } else {
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    buf.extend_from_slice(&src[(idx - 1) as usize].to_ne_bytes());
                }
            }
        }
        COLUMN_TYPE_FLOAT32 => {
            let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const f32, len) };
            if slice.sel_ptr.is_null() {
                buf.extend_from_slice(as_bytes(src));
            } else {
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    buf.extend_from_slice(&src[(idx - 1) as usize].to_ne_bytes());
                }
            }
        }
        COLUMN_TYPE_FLOAT64 => {
            let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const f64, len) };
            if slice.sel_ptr.is_null() {
                buf.extend_from_slice(as_bytes(src));
            } else {
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    buf.extend_from_slice(&src[(idx - 1) as usize].to_ne_bytes());
                }
            }
        }
        COLUMN_TYPE_DECIMAL_INT32 => {
            let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const i32, len) };
            if slice.sel_ptr.is_null() {
                for &x in src {
                    buf.extend_from_slice(&(x as i128).to_ne_bytes());
                }
            } else {
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    buf.extend_from_slice(&(src[(idx - 1) as usize] as i128).to_ne_bytes());
                }
            }
        }
        COLUMN_TYPE_DECIMAL_INT64 => {
            let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const i64, len) };
            if slice.sel_ptr.is_null() {
                for &x in src {
                    buf.extend_from_slice(&(x as i128).to_ne_bytes());
                }
            } else {
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    buf.extend_from_slice(&(src[(idx - 1) as usize] as i128).to_ne_bytes());
                }
            }
        }
        COLUMN_TYPE_DECIMAL_INT128 | COLUMN_TYPE_UUID => {
            // 16-byte elements
            let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const u8, len * 16) };
            if slice.sel_ptr.is_null() {
                buf.extend_from_slice(src);
            } else {
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    let off = (idx - 1) as usize * 16;
                    buf.extend_from_slice(&src[off..off + 16]);
                }
            }
        }
        COLUMN_TYPE_JULIA_DATE => {
            // Source: i64[] of Julia days (since 0001-01-01). Destination: i32 Date32 (since Unix epoch).
            let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const i64, len) };
            if slice.sel_ptr.is_null() {
                for &v in src {
                    buf.extend_from_slice(&((v - JULIA_DATE_OFFSET) as i32).to_ne_bytes());
                }
            } else {
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    let v = src[(idx - 1) as usize];
                    buf.extend_from_slice(&((v - JULIA_DATE_OFFSET) as i32).to_ne_bytes());
                }
            }
        }
        COLUMN_TYPE_JULIA_TIMESTAMP | COLUMN_TYPE_JULIA_TIMESTAMPTZ => {
            // Source: i64[] of Julia ms (since 0001-01-01). Destination: i64 μs since Unix epoch.
            let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const i64, len) };
            if slice.sel_ptr.is_null() {
                for &v in src {
                    buf.extend_from_slice(&((v - JULIA_TIMESTAMP_OFFSET_MS) * 1_000).to_ne_bytes());
                }
            } else {
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    let v = src[(idx - 1) as usize];
                    buf.extend_from_slice(&((v - JULIA_TIMESTAMP_OFFSET_MS) * 1_000).to_ne_bytes());
                }
            }
        }
        COLUMN_TYPE_JULIA_TIMESTAMP_NS | COLUMN_TYPE_JULIA_TIMESTAMPTZ_NS => {
            // Source: i64[] of Julia ms (since 0001-01-01). Destination: i64 ns since Unix epoch.
            let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const i64, len) };
            if slice.sel_ptr.is_null() {
                for &v in src {
                    buf.extend_from_slice(
                        &((v - JULIA_TIMESTAMP_OFFSET_MS) * 1_000_000).to_ne_bytes(),
                    );
                }
            } else {
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    let v = src[(idx - 1) as usize];
                    buf.extend_from_slice(
                        &((v - JULIA_TIMESTAMP_OFFSET_MS) * 1_000_000).to_ne_bytes(),
                    );
                }
            }
        }
        _ => {
            return Err(anyhow::anyhow!("unsupported column type {}", column_type));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Finalize

/// Build an Arrow array from `state`'s accumulated data and reset all buffers
/// in-place for the next coalesce window.
fn finalize_and_reset(
    state: &mut ColumnBuilderState,
    schema_field: &arrow_schema::Field,
    coalesce_rows: usize,
) -> Result<ArrayRef, anyhow::Error> {
    let rows = state.rows;
    state.rows = 0;
    state.has_nulls = false;

    // Build NullBuffer from lazily-accumulated bits (None if no nulls seen).
    // Preserve the Vec's capacity for the next window to avoid repeated allocation.
    let null_buf: Option<NullBuffer> = if state.is_nullable && !state.null_bits.is_empty() {
        let cap = state.null_bits.capacity();
        let bits = std::mem::replace(&mut state.null_bits, Vec::with_capacity(cap));
        Some(NullBuffer::new(BooleanBuffer::new(
            Buffer::from_vec(bits),
            0,
            rows,
        )))
    } else {
        state.null_bits.clear();
        None
    };

    let array: ArrayRef = match &mut state.values {
        ColumnValues::Numeric(buf) => {
            // Swap in a fresh pre-allocated buffer; take the old one as Arrow Buffer.
            let old = std::mem::replace(
                buf,
                MutableBuffer::with_capacity(coalesce_rows * state.bytes_per_row),
            );
            let arrow_buf: Buffer = old.into();
            build_numeric_array(state.column_type, arrow_buf, rows, null_buf, schema_field)?
        }
        ColumnValues::Bool(v) => {
            let cap = v.capacity();
            let taken = std::mem::replace(v, Vec::with_capacity(cap));
            let mut bits = vec![0u8; (rows + 7) / 8];
            for (i, &b) in taken.iter().enumerate().take(rows) {
                if b != 0 {
                    bits[i / 8] |= 1u8 << (i % 8);
                }
            }
            Arc::new(BooleanArray::new(
                BooleanBuffer::new(Buffer::from_vec(bits), 0, rows),
                null_buf,
            ))
        }
        ColumnValues::Str { bytes, offsets } => {
            let taken_bytes = std::mem::replace(bytes, Vec::with_capacity(bytes.capacity()));
            let taken_offsets = std::mem::replace(offsets, {
                let mut v = Vec::with_capacity(coalesce_rows + 1);
                v.push(0i32);
                v
            });
            let arr = unsafe {
                StringArray::new_unchecked(
                    OffsetBuffer::new(ScalarBuffer::from(taken_offsets)),
                    Buffer::from_vec(taken_bytes),
                    null_buf,
                )
            };
            Arc::new(arr)
        }
    };

    Ok(array)
}

fn build_numeric_array(
    column_type: i32,
    buf: Buffer,
    rows: usize,
    null_buf: Option<NullBuffer>,
    schema_field: &arrow_schema::Field,
) -> Result<ArrayRef, anyhow::Error> {
    Ok(match column_type {
        COLUMN_TYPE_INT32 => Arc::new(PrimitiveArray::<Int32Type>::new(
            ScalarBuffer::new(buf, 0, rows),
            null_buf,
        )),
        COLUMN_TYPE_DATE => Arc::new(PrimitiveArray::<Date32Type>::new(
            ScalarBuffer::new(buf, 0, rows),
            null_buf,
        )),
        COLUMN_TYPE_INT64 => Arc::new(PrimitiveArray::<Int64Type>::new(
            ScalarBuffer::new(buf, 0, rows),
            null_buf,
        )),
        COLUMN_TYPE_TIMESTAMP => Arc::new(PrimitiveArray::<TimestampMicrosecondType>::new(
            ScalarBuffer::new(buf, 0, rows),
            null_buf,
        )),
        COLUMN_TYPE_TIMESTAMPTZ => Arc::new(
            PrimitiveArray::<TimestampMicrosecondType>::new(
                ScalarBuffer::new(buf, 0, rows),
                null_buf,
            )
            .with_timezone("UTC"),
        ),
        COLUMN_TYPE_FLOAT32 => Arc::new(PrimitiveArray::<Float32Type>::new(
            ScalarBuffer::new(buf, 0, rows),
            null_buf,
        )),
        COLUMN_TYPE_FLOAT64 => Arc::new(PrimitiveArray::<Float64Type>::new(
            ScalarBuffer::new(buf, 0, rows),
            null_buf,
        )),
        COLUMN_TYPE_DECIMAL_INT32 | COLUMN_TYPE_DECIMAL_INT64 | COLUMN_TYPE_DECIMAL_INT128 => {
            let (precision, scale) = match schema_field.data_type() {
                arrow_schema::DataType::Decimal128(p, s) => (*p, *s),
                dt => {
                    return Err(anyhow::anyhow!(
                        "Expected Decimal128 for field {}, got {:?}",
                        schema_field.name(),
                        dt
                    ))
                }
            };
            Arc::new(
                PrimitiveArray::<Decimal128Type>::new(ScalarBuffer::new(buf, 0, rows), null_buf)
                    .with_precision_and_scale(precision, scale)
                    .map_err(|e| anyhow::anyhow!("Decimal precision/scale: {}", e))?,
            )
        }
        COLUMN_TYPE_UUID => {
            // UUID: FixedSizeBinary(16)
            Arc::new(
                FixedSizeBinaryArray::try_new(16, buf, null_buf)
                    .map_err(|e| anyhow::anyhow!("UUID FixedSizeBinary: {}", e))?,
            )
        }
        COLUMN_TYPE_JULIA_DATE => Arc::new(PrimitiveArray::<Date32Type>::new(
            ScalarBuffer::new(buf, 0, rows),
            null_buf,
        )),
        COLUMN_TYPE_JULIA_TIMESTAMP => Arc::new(PrimitiveArray::<TimestampMicrosecondType>::new(
            ScalarBuffer::new(buf, 0, rows),
            null_buf,
        )),
        COLUMN_TYPE_JULIA_TIMESTAMPTZ => Arc::new(
            PrimitiveArray::<TimestampMicrosecondType>::new(
                ScalarBuffer::new(buf, 0, rows),
                null_buf,
            )
            .with_timezone("UTC"),
        ),
        COLUMN_TYPE_JULIA_TIMESTAMP_NS => Arc::new(PrimitiveArray::<TimestampNanosecondType>::new(
            ScalarBuffer::new(buf, 0, rows),
            null_buf,
        )),
        COLUMN_TYPE_JULIA_TIMESTAMPTZ_NS => Arc::new(
            PrimitiveArray::<TimestampNanosecondType>::new(
                ScalarBuffer::new(buf, 0, rows),
                null_buf,
            )
            .with_timezone("UTC"),
        ),
        ct => {
            return Err(anyhow::anyhow!(
                "unsupported column type {} in finalize",
                ct
            ))
        }
    })
}

// ---------------------------------------------------------------------------
// Helpers

/// Set bits [start, end) to 1 in `bits` (Arrow bit layout: bit i at byte i/8, shift i%8).
fn set_bits_range(bits: &mut [u8], start: usize, end: usize) {
    if start >= end {
        return;
    }
    let (fb, lb) = (start / 8, (end - 1) / 8);
    let (fi, li) = (start % 8, (end - 1) % 8);
    if fb == lb {
        // All bits in the same byte: set bits [fi, li].
        bits[fb] |= ((1u16 << (li + 1)) - 1) as u8 & (0xFF_u8 << fi);
    } else {
        bits[fb] |= 0xFF_u8 << fi; // partial first byte: bits [fi, 7]
        bits[(fb + 1)..lb].fill(0xFF); // full middle bytes
        bits[lb] |= ((1u16 << (li + 1)) - 1) as u8; // partial last byte: bits [0, li]
    }
}

/// Reinterpret a typed slice as bytes.
#[inline(always)]
unsafe fn as_bytes<T>(s: &[T]) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(s.as_ptr() as *const u8, s.len() * std::mem::size_of::<T>())
    }
}

// ---------------------------------------------------------------------------
// FFI entry points

#[no_mangle]
pub extern "C" fn iceberg_batch_builder_new(
    writer: *mut IcebergDataFileWriter,
    col_types: *const i32,
    num_columns: usize,
) -> *mut ColumnBatchBuilder {
    if writer.is_null() || col_types.is_null() || num_columns == 0 {
        return std::ptr::null_mut();
    }
    let writer_ref = unsafe { &*writer };
    let col_types_slice = unsafe { std::slice::from_raw_parts(col_types, num_columns) };
    match ColumnBatchBuilder::new(
        writer_ref.arrow_schema.clone(),
        col_types_slice,
        DEFAULT_COALESCE_ROWS,
    ) {
        Ok(b) => Box::into_raw(Box::new(b)),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn iceberg_batch_builder_append_slice(
    builder: *mut ColumnBatchBuilder,
    slices: *const SliceRef,
    num_columns: usize,
) -> i32 {
    if builder.is_null() || slices.is_null() || num_columns == 0 {
        return -1;
    }
    let builder_ref = unsafe { &mut *builder };
    let slices_slice = unsafe { std::slice::from_raw_parts(slices, num_columns) };
    match unsafe { builder_ref.append_slice(slices_slice) } {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn iceberg_batch_builder_write(
    writer: *mut IcebergDataFileWriter,
    builder: *mut ColumnBatchBuilder,
) -> i32 {
    if writer.is_null() || builder.is_null() {
        return -1;
    }
    let writer_ref = unsafe { &*writer };
    let builder_ref = unsafe { &mut *builder };
    let pool = match GLOBAL_ENCODE_POOL.get() {
        Some(p) => p,
        None => {
            eprintln!("[iceberg] encode pool not initialized");
            return -1;
        }
    };
    match builder_ref.write_and_reset(writer_ref, pool) {
        Ok(()) => 0,
        Err(e) => {
            crate::writer::store_writer_error_pub(writer_ref, e);
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn iceberg_batch_builder_free(builder: *mut ColumnBatchBuilder) {
    if !builder.is_null() {
        unsafe { drop(Box::from_raw(builder)) }
    }
}
