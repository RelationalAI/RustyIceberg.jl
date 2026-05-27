/// Incremental Arrow `RecordBatch` builder for zero-copy-from-Julia writes.
///
/// Lives as an internal field of `IcebergDataFileWriter`. Julia drives it through
/// `iceberg_writer_append` (per upstream slice) and `iceberg_writer_flush` (explicit
/// boundary). Auto-flush at `coalesce_rows` happens inside the writer's append entry
/// point; the builder itself is mechanical.
///
/// Each `append_slice` call copies one slice's data per column directly into per-column
/// `MutableBuffer`s that already match Arrow's physical layout. At finalize time those
/// buffers become Arrow `Buffer`s via a zero-copy `.into()` move, get wrapped in typed
/// arrays, and assemble into a `RecordBatch` — no further copy. Fresh same-capacity
/// buffers swap in for the next window, so steady-state reallocation is zero.
///
/// Null bits are populated lazily: all-valid slices skip the bitmap entirely. The first
/// null slice triggers a one-time backfill of all prior rows as valid, then subsequent
/// slices proceed normally. If no null slice ever arrives, no `NullBuffer` is emitted.
use std::sync::Arc;

use arrow_array::{
    types::*, ArrayRef, BooleanArray, FixedSizeBinaryArray, PrimitiveArray, StringArray,
};
use arrow_buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::SchemaRef as ArrowSchemaRef;

use crate::writer_columns::{
    ColumnSlice, COLUMN_TYPE_BOOLEAN, COLUMN_TYPE_DATE, COLUMN_TYPE_DECIMAL_INT128,
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

/// Default coalesce-window size for the embedded builder.
pub(crate) const DEFAULT_COALESCE_ROWS: usize = 1_048_576;

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
                // Start empty; finalize_and_reset right-sizes to the actual slice length
                // after the first flush, so we never hold a 4MB coalesce_rows-sized Vec.
                offsets: {
                    let mut v = Vec::new();
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
// Builder type

pub(crate) struct RecordBatchBuilder {
    columns: Vec<ColumnBuilderState>,
    arrow_schema: ArrowSchemaRef,
    coalesce_rows: usize,
}

impl RecordBatchBuilder {
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

    /// Rows accumulated in the current window (across all columns; they stay in sync).
    pub(crate) fn rows(&self) -> usize {
        self.columns.first().map(|c| c.rows).unwrap_or(0)
    }

    /// True when the current window has reached or passed `coalesce_rows` — the writer
    /// should finalize and reset before continuing.
    pub(crate) fn should_flush(&self) -> bool {
        self.rows() >= self.coalesce_rows
    }

    /// Append one slice per column. Rust copies all data synchronously; source memory
    /// may be released the moment this call returns.
    ///
    /// # Safety
    /// All pointers inside the `ColumnSlice`s must be valid for `len` elements for the
    /// duration of this call.
    pub(crate) unsafe fn append_slice(
        &mut self,
        slices: &[ColumnSlice],
    ) -> Result<(), anyhow::Error> {
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

    /// Finalize the accumulated columns into a `RecordBatch` and reset all column
    /// buffers in-place for the next window. The buffers are swapped with fresh
    /// pre-allocated `MutableBuffer`s of the same capacity, so the next window never
    /// reallocates.
    pub(crate) fn take_record_batch(&mut self) -> Result<arrow_array::RecordBatch, anyhow::Error> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for (i, state) in self.columns.iter_mut().enumerate() {
            let field = self.arrow_schema.field(i);
            arrays.push(finalize_and_reset(state, field, self.coalesce_rows)?);
        }
        arrow_array::RecordBatch::try_new(self.arrow_schema.clone(), arrays)
            .map_err(|e| anyhow::anyhow!("RecordBatch: {}", e))
    }
}

// ---------------------------------------------------------------------------
// Append logic

unsafe fn append_to_state(
    state: &mut ColumnBuilderState,
    slice: &ColumnSlice,
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
            } else if state.null_bits.len() < needed {
                state.null_bits.resize(needed, 0u8);
            }
            // Copy validity bits. When source and destination are byte-aligned (out_start
            // is a multiple of 8 — always true for flush-per-slice), one copy_nonoverlapping
            // replaces the 4096-iteration per-bit loop.
            if out_start % 8 == 0 {
                let dst = out_start / 8;
                let n_bytes = (len + 7) / 8;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        slice.validity_ptr,
                        state.null_bits.as_mut_ptr().add(dst),
                        n_bytes,
                    );
                }
                // Mask off garbage bits beyond `len` in the last byte so they don't
                // corrupt a subsequent coalesced slice that shares that byte.
                if len % 8 != 0 {
                    let tail = state.null_bits.last_mut().unwrap();
                    *tail &= (1u8 << (len % 8)) - 1;
                }
            } else {
                for i in 0..len {
                    let b = unsafe { (*slice.validity_ptr.add(i / 8) >> (i % 8)) & 1 };
                    let pos = out_start + i;
                    state.null_bits[pos / 8] |= b << (pos % 8);
                }
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
            if slice.sel_ptr.is_null() {
                let src = unsafe { std::slice::from_raw_parts(slice.data_ptr as *const u8, len) };
                v.extend_from_slice(src);
            } else {
                // See `append_primitive!` for why scatter uses a raw pointer.
                let src = slice.data_ptr as *const u8;
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for &idx in sel {
                    v.push(unsafe { *src.add((idx - 1) as usize) });
                }
            }
        }
        ColumnValues::Str { bytes, offsets } => {
            // Pointer-of-pointers protocol: data_ptr is *const *const u8 (array of pointers
            // into Julia source strings), lengths_ptr is *const i64 (byte lengths per row).
            // Null and empty rows have length 0; the nb>0 guard is sufficient since Julia
            // always sets len=0 for null/empty rows (no null-pointer check needed).
            if slice.lengths_ptr.is_null() {
                return Err(anyhow::anyhow!("String column: lengths_ptr is null"));
            }
            let ptrs =
                unsafe { std::slice::from_raw_parts(slice.data_ptr as *const *const u8, len) };
            let lens = unsafe { std::slice::from_raw_parts(slice.lengths_ptr, len) };
            // Pre-reserve so bytes/offsets never reallocate mid-loop.
            let total: usize = lens.iter().map(|&l| l as usize).sum();
            bytes.reserve(total);
            offsets.reserve(len);
            // Track running offset locally instead of reading bytes.len() each iteration.
            let mut cur_off = bytes.len();
            for i in 0..len {
                // Prefetch the string data that will be read PREFETCH_DIST iterations ahead.
                if i + PREFETCH_DIST < len {
                    unsafe { prefetch_read(ptrs[i + PREFETCH_DIST]) };
                }
                let nb = lens[i] as usize;
                if nb > 0 {
                    bytes.extend_from_slice(unsafe { std::slice::from_raw_parts(ptrs[i], nb) });
                    cur_off += nb;
                }
                offsets.push(cur_off as i32);
            }
        }
    }

    state.rows += len;
    Ok(())
}

/// Issue a read prefetch for the cache line at `ptr`.
/// Compiles to a real prefetch on x86_64 and aarch64; no-op elsewhere.
#[inline(always)]
unsafe fn prefetch_read(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T1);
    #[cfg(target_arch = "aarch64")]
    core::arch::asm!("prfm pldl2keep, [{ptr}]", ptr = in(reg) ptr, options(nostack, readonly));
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}

// Bulk-copy or 1-based-scattered-gather a slice of primitive T into buf (no transform).
// Identity selection → single memcpy; scattered selection → element-wise gather.
// Prefetch distance for scatter-gather loops: enough to cover ~200-cycle cache miss
// latency at typical throughput of a few cycles per element.
const PREFETCH_DIST: usize = 16;

macro_rules! append_primitive {
    ($buf:expr, $slice:expr, $len:expr, $T:ty) => {{
        if $slice.sel_ptr.is_null() {
            let src = unsafe { std::slice::from_raw_parts($slice.data_ptr as *const $T, $len) };
            $buf.extend_from_slice(unsafe { as_bytes(src) });
        } else {
            // Scatter: index into the source array via raw pointer arithmetic. The
            // source array may be longer than `$len` (which is the output stripe
            // length), so a `&[T]` of length `$len` would false-positive on any
            // sel index ≥ $len.
            let src = $slice.data_ptr as *const $T;
            let sel = unsafe { std::slice::from_raw_parts($slice.sel_ptr, $len) };
            for (i, &idx) in sel.iter().enumerate() {
                if i + PREFETCH_DIST < $len {
                    unsafe {
                        prefetch_read(
                            src.add((sel[i + PREFETCH_DIST] - 1) as usize) as *const u8
                        )
                    };
                }
                let v = unsafe { *src.add((idx - 1) as usize) };
                $buf.extend_from_slice(&v.to_ne_bytes());
            }
        }
    }};
}

// Element-wise transform from source type S with optional 1-based scattered gather.
// `$f` maps S → a value whose `.to_ne_bytes()` is written to buf.
macro_rules! append_transform {
    ($buf:expr, $slice:expr, $len:expr, $S:ty, $f:expr) => {{
        if $slice.sel_ptr.is_null() {
            let src = unsafe { std::slice::from_raw_parts($slice.data_ptr as *const $S, $len) };
            for &v in src {
                $buf.extend_from_slice(&($f)(v).to_ne_bytes());
            }
        } else {
            // See `append_primitive!` for why scatter uses a raw pointer.
            let src = $slice.data_ptr as *const $S;
            let sel = unsafe { std::slice::from_raw_parts($slice.sel_ptr, $len) };
            for &idx in sel {
                let v = unsafe { *src.add((idx - 1) as usize) };
                $buf.extend_from_slice(&($f)(v).to_ne_bytes());
            }
        }
    }};
}

/// Append numeric slice data directly into a `MutableBuffer`.
/// Identity (sequential) slices use a bulk byte copy; scattered slices loop element-wise.
unsafe fn append_numeric(
    buf: &mut MutableBuffer,
    slice: &ColumnSlice,
    column_type: i32,
    len: usize,
) -> Result<(), anyhow::Error> {
    match column_type {
        COLUMN_TYPE_INT32 | COLUMN_TYPE_DATE => append_primitive!(buf, slice, len, i32),
        COLUMN_TYPE_INT64 | COLUMN_TYPE_TIMESTAMP | COLUMN_TYPE_TIMESTAMPTZ => {
            append_primitive!(buf, slice, len, i64)
        }
        COLUMN_TYPE_FLOAT32 => append_primitive!(buf, slice, len, f32),
        COLUMN_TYPE_FLOAT64 => append_primitive!(buf, slice, len, f64),
        COLUMN_TYPE_DECIMAL_INT32 => {
            append_transform!(buf, slice, len, i32, |x: i32| x as i128)
        }
        COLUMN_TYPE_DECIMAL_INT64 => {
            append_transform!(buf, slice, len, i64, |x: i64| x as i128)
        }
        COLUMN_TYPE_DECIMAL_INT128 | COLUMN_TYPE_UUID => {
            // 16-byte elements — no primitive type; copy as raw bytes.
            if slice.sel_ptr.is_null() {
                let src =
                    unsafe { std::slice::from_raw_parts(slice.data_ptr as *const u8, len * 16) };
                buf.extend_from_slice(src);
            } else {
                // See `append_primitive!` for why scatter uses a raw pointer.
                let src = slice.data_ptr as *const u8;
                let sel = unsafe { std::slice::from_raw_parts(slice.sel_ptr, len) };
                for (i, &idx) in sel.iter().enumerate() {
                    if i + PREFETCH_DIST < len {
                        unsafe {
                            prefetch_read(
                                src.add((sel[i + PREFETCH_DIST] - 1) as usize * 16),
                            )
                        };
                    }
                    let off = (idx - 1) as usize * 16;
                    let chunk = unsafe { std::slice::from_raw_parts(src.add(off), 16) };
                    buf.extend_from_slice(chunk);
                }
            }
        }
        // Julia date/timestamp types carry a Julia-epoch offset that Rust removes here.
        // Source: i64[] days since 0001-01-01 → i32 days since 1970-01-01 (Date32).
        COLUMN_TYPE_JULIA_DATE => {
            append_transform!(buf, slice, len, i64, |v: i64| (v - JULIA_DATE_OFFSET)
                as i32)
        }
        // Source: i64[] ms since 0001-01-01 → i64 μs since 1970-01-01.
        COLUMN_TYPE_JULIA_TIMESTAMP | COLUMN_TYPE_JULIA_TIMESTAMPTZ => {
            append_transform!(buf, slice, len, i64, |v: i64| (v
                - JULIA_TIMESTAMP_OFFSET_MS)
                * 1_000)
        }
        // Source: i64[] ms since 0001-01-01 → i64 ns since 1970-01-01.
        COLUMN_TYPE_JULIA_TIMESTAMP_NS | COLUMN_TYPE_JULIA_TIMESTAMPTZ_NS => {
            append_transform!(buf, slice, len, i64, |v: i64| (v
                - JULIA_TIMESTAMP_OFFSET_MS)
                * 1_000_000)
        }
        _ => return Err(anyhow::anyhow!("unsupported column type {}", column_type)),
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
            // Capacity hints from the previous window so the reset never over-allocates.
            // With has_strings flush-per-slice, offsets.len() == slice_len+1 (~4097), not
            // coalesce_rows+1 (1M). Using len() here shrinks the reset from 4MB to ~16KB.
            let bytes_cap = bytes.capacity();
            let offsets_hint = offsets.len(); // rows+1 from the window just taken
            let taken_bytes = std::mem::replace(bytes, Vec::with_capacity(bytes_cap));
            let taken_offsets = std::mem::replace(offsets, {
                let mut v = Vec::with_capacity(offsets_hint.max(1));
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
        // JULIA_DATE stores the epoch-adjusted i32 value; Arrow type is the same as DATE.
        COLUMN_TYPE_DATE | COLUMN_TYPE_JULIA_DATE => Arc::new(PrimitiveArray::<Date32Type>::new(
            ScalarBuffer::new(buf, 0, rows),
            null_buf,
        )),
        COLUMN_TYPE_INT64 => Arc::new(PrimitiveArray::<Int64Type>::new(
            ScalarBuffer::new(buf, 0, rows),
            null_buf,
        )),
        // JULIA_TIMESTAMP stores epoch-adjusted μs; Arrow type is the same as TIMESTAMP.
        COLUMN_TYPE_TIMESTAMP | COLUMN_TYPE_JULIA_TIMESTAMP => {
            Arc::new(PrimitiveArray::<TimestampMicrosecondType>::new(
                ScalarBuffer::new(buf, 0, rows),
                null_buf,
            ))
        }
        // JULIA_TIMESTAMPTZ stores epoch-adjusted μs; Arrow type is the same as TIMESTAMPTZ.
        COLUMN_TYPE_TIMESTAMPTZ | COLUMN_TYPE_JULIA_TIMESTAMPTZ => Arc::new(
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
