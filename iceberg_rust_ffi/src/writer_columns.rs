/// Shared FFI types and column-type constants for the column-based write path.
///
/// The only consumer of `ColumnSlice` outside this module is `record_batch_builder.rs`,
/// which owns all Arrow array construction. The struct stays here because it is part of
/// the FFI ABI surface that Julia constructs.
use std::ffi::c_void;

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
/// Julia-epoch date: source data is i64[] of days since 0001-01-01; Rust subtracts 719163 and writes i32 Date32.
pub const COLUMN_TYPE_JULIA_DATE: i32 = 13;
/// Julia-epoch timestamp: source data is i64[] of ms since 0001-01-01; Rust converts to μs since Unix epoch.
pub const COLUMN_TYPE_JULIA_TIMESTAMP: i32 = 14;
/// Julia-epoch timestamp with UTC timezone: same conversion as JULIA_TIMESTAMP, UTC-tagged.
pub const COLUMN_TYPE_JULIA_TIMESTAMPTZ: i32 = 15;
/// Julia-epoch nanosecond timestamp: source data is i64[] of ms since 0001-01-01; Rust converts to ns since Unix epoch.
pub const COLUMN_TYPE_JULIA_TIMESTAMP_NS: i32 = 16;
/// Julia-epoch nanosecond timestamp with UTC timezone.
pub const COLUMN_TYPE_JULIA_TIMESTAMPTZ_NS: i32 = 17;

/// One column's contribution to a single `RowChunk` — a reference to source data the
/// builder will copy on `append`. All fields are 8 bytes; total struct size is 40 bytes
/// with no padding.
///
/// - `sel_ptr = null`  → sequential (identity) access: read `data[0..len]`. `data_ptr`
///   must be valid for `len` elements.
/// - `sel_ptr != null` → scattered access: read `data[sel[i] - 1]` for `i in 0..len`
///   (1-based Julia indices). `data_ptr` must be valid for `max(sel)` elements; the
///   source array is typically longer than `len`, so the gather path uses raw pointer
///   arithmetic rather than a `&[T]` of length `len`.
/// - `validity_ptr = null` → all rows in this slice are valid.
/// - `lengths_ptr != null` → string column: `data_ptr` is `*const *const u8`,
///   `lengths_ptr` is `*const i64` of byte lengths per string.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ColumnSlice {
    pub data_ptr: *const c_void,
    pub lengths_ptr: *const i64,
    pub validity_ptr: *const u8,
    pub sel_ptr: *const i64,
    pub len: usize,
}

unsafe impl Send for ColumnSlice {}
unsafe impl Sync for ColumnSlice {}
