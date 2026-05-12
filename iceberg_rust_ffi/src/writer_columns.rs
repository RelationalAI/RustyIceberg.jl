/// Column-based writer support for iceberg_rust_ffi
///
/// This module provides the FFI structs and column type constants shared between the
/// flat-column write path (`iceberg_writer_write_columns`) and the incremental batch
/// builder (`batch_builder.rs`). All Arrow array construction logic lives in
/// `batch_builder.rs`; this file is intentionally thin.
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
