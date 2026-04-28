# Writer API for RustyIceberg
#
# This module provides Julia wrappers for the iceberg-rust Writer API,
# enabling Julia to write Arrow data to Parquet files as Iceberg data files.

# Parquet field ID metadata key (must match iceberg-rust's PARQUET_FIELD_ID_META_KEY)
const PARQUET_FIELD_ID_META_KEY = "PARQUET:field_id"

# Default target file size: 512 MB (matches iceberg-rust default)
const DEFAULT_TARGET_FILE_SIZE_BYTES = 512 * 1024 * 1024

"""
    CompressionCodec

Compression codec for Parquet files.

# Values
- `UNCOMPRESSED`: No compression
- `SNAPPY`: Snappy compression (fast, moderate compression)
- `GZIP`: Gzip compression (slower, better compression)
- `LZ4`: LZ4 compression (very fast, lower compression)
- `ZSTD`: Zstandard compression (good balance of speed and compression)
"""
@enum CompressionCodec begin
    UNCOMPRESSED = 0
    SNAPPY = 1
    GZIP = 2
    LZ4 = 3
    ZSTD = 4
end

"""
    WriterConfig

Configuration options for the DataFileWriter.

# Fields
- `prefix::String`: Prefix for output file names (default: "data")
- `target_file_size_bytes::Int`: Target file size before rolling to a new file (default: 512 MB)
- `compression::CompressionCodec`: Parquet compression codec (default: UNCOMPRESSED)
- `dictionary_enabled::Bool`: Enable Parquet dictionary encoding globally (default: true)
- `max_row_group_size::Int`: Maximum rows per Parquet row group, 0 = parquet default (1 048 576)
- `data_page_size::Int`: Target uncompressed data page size in bytes, 0 = parquet default (1 048 576)
- `write_batch_size::Int`: Rows encoded per column chunk within a row group, 0 = parquet default (1024).
  Increasing this (e.g. 65536) reduces encoding overhead for large row groups.
- `plain_encoding::Bool`: Force PLAIN encoding for all columns, bypassing the DELTA_BINARY_PACKED
  default that parquet-rs uses for INT64/INT32 under PARQUET_2_0 (default: false).

# Example
```julia
config = WriterConfig(
    prefix = "my_data",
    compression = ZSTD,
    dictionary_enabled = false,  # disable dict encoding for benchmarking
    plain_encoding = true,       # use PLAIN to avoid DELTA_BINARY_PACKED overhead
    write_batch_size = 65536,    # larger batches for better throughput
)
writer = DataFileWriter(table, config)
```
"""
@kwdef struct WriterConfig
    prefix::String = "data"
    target_file_size_bytes::Int = DEFAULT_TARGET_FILE_SIZE_BYTES
    compression::CompressionCodec = UNCOMPRESSED
    dictionary_enabled::Bool = true
    max_row_group_size::Int = 0
    data_page_size::Int = 0
    write_batch_size::Int = 0
    plain_encoding::Bool = false
    statistics_enabled::Bool = true
end

# C-layout struct matching ParquetWriterPropertiesFFI in Rust.
# Fields are ordered largest-to-smallest to match repr(C) padding rules.
struct ParquetWriterPropertiesFFI
    max_row_group_size::Int64
    data_page_size::Int64
    write_batch_size::Int64
    compression_codec::Int32
    dictionary_enabled::Bool
    use_plain_encoding::Bool
    statistics_enabled::Bool
end

"""
    DataFileWriter

Opaque handle representing an Iceberg data file writer.

Create a writer using `DataFileWriter(table)` and free it with `free_writer!`
when done. Writers should be closed using `close_writer` to get the written
data files.

The writer tracks any `DataFiles` produced by `close_writer` and automatically
frees them when `free_writer!` is called, unless they have already been freed
or consumed.
"""
mutable struct DataFileWriter
    ptr::Ptr{Cvoid}
    table::Table  # Keep reference to table to prevent GC
    colmeta::Dict{Symbol, Vector{Pair{String, String}}}  # Column metadata with Iceberg field IDs
    data_files::Union{DataFiles, Nothing}  # Track DataFiles for automatic cleanup
end

# Response type for writer creation
const DataFileWriterResponse = Response{Ptr{Cvoid}}

# Response type for close operation (returns DataFiles)
const WriterCloseResponse = Response{Ptr{Cvoid}}

"""
    get_column_metadata(table::Table) -> Dict{Symbol, Vector{Pair{String, String}}}

Extract column metadata with Iceberg field IDs from a table's schema.

This function retrieves the table's schema and builds a metadata dictionary
that maps column names (as symbols) to their Iceberg field IDs. This metadata
is essential for writing Arrow tables with proper field ID information.

# Arguments
- `table::Table`: The Iceberg table to extract the schema from

# Returns
A dictionary mapping column names (Symbol) to metadata pairs containing
the PARQUET field ID for each column.

# Example
```julia
table = load_table(catalog, ["db"], "users")
colmeta = get_column_metadata(table)
# Use colmeta with Arrow.Table creation
arrow_table = Arrow.Table(Arrow.tobuffer(data; colmetadata=colmeta))
```
"""
function get_column_metadata(table::Table)::Dict{Symbol, Vector{Pair{String, String}}}
    schema_json = table_schema(table)
    schema = JSON.parse(schema_json)
    colmeta = Dict{Symbol, Vector{Pair{String, String}}}()
    if haskey(schema, "fields")
        for field in schema["fields"]
            if haskey(field, "name") && haskey(field, "id")
                colmeta[Symbol(field["name"])] = [PARQUET_FIELD_ID_META_KEY => string(field["id"])]
            end
        end
    end
    return colmeta
end

"""
    set_encode_workers!(n::Int)

Set the number of threads in the global Parquet encode worker pool.

Must be called before creating any `DataFileWriter`; has no effect once the pool
has been initialized (i.e. after the first writer is created). Defaults to
`Sys.CPU_THREADS` if not set.
"""
function set_encode_workers!(n::Int)
    n > 0 || throw(ArgumentError("n must be positive, got $n"))
    @ccall rust_lib.iceberg_set_encode_workers(n::Cint)::Cvoid
    return nothing
end

"""
    DataFileWriter(table::Table, config::WriterConfig) -> DataFileWriter
    DataFileWriter(table::Table; prefix="data", target_file_size_bytes=512MB, compression=UNCOMPRESSED) -> DataFileWriter

Create a new data file writer for the given table.

The writer writes data to Parquet files in the table's data directory.
Files are named using the prefix (e.g., "data-xxx.parquet").

# Arguments
- `table::Table`: The table to write data files for
- `config::WriterConfig`: Configuration options for the writer

Or use keyword arguments:
- `prefix::String`: Prefix for output file names (default: "data")
- `target_file_size_bytes::Int`: Target file size before rolling (default: 512 MB)
- `compression::CompressionCodec`: Compression codec (default: UNCOMPRESSED)

# Returns
A new `DataFileWriter` handle that must be freed with `free_writer!`.

# Example
```julia
# Using keyword arguments
writer = DataFileWriter(table; compression=ZSTD)

# Using WriterConfig
config = WriterConfig(prefix="mydata", compression=ZSTD)
writer = DataFileWriter(table, config)

write(writer, arrow_batch)
data_files = close_writer(writer)
free_writer!(writer)
```
"""
function DataFileWriter(table::Table, config::WriterConfig)
    response = DataFileWriterResponse()
    parquet_props = Ref(ParquetWriterPropertiesFFI(
        Int64(config.max_row_group_size),
        Int64(config.data_page_size),
        Int64(config.write_batch_size),
        Int32(config.compression),
        config.dictionary_enabled,
        config.plain_encoding,
        config.statistics_enabled,
    ))

    async_ccall(response, config.prefix, parquet_props) do handle
        @ccall rust_lib.iceberg_writer_new(
            table::Table,
            config.prefix::Cstring,
            config.target_file_size_bytes::Int64,
            parquet_props::Ref{ParquetWriterPropertiesFFI},
            response::Ref{DataFileWriterResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "DataFileWriter", IcebergException)

    # Get the table schema and build column metadata with Iceberg field IDs
    # This metadata is added to Arrow IPC data so iceberg-rust can match fields
    colmeta = get_column_metadata(table)

    return DataFileWriter(response.value, table, colmeta, nothing)
end

# Convenience constructor with keyword arguments
function DataFileWriter(table::Table;
    prefix::String="data",
    target_file_size_bytes::Int=DEFAULT_TARGET_FILE_SIZE_BYTES,
    compression::CompressionCodec=UNCOMPRESSED
)
    config = WriterConfig(prefix=prefix, target_file_size_bytes=target_file_size_bytes, compression=compression)
    return DataFileWriter(table, config)
end
"""
    with_data_file_writer(f::Function, table::Table, config::WriterConfig) -> DataFiles
    with_data_file_writer(f::Function, table::Table; prefix="data", ...) -> DataFiles

Create a writer, pass it to `f` for writing, then close and free it.

This provides a convenient way to write data with automatic cleanup,
ensuring the writer is closed and freed even if an error occurs. The
returned `DataFiles` handle is detached from the writer and must be
used in a transaction via `add_data_files`.

# Arguments
- `f`: A function that takes a `DataFileWriter` and writes data to it
- `table::Table`: The table to create a writer for
- `config::WriterConfig`: Configuration options for the writer

Or use keyword arguments:
- `prefix::String`: Prefix for generated file names (default: "data")
- `target_file_size_bytes::Int`: Target file size before rolling (default: 512 MB)
- `compression::CompressionCodec`: Compression codec (default: UNCOMPRESSED)

# Returns
A `DataFiles` handle containing the written files. This handle will be
automatically freed when passed to `add_data_files`.

# Example
```julia
# Using keyword arguments
data_files = with_data_file_writer(table; compression=ZSTD) do writer
    write(writer, batch1)
    write(writer, batch2)
end

# Using WriterConfig
config = WriterConfig(compression=ZSTD)
data_files = with_data_file_writer(table, config) do writer
    write(writer, batch1)
end

# Use the data files in a transaction
with_transaction(table, catalog) do tx
    with_fast_append(tx) do action
        add_data_files(action, data_files)
    end
end
```
"""
function with_data_file_writer(f::Function, table::Table, config::WriterConfig)
    writer = DataFileWriter(table, config)
    try
        f(writer)
        data_files = close_writer(writer)
        # Detach data_files from writer so they won't be freed when writer is freed
        # The caller is responsible for these data_files now
        writer.data_files = nothing
        return data_files
    finally
        free_writer!(writer)
    end
end

# Convenience do-block constructor with keyword arguments
function with_data_file_writer(f::Function, table::Table;
    prefix::String="data",
    target_file_size_bytes::Int=DEFAULT_TARGET_FILE_SIZE_BYTES,
    compression::CompressionCodec=UNCOMPRESSED
)
    config = WriterConfig(prefix=prefix, target_file_size_bytes=target_file_size_bytes, compression=compression)
    return with_data_file_writer(f, table, config)
end

"""
    free_writer!(writer::DataFileWriter)

Free the memory associated with a data file writer.

This also frees any `DataFiles` produced by `close_writer` that haven't been
freed yet. This ensures that data files are always cleaned up, even if the
user forgets to add them to a transaction.

This should be called after the writer has been closed or if
the writer is no longer needed.
"""
function free_writer!(writer::DataFileWriter)
    # Free any associated DataFiles first
    if writer.data_files !== nothing
        free_data_files!(writer.data_files)
        writer.data_files = nothing
    end
    if writer.ptr == C_NULL
        return nothing
    end
    @ccall rust_lib.iceberg_writer_free(writer.ptr::Ptr{Cvoid})::Cvoid
    writer.ptr = C_NULL
    return nothing
end

"""
    write(writer::DataFileWriter, data)

Write data to the writer.

The data is serialized to Arrow IPC format and written to Parquet files.
Multiple calls to `write` accumulate data until `close_writer` is called.

# Arguments
- `writer::DataFileWriter`: The writer to write to
- `data`: Arrow-compatible data (anything that Arrow.write can serialize, e.g. NamedTuple, DataFrame)

# Throws
- `IcebergException` if the write fails

# Example
```julia
# Write a single batch
write(writer, (id=[1, 2, 3], name=["a", "b", "c"]))

# Write multiple batches
for batch in batches
    write(writer, batch)
end
```
"""
function Base.write(writer::DataFileWriter, data)
    if writer.ptr == C_NULL
        throw(IcebergException("Writer has been freed"))
    end

    # Serialize data to Arrow IPC format with field ID metadata
    # The colmeta contains PARQUET:field_id for each column so iceberg-rust can match fields
    io = IOBuffer()
    Arrow.write(io, data; colmetadata=writer.colmeta)
    ipc_bytes = take!(io)

    _write_ipc_bytes(writer, ipc_bytes)
end

"""
    write(writer::DataFileWriter, table::Arrow.Table)

Write an Arrow.Table to the writer.

This method accepts an already-constructed Arrow.Table and serializes it to IPC format
for writing. Field ID metadata is added automatically to match the Iceberg schema.

# Arguments
- `writer::DataFileWriter`: The writer to write to
- `table::Arrow.Table`: An Arrow table to write

# Throws
- `IcebergException` if the write fails

# Example
```julia
# Write an Arrow.Table directly
arrow_table = Arrow.Table(id=[1, 2, 3], name=["a", "b", "c"])
write(writer, arrow_table)

# Or from reading an Arrow file
arrow_table = Arrow.Table(read("data.arrow"))
write(writer, arrow_table)
```
"""
function Base.write(writer::DataFileWriter, table::Arrow.Table)
    if writer.ptr == C_NULL
        throw(IcebergException("Writer has been freed"))
    end

    # Serialize Arrow.Table to IPC format with field ID metadata
    io = IOBuffer()
    Arrow.write(io, table; colmetadata=writer.colmeta)
    ipc_bytes = take!(io)

    _write_ipc_bytes(writer, ipc_bytes)
end

# Internal helper to write raw IPC bytes to the Rust writer
function _write_ipc_bytes(writer::DataFileWriter, ipc_bytes::Vector{UInt8})
    ipc_data = pointer(ipc_bytes)
    ipc_len = length(ipc_bytes)

    response = Response{Cvoid}(-1, nothing, C_NULL, C_NULL)

    async_ccall(response, ipc_bytes) do handle
        @ccall rust_lib.iceberg_writer_write(
            writer.ptr::Ptr{Cvoid},
            ipc_data::Ptr{UInt8},
            ipc_len::Csize_t,
            response::Ref{Response{Cvoid}},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "write", IcebergException)

    return nothing
end

"""
    close_writer(writer::DataFileWriter) -> DataFiles

Close the writer and return the written data files.

This flushes any remaining data, closes the Parquet file(s), and returns
a `DataFiles` handle containing metadata about the written files. The
`DataFiles` can then be used with `add_data_files` in a `FastAppendAction`.

After calling this, the writer cannot be used for writing again.

# Arguments
- `writer::DataFileWriter`: The writer to close

# Returns
A `DataFiles` handle that can be used with `fast_append`.

# Throws
- `IcebergException` if the close fails

# Example
```julia
writer = DataFileWriter(table)
write(writer, data)
data_files = close_writer(writer)

tx = Transaction(table)
with_fast_append(tx) do action
    add_data_files(action, data_files)
end
updated_table = commit(tx, catalog)

free_writer!(writer)  # Also frees data_files
```
"""
function close_writer(writer::DataFileWriter)
    if writer.ptr == C_NULL
        throw(IcebergException("Writer has been freed"))
    end

    response = WriterCloseResponse()

    async_ccall(response, writer) do handle
        @ccall rust_lib.iceberg_writer_close(
            writer.ptr::Ptr{Cvoid},
            response::Ref{WriterCloseResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "close_writer", IcebergException)

    data_files = DataFiles(response.value)
    writer.data_files = data_files  # Track for automatic cleanup
    return data_files
end

# ==========================================================================================
# Column-based writing (zero-copy from Julia)
# ==========================================================================================

"""
    SliceRef

FFI reference to a single slice of source column data for the scattered-gather writer.

- `data_ptr`: pointer to source data array (T[]) or string pointers (Ptr{UInt8}[])
- `lengths_ptr`: for string columns, pointer to lengths array; null for other types
- `validity_ptr`: pointer to validity bitmap (BitVector.chunks); null if all rows valid
- `sel_ptr`: pointer to selection index array (1-based Julia indices); null for sequential access
- `len`: number of rows in this slice

All fields are 8 bytes — total struct size is 40 bytes with no padding.
"""
struct SliceRef
    data_ptr::Ptr{Cvoid}
    lengths_ptr::Ptr{Int64}
    validity_ptr::Ptr{UInt8}
    sel_ptr::Ptr{Int64}
    len::Csize_t
end

"""
    GatheredColumnDescriptor

FFI descriptor for a column to be gathered from multiple SliceRefs.
Pass an array of these to `write_scattered_columns`.

- `slices_ptr`: pointer to array of SliceRef structs
- `num_slices`: number of SliceRef entries
- `total_rows`: sum of all slice lengths
- `column_type`: ColumnType enum value
- `is_nullable`: whether the column may contain null values
"""
struct GatheredColumnDescriptor
    slices_ptr::Ptr{SliceRef}
    num_slices::Csize_t
    total_rows::Csize_t
    column_type::Int32
    is_nullable::Bool
end

"""
    ColumnType

Enum for column data types, matching the Rust FFI constants.
"""
@enum ColumnType::Int32 begin
    COLUMN_TYPE_INT32 = 0
    COLUMN_TYPE_INT64 = 1
    COLUMN_TYPE_FLOAT32 = 2
    COLUMN_TYPE_FLOAT64 = 3
    COLUMN_TYPE_STRING = 4
    COLUMN_TYPE_DATE = 5
    COLUMN_TYPE_TIMESTAMP = 6      # Timestamp without timezone (Iceberg `timestamp`)
    COLUMN_TYPE_BOOLEAN = 7
    COLUMN_TYPE_UUID = 8
    COLUMN_TYPE_TIMESTAMPTZ = 9    # Timestamp with UTC timezone (Iceberg `timestamptz`)
    COLUMN_TYPE_DECIMAL_INT32 = 10  # Decimal backed by Int32 (precision ≤ 9)
    COLUMN_TYPE_DECIMAL_INT64 = 11  # Decimal backed by Int64 (precision ≤ 18)
    COLUMN_TYPE_DECIMAL_INT128 = 12 # Decimal backed by Int128 (precision > 18)
end

"""
    ColumnDescriptor

FFI structure describing a single column for direct column writing.
This struct must match the Rust `ColumnDescriptor` layout exactly.

# Fields
- `data_ptr::Ptr{Cvoid}`: Pointer to the raw column data. For strings, this is a
  pointer to an array of string pointers (Ptr{UInt8}[]).
- `lengths_ptr::Ptr{Int64}`: For string columns, pointer to lengths array (Int64[]).
  For other types, this is C_NULL.
- `validity_ptr::Ptr{UInt8}`: Pointer to validity bitmap (BitVector.chunks, bit-packed)
- `num_rows::Csize_t`: Number of rows in the column
- `column_type::Int32`: Type of the column (see `ColumnType` enum)
- `is_nullable::Bool`: Whether this column can contain null values

Note: Fields are ordered to avoid padding (8-byte fields first, then 4-byte, then 1-byte).
"""
struct ColumnDescriptor
    data_ptr::Ptr{Cvoid}        # 8 bytes, offset 0
    lengths_ptr::Ptr{Int64}     # 8 bytes, offset 8
    validity_ptr::Ptr{UInt8}    # 8 bytes, offset 16
    num_rows::Csize_t           # 8 bytes, offset 24
    column_type::Int32          # 4 bytes, offset 32
    is_nullable::Bool           # 1 byte,  offset 36
    # (3 bytes trailing padding added by compiler, total 40 bytes)
end

"""
    julia_type_to_column_type(::Type{T}) -> ColumnType

Map Julia types to the corresponding ColumnType enum value.
"""
julia_type_to_column_type(::Type{Int32}) = COLUMN_TYPE_INT32
julia_type_to_column_type(::Type{Int64}) = COLUMN_TYPE_INT64
julia_type_to_column_type(::Type{Float32}) = COLUMN_TYPE_FLOAT32
julia_type_to_column_type(::Type{Float64}) = COLUMN_TYPE_FLOAT64
julia_type_to_column_type(::Type{String}) = COLUMN_TYPE_STRING
julia_type_to_column_type(::Type{Dates.Date}) = COLUMN_TYPE_DATE
julia_type_to_column_type(::Type{Dates.DateTime}) = COLUMN_TYPE_TIMESTAMP
julia_type_to_column_type(::Type{Bool}) = COLUMN_TYPE_BOOLEAN
julia_type_to_column_type(::Type{UInt128}) = COLUMN_TYPE_UUID  # UUID stored as UInt128

"""
    iceberg_column_type(type::AbstractIcebergType) -> ColumnType

Map an Iceberg type to the corresponding ColumnType enum value for column writing.

This function enables using the Iceberg type system directly with the column-based
writer, avoiding the need to manually specify column types.

# Example
```julia
field = Field(Int32(1), "event_time", IcebergTimestamp())
col_type = iceberg_column_type(field.type)
# Returns COLUMN_TYPE_TIMESTAMP
```
"""
iceberg_column_type(::IcebergInt) = COLUMN_TYPE_INT32
iceberg_column_type(::IcebergLong) = COLUMN_TYPE_INT64
iceberg_column_type(::IcebergFloat) = COLUMN_TYPE_FLOAT32
iceberg_column_type(::IcebergDouble) = COLUMN_TYPE_FLOAT64
iceberg_column_type(::IcebergString) = COLUMN_TYPE_STRING
iceberg_column_type(::IcebergDate) = COLUMN_TYPE_DATE
iceberg_column_type(::IcebergTimestamp) = COLUMN_TYPE_TIMESTAMP
iceberg_column_type(::IcebergTimestamptz) = COLUMN_TYPE_TIMESTAMPTZ
iceberg_column_type(::IcebergBoolean) = COLUMN_TYPE_BOOLEAN
iceberg_column_type(::IcebergUuid) = COLUMN_TYPE_UUID
function iceberg_column_type(d::IcebergDecimal)
    if d.precision <= 9
        return COLUMN_TYPE_DECIMAL_INT32
    elseif d.precision <= 18
        return COLUMN_TYPE_DECIMAL_INT64
    else
        return COLUMN_TYPE_DECIMAL_INT128
    end
end

"""
    ColumnBatch

A builder for collecting column descriptors and their underlying arrays.
Automatically tracks arrays that need to be preserved during FFI calls.

# Example
```julia
batch = ColumnBatch()
push!(batch, ids)                           # non-nullable column
push!(batch, values; validity=validity_vec) # nullable column
write_columns(writer, batch)
```
"""
mutable struct ColumnBatch
    descriptors::Vector{ColumnDescriptor}
    arrays_to_preserve::Vector{Any}

    ColumnBatch() = new(ColumnDescriptor[], Any[])
end

"""
    push!(batch::ColumnBatch, data::Vector{String}; validity=nothing, length=nothing, column_type=nothing)

Add a string column to the batch. Strings are passed as an array of pointers with lengths.
Note: While this avoids copying on the Julia side, Arrow still copies the string data
into its internal buffer on the Rust side.

# Arguments
- `data`: The string column data array
- `validity`: Optional validity mask (BitVector where false=null, true=valid)
- `length`: Optional number of rows to use from the array. If not specified,
  uses the full array length.
- `column_type`: Optional explicit column type (defaults to COLUMN_TYPE_STRING)
"""
function Base.push!(
    batch::ColumnBatch,
    data::Vector{String};
    validity::Union{Nothing, BitVector}=nothing,
    length::Union{Nothing, Int}=nothing,
    column_type::Union{Nothing, ColumnType}=nothing
)
    num_rows = length === nothing ? Base.length(data) : length
    is_nullable = validity !== nothing
    col_type = column_type === nothing ? COLUMN_TYPE_STRING : column_type

    # Build arrays of string pointers and lengths (no copy on Julia side)
    # Each String in Julia is a pointer to contiguous UTF-8 bytes
    # For null values, we use null pointer and zero length - Rust will check validity mask
    str_ptrs = Vector{Ptr{UInt8}}(undef, num_rows)
    str_lens = Vector{Int64}(undef, num_rows)
    for i in 1:num_rows
        if is_nullable && !validity[i]
            # Null value - use null pointer and zero length
            str_ptrs[i] = Ptr{UInt8}(C_NULL)
            str_lens[i] = 0
        else
            str_ptrs[i] = pointer(data[i])
            str_lens[i] = sizeof(data[i])
        end
    end

    # Preserve all arrays (original strings + metadata arrays)
    push!(batch.arrays_to_preserve, data)
    push!(batch.arrays_to_preserve, str_ptrs)
    push!(batch.arrays_to_preserve, str_lens)

    validity_ptr = if is_nullable
        push!(batch.arrays_to_preserve, validity)
        Ptr{UInt8}(pointer(validity.chunks))
    else
        Ptr{UInt8}(C_NULL)
    end

    # For strings: data_ptr = pointer to string pointers, offsets_ptr = pointer to lengths
    desc = ColumnDescriptor(
        Ptr{Cvoid}(pointer(str_ptrs)),
        pointer(str_lens),  # Reuse offsets_ptr for lengths array
        validity_ptr,
        Csize_t(num_rows),
        Int32(col_type),
        is_nullable
    )
    push!(batch.descriptors, desc)
    return batch
end

"""
    push!(batch::ColumnBatch, data::Vector{String}, str_ptrs::Vector{Ptr{UInt8}}, str_lens::Vector{Int64}; validity=nothing, length=nothing, column_type=nothing)

Add a string column to the batch using pre-allocated pointer/length buffers.
The caller is responsible for filling `str_ptrs` and `str_lens` before calling this.
Avoids allocating new pointer/length arrays on every write.
"""
function Base.push!(
    batch::ColumnBatch,
    data::Vector{String},
    str_ptrs::Vector{Ptr{UInt8}},
    str_lens::Vector{Int64};
    validity::Union{Nothing, BitVector}=nothing,
    length::Union{Nothing, Int}=nothing,
    column_type::Union{Nothing, ColumnType}=nothing,
)
    num_rows = length === nothing ? Base.length(str_ptrs) : length
    is_nullable = validity !== nothing
    col_type = column_type === nothing ? COLUMN_TYPE_STRING : column_type

    push!(batch.arrays_to_preserve, data, str_ptrs, str_lens)

    validity_ptr = if is_nullable
        push!(batch.arrays_to_preserve, validity)
        Ptr{UInt8}(pointer(validity.chunks))
    else
        Ptr{UInt8}(C_NULL)
    end

    desc = ColumnDescriptor(
        Ptr{Cvoid}(pointer(str_ptrs)),
        pointer(str_lens),
        validity_ptr,
        Csize_t(num_rows),
        Int32(col_type),
        is_nullable
    )
    push!(batch.descriptors, desc)
    return batch
end

"""
    push!(batch::ColumnBatch, data::Vector{T}; validity=nothing, length=nothing, column_type=nothing) where T

Add a column to the batch. The column type is inferred from the element type unless
explicitly specified.

# Arguments
- `data`: The column data array
- `validity`: Optional validity mask (BitVector where false=null, true=valid)
- `length`: Optional number of rows to use from the array. If not specified,
  uses the full array length. This allows writing only a prefix of the array.
- `column_type`: Optional explicit column type (ColumnType enum). If not specified,
  inferred from the element type T. Use this when the physical storage type differs
  from the logical type (e.g., Int32 data that represents Date32).
"""
function Base.push!(
    batch::ColumnBatch,
    data::Vector{T};
    validity::Union{Nothing, BitVector}=nothing,
    length::Union{Nothing, Int}=nothing,
    column_type::Union{Nothing, ColumnType}=nothing
) where T
    push!(batch.arrays_to_preserve, data)

    col_type = column_type === nothing ? julia_type_to_column_type(T) : column_type
    num_rows = length === nothing ? Base.length(data) : length
    is_nullable = validity !== nothing

    validity_ptr = if is_nullable
        # BitVector stores bits in UInt64 chunks - pass pointer to chunks directly
        push!(batch.arrays_to_preserve, validity)
        Ptr{UInt8}(pointer(validity.chunks))
    else
        Ptr{UInt8}(C_NULL)
    end

    desc = ColumnDescriptor(
        Ptr{Cvoid}(pointer(data)),
        Ptr{Int64}(C_NULL),  # lengths_ptr not used for non-string types
        validity_ptr,
        Csize_t(num_rows),
        Int32(col_type),
        is_nullable
    )
    push!(batch.descriptors, desc)
    return batch
end

"""
    write_columns(writer::DataFileWriter, columns::Vector{ColumnDescriptor}, arrays_to_preserve)

Write raw column data directly to the Parquet writer, bypassing Arrow IPC serialization.

This is a low-level function that passes raw column pointers to Rust, which builds
Arrow arrays directly from them. This avoids one serialization step compared to
the standard `write` function.

# Arguments
- `writer::DataFileWriter`: The writer to write to
- `columns::Vector{ColumnDescriptor}`: Array of column descriptors
- `arrays_to_preserve`: A tuple/collection of arrays whose memory is referenced by the
  ColumnDescriptors. These will be GC-preserved during the FFI call.

# Safety
The ColumnDescriptors contain raw pointers that must point to valid data.
Pass all source arrays in `arrays_to_preserve` to ensure they are not garbage
collected during the FFI call.

# Throws
- `IcebergException` if the write fails

# Example
```julia
data = Int64[1, 2, 3]
validity = UInt8[1, 1, 1]
desc = ColumnDescriptor(pointer(data), ...)
write_columns(writer, [desc], (data, validity))  # Arrays preserved during call
```
"""
function write_columns(writer::DataFileWriter, columns::Vector{ColumnDescriptor}, arrays_to_preserve)
    writer.ptr == C_NULL && throw(IcebergException("Writer has been freed"))
    isempty(columns) && throw(IcebergException("No columns provided"))

    ret = GC.@preserve columns arrays_to_preserve begin
        @ccall rust_lib.iceberg_writer_write_columns_sync(
            writer.ptr::Ptr{Cvoid},
            pointer(columns)::Ptr{ColumnDescriptor},
            length(columns)::Csize_t,
        )::Int32
    end

    ret == 0 || throw(IcebergException("write_columns failed (see writer close for details)"))
    return nothing
end

"""
    write_columns(writer::DataFileWriter, batch::ColumnBatch)

Write columns from a ColumnBatch to the Parquet writer.

This is the recommended way to use write_columns - the ColumnBatch automatically
tracks all arrays that need to be preserved during the FFI call.

# Arguments
- `writer::DataFileWriter`: The writer to write to
- `batch::ColumnBatch`: The column batch to write

# Example
```julia
batch = ColumnBatch()
push!(batch, ids)
push!(batch, values; validity=validity_vec)
write_columns(writer, batch)

# To write only first 100 rows, use the length parameter on push!:
batch = ColumnBatch()
push!(batch, ids; length=100)
push!(batch, values; validity=validity_vec, length=100)
write_columns(writer, batch)
```
"""
function write_columns(writer::DataFileWriter, batch::ColumnBatch)
    write_columns(writer, batch.descriptors, batch.arrays_to_preserve)
end

# ==========================================================================================
# High-level gathered-column API
# ==========================================================================================

"""
    GatheredColumn

Accumulates one or more source slices for a single column. Rust gathers the data
directly from source buffers when the batch is written, avoiding a Julia-side staging
copy for numeric columns.

Typical usage:

```julia
col = GatheredColumn(COLUMN_TYPE_INT64)
add_slice!(col, src_array)                        # sequential: all rows
add_slice!(col, src_array2; sel=sel_indices)      # scattered: rows at sel_indices
add_slice!(col, src_array3; validity=valid_bv)    # nullable slice
```

String columns are not supported on the scattered path; use `ColumnBatch` instead.
"""
mutable struct GatheredColumn
    slices::Vector{SliceRef}
    total_rows::Int
    column_type::ColumnType
    is_nullable::Bool
    preserve::Vector{Any}   # source arrays kept alive until write
end

GatheredColumn(column_type::ColumnType; nullable::Bool=false) =
    GatheredColumn(SliceRef[], 0, column_type, nullable, Any[])

"""
    add_slice!(col::GatheredColumn, data::AbstractVector{T};
               sel=nothing, validity=nothing)

Append a slice of `data` to `col`.

- `sel`: optional `Vector{Int64}` of 1-based row indices into `data` to select.
  If omitted, all rows of `data` are used sequentially.
- `validity`: optional `BitVector` (length = number of selected rows, `true` = valid).
  Providing this marks the column as nullable.
"""
function add_slice!(
    col::GatheredColumn,
    data::AbstractVector{T};
    sel::Union{Nothing, Vector{Int64}} = nothing,
    validity::Union{Nothing, BitVector} = nothing,
) where T
    len = sel === nothing ? length(data) : length(sel)

    sel_ptr = if sel !== nothing
        push!(col.preserve, sel)
        pointer(sel)
    else
        Ptr{Int64}(C_NULL)
    end

    validity_ptr = if validity !== nothing
        col.is_nullable = true
        push!(col.preserve, validity)
        Ptr{UInt8}(pointer(validity.chunks))
    else
        Ptr{UInt8}(C_NULL)
    end

    push!(col.preserve, data)
    push!(col.slices, SliceRef(
        Ptr{Cvoid}(pointer(data)),
        Ptr{Int64}(C_NULL),   # lengths_ptr unused for non-string types
        validity_ptr,
        sel_ptr,
        Csize_t(len),
    ))
    col.total_rows += len
    return col
end

"""
    add_string_slice!(col::GatheredColumn, str_ptrs, str_lens; validity=nothing)

Append a string slice to `col`. `str_ptrs` is a `Vector{Ptr{UInt8}}` of pointers to
UTF-8 string data and `str_lens` is a `Vector{Int64}` of corresponding byte lengths.
The caller is responsible for keeping the pointed-to string bytes alive.
"""
function add_string_slice!(
    col::GatheredColumn,
    str_ptrs::Vector{Ptr{UInt8}},
    str_lens::Vector{Int64};
    validity::Union{Nothing, BitVector} = nothing,
)
    len = length(str_ptrs)

    validity_ptr = if validity !== nothing
        col.is_nullable = true
        push!(col.preserve, validity)
        Ptr{UInt8}(pointer(validity.chunks))
    else
        Ptr{UInt8}(C_NULL)
    end

    push!(col.preserve, str_ptrs, str_lens)
    push!(col.slices, SliceRef(
        Ptr{Cvoid}(pointer(str_ptrs)),
        pointer(str_lens),
        validity_ptr,
        Ptr{Int64}(C_NULL),
        Csize_t(len),
    ))
    col.total_rows += len
    return col
end

"""
    GatheredBatch

Collects a `GatheredColumn` per output column, then writes all of them in one call.

```julia
batch = GatheredBatch()
push!(batch, col_int64)
push!(batch, col_float64)
write_scattered_columns_sync(writer, batch)
```

You can also push a single-slice column inline without building a `GatheredColumn`
explicitly:

```julia
batch = GatheredBatch()
push!(batch, src_ints,   COLUMN_TYPE_INT64)
push!(batch, src_floats, COLUMN_TYPE_FLOAT64; sel=indices, validity=valid_bv)
write_scattered_columns_sync(writer, batch)
```
"""
mutable struct GatheredBatch
    columns::Vector{GatheredColumn}
end

GatheredBatch() = GatheredBatch(GatheredColumn[])

"""
    push!(batch::GatheredBatch, col::GatheredColumn)

Append an already-built `GatheredColumn` to the batch.
"""
Base.push!(batch::GatheredBatch, col::GatheredColumn) = (push!(batch.columns, col); batch)

"""
    push!(batch::GatheredBatch, data::AbstractVector, column_type::ColumnType;
          sel=nothing, validity=nothing, nullable=false)

Convenience: create a single-slice `GatheredColumn` from `data` and append it.
"""
function Base.push!(
    batch::GatheredBatch,
    data::AbstractVector,
    column_type::ColumnType;
    sel::Union{Nothing, Vector{Int64}} = nothing,
    validity::Union{Nothing, BitVector} = nothing,
    nullable::Bool = validity !== nothing,
)
    col = GatheredColumn(column_type; nullable)
    add_slice!(col, data; sel, validity)
    push!(batch.columns, col)
    return batch
end

"""
    write_scattered_columns_sync(writer::DataFileWriter, batch::GatheredBatch[, extra_preserve])

Gather column data from Julia memory synchronously, then encode asynchronously.

Gathers all column data from Julia memory in the calling thread using a plain blocking
`ccall`. Encode runs asynchronously in the global worker pool.

`extra_preserve` (optional) is an additional collection of objects whose memory must
stay alive during the gather (e.g. source string arrays for zero-copy string columns).

The source data pointed to by the `GatheredBatch` slices and `extra_preserve` must be
valid for the duration of this call. After the call returns, all Julia pointers have
been consumed and the source data may be safely released.
"""
function write_scattered_columns_sync(
    writer::DataFileWriter,
    batch::GatheredBatch,
    extra_preserve = nothing,
)
    isempty(batch.columns) && throw(IcebergException("GatheredBatch has no columns"))
    writer.ptr == C_NULL && throw(IcebergException("Writer has been freed"))

    all_slice_arrays = Vector{Vector{SliceRef}}(undef, length(batch.columns))
    descriptors = Vector{GatheredColumnDescriptor}(undef, length(batch.columns))
    preserve = Any[]

    for (i, col) in enumerate(batch.columns)
        slices = col.slices
        all_slice_arrays[i] = slices
        append!(preserve, col.preserve)
        push!(preserve, slices)
        descriptors[i] = GatheredColumnDescriptor(
            pointer(slices),
            Csize_t(length(slices)),
            Csize_t(col.total_rows),
            Int32(col.column_type),
            col.is_nullable,
        )
    end
    extra_preserve !== nothing && append!(preserve, extra_preserve)

    ret = GC.@preserve preserve all_slice_arrays descriptors begin
        @ccall rust_lib.iceberg_writer_write_scattered_columns_sync(
            writer.ptr::Ptr{Cvoid},
            pointer(descriptors)::Ptr{GatheredColumnDescriptor},
            length(descriptors)::Csize_t,
        )::Int32
    end
    ret == 0 || throw(IcebergException("write_scattered_columns_sync: gather failed (see writer close for details)"))
    return nothing
end
