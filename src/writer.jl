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
- `LZ4`: LZ4 compression, legacy Hadoop-framed variant (deprecated in the parquet spec; kept for
  backward compatibility — prefer `LZ4_RAW`)
- `ZSTD`: Zstandard compression (good balance of speed and compression)
- `LZ4_RAW`: LZ4 compression, raw blocks with no framing overhead (modern parquet variant; faster
  than `LZ4`)
"""
@enum CompressionCodec begin
    UNCOMPRESSED = 0
    SNAPPY = 1
    GZIP = 2
    LZ4 = 3
    ZSTD = 4
    LZ4_RAW = 5
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

Must be called before the first `DataFileWriter` is created (i.e. before the pool is
initialized). Throws if the pool is already running. Defaults to `Sys.CPU_THREADS`
if not set.
"""
function set_encode_workers!(n::Int)
    n > 0 || throw(ArgumentError("n must be positive, got $n"))
    ret = @ccall rust_lib.iceberg_set_encode_workers(n::Cint)::Int32
    ret == 0 || throw(IcebergException(
        INTERNAL,
        "Internal error (please report this as a bug)",
        "set_encode_workers! must be called before creating any DataFileWriter",
    ))
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
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "Writer has been freed",
        ))
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
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "Writer has been freed",
        ))
    end

    # Serialize Arrow.Table to IPC format with field ID metadata
    io = IOBuffer()
    Arrow.write(io, table; colmetadata=writer.colmeta)
    ipc_bytes = take!(io)

    _write_ipc_bytes(writer, ipc_bytes)
end

# Internal helper to write raw IPC bytes to the Rust writer
function _write_ipc_bytes(writer::DataFileWriter, ipc_bytes::Vector{UInt8})
    ret = GC.@preserve ipc_bytes begin
        @ccall rust_lib.iceberg_writer_write(
            writer.ptr::Ptr{Cvoid},
            pointer(ipc_bytes)::Ptr{UInt8},
            length(ipc_bytes)::Csize_t,
        )::Int32
    end
    ret == 0 || throw(IcebergException(
        DATA_SCHEMA_MISMATCH,
        "Column not found in table schema",
        "write failed (see writer close for details)",
    ))
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
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "Writer has been freed",
        ))
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
#
# A user produces one `RowChunk` per upstream slice (a horizontal stripe of rows across all
# output columns), then calls `append!(writer, chunk)`. The writer copies the data eagerly
# into Rust-owned per-column buffers; the source Julia arrays may be released the moment
# `append!` returns. When the accumulated row count reaches the coalesce window, the writer
# finalizes a `RecordBatch` and ships it to the async encode pool automatically. Callers
# that need flush control on logical boundaries can call `flush!(writer)`.
# ==========================================================================================

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
    # Julia-epoch variants: source data uses Julia's internal epoch (0001-01-01);
    # Rust applies the offset to produce Iceberg's Unix-epoch representation.
    COLUMN_TYPE_JULIA_DATE = 13           # i64[] days since year 1 → i32 days since 1970-01-01
    COLUMN_TYPE_JULIA_TIMESTAMP = 14      # i64[] ms since year 1 → i64 μs since 1970-01-01
    COLUMN_TYPE_JULIA_TIMESTAMPTZ = 15    # same + UTC timezone
    COLUMN_TYPE_JULIA_TIMESTAMP_NS = 16   # i64[] ms since year 1 → i64 ns since 1970-01-01
    COLUMN_TYPE_JULIA_TIMESTAMPTZ_NS = 17 # same + UTC timezone
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
iceberg_column_type(::IcebergDate)          = COLUMN_TYPE_JULIA_DATE
iceberg_column_type(::IcebergTimestamp)     = COLUMN_TYPE_JULIA_TIMESTAMP
iceberg_column_type(::IcebergTimestamptz)   = COLUMN_TYPE_JULIA_TIMESTAMPTZ
iceberg_column_type(::IcebergTimestampNs)   = COLUMN_TYPE_JULIA_TIMESTAMP_NS
iceberg_column_type(::IcebergTimestamptzNs) = COLUMN_TYPE_JULIA_TIMESTAMPTZ_NS
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
    ColumnSlice

FFI struct describing one column's contribution to a single `RowChunk`. Internal —
users build `RowChunk`s via `push!` instead of constructing `ColumnSlice` directly.

All fields are 8 bytes; total struct size is 40 bytes with no padding (matches Rust's
`ColumnSlice` layout).
"""
struct ColumnSlice
    data_ptr::Ptr{Cvoid}
    lengths_ptr::Ptr{Int64}
    validity_ptr::Ptr{UInt8}
    sel_ptr::Ptr{Int64}
    len::Csize_t
end

"""
    RowChunk

A horizontal stripe of rows across all output columns — what a streaming producer hands
to the writer at each step. Build one by pushing column data in schema order, then call
`append!(writer, chunk)`.

```julia
chunk = RowChunk()
push!(chunk, ids)                                            # non-nullable numeric, sequential
push!(chunk, values; validity=src_valid_bv)                  # nullable numeric, sequential
push!(chunk, source_scores;
      validity=src_valid_bv, sel=sel_indices)                # nullable scattered: validity is
                                                              # aligned to `source_scores`; the
                                                              # library gathers through `sel`.
push!(chunk, tags; validity=src_valid_bv)                    # nullable strings
append!(writer, chunk)
```

For streaming pipelines that push many chunks, reuse the same `RowChunk` and call
`empty!(chunk)` at the top of each iteration. `empty!` clears the working vectors but
retains the chunk's internal pool of `str_ptrs` / `str_lens` buffers, so string columns
amortize their per-chunk gather work to zero.

```julia
chunk = RowChunk()
for slice in upstream
    empty!(chunk)
    push!(chunk, slice.ids)
    push!(chunk, slice.tags; validity=slice.v)
    append!(writer, chunk)
end
```

Column order must be stable across iterations (it has to match the writer's schema
anyway). The internal string pool is keyed by column position.
"""
mutable struct RowChunk
    slices::Vector{ColumnSlice}                  # FFI-ready, one entry per push!
    preserve::Vector{Any}                        # GC roots for source data / validity / sel
    # Per-column-position string scratch. Entry `i` holds the `str_ptrs` / `str_lens` /
    # `str_validity` buffers for the i-th pushed column when it was a string column.
    # `str_validity` is the output-aligned validity bitmap Rust receives when both `sel`
    # and `validity` are present on a string push — see that `push!` overload for why.
    # Non-string positions have unused (length-0) entries. Retained across `empty!` so
    # streaming reuse is free.
    str_ptrs::Vector{Vector{Ptr{UInt8}}}
    str_lens::Vector{Vector{Int64}}
    str_validity::Vector{BitVector}
end

RowChunk() = RowChunk(
    ColumnSlice[], Any[], Vector{Ptr{UInt8}}[], Vector{Int64}[], BitVector[],
)

"""
    empty!(chunk::RowChunk)

Reset the chunk's working vectors so the next pass can refill them. The internal string
ptr/len pool is *not* freed — it stays sized at the previous high-water mark, so a
streaming loop that calls `empty!` then `push!` repeatedly pays zero per-iteration
allocation for the string-gather buffers.

Drop the chunk and create a new one if you want to actually reclaim the pool memory.
"""
function Base.empty!(chunk::RowChunk)
    empty!(chunk.slices)
    empty!(chunk.preserve)
    return chunk
end

"""
    push!(chunk::RowChunk, data::AbstractVector{T};
          validity=nothing, sel=nothing)

Add a non-string column slice to the chunk. Builds the FFI-ready `ColumnSlice` inline —
just `pointer(data)` and pointer arithmetic, no allocation beyond the chunk's own
`slices` / `preserve` growth.

- `validity`: optional `BitVector` *aligned to `data`* — `length(validity) >= length(data)`,
  bit `i` describes whether `data[i]` is valid (`true` = valid, `false` = null). When
  `sel` is also provided, Rust gathers validity through `sel` alongside the value gather:
  bit `sel[i] - 1` of the source bitmap becomes bit `i` of the output bitmap. When `sel`
  is omitted, source and output positions coincide.
- `sel`: optional contiguous `AbstractVector{Int64}` of 1-based indices into `data` for
  scattered access. Output row count is `length(sel)`. If omitted, all rows of `data`
  are used sequentially. A `view(sel_buf, 1:n)` is accepted — useful when the caller
  has a sel buffer with stale capacity beyond the live region.
"""
function Base.push!(
    chunk::RowChunk,
    data::AbstractVector{T};
    validity::Union{Nothing, BitVector} = nothing,
    sel::Union{Nothing, AbstractVector{Int64}} = nothing,
) where T
    len = sel === nothing ? length(data) : length(sel)

    sel_ptr = if sel !== nothing
        push!(chunk.preserve, sel)
        pointer(sel)
    else
        Ptr{Int64}(C_NULL)
    end

    validity_ptr = if validity !== nothing
        push!(chunk.preserve, validity)
        Ptr{UInt8}(pointer(validity.chunks))
    else
        Ptr{UInt8}(C_NULL)
    end

    push!(chunk.preserve, data)
    push!(chunk.slices, ColumnSlice(
        Ptr{Cvoid}(pointer(data)),
        Ptr{Int64}(C_NULL),
        validity_ptr,
        sel_ptr,
        Csize_t(len),
    ))
    return chunk
end

"""
    push!(
        chunk::RowChunk, strings::AbstractVector{<:AbstractString};
        validity=nothing, sel=nothing
    )

Add a string column slice to the chunk. Accepts any `AbstractString` element type
(`String`, `SubString{String}`, Arrow's `VariableSizeString`, …) — `pointer(s)` and
`ncodeunits(s)` are used directly, no materialization through `Vector{String}`.

The ptr/len gather buffers live in the chunk's per-position pool and are reused across
`empty!`/refill cycles, so a streaming loop pays no allocation for them after the first
chunk.

- `validity`: optional `BitVector` *aligned to `strings`* — `length(validity) >= length(strings)`,
  bit `i` describes whether `strings[i]` is valid (`true` = valid, `false` = null).
- `sel`: optional contiguous `AbstractVector{Int64}` of 1-based indices into `strings`
  for scattered access. Output row count is `length(sel)`. If omitted, all rows of
  `strings` are used sequentially.

Unlike the numeric `push!`, the value gather is performed here on the Julia side
(`pointer(strings[sel[i]])` per row) because Rust can't walk a Julia `AbstractString`
vector across the FFI. When both `sel` and `validity` are supplied, the validity gather
is folded into that same loop and an output-aligned bitmap is materialized in the
chunk's per-position pool. The consumer-visible contract is therefore identical to the
numeric case — pass a source-aligned `validity` either way.
"""
function Base.push!(
    chunk::RowChunk,
    strings::AbstractVector{<:AbstractString};
    validity::Union{Nothing, BitVector} = nothing,
    sel::Union{Nothing, AbstractVector{Int64}} = nothing,
)
    pos = length(chunk.slices) + 1
    # Grow the per-position pool to cover this column's slot. Lazy and retained across
    # `empty!` calls, so subsequent iterations are no-ops here.
    while length(chunk.str_ptrs) < pos
        push!(chunk.str_ptrs, Ptr{UInt8}[])
        push!(chunk.str_lens, Int64[])
        push!(chunk.str_validity, BitVector())
    end
    str_ptrs = chunk.str_ptrs[pos]
    str_lens = chunk.str_lens[pos]

    n = sel === nothing ? length(strings) : length(sel)
    resize!(str_ptrs, n)        # amortized O(1) once the pool is sized
    resize!(str_lens, n)

    is_nullable = validity !== nothing
    # Rust never sees `sel_ptr` for string columns (the value gather is pre-applied
    # below), so when `sel` *and* `validity` are both supplied we have to rewrite the
    # source-aligned validity bitmap to an output-aligned one here. With `sel === nothing`,
    # source and output positions coincide and we pass `validity` through unchanged.
    needs_validity_gather = is_nullable && sel !== nothing
    str_validity = needs_validity_gather ? chunk.str_validity[pos] : nothing
    if needs_validity_gather
        resize!(str_validity, n)
    end

    if sel === nothing
        @inbounds for i in 1:n
            if is_nullable && !validity[i]
                str_ptrs[i] = Ptr{UInt8}(C_NULL)
                str_lens[i] = 0
            else
                s = strings[i]
                str_ptrs[i] = pointer(s)
                str_lens[i] = ncodeunits(s)
            end
        end
    elseif needs_validity_gather
        @inbounds for i in 1:n
            src = sel[i]
            if !validity[src]
                str_ptrs[i] = Ptr{UInt8}(C_NULL)
                str_lens[i] = 0
                str_validity[i] = false
            else
                s = strings[src]
                str_ptrs[i] = pointer(s)
                str_lens[i] = ncodeunits(s)
                str_validity[i] = true
            end
        end
        push!(chunk.preserve, sel)
    else
        @inbounds for i in 1:n
            s = strings[sel[i]]
            str_ptrs[i] = pointer(s)
            str_lens[i] = ncodeunits(s)
        end
        push!(chunk.preserve, sel)
    end

    push!(chunk.preserve, strings)
    validity_ptr = if needs_validity_gather
        push!(chunk.preserve, str_validity)
        Ptr{UInt8}(pointer(str_validity.chunks))
    elseif validity !== nothing
        push!(chunk.preserve, validity)
        Ptr{UInt8}(pointer(validity.chunks))
    else
        Ptr{UInt8}(C_NULL)
    end

    push!(chunk.slices, ColumnSlice(
        Ptr{Cvoid}(pointer(str_ptrs)),
        pointer(str_lens),
        validity_ptr,
        Ptr{Int64}(C_NULL),
        Csize_t(n),
    ))
    return chunk
end

"""
    append!(writer::DataFileWriter, chunk::RowChunk)

Hand one `RowChunk` to the writer. The chunk's `slices` are already FFI-ready, so this
just pins memory, fires the FFI, and returns. Rust copies all slice data synchronously
into per-column buffers; the source arrays may be released the moment this call returns.

When the accumulated window reaches the coalesce size the writer auto-flushes a
`RecordBatch` to the encode pool. `append!` calls are appended in order — no reordering.
"""
function Base.append!(writer::DataFileWriter, chunk::RowChunk)
    writer.ptr == C_NULL && throw(IcebergException(
        STATE_RESOURCE_FREED,
        "Resource has been freed",
        "Writer has been freed",
    ))
    isempty(chunk.slices) && throw(IcebergException(
        DATA_SCHEMA_MISMATCH,
        "Column not found in table schema",
        "RowChunk has no columns",
    ))
    ret = GC.@preserve chunk begin
        @ccall rust_lib.iceberg_writer_append(
            writer.ptr::Ptr{Cvoid},
            pointer(chunk.slices)::Ptr{ColumnSlice},
            length(chunk.slices)::Csize_t,
        )::Int32
    end
    ret == 0 || throw(IcebergException(
        INTERNAL,
        "Internal error (please report this as a bug)",
        "append! failed (see close_writer for details)",
    ))
    return writer
end

"""
    flush!(writer::DataFileWriter)

Force the writer to flush its current partial window to the encode pool. Useful on logical
boundaries (end of transaction, time tick) where a Parquet row-group break is desired
without waiting for the natural coalesce-window boundary. No-op if the buffer is empty.

`close_writer` flushes any remainder automatically, so explicit `flush!` is only needed
when the caller wants control over flush timing.
"""
function flush!(writer::DataFileWriter)
    writer.ptr == C_NULL && throw(IcebergException(
        STATE_RESOURCE_FREED,
        "Resource has been freed",
        "Writer has been freed",
    ))
    ret = @ccall rust_lib.iceberg_writer_flush(writer.ptr::Ptr{Cvoid})::Int32
    ret == 0 || throw(IcebergException(
        INTERNAL,
        "Internal error (please report this as a bug)",
        "flush! failed (see close_writer for details)",
    ))
    return writer
end
