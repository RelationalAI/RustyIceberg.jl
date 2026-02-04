# Writer API for RustyIceberg
#
# This module provides Julia wrappers for the iceberg-rust Writer API,
# enabling Julia to write Arrow data to Parquet files as Iceberg data files.

# Parquet field ID metadata key (must match iceberg-rust's PARQUET_FIELD_ID_META_KEY)
const PARQUET_FIELD_ID_META_KEY = "PARQUET:field_id"

"""
    DataFileWriter

Opaque handle representing an Iceberg data file writer.

Create a writer using `DataFileWriter(table)` and free it with `free_writer!`
when done. Writers should be closed using `close_writer` to get the written
data files.
"""
mutable struct DataFileWriter
    ptr::Ptr{Cvoid}
    table::Table  # Keep reference to table to prevent GC
    colmeta::Dict{Symbol, Vector{Pair{String, String}}}  # Column metadata with Iceberg field IDs
end

# Response type for writer creation
const DataFileWriterResponse = Response{Ptr{Cvoid}}

# Response type for close operation (returns DataFiles)
const WriterCloseResponse = Response{Ptr{Cvoid}}

"""
    DataFileWriter(table::Table; prefix::String="data") -> DataFileWriter

Create a new data file writer for the given table.

The writer writes data to Parquet files in the table's data directory.
Files are named using the prefix (e.g., "data-xxx.parquet").

# Arguments
- `table::Table`: The table to write data files for
- `prefix::String`: Prefix for output file names (default: "data")

# Returns
A new `DataFileWriter` handle that must be freed with `free_writer!`.

# Example
```julia
table = load_table(catalog, ["db"], "users")
writer = DataFileWriter(table)
write(writer, arrow_batch)
data_files = close_writer(writer)
free_writer!(writer)
```
"""
function DataFileWriter(table::Table; prefix::String="data")
    response = DataFileWriterResponse()

    async_ccall(response, prefix) do handle
        @ccall rust_lib.iceberg_writer_new(
            table::Table,
            prefix::Cstring,
            response::Ref{DataFileWriterResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "DataFileWriter", IcebergException)

    # Get the table schema and build column metadata with Iceberg field IDs
    # This metadata is added to Arrow IPC data so iceberg-rust can match fields
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

    return DataFileWriter(response.value, table, colmeta)
end
"""
    DataFileWriter(f::Function, table::Table; prefix::String="data") -> DataFiles

Create a writer, pass it to `f` for writing, then close and free it.

This provides a convenient way to write data with automatic cleanup,
ensuring the writer is closed and freed even if an error occurs.

# Arguments
- `f`: A function that takes a `DataFileWriter` and writes data to it
- `table::Table`: The table to create a writer for
- `prefix::String`: Prefix for generated file names (default: "data")

# Returns
A `DataFiles` handle containing the written files.

# Example
```julia
data_files = DataFileWriter(table) do writer
    write(writer, batch1)
    write(writer, batch2)
end
```
"""
function DataFileWriter(f::Function, table::Table; prefix::String="data")
    writer = DataFileWriter(table; prefix=prefix)
    try
        f(writer)
        return close_writer(writer)
    finally
        free_writer!(writer)
    end
end

"""
    free_writer!(writer::DataFileWriter)

Free the memory associated with a data file writer.

This should be called after the writer has been closed or if
the writer is no longer needed.
"""
function free_writer!(writer::DataFileWriter)
    if writer.ptr == C_NULL
        return nothing
    end
    @ccall rust_lib.iceberg_writer_free(writer.ptr::Ptr{Cvoid})::Cvoid
    writer.ptr = C_NULL
    return nothing
end

"""
    write(writer::DataFileWriter, data)

Write Arrow data to the writer.

The data is serialized to Arrow IPC format and written to Parquet files.
Multiple calls to `write` accumulate data until `close_writer!` is called.

# Arguments
- `writer::DataFileWriter`: The writer to write to
- `data`: Arrow-compatible data (anything that Arrow.tobuffer can serialize)

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

free_data_files!(data_files)
free_writer!(writer)
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

    return DataFiles(response.value)
end
