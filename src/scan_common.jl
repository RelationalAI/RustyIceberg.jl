# Common scan utilities shared between full and incremental scans

"""
    ArrowStream

Opaque pointer type representing an Arrow stream from the Rust FFI layer.
This stream can be used to fetch batches of Arrow data asynchronously.
"""
const ArrowStream = Ptr{Cvoid}

"""
    FILE_COLUMN

The name of the metadata column containing file paths (_file).

This constant can be used with the `select_columns!` function to include
file path information in query results. It corresponds to the _file metadata
column in Iceberg tables.

# Example
```julia
# Select specific columns including the file path
scan = new_scan(table)
select_columns!(scan, ["id", "name", FILE_COLUMN])
stream = scan!(scan)
```
"""
const FILE_COLUMN = "_file"

"""
    POS_COLUMN

The name of the metadata column containing row positions within files (_pos).

This constant can be used with the `select_columns!` function to include
position information in query results. It corresponds to the _pos metadata
column in Iceberg tables, which represents the row's position within its data file.

# Example
```julia
# Select specific columns including the position
scan = new_scan(table)
select_columns!(scan, ["id", "name", POS_COLUMN])
stream = scan!(scan)
```
"""
const POS_COLUMN = "_pos"

# Type alias using generic Response{T}
const BatchResponse = Response{Ptr{ArrowBatch}}
Response{Ptr{ArrowBatch}}() = Response{Ptr{ArrowBatch}}(-1, C_NULL, C_NULL, C_NULL)

"""
    next_batch(stream::ArrowStream)::Ptr{ArrowBatch}

Wait for the next batch from the initialized stream asynchronously and return it directly.
Returns C_NULL if end of stream is reached.
"""
function next_batch(stream::ArrowStream)
    response = BatchResponse()

    async_ccall(response) do handle
        @ccall rust_lib.iceberg_next_batch(
            stream::ArrowStream,
            response::Ref{BatchResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "iceberg_next_batch", IcebergException)

    # Return the batch pointer directly
    return response.value
end

"""
    free_batch(batch::Ptr{ArrowBatch})

Free the memory associated with an Arrow batch.
"""
function free_batch(batch::Ptr{ArrowBatch})
    @ccall rust_lib.iceberg_arrow_batch_free(batch::Ptr{ArrowBatch})::Cvoid
end

"""
    free_stream(stream::ArrowStream)

Free the memory associated with an Arrow stream.
"""
function free_stream(stream::ArrowStream)
    @ccall rust_lib.iceberg_arrow_stream_free(stream::ArrowStream)::Cvoid
end

"""
    next_arrow_batch(stream::ArrowStream) -> (Arrow.CImportedArray, Ptr{ArrowBatch}) | nothing

Fetch the next batch from `stream` as a zero-copy Arrow table.
Returns `nothing` when the stream is exhausted.

The caller must call `Arrow.release_c_data(handle)` followed by
`free_batch(batch_ptr)` on the returned pair when done with the data.
Prefer `with_next_arrow_batch` when the batch lifetime fits a single block.

# Example
```julia
stream = scan!(scan)
while true
    result = next_arrow_batch(stream)
    result === nothing && break
    handle, batch_ptr = result
    df = DataFrame(handle)
    Arrow.release_c_data(handle)
    free_batch(batch_ptr)
end
free_stream(stream)
```
"""
function next_arrow_batch(stream::ArrowStream)
    batch_ptr = next_batch(stream)
    batch_ptr == C_NULL && return nothing
    batch = unsafe_load(batch_ptr)
    if batch.schema != C_NULL && batch.array != C_NULL
        return Arrow.from_c_data(batch.schema, batch.array), batch_ptr
    else
        free_batch(batch_ptr)
        return nothing
    end
end

"""
    foreach_arrow_batch(f, stream::ArrowStream)

Call `f(batch)` for every batch in `stream`, releasing each batch's Rust
memory after `f` returns. Returns `nothing`.

Each `batch` is an `Arrow.Table` whose columns are zero-copy views into the
underlying Rust-owned memory. The table is valid only for the duration of `f`;
any data needed beyond that scope must be copied inside `f`
(e.g. `collect(batch[:col])` or `DataFrame(batch)`).

The last column of every batch is `_pos::Arrow.Primitive{Int64}` containing
the 1-based row positions within the source Parquet file.

# Example
```julia
stream = scan!(scan)
foreach_arrow_batch(stream) do batch
    process(DataFrame(batch))
end
free_stream(stream)
```
"""
function foreach_arrow_batch(f::F, stream::ArrowStream) where {F}
    while true
        result = next_arrow_batch(stream)
        result === nothing && return nothing
        handle, batch_ptr = result
        try
            f(_cimported_to_table(handle))
        finally
            Arrow.release_c_data(handle)
            free_batch(batch_ptr)
        end
    end
end

# Wrap a CImportedArray struct batch as an Arrow.Table, reusing the child ArrowVectors
# without copying. The returned table is only valid for the duration of the batch callback.
function _cimported_to_table(handle::Arrow.CImportedArray)
    return Arrow.Table(NamedTuple{fieldnames(eltype(handle))}(handle.data.data))
end
