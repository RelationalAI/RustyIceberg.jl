# Common scan utilities shared between full and incremental scans

"""
    IcebergPerfConfig

Scan pipeline & tuning parameters. **This is the single authoritative home for every
scan tuning default.** It is passed by value across the FFI into [`new_scan`](@ref) /
[`new_incremental_scan`](@ref); its memory layout must stay in lock-step with the Rust
`IcebergPerfConfigFFI` helper struct (see `scan_common.rs`). The Rust struct has no
defaults of its own — every field is supplied from here. A binary-compatibility test
(`iceberg_perf_config_roundtrip`) guards the layout agreement.

The fields are `UInt64` so the struct is `isbits` and passes by value to `@ccall` with
no conversion (mirroring the C `u64` fields exactly).

# Pipeline architecture

Reading proceeds in three pipelined stages that overlap concurrently:

  1. Manifest planning (metadata only, no data bytes read)
     - Fetches manifest list from object storage (one small file per snapshot)
     - Fetches manifest files concurrently (small Avro files, one per write batch)
     - Processes manifest entries to build FileScanTask structs, each containing:
       file path, byte range, record count, projected field IDs, bound predicate,
       associated delete file list, partition metadata
     - No Parquet data is opened at this stage

  2. File Scan Task buffering (prefetch window)
     - Both full and incremental scans run `Stream::buffered(file_prefetch_depth)` over
       the planned per-file tasks. See the `nested_pipeline` module docs for the exact
       cap on alive `process_file` tasks.
     - Upstream is iceberg-rust's own internal channel (capacity =
       manifest_entry_concurrency_limit).
     - Purpose: hide manifest fetch latency (S3 round trips ~5–50ms each) behind data
       read time.

  3. Data reading
     - Each per-file task opens one Parquet file, applies column projection and predicate,
       loads associated delete files (full scan) or merges position deletes (incremental),
       and streams Arrow record batches.
     - Batches are serialized to Arrow IPC via tokio::task::spawn_blocking on both paths.

# Tuning parameters

  batch_size
    Number of rows per Arrow record batch. Smaller batches reduce memory pressure;
    larger batches reduce per-batch overhead. Applies to both data and delete file reads.

  manifest_file_concurrency_limit
    Per scan cap on how many manifest files are fetched from object storage concurrently.
    All manifests for the snapshot share one pool.

  manifest_entry_concurrency_limit
    Per scan cap on how many manifest entries are processed concurrently — total across
    all in-flight manifests, not per-manifest. Also sets the internal task channel
    capacity, so it influences pipeline lookahead.

  file_prefetch_depth
    Width of the pipeline's `Stream::buffered(file_prefetch_depth)` window. Higher
    values allow manifest planning to run further ahead of consumption, smoothing
    bursts of object-storage latency. Each buffered reader holds open one file
    and may have deserialized Arrow batches in memory, so this parameter also
    bounds how much batch memory the FFI layer holds at once. Applies to both the
    full-scan and incremental pipelines (both consume the value verbatim).

  serialization_concurrency_limit
    No-op for full scans (Arrow IPC serialization is dispatched per batch via
    tokio::task::spawn_blocking inside the shared per-file pipeline, with parallelism
    implicitly bounded by file_prefetch_depth). Incremental scans still honour it on
    the delete-stream fan-out side (position deletes are flat-stream-serialized). The
    legacy iterator-API path also honours it via its own spawn_blocking fan-out.
"""
Base.@kwdef struct IcebergPerfConfig
    batch_size::UInt64 = 64 * 1024
    manifest_file_concurrency_limit::UInt64 = Threads.nthreads() * 2
    manifest_entry_concurrency_limit::UInt64 = Threads.nthreads() * 2
    file_prefetch_depth::UInt64 = 1
    serialization_concurrency_limit::UInt64 = Threads.nthreads()
end

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
scan = new_scan(table, IcebergPerfConfig())
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
scan = new_scan(table, IcebergPerfConfig())
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
