# Incremental table scan implementation

# Sentinel value for optional snapshot IDs (matches Rust FFI SNAPSHOT_ID_NONE constant)
const SNAPSHOT_ID_NONE = Int64(-1)

"""
    IncrementalScan

A mutable wrapper around a pointer to an incremental table scan.

Incremental scans read changes between two snapshots in an Iceberg table.
Use `new_incremental_scan` to create a scan between two snapshot IDs and call
`scan!` to obtain separate Arrow streams for inserts and deletes.

# Example
```julia
table = table_open("s3://path/to/table/metadata.json")
scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id; batch_size=Int64(1024))
inserts_stream, deletes_stream = scan!(scan)
# ... process batches from both streams
free_stream!(inserts_stream)
free_stream!(deletes_stream)
free_scan!(scan)
free_table!(table)
```
"""
mutable struct IncrementalScan
    ptr::Ptr{Cvoid}
end

"""
    UnzippedStreamsResponse

Response structure for asynchronous incremental scan operations that return separate streams.

# Fields
- `result::Cint`: Result code from the operation (0 for success)
- `inserts_stream::ArrowStream`: Stream containing inserted rows
- `deletes_stream::ArrowStream`: Stream containing position delete metadata
- `error_message::Ptr{Cchar}`: Error message string if operation failed
- `context::Ptr{Cvoid}`: Context pointer for operation cancellation
"""
mutable struct UnzippedStreamsResponse
    result::Cint
    inserts_stream::ArrowStream
    deletes_stream::ArrowStream
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    UnzippedStreamsResponse() = new(-1, C_NULL, C_NULL, C_NULL, C_NULL)
end

"""
    new_incremental_scan(table, from_snapshot_id, to_snapshot_id; kwargs...) -> IncrementalScan

Create an incremental scan for the given table between two snapshots.

# Arguments
- `table::Table`: The Iceberg table to scan
- `from_snapshot_id`: Starting snapshot ID, or `nothing` (or `Int64(-1)`) for oldest snapshot
- `to_snapshot_id`: Ending snapshot ID, or `nothing` (or `Int64(-1)`) for current snapshot

# Keyword Arguments
- `column_names`: columns to read (default: all columns)
- `data_file_concurrency_limit`: data file concurrency (`-1` = reader default)
- `manifest_file_concurrency_limit`: manifest file concurrency (`-1` = don't set)
- `manifest_entry_concurrency_limit`: manifest entry concurrency (`-1` = don't set)
- `batch_size`: Arrow record batch size (`-1` = no override)
- `file_column`: include `_file` metadata column (default: `false`)
- `pos_column`: include `_pos` metadata column (default: `false`)
- `serialization_concurrency`: parallel batch serializations (`-1` = auto-detect)
- `task_prefetch_depth`: file scan task prefetch depth (`-1` = use default)

# Examples
```julia
# Scan full history (root to current)
scan = new_incremental_scan(table, nothing, nothing)

# Scan between specific snapshots with batch size
scan = new_incremental_scan(table, from_id, to_id; batch_size=Int64(1024))
```
"""
function new_incremental_scan(
    table::Table,
    from_snapshot_id::Union{Int64, Nothing},
    to_snapshot_id::Union{Int64, Nothing};
    column_names::Union{Vector{String}, Nothing} = nothing,
    data_file_concurrency_limit::Int64 = Int64(-1),
    manifest_file_concurrency_limit::Int64 = Int64(-1),
    manifest_entry_concurrency_limit::Int64 = Int64(-1),
    batch_size::Int64 = Int64(-1),
    file_column::Bool = false,
    pos_column::Bool = false,
    serialization_concurrency::Int64 = Int64(-1),
    task_prefetch_depth::Int64 = Int64(-1),
)
    from_id = from_snapshot_id === nothing ? SNAPSHOT_ID_NONE : from_snapshot_id
    to_id = to_snapshot_id === nothing ? SNAPSHOT_ID_NONE : to_snapshot_id
    c_column_names = isnothing(column_names) ? C_NULL : pointer(pointer.(column_names))
    column_count = isnothing(column_names) ? 0 : length(column_names)


    scan_ptr = GC.@preserve column_names c_column_names @ccall rust_lib.iceberg_new_incremental_scan(
        table::Table,
        from_id::Int64,
        to_id::Int64,
        c_column_names::Ptr{Cstring},
        column_count::Csize_t,
        data_file_concurrency_limit::Int64,
        manifest_file_concurrency_limit::Int64,
        manifest_entry_concurrency_limit::Int64,
        batch_size::Int64,
        file_column::Bool,
        pos_column::Bool,
        serialization_concurrency::Int64,
        task_prefetch_depth::Int64,
        )::Ptr{Cvoid}
    scan_ptr == C_NULL && throw(IcebergException("Failed to create incremental scan"))
    return IncrementalScan(scan_ptr)
end

"""
    incremental_arrow_stream_unzipped(scan::IncrementalScan) -> (ArrowStream, ArrowStream)

Initialize unzipped Arrow streams (inserts and deletes) for the incremental scan asynchronously.
Returns a tuple of (inserts_stream, deletes_stream).
"""
function incremental_arrow_stream_unzipped(scan::IncrementalScan)
    response = UnzippedStreamsResponse()

    async_ccall(response) do handle
        @ccall rust_lib.iceberg_incremental_arrow_stream_unzipped(
            scan.ptr::Ptr{Cvoid},
            response::Ref{UnzippedStreamsResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "iceberg_incremental_arrow_stream_unzipped", IcebergException)

    return (response.inserts_stream, response.deletes_stream)
end

"""
    scan!(scan::IncrementalScan) -> (ArrowStream, ArrowStream)

Execute the incremental scan and return unzipped Arrow streams.
Returns a tuple of (inserts_stream, deletes_stream).
"""
function scan!(scan::IncrementalScan)
    return incremental_arrow_stream_unzipped(scan)
end

"""
    free_scan!(scan::IncrementalScan)

Free the memory associated with an incremental scan.
"""
function free_scan!(scan::IncrementalScan)
    GC.@preserve scan @ccall rust_lib.iceberg_free_incremental_scan(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cvoid
end

# ---------------------------------------------------------------------------
# Incremental split-scan API
#
# Usage:
#   reader = create_reader(scan)
#   append_stream, delete_stream = plan_files(scan)
#   while (at = next_file!(append_stream)) !== nothing
#       stream = read_file!(reader, at)   # consumes at
#       while (bp = next_batch(stream)) != C_NULL
#           # ... process batch ...
#           free_batch!(bp)
#       end
#       free_stream!(stream)
#   end
#   while (dt = next_file!(delete_stream)) !== nothing
#       stream = read_file!(reader, dt)  # consumes dt; yields (file_path, pos) batches
#       while (bp = next_batch(stream)) != C_NULL
#           free_batch!(bp)
#       end
#       free_stream!(stream)
#   end
#   free_file_stream!(append_stream)
#   free_file_stream!(delete_stream)
#   free_reader!(reader)
# ---------------------------------------------------------------------------

"""Opaque handle to a buffered stream of incremental append tasks."""
mutable struct AppendFileStream
    ptr::Ptr{Cvoid}
end

"""Opaque handle to a buffered stream of positional-delete tasks."""
mutable struct DeleteFileStream
    ptr::Ptr{Cvoid}
end

"""Handle to a single incremental append task."""
mutable struct AppendFileHandle
    ptr::Ptr{Cvoid}
end

"""Handle to a single positional-delete task (file_path + row positions)."""
mutable struct DeleteFileHandle
    ptr::Ptr{Cvoid}
end

mutable struct IncrementalTaskStreamsResponse
    result::Cint
    append_stream::Ptr{Cvoid}
    delete_stream::Ptr{Cvoid}
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    IncrementalTaskStreamsResponse() = new(-1, C_NULL, C_NULL, C_NULL, C_NULL)
end

"""
    plan_files(scan::IncrementalScan) -> (AppendFileStream, DeleteFileStream)

Plan which files to read for an incremental scan. The scan must be built first via `build!`.
Returns separate streams for append tasks and positional-delete tasks.
"""
function plan_files(scan::IncrementalScan)
    response = IncrementalTaskStreamsResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_incremental_plan_files(
            scan.ptr::Ptr{Cvoid},
            response::Ref{IncrementalTaskStreamsResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_incremental_plan_files", IcebergException)
    return AppendFileStream(response.append_stream),
           DeleteFileStream(response.delete_stream)
end

"""
    create_reader(scan::IncrementalScan; reader_concurrency::UInt=UInt(0)) -> ArrowReaderContext

Create a shared reader context from the incremental scan's configuration.
Pass this to every `read_file` and `read_file` call.
"""
function create_reader(scan::IncrementalScan; reader_concurrency::UInt=UInt(0))
    ptr = @ccall rust_lib.iceberg_incremental_create_reader(
        scan.ptr::Ptr{Cvoid},
        reader_concurrency::Csize_t
    )::Ptr{Cvoid}
    if ptr == C_NULL
        throw(IcebergException("Failed to create reader from incremental scan"))
    end
    return ArrowReaderContext(ptr)
end

"""
    next_file!(stream::AppendFileStream) -> Union{AppendFileHandle, Nothing}

Pull the next append task from the stream. Returns `nothing` at end-of-stream.
"""
function next_file!(stream::AppendFileStream)
    response = OpaqueResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_incremental_next_append_file(
            stream.ptr::Ptr{Cvoid},
            response::Ref{OpaqueResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_incremental_next_append_file", IcebergException)
    return response.value == C_NULL ? nothing : AppendFileHandle(response.value)
end

"""
    read_file!(reader::ArrowReaderContext, task::AppendFileHandle) -> ArrowStream

Read a single incremental append task into an Arrow stream. **Consumes `task`**.
"""
function read_file!(reader::ArrowReaderContext, task::AppendFileHandle)
    response = ArrowStreamResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_incremental_read_append_file(
            reader.ptr::Ptr{Cvoid},
            task.ptr::Ptr{Cvoid},
            response::Ref{ArrowStreamResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_incremental_read_append_file", IcebergException)
    return response.value
end

"""
    next_file!(stream::DeleteFileStream) -> Union{DeleteFileHandle, Nothing}

Pull the next positional-delete task from the stream. Returns `nothing` at end-of-stream.
Only `PositionalDeletes` tasks are returned; `DeletedFile` and `EqualityDeletes` are skipped.
"""
function next_file!(stream::DeleteFileStream)
    response = OpaqueResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_incremental_next_pos_delete_file(
            stream.ptr::Ptr{Cvoid},
            response::Ref{OpaqueResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_incremental_next_pos_delete_file", IcebergException)
    return response.value == C_NULL ? nothing : DeleteFileHandle(response.value)
end

"""
    read_file!(reader::ArrowReaderContext, task::DeleteFileHandle) -> ArrowStream

Convert a positional-delete task into an Arrow stream of `(file_path, pos)` batches.
**Consumes `task`**.
"""
function read_file!(reader::ArrowReaderContext, task::DeleteFileHandle)
    response = ArrowStreamResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_incremental_read_pos_delete_file(
            reader.ptr::Ptr{Cvoid},
            task.ptr::Ptr{Cvoid},
            response::Ref{ArrowStreamResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_incremental_read_pos_delete_file", IcebergException)
    return response.value
end

"""
    record_count(task::AppendFileHandle) -> Union{Int64, Nothing}

Return the record count for this append task, or `nothing` if not available.
"""
function record_count(task::AppendFileHandle)
    count = @ccall rust_lib.iceberg_incremental_append_file_record_count(
        task.ptr::Ptr{Cvoid}
    )::Int64
    return count == -1 ? nothing : count
end

"""
    record_count(task::DeleteFileHandle) -> Int64

Return the number of deleted row positions in this positional-delete file.
"""
function record_count(task::DeleteFileHandle)
    count = @ccall rust_lib.iceberg_incremental_pos_delete_file_record_count(
        task.ptr::Ptr{Cvoid}
    )::Int64
    return count
end

"""
    file_path(fs::AppendFileHandle)::String

Return the data file path for this incremental append file.
"""
function file_path(fs::AppendFileHandle)
    ptr = @ccall rust_lib.iceberg_incremental_append_file_path(
        fs.ptr::Ptr{Cvoid}
    )::Ptr{Cchar}
    ptr == C_NULL && throw(IcebergException("Failed to get append file path"))
    path = unsafe_string(ptr)
    @ccall rust_lib.iceberg_destroy_cstring(ptr::Ptr{Cchar})::Cint
    return path
end

"""
    file_path(fs::DeleteFileHandle)::String

Return the data file path for this positional-delete file.
"""
function file_path(fs::DeleteFileHandle)
    ptr = @ccall rust_lib.iceberg_incremental_pos_delete_file_path(
        fs.ptr::Ptr{Cvoid}
    )::Ptr{Cchar}
    ptr == C_NULL && throw(IcebergException("Failed to get pos-delete file path"))
    path = unsafe_string(ptr)
    @ccall rust_lib.iceberg_destroy_cstring(ptr::Ptr{Cchar})::Cint
    return path
end

"""Free a stream of incremental append tasks."""
function free_file_stream!(stream::AppendFileStream)
    @ccall rust_lib.iceberg_incremental_append_file_stream_free(stream.ptr::Ptr{Cvoid})::Cvoid
end

"""Free a stream of positional-delete tasks."""
function free_file_stream!(stream::DeleteFileStream)
    @ccall rust_lib.iceberg_incremental_pos_delete_file_stream_free(stream.ptr::Ptr{Cvoid})::Cvoid
end

"""Free an append task handle. Only call if NOT passed to `read_file`."""
function free_file!(task::AppendFileHandle)
    @ccall rust_lib.iceberg_incremental_append_file_free(task.ptr::Ptr{Cvoid})::Cvoid
end

"""Free a positional-delete task handle. Only call if NOT passed to `read_file`."""
function free_file!(task::DeleteFileHandle)
    @ccall rust_lib.iceberg_incremental_pos_delete_file_free(task.ptr::Ptr{Cvoid})::Cvoid
end
