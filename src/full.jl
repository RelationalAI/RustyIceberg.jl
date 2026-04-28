# Regular (full) table scan implementation

"""
    Scan

A mutable wrapper around a pointer to a regular (full) table scan.

This type enables multiple dispatch and safe memory management for Iceberg table scans.
Use `new_scan` to create a scan and call `scan!` to build it and obtain an Arrow stream.

# Example
```julia
table = table_open("s3://path/to/table/metadata.json")
scan = new_scan(table; column_names=["col1", "col2"], batch_size=Int64(1024))
stream = scan!(scan)
# ... process batches from stream
free_stream!(stream)
free_scan!(scan)
free_table!(table)
```
"""
mutable struct Scan
    ptr::Ptr{Cvoid}
end

"""
    new_scan(table::Table; kwargs...) -> Scan

Create a scan for the given table. All scan parameters are configured via keyword arguments.

# Keyword Arguments
- `column_names`: columns to read (default: all columns)
- `data_file_concurrency_limit`: data file concurrency (`-1` = reader default)
- `manifest_file_concurrency_limit`: manifest file concurrency (`-1` = don't set)
- `manifest_entry_concurrency_limit`: manifest entry concurrency (`-1` = don't set)
- `batch_size`: Arrow record batch size (`-1` = no override)
- `file_column`: include `_file` metadata column (default: `false`)
- `pos_column`: include `_pos` metadata column (default: `false`)
- `serialization_concurrency`: parallel batch serializations (`-1` = auto-detect)
- `snapshot_id`: snapshot ID to scan (`-1` = current snapshot)
- `task_prefetch_depth`: file scan task prefetch depth (`-1` = use default)
"""
function new_scan(table::Table;
    column_names::Union{Vector{String}, Nothing} = nothing,
    data_file_concurrency_limit::Int64 = Int64(-1),
    manifest_file_concurrency_limit::Int64 = Int64(-1),
    manifest_entry_concurrency_limit::Int64 = Int64(-1),
    batch_size::Int64 = Int64(-1),
    file_column::Bool = false,
    pos_column::Bool = false,
    serialization_concurrency::Int64 = Int64(-1),
    snapshot_id::Int64 = Int64(-1),
    task_prefetch_depth::Int64 = Int64(-1),
)
    if column_names !== nothing
        c_strings = [pointer(col) for col in column_names]
        scan_ptr = GC.@preserve column_names c_strings @ccall rust_lib.iceberg_new_scan(
            table::Table,
            pointer(c_strings)::Ptr{Cstring},
            length(column_names)::Csize_t,
            data_file_concurrency_limit::Int64,
            manifest_file_concurrency_limit::Int64,
            manifest_entry_concurrency_limit::Int64,
            batch_size::Int64,
            file_column::Bool,
            pos_column::Bool,
            serialization_concurrency::Int64,
            snapshot_id::Int64,
            task_prefetch_depth::Int64,
        )::Ptr{Cvoid}
    else
        scan_ptr = @ccall rust_lib.iceberg_new_scan(
            table::Table,
            C_NULL::Ptr{Cstring},
            Csize_t(0)::Csize_t,
            data_file_concurrency_limit::Int64,
            manifest_file_concurrency_limit::Int64,
            manifest_entry_concurrency_limit::Int64,
            batch_size::Int64,
            file_column::Bool,
            pos_column::Bool,
            serialization_concurrency::Int64,
            snapshot_id::Int64,
            task_prefetch_depth::Int64,
        )::Ptr{Cvoid}
    end
    scan_ptr == C_NULL && throw(IcebergException("Failed to create scan"))
    return Scan(scan_ptr)
end

# Type alias using generic Response{T}
# Note: ArrowStream = Ptr{Cvoid}, so Response{ArrowStream} uses the Response{Ptr{Cvoid}} constructor
const ArrowStreamResponse = Response{ArrowStream}

"""
    arrow_stream(scan::Scan)::ArrowStream

Initialize an Arrow stream for the scan asynchronously.
"""
function arrow_stream(scan::Scan)
    response = ArrowStreamResponse()

    async_ccall(response) do handle
        @ccall rust_lib.iceberg_arrow_stream(
            scan.ptr::Ptr{Cvoid},
            response::Ref{ArrowStreamResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "iceberg_arrow_stream", IcebergException)

    return response.value
end

"""
    scan!(scan::Scan) -> ArrowStream

Execute the scan and return an Arrow stream.
"""
function scan!(scan::Scan)
    return arrow_stream(scan)
end

"""
    free_scan!(scan::Scan)

Free the memory associated with a scan.
"""
function free_scan!(scan::Scan)
    GC.@preserve scan @ccall rust_lib.iceberg_scan_free(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cvoid
end

# ---------------------------------------------------------------------------
# Split-scan API
#
# Usage:
#   reader = create_reader(scan)
#   file_stream = plan_files(scan)
#   while (fs = next_file!(file_stream)) !== nothing
#       stream = read_file_scan!(reader, fs)   # consumes fs
#       while (bp = next_batch(stream)) != C_NULL
#           # ... process batch ...
#           free_batch!(bp)
#       end
#       free_stream!(stream)
#   end
#   free_file_stream!(file_stream)
#   free_reader!(reader)
# ---------------------------------------------------------------------------

"""Opaque pointer to a stream of FileScanTasks."""
mutable struct FileScanStream
    ptr::Ptr{Cvoid}
end

"""Shared ArrowReader context — pass to every read_file_scan! call."""
mutable struct ArrowReaderContext
    ptr::Ptr{Cvoid}
end

"""Handle to a single file scan task returned by next_file!."""
mutable struct FileScanHandle
    ptr::Ptr{Cvoid}
end


"""
    plan_files(scan::Scan)::FileScanStream

Plan which files to read. Returns a concurrent-safe task stream.
The scan must be built first via `build!`.
"""
function plan_files(scan::Scan)
    response = OpaqueResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_plan_files(
            scan.ptr::Ptr{Cvoid},
            response::Ref{OpaqueResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_plan_files", IcebergException)
    return FileScanStream(response.value)
end

"""
    create_reader(scan::Scan; reader_concurrency::UInt=UInt(0), batch_prefetch_depth::UInt=UInt(0))::ArrowReaderContext

Create a shared reader context from the scan's configuration.
Pass this to every `read_file_scan!` call.
`reader_concurrency` overrides the scan-level data_file_concurrency_limit when > 0.
`batch_prefetch_depth` sets the per-file batch prefetch queue depth (0 = default of 4).
"""
function create_reader(scan::Scan; reader_concurrency::UInt=UInt(0), batch_prefetch_depth::UInt=UInt(0))
    ptr = @ccall rust_lib.iceberg_create_reader(
        scan.ptr::Ptr{Cvoid},
        reader_concurrency::Csize_t,
        batch_prefetch_depth::Csize_t
    )::Ptr{Cvoid}
    if ptr == C_NULL
        throw(IcebergException("Failed to create reader from scan"))
    end
    return ArrowReaderContext(ptr)
end

"""
    next_file!(stream::FileScanStream)::Union{FileScanHandle, Nothing}

Pull the next file scan from the stream. Returns `nothing` at end-of-stream.
"""
function next_file!(stream::FileScanStream)
    response = OpaqueResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_next_file_scan_task(
            stream.ptr::Ptr{Cvoid},
            response::Ref{OpaqueResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_next_file_scan_task", IcebergException)
    return response.value == C_NULL ? nothing : FileScanHandle(response.value)
end

"""
    read_file_scan!(reader::ArrowReaderContext, fs::FileScanHandle)::ArrowStream

Read a single file scan into an Arrow stream. **Consumes `fs`** — do not call
`free_file_scan` afterwards.
"""
function read_file_scan!(reader::ArrowReaderContext, fs::FileScanHandle)
    response = ArrowStreamResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_read_file_scan_task(
            reader.ptr::Ptr{Cvoid},
            fs.ptr::Ptr{Cvoid},
            response::Ref{ArrowStreamResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_read_file_scan_task", IcebergException)
    return response.value
end

"""
    record_count(fs::FileScanHandle)::Union{Int64, Nothing}

Return the record count for this file scan, or `nothing` if not available.
"""
function record_count(fs::FileScanHandle)
    count = @ccall rust_lib.iceberg_file_scan_task_record_count(
        fs.ptr::Ptr{Cvoid}
    )::Int64
    return count == -1 ? nothing : count
end

"""
    file_path(fs::FileScanHandle)::String

Return the data file path for this file scan task.
"""
function file_path(fs::FileScanHandle)
    ptr = @ccall rust_lib.iceberg_file_scan_task_file_path(
        fs.ptr::Ptr{Cvoid}
    )::Ptr{Cchar}
    ptr == C_NULL && throw(IcebergException("Failed to get file path"))
    path = unsafe_string(ptr)
    @ccall rust_lib.iceberg_destroy_cstring(ptr::Ptr{Cchar})::Cint
    return path
end

"""
    file_metadata(scan::Scan) -> Vector{NamedTuple{(:path, :record_count), Tuple{String, Union{Int64, Nothing}}}}

Return all file paths and record counts for a scan by draining the plan_files stream.
No data files are opened. The returned record counts come from manifest metadata and
may be `nothing` for delete files or partial scans.
"""
function file_metadata(scan::Scan)
    stream = plan_files(scan)
    result = NamedTuple{(:path, :record_count), Tuple{String, Union{Int64, Nothing}}}[]
    try
        while true
            handle = next_file!(stream)
            handle === nothing && break
            push!(result, (path=file_path(handle), record_count=record_count(handle)))
            free_file!(handle)
        end
    finally
        free_file_stream!(stream)
    end
    return result
end

"""Free a file scan stream (from plan_files)."""
function free_file_stream!(stream::FileScanStream)
    @ccall rust_lib.iceberg_file_scan_task_stream_free(stream.ptr::Ptr{Cvoid})::Cvoid
end

"""Free an ArrowReaderContext (from create_reader)."""
function free_reader!(reader::ArrowReaderContext)
    @ccall rust_lib.iceberg_arrow_reader_context_free(reader.ptr::Ptr{Cvoid})::Cvoid
end

"""
Free a FileScanHandle. Only call this if the handle was NOT passed to read_file_scan!,
since read_file_scan! consumes the handle.
"""
function free_file!(fs::FileScanHandle)
    @ccall rust_lib.iceberg_file_scan_task_free(fs.ptr::Ptr{Cvoid})::Cvoid
end
