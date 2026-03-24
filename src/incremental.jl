# Incremental table scan implementation

# Sentinel value for optional snapshot IDs (matches Rust FFI SNAPSHOT_ID_NONE constant)
const SNAPSHOT_ID_NONE = Int64(-1)

"""
    IncrementalScan

A mutable wrapper around a pointer to an incremental table scan.

Incremental scans read changes between two snapshots in an Iceberg table.
Use `new_incremental_scan` to create a scan between two snapshot IDs, configure it
with builder methods, and call `scan!` to obtain separate Arrow streams for inserts and deletes.

# Example
```julia
table = table_open("s3://path/to/table/metadata.json")
scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)
with_batch_size!(scan, UInt(1024))
inserts_stream, deletes_stream = scan!(scan)
# ... process batches from both streams
free_stream(inserts_stream)
free_stream(deletes_stream)
free_incremental_scan!(scan)
free_table(table)
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
    new_incremental_scan(table::Table, from_snapshot_id::Union{Int64,Nothing}, to_snapshot_id::Union{Int64,Nothing}) -> IncrementalScan

Create an incremental scan for the given table between two snapshots.

# Arguments
- `table::Table`: The Iceberg table to scan
- `from_snapshot_id`: Starting snapshot ID, or `nothing` to scan from the root (oldest) snapshot
- `to_snapshot_id`: Ending snapshot ID, or `nothing` to scan to the current (latest) snapshot

# Examples
```julia
# Scan full history (root to current)
scan = new_incremental_scan(table, nothing, nothing)

# Scan from root to specific snapshot
scan = new_incremental_scan(table, nothing, snapshot_id)

# Scan from specific snapshot to current
scan = new_incremental_scan(table, snapshot_id, nothing)

# Scan between specific snapshots
scan = new_incremental_scan(table, from_id, to_id)
```
"""
function new_incremental_scan(table::Table, from_snapshot_id::Union{Int64,Nothing}, to_snapshot_id::Union{Int64,Nothing})
    # Convert nothing to SNAPSHOT_ID_NONE for C API
    from_id = from_snapshot_id === nothing ? SNAPSHOT_ID_NONE : from_snapshot_id
    to_id = to_snapshot_id === nothing ? SNAPSHOT_ID_NONE : to_snapshot_id

    scan_ptr = @ccall rust_lib.iceberg_new_incremental_scan(
        table::Table,
        from_id::Int64,
        to_id::Int64
    )::Ptr{Cvoid}
    return IncrementalScan(scan_ptr)
end

"""
    select_columns!(scan::IncrementalScan, column_names::Vector{String})

Select specific columns for the incremental scan.
"""
function select_columns!(scan::IncrementalScan, column_names::Vector{String})
    # Convert String vector to Cstring array
    c_strings = [pointer(col) for col in column_names]
    result = GC.@preserve scan c_strings @ccall rust_lib.iceberg_incremental_select_columns(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        pointer(c_strings)::Ptr{Cstring},
        length(column_names)::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to select columns for incremental scan", result))
    end
    return nothing
end

"""
    with_manifest_file_concurrency_limit!(scan::IncrementalScan, n::UInt)

Sets the manifest file concurrency level for the incremental scan.
"""
function with_manifest_file_concurrency_limit!(scan::IncrementalScan, n::UInt)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_with_manifest_file_concurrency_limit(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set manifest file concurrency limit for incremental scan", result))
    end
    return nothing
end

"""
    with_data_file_concurrency_limit!(scan::IncrementalScan, n::UInt)

Sets the data file concurrency level for the incremental scan.
"""
function with_data_file_concurrency_limit!(scan::IncrementalScan, n::UInt)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_with_data_file_concurrency_limit(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set data file concurrency limit for incremental scan", result))
    end
    return nothing
end

"""
    with_manifest_entry_concurrency_limit!(scan::IncrementalScan, n::UInt)

Sets the manifest entry concurrency level for the incremental scan.
"""
function with_manifest_entry_concurrency_limit!(scan::IncrementalScan, n::UInt)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_with_manifest_entry_concurrency_limit(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set manifest entry concurrency limit for incremental scan", result))
    end
    return nothing
end

"""
    with_batch_size!(scan::IncrementalScan, n::UInt)

Sets the batch size for the incremental scan.
"""
function with_batch_size!(scan::IncrementalScan, n::UInt)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_with_batch_size(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set batch size for incremental scan", result))
    end
    return nothing
end

"""
    with_file_column!(scan::IncrementalScan)

Add the _file metadata column to the incremental scan.

The _file column contains the file path for each row, which can be useful for
tracking which data files contain specific rows during incremental scans.

# Example
```julia
scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)
with_file_column!(scan)
inserts_stream, deletes_stream = scan!(scan)
```
"""
function with_file_column!(scan::IncrementalScan)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_with_file_column(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cint

    if result != 0
        throw(IcebergException("Failed to add file column to incremental scan", result))
    end
    return nothing
end

"""
    with_serialization_concurrency_limit!(scan::IncrementalScan, n::UInt)

Set the serialization concurrency limit for the incremental scan.

This controls how many RecordBatch serializations can happen in parallel for each stream.
- `n = 0`: Auto-detect based on CPU cores (default)
- `n > 0`: Use exactly n concurrent serializations per stream

Note: Incremental scans have two separate streams (inserts and deletes), so total
concurrency can be up to 2×n.

# Example
```julia
scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)
with_serialization_concurrency_limit!(scan, UInt(8))  # Each stream serializes up to 8 batches in parallel
inserts_stream, deletes_stream = scan!(scan)
```
"""
function with_serialization_concurrency_limit!(scan::IncrementalScan, n::UInt)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_with_serialization_concurrency_limit(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set serialization concurrency limit for incremental scan", result))
    end
    return nothing
end

"""
    with_prefetch_depth!(scan::IncrementalScan, n::UInt)

Set the number of serialized batches buffered ahead of the consumer for each stream.
See `with_prefetch_depth!(::Scan, ::UInt)` for details.
"""
function with_prefetch_depth!(scan::IncrementalScan, n::UInt)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_with_prefetch_depth(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set prefetch depth for incremental scan", result))
    end
    return nothing
end

"""
    with_task_prefetch_depth!(scan::IncrementalScan, n::UInt)

Set the number of file scan tasks buffered ahead of the consumer.
See `with_task_prefetch_depth!(::Scan, ::UInt)` for details.
"""
function with_task_prefetch_depth!(scan::IncrementalScan, n::UInt)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_with_task_prefetch_depth(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set task prefetch depth for incremental scan", result))
    end
    return nothing
end

"""
    with_pos_column!(scan::IncrementalScan)

Add the _pos metadata column to the incremental scan.

The _pos column contains the position of each row within its data file, which can
be useful for tracking row locations during incremental scans.

# Example
```julia
scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)
with_pos_column!(scan)
inserts_stream, deletes_stream = scan!(scan)
```
"""
function with_pos_column!(scan::IncrementalScan)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_with_pos_column(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cint

    if result != 0
        throw(IcebergException("Failed to add pos column to incremental scan", result))
    end
    return nothing
end

"""
    build!(scan::IncrementalScan)

Build the provided incremental table scan object.
"""
function build!(scan::IncrementalScan)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_build(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cint

    if result != 0
        throw(IcebergException("Failed to build incremental scan", result))
    end
    return nothing
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

Build the provided incremental table scan object and return unzipped Arrow streams.
Returns a tuple of (inserts_stream, deletes_stream).
"""
function scan!(scan::IncrementalScan)
    build!(scan)
    return incremental_arrow_stream_unzipped(scan)
end

"""
    free_incremental_scan!(scan::IncrementalScan)

Free the memory associated with an incremental scan.
"""
function free_incremental_scan!(scan::IncrementalScan)
    GC.@preserve scan @ccall rust_lib.iceberg_free_incremental_scan(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cvoid
end

# ---------------------------------------------------------------------------
# Split-scan API for incremental scans
#
# Same pattern as full scans but with separate append/delete streams.
# ---------------------------------------------------------------------------

"""
    plan_files(scan::IncrementalScan)::IncrementalFileScanTaskStreams

Plan files for incremental scan. Returns two sub-streams (appends + deletes)
for use with `next_append_task` / `next_delete_task`.
"""
function plan_files(scan::IncrementalScan)
    response = OpaqueResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_incremental_plan_files(
            scan.ptr::Ptr{Cvoid},
            response::Ref{OpaqueResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_incremental_plan_files", IcebergException)
    return IncrementalFileScanTaskStreams(response.value)
end

"""
    create_reader(scan::IncrementalScan; reader_concurrency::UInt=UInt(0))::ArrowReaderContext

Create a shared reader context from the incremental scan's configuration.

`reader_concurrency` sets the data-file concurrency for the reader.
- `0`: Use the scan-level `data_file_concurrency_limit` (default)
- `> 0`: Override with this value
"""
function create_reader(scan::IncrementalScan; reader_concurrency::UInt=UInt(0))
    ptr = @ccall rust_lib.iceberg_create_incremental_reader(
        scan.ptr::Ptr{Cvoid},
        reader_concurrency::Csize_t,
    )::Ptr{Cvoid}
    if ptr == C_NULL
        throw(IcebergException("Failed to create reader from incremental scan"))
    end
    return ArrowReaderContext(ptr)
end

"""
    next_append_task(streams::IncrementalFileScanTaskStreams)::Union{AppendTaskHandle, Nothing}

Pull next append task. Returns `nothing` when exhausted.
"""
function next_append_task(streams::IncrementalFileScanTaskStreams)
    response = OpaqueResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_next_append_task(
            streams.ptr::Ptr{Cvoid},
            response::Ref{OpaqueResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_next_append_task", IcebergException)
    return response.value == C_NULL ? nothing : AppendTaskHandle(response.value)
end

"""
    next_delete_task(streams::IncrementalFileScanTaskStreams)::Union{DeleteTaskHandle, Nothing}

Pull next delete task. Returns `nothing` when exhausted.
"""
function next_delete_task(streams::IncrementalFileScanTaskStreams)
    response = OpaqueResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_next_delete_task(
            streams.ptr::Ptr{Cvoid},
            response::Ref{OpaqueResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_next_delete_task", IcebergException)
    return response.value == C_NULL ? nothing : DeleteTaskHandle(response.value)
end

"""
    read_append_task(reader::ArrowReaderContext, task::AppendTaskHandle)::ArrowStream

Read a single append task. **Consumes the task.**
"""
function read_append_task(reader::ArrowReaderContext, task::AppendTaskHandle)
    response = ArrowStreamResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_read_append_task(
            reader.ptr::Ptr{Cvoid},
            task.ptr::Ptr{Cvoid},
            response::Ref{ArrowStreamResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_read_append_task", IcebergException)
    return response.value
end

"""
    read_delete_task(reader::ArrowReaderContext, task::DeleteTaskHandle)::ArrowStream

Read a single delete task. **Consumes the task.**
"""
function read_delete_task(reader::ArrowReaderContext, task::DeleteTaskHandle)
    response = ArrowStreamResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_read_delete_task(
            reader.ptr::Ptr{Cvoid},
            task.ptr::Ptr{Cvoid},
            response::Ref{ArrowStreamResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_read_delete_task", IcebergException)
    return response.value
end
