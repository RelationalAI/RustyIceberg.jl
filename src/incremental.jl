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
    result = @ccall rust_lib.iceberg_incremental_select_columns(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        pointer(c_strings)::Ptr{Cstring},
        length(column_names)::Csize_t
    )::Cint

    if result != 0
        error("Failed to select columns for incremental scan")
    end
    return nothing
end

"""
    with_data_file_concurrency_limit!(scan::IncrementalScan, n::UInt)

Sets the data file concurrency level for the incremental scan.
"""
function with_data_file_concurrency_limit!(scan::IncrementalScan, n::UInt)
    result = @ccall rust_lib.iceberg_incremental_scan_with_data_file_concurrency_limit(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        error("Failed to set data file concurrency limit for incremental scan")
    end
    return nothing
end

"""
    with_manifest_entry_concurrency_limit!(scan::IncrementalScan, n::UInt)

Sets the manifest entry concurrency level for the incremental scan.
"""
function with_manifest_entry_concurrency_limit!(scan::IncrementalScan, n::UInt)
    result = @ccall rust_lib.iceberg_incremental_scan_with_manifest_entry_concurrency_limit(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        error("Failed to set manifest entry concurrency limit for incremental scan")
    end
    return nothing
end

"""
    with_batch_size!(scan::IncrementalScan, n::UInt)

Sets the batch size for the incremental scan.
"""
function with_batch_size!(scan::IncrementalScan, n::UInt)
    result = @ccall rust_lib.iceberg_incremental_scan_with_batch_size(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        error("Failed to set batch size for incremental scan")
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
    result = @ccall rust_lib.iceberg_incremental_scan_with_file_column(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cint

    if result != 0
        error("Failed to add file column to incremental scan")
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
    result = @ccall rust_lib.iceberg_incremental_scan_with_pos_column(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cint

    if result != 0
        error("Failed to add pos column to incremental scan")
    end
    return nothing
end

"""
    build!(scan::IncrementalScan)

Build the provided incremental table scan object.
"""
function build!(scan::IncrementalScan)
    result = @ccall rust_lib.iceberg_incremental_scan_build(
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
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)

    preserve_task(ct)
    result = GC.@preserve response event try
        result = @ccall rust_lib.iceberg_incremental_arrow_stream_unzipped(
            scan.ptr::Ptr{Cvoid},
            response::Ref{UnzippedStreamsResponse},
            handle::Ptr{Cvoid}
        )::Cint

        wait_or_cancel(event, response)

        result
    finally
        unpreserve_task(ct)
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
    @ccall rust_lib.iceberg_free_incremental_scan(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cvoid
end
