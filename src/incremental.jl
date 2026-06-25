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
scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id, IcebergPerfConfig(batch_size=1024))
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
    new_incremental_scan(table::Table, from_snapshot_id::Union{Int64,Nothing}, to_snapshot_id::Union{Int64,Nothing}, perf::IcebergPerfConfig) -> IncrementalScan

Create an incremental scan for the given table between two snapshots, configured with
the tuning parameters in `perf`.

# Arguments
- `table::Table`: The Iceberg table to scan
- `from_snapshot_id`: Starting snapshot ID, or `nothing` to scan from the root (oldest) snapshot
- `to_snapshot_id`: Ending snapshot ID, or `nothing` to scan to the current (latest) snapshot
- `perf::IcebergPerfConfig`: Tuning parameters, passed by value (the single source of
  every tuning default). There are no per-field setters: configuration is fixed at
  construction.

# Examples
```julia
# Scan full history (root to current)
scan = new_incremental_scan(table, nothing, nothing, IcebergPerfConfig())

# Scan from root to specific snapshot
scan = new_incremental_scan(table, nothing, snapshot_id, IcebergPerfConfig())

# Scan from specific snapshot to current
scan = new_incremental_scan(table, snapshot_id, nothing, IcebergPerfConfig())

# Scan between specific snapshots
scan = new_incremental_scan(table, from_id, to_id, IcebergPerfConfig())
```
"""
function new_incremental_scan(table::Table, from_snapshot_id::Union{Int64,Nothing}, to_snapshot_id::Union{Int64,Nothing}, perf::IcebergPerfConfig)
    # Convert nothing to SNAPSHOT_ID_NONE for C API
    from_id = from_snapshot_id === nothing ? SNAPSHOT_ID_NONE : from_snapshot_id
    to_id = to_snapshot_id === nothing ? SNAPSHOT_ID_NONE : to_snapshot_id

    scan_ptr = @ccall rust_lib.iceberg_new_incremental_scan(
        table::Table,
        from_id::Int64,
        to_id::Int64,
        perf::IcebergPerfConfig
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
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "iceberg_incremental_select_columns returned $result",
        ))
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
scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id, IcebergPerfConfig())
with_file_column!(scan)
inserts_stream, deletes_stream = scan!(scan)
```
"""
function with_file_column!(scan::IncrementalScan)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_with_file_column(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cint

    if result != 0
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "iceberg_incremental_scan_with_file_column returned $result",
        ))
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
scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id, IcebergPerfConfig())
with_pos_column!(scan)
inserts_stream, deletes_stream = scan!(scan)
```
"""
function with_pos_column!(scan::IncrementalScan)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_with_pos_column(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cint

    if result != 0
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "iceberg_incremental_scan_with_pos_column returned $result",
        ))
    end
    return nothing
end

"""
    build!(scan::IncrementalScan)

Build the provided incremental table scan object.
"""
function build!(scan::IncrementalScan)
    error_ptr = Ref{Ptr{Cchar}}(C_NULL)
    result = GC.@preserve scan @ccall rust_lib.iceberg_incremental_scan_build(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        error_ptr::Ref{Ptr{Cchar}}
    )::Cint

    if result != 0
        if error_ptr[] != C_NULL
            parse_and_throw(error_ptr[], "iceberg_incremental_scan_build")
        end
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "iceberg_incremental_scan_build returned $result",
        ))
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

# ── Nested incremental scan ────────────────────────────────────────────────
#
# Returns (append_stream::FileScanStream, delete_stream::ArrowStream).
# The append stream yields one FileScan per appended parquet file (use the
# same next_file_scan / file_scan_* / free_file_scan! helpers as the full
# nested scan).  The delete stream is a flat ArrowStream; drain with next_batch
# and free with free_stream.

"""
    IncrementalFileScanStreamResponse

Response structure for `iceberg_incremental_file_scan_stream`.
"""
mutable struct IncrementalFileScanStreamResponse
    result::Cint
    append_stream::FileScanStream
    delete_stream::ArrowStream
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    IncrementalFileScanStreamResponse() = new(-1, C_NULL, C_NULL, C_NULL, C_NULL)
end

"""
    nested_incremental_arrow_stream(scan::IncrementalScan) -> (FileScanStream, ArrowStream)

Initialize a nested incremental scan asynchronously.
Returns `(append_stream, delete_stream)`:
- `append_stream` — iterate with `next_file_scan`; each item carries filename,
  record count, and a prefetched inner batch stream.
- `delete_stream` — flat stream of delete records; drain with `next_batch`.
"""
function nested_incremental_arrow_stream(scan::IncrementalScan)
    response = IncrementalFileScanStreamResponse()

    async_ccall(response) do handle
        @ccall rust_lib.iceberg_incremental_file_scan_stream(
            scan.ptr::Ptr{Cvoid},
            response::Ref{IncrementalFileScanStreamResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "iceberg_incremental_file_scan_stream", IcebergException)

    return (response.append_stream, response.delete_stream)
end

"""
    scan_incremental_nested!(scan::IncrementalScan) -> (FileScanStream, ArrowStream)

Build the incremental scan and return a nested stream pair.
Convenience wrapper around `build!` + `nested_incremental_arrow_stream`.
"""
function scan_incremental_nested!(scan::IncrementalScan)
    build!(scan)
    return nested_incremental_arrow_stream(scan)
end
