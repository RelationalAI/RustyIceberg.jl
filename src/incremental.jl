# Incremental table scan implementation

# Wrapper type for incremental scan to enable multiple dispatch
mutable struct IncrementalScan
    ptr::Ptr{Cvoid}
end

# Response structure for unzipped streams
mutable struct UnzippedStreamsResponse
    result::Cint
    inserts_stream::ArrowStream
    deletes_stream::ArrowStream
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    UnzippedStreamsResponse() = new(-1, C_NULL, C_NULL, C_NULL, C_NULL)
end

"""
    new_incremental_scan(table::Table, from_snapshot_id::Int64, to_snapshot_id::Int64) -> IncrementalScan

Create an incremental scan for the given table between two snapshots.
"""
function new_incremental_scan(table::Table, from_snapshot_id::Int64, to_snapshot_id::Int64)
    scan_ptr = @ccall rust_lib.iceberg_new_incremental_scan(
        table::Table,
        from_snapshot_id::Int64,
        to_snapshot_id::Int64
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
