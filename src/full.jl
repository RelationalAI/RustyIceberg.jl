# Regular (full) table scan implementation

# Wrapper type for regular scan to enable multiple dispatch
mutable struct Scan
    ptr::Ptr{Cvoid}
end

"""
    new_scan(table::Table) -> Scan

Create a scan for the given table.
"""
function new_scan(table::Table)
    scan_ptr = @ccall rust_lib.iceberg_new_scan(table::Table)::Ptr{Cvoid}
    return Scan(scan_ptr)
end

"""
    select_columns!(scan::Scan, column_names::Vector{String})

Select specific columns for the scan.
"""
function select_columns!(scan::Scan, column_names::Vector{String})
    # Convert String vector to Cstring array
    c_strings = [pointer(col) for col in column_names]
    result = @ccall rust_lib.iceberg_select_columns(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        pointer(c_strings)::Ptr{Cstring},
        length(column_names)::Csize_t
    )::Cint

    if result != 0
        error("Failed to select columns")
    end
    return nothing
end

"""
    with_data_file_concurrency_limit!(scan::Scan, n::UInt)

Sets the data file concurrency level for the scan.
"""
function with_data_file_concurrency_limit!(scan::Scan, n::UInt)
    result = @ccall rust_lib.iceberg_scan_with_data_file_concurrency_limit(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        error("Failed to set data file concurrency limit")
    end
    return nothing
end

"""
    with_manifest_entry_concurrency_limit!(scan::Scan, n::UInt)

Sets the manifest entry concurrency level for the scan.
"""
function with_manifest_entry_concurrency_limit!(scan::Scan, n::UInt)
    result = @ccall rust_lib.iceberg_scan_with_manifest_entry_concurrency_limit(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        error("Failed to set manifest entry concurrency limit")
    end
    return nothing
end

"""
    with_batch_size!(scan::Scan, n::UInt)

Sets the batch size for the scan.
"""
function with_batch_size!(scan::Scan, n::UInt)
    result = @ccall rust_lib.iceberg_scan_with_batch_size(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        error("Failed to set batch size")
    end
    return nothing
end

"""
    build!(scan::Scan)

Build the provided table scan object.
"""
function build!(scan::Scan)
    result = @ccall rust_lib.iceberg_scan_build(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cint

    if result != 0
        throw(IcebergException("Failed to build scan", result))
    end
    return nothing
end

# Response structure for async arrow stream operations
mutable struct ArrowStreamResponse
    result::Cint
    stream::ArrowStream
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    ArrowStreamResponse() = new(-1, C_NULL, C_NULL, C_NULL)
end

"""
    arrow_stream(scan::Scan)::ArrowStream

Initialize an Arrow stream for the scan asynchronously.
"""
function arrow_stream(scan::Scan)
    response = ArrowStreamResponse()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)

    preserve_task(ct)
    result = GC.@preserve response event try
        result = @ccall rust_lib.iceberg_arrow_stream(
            scan.ptr::Ptr{Cvoid},
            response::Ref{ArrowStreamResponse},
            handle::Ptr{Cvoid}
        )::Cint

        wait_or_cancel(event, response)

        result
    finally
        unpreserve_task(ct)
    end

    @throw_on_error(response, "iceberg_arrow_stream", IcebergException)

    return response.stream
end

"""
    scan!(scan::Scan) -> ArrowStream

Build the provided table scan object and return an Arrow stream.
"""
function scan!(scan::Scan)
    build!(scan)
    return arrow_stream(scan)
end

"""
    free_scan!(scan::Scan)

Free the memory associated with a scan.
"""
function free_scan!(scan::Scan)
    @ccall rust_lib.iceberg_scan_free(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cvoid
end
