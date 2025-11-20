# Regular (full) table scan implementation

"""
    Scan

A mutable wrapper around a pointer to a regular (full) table scan.

This type enables multiple dispatch and safe memory management for Iceberg table scans.
Use `new_scan` to create a scan, configure it with builder methods, and call `scan!`
to build the scan and obtain an Arrow stream.

# Example
```julia
table = table_open("s3://path/to/table/metadata.json")
scan = new_scan(table)
select_columns!(scan, ["col1", "col2"])
with_batch_size!(scan, UInt(1024))
stream = scan!(scan)
# ... process batches from stream
free_stream(stream)
free_scan!(scan)
free_table(table)
```
"""
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
    with_file_column!(scan::Scan)

Add the _file metadata column to the scan.

The _file column contains the file path for each row, which can be useful for
tracking which data files contain specific rows.

# Example
```julia
scan = new_scan(table)
with_file_column!(scan)
stream = scan!(scan)
```
"""
function with_file_column!(scan::Scan)
    result = @ccall rust_lib.iceberg_scan_with_file_column(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cint

    if result != 0
        error("Failed to add file column to scan")
    end
    return nothing
end

"""
    with_pos_column!(scan::Scan)

Add the _pos metadata column to the scan.

The _pos column contains the position of each row within its data file, which can
be useful for tracking row locations and debugging.

# Example
```julia
scan = new_scan(table)
with_pos_column!(scan)
stream = scan!(scan)
```
"""
function with_pos_column!(scan::Scan)
    result = @ccall rust_lib.iceberg_scan_with_pos_column(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cint

    if result != 0
        error("Failed to add pos column to scan")
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

"""
    ArrowStreamResponse

Response structure for asynchronous Arrow stream initialization operations.

# Fields
- `result::Cint`: Result code from the operation (0 for success)
- `stream::ArrowStream`: The initialized Arrow stream
- `error_message::Ptr{Cchar}`: Error message string if operation failed
- `context::Ptr{Cvoid}`: Context pointer for operation cancellation
"""
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
