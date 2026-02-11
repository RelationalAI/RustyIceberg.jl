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
        throw(IcebergException("Failed to select columns", result))
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
        throw(IcebergException("Failed to set data file concurrency limit", result))
    end
    return nothing
end

"""
    with_manifest_file_concurrency_limit!(scan::Scan, n::UInt)

Sets the manifest file concurrency level for the full scan.
"""
function with_manifest_file_concurrency_limit!(scan::Scan, n::UInt)
    result = @ccall rust_lib.iceberg_scan_with_manifest_file_concurrency_limit(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set manifest file concurrency limit", result))
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
        throw(IcebergException("Failed to set manifest entry concurrency limit", result))
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
        throw(IcebergException("Failed to set batch size", result))
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
        throw(IcebergException("Failed to add file column to scan", result))
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
        throw(IcebergException("Failed to add pos column to scan", result))
    end
    return nothing
end

"""
    with_serialization_concurrency_limit!(scan::Scan, n::UInt)

Set the serialization concurrency limit for the scan.

This controls how many RecordBatch serializations can happen in parallel.
- `n = 0`: Auto-detect based on CPU cores (default)
- `n > 0`: Use exactly n concurrent serializations

Higher values improve throughput for scans with many batches, but use more memory.

# Example
```julia
scan = new_scan(table)
with_serialization_concurrency_limit!(scan, UInt(8))  # Serialize up to 8 batches in parallel
stream = scan!(scan)
```
"""
function with_serialization_concurrency_limit!(scan::Scan, n::UInt)
    result = @ccall rust_lib.iceberg_scan_with_serialization_concurrency_limit(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set serialization concurrency limit", result))
    end
    return nothing
end

"""
    with_snapshot_id!(scan::Scan, snapshot_id::Int64)

Set the snapshot ID for the scan.

By default, a scan uses the current (latest) snapshot of the table. This method allows
you to scan a specific snapshot by ID, which is useful for reading historical data or
comparing data across different points in time.

# Example
```julia
table = table_open("s3://path/to/table/metadata.json")
# List available snapshots (if method is available)
scan = new_scan(table)
with_snapshot_id!(scan, Int64(123))  # Scan snapshot with ID 123
stream = scan!(scan)
```
"""
function with_snapshot_id!(scan::Scan, snapshot_id::Int64)
    result = @ccall rust_lib.iceberg_scan_with_snapshot_id(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        snapshot_id::Int64
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set snapshot ID", result))
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
