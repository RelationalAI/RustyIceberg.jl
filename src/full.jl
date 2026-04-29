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
    result = GC.@preserve scan c_strings @ccall rust_lib.iceberg_select_columns(
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
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_with_data_file_concurrency_limit(
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
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_with_manifest_file_concurrency_limit(
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
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_with_manifest_entry_concurrency_limit(
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
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_with_batch_size(
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
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_with_file_column(
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
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_with_pos_column(
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
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_with_serialization_concurrency_limit(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set serialization concurrency limit", result))
    end
    return nothing
end

"""
    with_file_prefetch_depth!(scan::Scan, n::UInt)

Set how many FileScan tasks are queued ahead in the outer FileScanStream.
Higher values keep the Julia consumer busy but use more memory.
"""
function with_file_prefetch_depth!(scan::Scan, n::UInt)
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_with_file_prefetch_depth(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint
    if result != 0
        throw(IcebergException("Failed to set file prefetch depth", result))
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
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_with_snapshot_id(
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
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_build(
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
    GC.@preserve scan @ccall rust_lib.iceberg_scan_free(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cvoid
end

# ── Nested file-scan pipeline ──────────────────────────────────────────────
#
# Returns a FileScanStream (outer stream). Each item yielded by next_file_scan
# is an opaque FileScan pointer carrying the filename, record count, and an
# inner IcebergArrowStream of prefetched batches.
#
# Ownership rules:
#   - Call free_file_scan! after processing each FileScan.
#   - Do NOT call free_stream on the pointer returned by file_scan_arrow_stream;
#     iceberg_file_scan_free handles that.
#   - Call free_file_scan_stream! after the outer loop completes.

"""Opaque pointer type for an outer file-scan stream (IcebergFileScanStream)."""
const FileScanStream = Ptr{Cvoid}

const FileScanStreamResponse = Response{FileScanStream}

"""
    nested_arrow_stream(scan::Scan)::FileScanStream

Initialize a nested Arrow stream for the scan asynchronously.
Returns an outer FileScanStream whose items are per-file scans.
"""
function nested_arrow_stream(scan::Scan)
    response = FileScanStreamResponse()

    async_ccall(response) do handle
        @ccall rust_lib.iceberg_file_scan_stream(
            scan.ptr::Ptr{Cvoid},
            response::Ref{FileScanStreamResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "iceberg_file_scan_stream", IcebergException)

    return response.value
end

"""
    scan_nested!(scan::Scan)::FileScanStream

Build the scan and return a nested FileScanStream (one item per file).
"""
function scan_nested!(scan::Scan)
    build!(scan)
    return nested_arrow_stream(scan)
end

# Response type for next_file_scan
const FileScanResponse = Response{Ptr{Cvoid}}

"""
    next_file_scan(stream::FileScanStream)::Ptr{Cvoid}

Wait for the next per-file scan from the outer stream asynchronously.
Returns C_NULL when the stream is exhausted.
"""
function next_file_scan(stream::FileScanStream)
    response = FileScanResponse()

    async_ccall(response) do handle
        @ccall rust_lib.iceberg_next_file_scan(
            stream::FileScanStream,
            response::Ref{FileScanResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "iceberg_next_file_scan", IcebergException)

    return response.value
end

"""
    file_scan_record_count(fs::Ptr{Cvoid})::Int64

Return the record count of the file scan. Returns -1 on null input.
"""
function file_scan_record_count(fs::Ptr{Cvoid})::Int64
    return @ccall rust_lib.iceberg_file_scan_record_count(fs::Ptr{Cvoid})::Int64
end

"""
    file_scan_filename(fs::Ptr{Cvoid})::String

Return the file path of the file scan as a Julia String.
"""
function file_scan_filename(fs::Ptr{Cvoid})::String
    ptr = @ccall rust_lib.iceberg_file_scan_filename(fs::Ptr{Cvoid})::Ptr{Cchar}
    ptr == C_NULL && return ""
    return unsafe_string(ptr)
end

"""
    file_scan_arrow_stream(fs::Ptr{Cvoid})::ArrowStream

Return the inner batch stream of the file scan (borrowed pointer).
Do NOT call free_stream on the returned pointer; use free_file_scan! instead.
"""
function file_scan_arrow_stream(fs::Ptr{Cvoid})::ArrowStream
    # IcebergFileScan layout (repr(C)):
    #   filename:     *mut c_char   (offset 0, 8 bytes)
    #   record_count: i64           (offset 8, 8 bytes)
    #   stream:       *mut IcebergArrowStream (offset 16, 8 bytes)
    offset = sizeof(Ptr{Cvoid}) + sizeof(Int64)
    return unsafe_load(convert(Ptr{Ptr{Cvoid}}, fs + offset))
end

"""
    free_file_scan!(fs::Ptr{Cvoid})

Free the memory associated with a file scan (filename and inner stream).
"""
function free_file_scan!(fs::Ptr{Cvoid})
    @ccall rust_lib.iceberg_file_scan_free(fs::Ptr{Cvoid})::Cvoid
end

"""
    free_file_scan_stream!(stream::FileScanStream)

Free the memory associated with a file scan stream.
"""
function free_file_scan_stream!(stream::FileScanStream)
    @ccall rust_lib.iceberg_file_scan_stream_free(stream::FileScanStream)::Cvoid
end
