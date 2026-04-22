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

# ---------------------------------------------------------------------------
# Split-scan API
#
# Usage:
#   build!(scan)
#   reader = create_reader(scan)
#   file_stream = plan_files(scan)
#   while (fs = next_file_scan(file_stream)) !== nothing
#       stream = read_file_scan(reader, fs)   # consumes fs
#       while (bp = next_batch(stream)) != C_NULL
#           # ... process batch ...
#           free_batch(bp)
#       end
#       free_stream(stream)
#   end
#   free_file_scan_stream(file_stream)
#   free_reader(reader)
# ---------------------------------------------------------------------------

"""Opaque pointer to a stream of FileScanTasks."""
mutable struct FileScanTaskStream
    ptr::Ptr{Cvoid}
end

"""Shared ArrowReader context — pass to every read_file_scan call."""
mutable struct ArrowReaderContext
    ptr::Ptr{Cvoid}
end

"""Handle to a single file scan task returned by next_file_scan."""
mutable struct FileScanHandle
    ptr::Ptr{Cvoid}
end

const OpaqueResponse = Response{Ptr{Cvoid}}
OpaqueResponse() = Response{Ptr{Cvoid}}(-1, C_NULL, C_NULL, C_NULL)

"""
    plan_files(scan::Scan)::FileScanTaskStream

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
    return FileScanTaskStream(response.value)
end

"""
    create_reader(scan::Scan; reader_concurrency::UInt=UInt(0))::ArrowReaderContext

Create a shared reader context from the scan's configuration.
Pass this to every `read_file_scan` call.
`reader_concurrency` overrides the scan-level data_file_concurrency_limit when > 0.
"""
function create_reader(scan::Scan; reader_concurrency::UInt=UInt(0))
    ptr = @ccall rust_lib.iceberg_create_reader(
        scan.ptr::Ptr{Cvoid},
        reader_concurrency::Csize_t
    )::Ptr{Cvoid}
    if ptr == C_NULL
        throw(IcebergException("Failed to create reader from scan"))
    end
    return ArrowReaderContext(ptr)
end

"""
    next_file_scan(stream::FileScanTaskStream)::Union{FileScanHandle, Nothing}

Pull the next file scan from the stream. Returns `nothing` at end-of-stream.
"""
function next_file_scan(stream::FileScanTaskStream)
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
    read_file_scan(reader::ArrowReaderContext, fs::FileScanHandle)::ArrowStream

Read a single file scan into an Arrow stream. **Consumes `fs`** — do not call
`free_file_scan` afterwards.
"""
function read_file_scan(reader::ArrowReaderContext, fs::FileScanHandle)
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
    file_scan_record_count(fs::FileScanHandle)::Union{Int64, Nothing}

Return the record count for this file scan, or `nothing` if not available.
"""
function file_scan_record_count(fs::FileScanHandle)
    count = @ccall rust_lib.iceberg_file_scan_task_record_count(
        fs.ptr::Ptr{Cvoid}
    )::Int64
    return count == -1 ? nothing : count
end

"""Free a file scan stream (from plan_files)."""
function free_file_scan_stream(stream::FileScanTaskStream)
    @ccall rust_lib.iceberg_file_scan_task_stream_free(stream.ptr::Ptr{Cvoid})::Cvoid
end

"""Free an ArrowReaderContext (from create_reader)."""
function free_reader(reader::ArrowReaderContext)
    @ccall rust_lib.iceberg_arrow_reader_context_free(reader.ptr::Ptr{Cvoid})::Cvoid
end

"""
Free a FileScanHandle. Only call this if the handle was NOT passed to read_file_scan,
since read_file_scan consumes the handle.
"""
function free_file_scan(fs::FileScanHandle)
    @ccall rust_lib.iceberg_file_scan_task_free(fs.ptr::Ptr{Cvoid})::Cvoid
end
