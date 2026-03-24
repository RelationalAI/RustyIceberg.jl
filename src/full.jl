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
    with_prefetch_depth!(scan::Scan, n::UInt)

Set the number of serialized batches buffered ahead of the consumer.

The Rust FFI layer eagerly drains the serialized batch stream into a bounded
buffer of this size, decoupling Parquet decode + IPC serialization from Julia's
pull rate. Higher values hide more latency but consume more memory.
- `n = 0`: Use the Rust-side default (currently 8)
- `n > 0`: Use exactly n buffer slots

# Example
```julia
scan = new_scan(table)
with_prefetch_depth!(scan, UInt(16))
```
"""
function with_prefetch_depth!(scan::Scan, n::UInt)
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_with_prefetch_depth(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set prefetch depth", result))
    end
    return nothing
end

"""
    with_task_prefetch_depth!(scan::Scan, n::UInt)

Set the number of file scan tasks buffered ahead of the consumer.

The Rust FFI layer eagerly plans file scan tasks into a bounded buffer,
so task planning runs ahead of Julia's consumption. Only a small number
of tasks need buffering — planning is cheap compared to reading data.
- `n = 0`: Use the Rust-side default (currently 4)
- `n > 0`: Use exactly n buffer slots
"""
function with_task_prefetch_depth!(scan::Scan, n::UInt)
    result = GC.@preserve scan @ccall rust_lib.iceberg_scan_with_task_prefetch_depth(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set task prefetch depth", result))
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
# Split-scan API for full scans
#
# Usage:
#   build!(scan)
#   task_stream = plan_files(scan)
#   reader = create_reader(scan)
#   while (task = next_task(task_stream)) !== nothing
#       stream = read_task(reader, task)  # consumes task
#       # ... iterate batches with next_batch(stream) ...
#       free_stream(stream)
#   end
#   free_reader(reader)
#   free_task_stream(task_stream)
#   free_scan!(scan)
#
# Multiple Julia tasks can call next_task concurrently on the same
# task_stream. Each read_task clones the ArrowReader internally,
# sharing the delete-file cache.
# ---------------------------------------------------------------------------

"""
    plan_files(scan::Scan)::FileScanTaskStream

Plan which files to read. Returns a task stream for use with `next_task`.
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
Pass this to every `read_task` call. The internal ArrowReader is cloned
per call, sharing the delete-file cache across consumers.

`reader_concurrency` sets the data-file concurrency for the reader (how many
files the reader can process in parallel within a single `read_task` call).
- `0`: Use the scan-level `data_file_concurrency_limit` (default)
- `> 0`: Override with this value
"""
function create_reader(scan::Scan; reader_concurrency::UInt=UInt(0))
    ptr = @ccall rust_lib.iceberg_create_reader(
        scan.ptr::Ptr{Cvoid},
        reader_concurrency::Csize_t,
    )::Ptr{Cvoid}
    if ptr == C_NULL
        throw(IcebergException("Failed to create reader from scan"))
    end
    return ArrowReaderContext(ptr)
end

"""
    next_task(task_stream::FileScanTaskStream)::Union{FileScanTaskHandle, Nothing}

Pull the next task from the stream. Returns `nothing` when exhausted.
Safe to call concurrently from multiple Julia tasks.
"""
function next_task(task_stream::FileScanTaskStream)
    response = OpaqueResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_next_file_scan_task(
            task_stream.ptr::Ptr{Cvoid},
            response::Ref{OpaqueResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_next_file_scan_task", IcebergException)
    return response.value == C_NULL ? nothing : FileScanTaskHandle(response.value)
end

"""
    read_task(reader::ArrowReaderContext, task::FileScanTaskHandle)::ArrowStream

Read a single task into an Arrow stream. **Consumes the task** — do not
call `free_task` afterwards.
"""
function read_task(reader::ArrowReaderContext, task::FileScanTaskHandle)
    response = ArrowStreamResponse()
    async_ccall(response) do handle
        @ccall rust_lib.iceberg_read_file_scan_task(
            reader.ptr::Ptr{Cvoid},
            task.ptr::Ptr{Cvoid},
            response::Ref{ArrowStreamResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "iceberg_read_file_scan_task", IcebergException)
    return response.value
end
