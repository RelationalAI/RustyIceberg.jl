# Common scan utilities shared between full and incremental scans

"""
    ArrowStream

Opaque pointer type representing an Arrow stream from the Rust FFI layer.
This stream can be used to fetch batches of Arrow data asynchronously.
"""
const ArrowStream = Ptr{Cvoid}

"""
    FILE_COLUMN

The name of the metadata column containing file paths (_file).

This constant can be used with the `select_columns!` function to include
file path information in query results. It corresponds to the _file metadata
column in Iceberg tables.

# Example
```julia
# Select specific columns including the file path
scan = new_scan(table)
select_columns!(scan, ["id", "name", FILE_COLUMN])
stream = scan!(scan)
```
"""
const FILE_COLUMN = "_file"

"""
    POS_COLUMN

The name of the metadata column containing row positions within files (_pos).

This constant can be used with the `select_columns!` function to include
position information in query results. It corresponds to the _pos metadata
column in Iceberg tables, which represents the row's position within its data file.

# Example
```julia
# Select specific columns including the position
scan = new_scan(table)
select_columns!(scan, ["id", "name", POS_COLUMN])
stream = scan!(scan)
```
"""
const POS_COLUMN = "_pos"

# Type alias using generic Response{T}
const BatchResponse = Response{Ptr{ArrowBatch}}
Response{Ptr{ArrowBatch}}() = Response{Ptr{ArrowBatch}}(-1, C_NULL, C_NULL, C_NULL)

"""
    next_batch(stream::ArrowStream)::Ptr{ArrowBatch}

Wait for the next batch from the initialized stream asynchronously and return it directly.
Returns C_NULL if end of stream is reached.
"""
function next_batch(stream::ArrowStream)
    response = BatchResponse()

    async_ccall(response) do handle
        @ccall rust_lib.iceberg_next_batch(
            stream::ArrowStream,
            response::Ref{BatchResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "iceberg_next_batch", IcebergException)

    # Return the batch pointer directly
    return response.value
end

"""
    free_batch(batch::Ptr{ArrowBatch})

Free the memory associated with an Arrow batch.
"""
function free_batch(batch::Ptr{ArrowBatch})
    @ccall rust_lib.iceberg_arrow_batch_free(batch::Ptr{ArrowBatch})::Cvoid
end

"""
    free_stream(stream::ArrowStream)

Free the memory associated with an Arrow stream.
"""
function free_stream(stream::ArrowStream)
    @ccall rust_lib.iceberg_arrow_stream_free(stream::ArrowStream)::Cvoid
end

# ---------------------------------------------------------------------------
# Split-scan API types
#
# These support a two-phase workflow:
#   task_stream = plan_files(scan)
#   reader = create_reader(scan)
#   task = next_task(task_stream)         # concurrent-safe
#   stream = read_task(reader, task)      # consumes task, shares reader cache
#
# ArrowReader is cloned per read_task call. The clone shares the internal
# CachingDeleteFileLoader via Arc, so delete files loaded by one consumer
# are cached for all others.
# ---------------------------------------------------------------------------

# Wrapper structs for opaque FFI pointers — enables multiple dispatch.
struct ArrowReaderContext
    ptr::Ptr{Cvoid}
end

struct FileScanTaskStream
    ptr::Ptr{Cvoid}
end

struct FileScanTaskHandle
    ptr::Ptr{Cvoid}
end

struct IncrementalFileScanTaskStreams
    ptr::Ptr{Cvoid}
end

struct AppendTaskHandle
    ptr::Ptr{Cvoid}
end

struct DeleteTaskHandle
    ptr::Ptr{Cvoid}
end

# All FFI responses use Ptr{Cvoid}; we wrap into structs after receiving.
const OpaqueResponse = Response{Ptr{Cvoid}}

"""
    free_reader(reader::ArrowReaderContext)

Free the shared ArrowReader context.
"""
function free_reader(reader::ArrowReaderContext)
    @ccall rust_lib.iceberg_arrow_reader_context_free(reader.ptr::Ptr{Cvoid})::Cvoid
end

"""
    free_task_stream(stream::FileScanTaskStream)

Free a file scan task stream.
"""
function free_task_stream(stream::FileScanTaskStream)
    @ccall rust_lib.iceberg_file_scan_task_stream_free(stream.ptr::Ptr{Cvoid})::Cvoid
end

"""
    free_task(task::FileScanTaskHandle)

Free a file scan task. Do NOT call this after passing the task to `read_task`
(which consumes it).
"""
function free_task(task::FileScanTaskHandle)
    @ccall rust_lib.iceberg_file_scan_task_free(task.ptr::Ptr{Cvoid})::Cvoid
end

"""
    free_task_stream(streams::IncrementalFileScanTaskStreams)

Free incremental file scan task streams.
"""
function free_task_stream(streams::IncrementalFileScanTaskStreams)
    @ccall rust_lib.iceberg_incremental_file_scan_task_streams_free(streams.ptr::Ptr{Cvoid})::Cvoid
end

"""
    free_task(task::AppendTaskHandle)

Free an append task. Do NOT call after `read_append_task` (which consumes it).
"""
function free_task(task::AppendTaskHandle)
    @ccall rust_lib.iceberg_append_task_free(task.ptr::Ptr{Cvoid})::Cvoid
end

"""
    free_task(task::DeleteTaskHandle)

Free a delete task. Do NOT call after `read_delete_task` (which consumes it).
"""
function free_task(task::DeleteTaskHandle)
    @ccall rust_lib.iceberg_delete_task_free(task.ptr::Ptr{Cvoid})::Cvoid
end

"""
    task_data_file_path(task::FileScanTaskHandle)::String

Get the data file path from a file scan task.
The returned string is a copy — safe to use after freeing the task.
"""
function task_data_file_path(task::FileScanTaskHandle)
    cstr = @ccall rust_lib.iceberg_file_scan_task_data_file_path(task.ptr::Ptr{Cvoid})::Ptr{Cchar}
    if cstr == C_NULL
        throw(IcebergException("Failed to get data file path"))
    end
    result = unsafe_string(cstr)
    @ccall rust_lib.iceberg_destroy_cstring(cstr::Ptr{Cchar})::Cint
    return result
end

"""
    task_data_file_path(task::AppendTaskHandle)::String

Get the data file path from an append task.
The returned string is a copy — safe to use after freeing the task.
"""
function task_data_file_path(task::AppendTaskHandle)
    cstr = @ccall rust_lib.iceberg_append_task_data_file_path(task.ptr::Ptr{Cvoid})::Ptr{Cchar}
    if cstr == C_NULL
        throw(IcebergException("Failed to get data file path"))
    end
    result = unsafe_string(cstr)
    @ccall rust_lib.iceberg_destroy_cstring(cstr::Ptr{Cchar})::Cint
    return result
end

"""
    task_data_file_path(task::DeleteTaskHandle)::String

Get the data file path from a delete task.
The returned string is a copy — safe to use after freeing the task.
"""
function task_data_file_path(task::DeleteTaskHandle)
    cstr = @ccall rust_lib.iceberg_delete_task_data_file_path(task.ptr::Ptr{Cvoid})::Ptr{Cchar}
    if cstr == C_NULL
        throw(IcebergException("Failed to get data file path"))
    end
    result = unsafe_string(cstr)
    @ccall rust_lib.iceberg_destroy_cstring(cstr::Ptr{Cchar})::Cint
    return result
end
