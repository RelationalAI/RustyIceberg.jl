module RustyIceberg

using Base: @kwdef, @lock
using Base.Threads: Atomic
using Arrow
using FunctionWrappers: FunctionWrapper
using JSON
using iceberg_rust_ffi_jll

export Table, Scan, IncrementalScan, ArrowBatch, StaticConfig, ArrowStream
export init_runtime
export IcebergException
export new_incremental_scan, free_incremental_scan!
export table_open, free_table, new_scan, free_scan!
export table_location, table_uuid, table_format_version, table_last_sequence_number, table_last_updated_ms, table_schema
export select_columns!, with_batch_size!, with_data_file_concurrency_limit!, with_manifest_entry_concurrency_limit!
export with_file_column!, with_pos_column!
export scan!, next_batch, free_batch, free_stream
export FILE_COLUMN, POS_COLUMN
export Catalog, catalog_create_rest, free_catalog
export load_table, list_tables, list_namespaces, table_exists, create_table, drop_table, drop_namespace, create_namespace
export Field, Schema, PartitionField, PartitionSpec, SortField, SortOrder
export SchemaBuilder, add_field, with_identifier, build
export schema_to_json, partition_spec_to_json, sort_order_to_json
export Transaction, DataFiles, free_transaction!, commit, transaction
export FastAppendAction, free_fast_append_action!, add_data_files, apply, with_fast_append
export DataFileWriter, free_writer!, close_writer

# Always use the JLL library - override via Preferences if needed for local development
# To use a local build, set the preference:
#   using Preferences; set_preferences!("iceberg_rust_ffi_jll", "libiceberg_rust_ffi_path" => "/path/to/target/release/")
const rust_lib = iceberg_rust_ffi_jll.libiceberg_rust_ffi

"""
    struct StaticConfig

Runtime configuration for the Iceberg library.
Value of 0 means use all CPU cores, regardless of the number of threads in the Julia runtime.
"""
struct StaticConfig
    n_threads::Culonglong
end

function default_panic_hook()
    println("Rust thread panicked, exiting the process")
    exit(1)
end

const _ICEBERG_STARTED = Atomic{Bool}(false)

function iceberg_started()
    return _ICEBERG_STARTED[]
end

const _INIT_LOCK::ReentrantLock = ReentrantLock()
_PANIC_HOOK::Function = default_panic_hook

Base.@ccallable function iceberg_panic_hook_wrapper()::Cint
    global _PANIC_HOOK
    _PANIC_HOOK()
    return 0
end

# This is the callback that Rust calls to notify a Julia task of a completed operation.
Base.@ccallable function notify_result_iceberg(event_ptr::Ptr{Nothing})::Cint
    event = unsafe_pointer_to_objref(event_ptr)::Base.Event
    notify(event)
    return 0
end

# A dict of all tasks that are waiting some result from Rust
const tasks_in_flight = IdDict{Task, Int64}()
const preserve_task_lock = Threads.SpinLock()
function preserve_task(x::Task)
    @lock preserve_task_lock begin
        v = get(tasks_in_flight, x, 0)::Int
        tasks_in_flight[x] = v + 1
    end
    nothing
end
function unpreserve_task(x::Task)
    @lock preserve_task_lock begin
        v = get(tasks_in_flight, x, 0)::Int
        if v == 0
            error("unbalanced call to unpreserve_task for $(typeof(x))")
        elseif v == 1
            pop!(tasks_in_flight, x)
        else
            tasks_in_flight[x] = v - 1
        end
    end
    nothing
end

"""
    init_runtime()
    init_runtime(config::IcebergConfig)
    init_runtime(config::IcebergConfig; on_rust_panic::Function)

Initialize the Iceberg runtime.

This starts a `tokio` runtime for handling Iceberg requests.
It must be called before sending a request.
"""
function init_runtime(
    config::StaticConfig=StaticConfig(Threads.nthreads());
    on_rust_panic::Function=default_panic_hook
)
    global _PANIC_HOOK
    @lock _INIT_LOCK begin
        if _ICEBERG_STARTED[]
            return nothing
        end
        _PANIC_HOOK = on_rust_panic
        panic_fn_ptr = @cfunction(iceberg_panic_hook_wrapper, Cint, ())
        fn_ptr = @cfunction(notify_result_iceberg, Cint, (Ptr{Nothing},))
        res = @ccall rust_lib.iceberg_init_runtime(config::StaticConfig, panic_fn_ptr::Ptr{Nothing}, fn_ptr::Ptr{Nothing})::Cint
        if res != 0
            throw(IcebergException("Failed to initialize Iceberg runtime.", res))
        end
        _ICEBERG_STARTED[] = true
    end
    return nothing
end

function response_error_to_string(response, operation)
    err = string("failed to process ", operation, " with error: ", unsafe_string(response.error_message))
    @ccall rust_lib.iceberg_destroy_cstring(response.error_message::Ptr{Cchar})::Cint
    return err
end

macro throw_on_error(response, operation, exception)
    throw_on_error(response, operation, exception)
end

function throw_on_error(response, operation, exception)
    return :( $(esc(:($response.result != 0))) ? throw($exception($response_error_to_string($(esc(response)), $operation))) : $(nothing) )
end

function ensure_wait(event::Base.Event)
    for _ in 1:20
        try
            return wait(event)
        catch e
            @error "cannot skip this wait point to prevent UB, ignoring exception: $(e)"
        end
    end

    @error "ignored too many wait exceptions, giving up"
    exit(1)
end

function wait_or_cancel(event::Base.Event, response)
    try
        return wait(event)
    catch e
        # Cancel the operation on the Rust side
        if response.context != C_NULL
            @ccall rust_lib.iceberg_cancel_context(response.context::Ptr{Cvoid})::Cint
        end
        ensure_wait(event)
        if response.error_message != C_NULL
            @ccall rust_lib.iceberg_destroy_cstring(response.error_message::Ptr{Cchar})::Cint
        end
        rethrow(e)
    finally
        # Always cleanup the context
        if response.context != C_NULL
            @ccall rust_lib.iceberg_destroy_context(response.context::Ptr{Cvoid})::Cint
        end
    end
end

"""
    async_ccall(f::F, response, preserve_objs...) -> result where F<:Function

Helper function to execute an async FFI call with proper task preservation and error handling.

This function handles the common pattern of:
1. Preserving the current task
2. Creating an event for synchronization
3. Calling an FFI function with GC protection
4. Waiting for the result or canceling on error
5. Unpreserving the task
6. Checking for errors

# Arguments
- `f::F`: A callable that takes (handle) and performs the FFI call
- `response`: Response object to pass to the FFI call
- `preserve_objs...`: Additional objects to preserve with GC.@preserve (e.g., arrays, strings)

# Returns
The result code from the FFI call (0 for success)

# Example
```julia
response = MyResponse()
async_ccall(response, obj1, obj2) do handle
    @ccall rust_lib.my_ffi_function(
        arg1::Type1,
        arg2::Type2,
        response::Ref{MyResponse},
        handle::Ptr{Cvoid}
    )::Cint
end
```
"""
function async_ccall(f::F, response, preserve_objs...) where F<:Function
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)

    preserve_task(ct)
    # Collect all objects to preserve in a tuple and unpack them
    objs_tuple = (response, event, preserve_objs...)
    try
        result = GC.@preserve objs_tuple begin
            result = f(handle)
            wait_or_cancel(event, response)
            result
        end
        return result
    finally
        unpreserve_task(ct)
    end
end

"""
    Table

Opaque pointer type representing an Iceberg table handle from the Rust FFI layer.

Create a table using `table_open` and free it with `free_table` when done.
"""
const Table = Ptr{Cvoid}

"""
    ArrowBatch

Structure representing a batch of Arrow data from the Rust FFI layer.

# Fields
- `data::Ptr{UInt8}`: Pointer to the Arrow IPC format data
- `length::Csize_t`: Length of the data buffer in bytes
- `rust_ptr::Ptr{Cvoid}`: Rust-side pointer for memory management

Batches should be freed using `free_batch` after processing.
"""
struct ArrowBatch
    data::Ptr{UInt8}
    length::Csize_t
    rust_ptr::Ptr{Cvoid}
end

# Define PropertyEntry before including modules that use it
"""
    PropertyEntry

FFI structure for passing key-value properties to Rust.

# Fields
- `key::Ptr{Cchar}`: Pointer to the key string
- `value::Ptr{Cchar}`: Pointer to the value string
"""
struct PropertyEntry
    key::Ptr{Cchar}
    value::Ptr{Cchar}
end

"""
    Response{T}

Generic response structure for asynchronous operations returning a value of type T.

# Fields
- `result::Cint`: Result code from the operation (0 for success)
- `value::T`: The response value
- `error_message::Ptr{Cchar}`: Error message string if operation failed
- `context::Ptr{Cvoid}`: Context pointer for operation cancellation
"""
mutable struct Response{T}
    result::Cint
    value::T
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}
end

# Default constructors for common response types
# Note: Table = Ptr{Cvoid}, so Response{Table} and Response{Ptr{Cvoid}} are the same type
Response{Ptr{Cvoid}}() = Response{Ptr{Cvoid}}(-1, C_NULL, C_NULL, C_NULL)
Response{Bool}() = Response{Bool}(-1, false, C_NULL, C_NULL)

# Type aliases for common response types
const TableResponse = Response{Table}

# Include scan modules
include("scan_common.jl")
include("full.jl")
include("incremental.jl")

# Include schema and write support
include("schema.jl")

# Include catalog module
include("catalog.jl")

# Include data file module
include("data_file.jl")

# Include transaction module
include("transaction.jl")

# Include writer module
include("writer.jl")

"""
    IcebergException <: Exception

Exception type for Iceberg operations.

# Fields
- `msg::String`: Error message describing what went wrong
- `code::Union{Int,Nothing}`: Optional error code from the FFI layer
"""
struct IcebergException <: Exception
    msg::String
    code::Union{Int,Nothing}
end

function IcebergException(msg::String)
    return IcebergException(msg, nothing)
end

# High-level functions using the async API pattern from RustyObjectStore.jl

"""
    table_open(snapshot_path::String; scheme::String="s3", properties::Dict{String,String}=Dict{String,String}())::Table

Open an Iceberg table from the given snapshot path.

# Arguments
- `snapshot_path::String`: Path to the metadata.json file for the table snapshot
- `scheme::String`: Storage scheme (e.g., "s3", "file"). Defaults to "s3"
- `properties::Dict{String,String}`: Optional key-value properties for the FileIO configuration.
  By default (empty dict), credentials are read from environment variables (AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT_URL, etc.).

  Common S3 properties include:
  - "s3.endpoint": Custom S3 endpoint URL
  - "s3.access-key-id": AWS access key ID
  - "s3.secret-access-key": AWS secret access key
  - "s3.session-token": AWS session token
  - "s3.region": AWS region
  - "s3.allow-anonymous": Set to "true" for anonymous access (no credentials)

# Example
```julia
# Open with credentials from environment variables (default)
table = table_open("s3://bucket/path/metadata/metadata.json")

# Open with anonymous S3 access
table = table_open(
    "s3://bucket/path/metadata/metadata.json",
    properties=Dict("s3.allow-anonymous" => "true")
)

# Open with custom S3 credentials
table = table_open(
    "s3://bucket/path/metadata/metadata.json",
    scheme="s3",
    properties=Dict(
        "s3.endpoint" => "http://localhost:9000",
        "s3.access-key-id" => "minioadmin",
        "s3.secret-access-key" => "minioadmin",
        "s3.region" => "us-east-1"
    )
)
```
"""
function table_open(snapshot_path::String; scheme::String="s3", properties::Dict{String,String}=Dict{String,String}())
    response = TableResponse()

    # Convert properties dict to array of PropertyEntry structs
    property_entries = [PropertyEntry(pointer(k), pointer(v)) for (k, v) in properties]
    properties_len = length(property_entries)

    async_ccall(response, property_entries, properties) do handle
        @ccall rust_lib.iceberg_table_open(
            snapshot_path::Cstring,
            scheme::Cstring,
            (properties_len > 0 ? pointer(property_entries) : C_NULL)::Ptr{PropertyEntry},
            properties_len::Csize_t,
            response::Ref{TableResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "table_open", IcebergException)

    return response.value
end

"""
    free_table(table::Table)

Free the memory associated with an Iceberg table.
"""
function free_table(table::Table)
    @ccall rust_lib.iceberg_table_free(table::Table)::Cvoid
end

"""
    table_location(table::Table)::String

Get the storage location of an Iceberg table.
"""
function table_location(table::Table)
    ptr = @ccall rust_lib.iceberg_table_location(table::Table)::Ptr{Cchar}
    if ptr == C_NULL
        throw(IcebergException("Failed to get table location"))
    end
    result = unsafe_string(ptr)
    @ccall rust_lib.iceberg_destroy_cstring(ptr::Ptr{Cchar})::Cint
    return result
end

"""
    table_uuid(table::Table)::String

Get the UUID of an Iceberg table.
"""
function table_uuid(table::Table)
    ptr = @ccall rust_lib.iceberg_table_uuid(table::Table)::Ptr{Cchar}
    if ptr == C_NULL
        throw(IcebergException("Failed to get table UUID"))
    end
    result = unsafe_string(ptr)
    @ccall rust_lib.iceberg_destroy_cstring(ptr::Ptr{Cchar})::Cint
    return result
end

"""
    table_format_version(table::Table)::Int64

Get the format version of an Iceberg table (1, 2, or 3).
"""
function table_format_version(table::Table)
    version = @ccall rust_lib.iceberg_table_format_version(table::Table)::Int64
    if version == 0
        throw(IcebergException("Failed to get table format version"))
    end
    return version
end

"""
    table_last_sequence_number(table::Table)::Int64

Get the last sequence number of an Iceberg table.
"""
function table_last_sequence_number(table::Table)
    seq_num = @ccall rust_lib.iceberg_table_last_sequence_number(table::Table)::Int64
    if seq_num == -1
        throw(IcebergException("Failed to get table last sequence number"))
    end
    return seq_num
end

"""
    table_last_updated_ms(table::Table)::Int64

Get the last updated timestamp of an Iceberg table in milliseconds since epoch.
"""
function table_last_updated_ms(table::Table)
    timestamp = @ccall rust_lib.iceberg_table_last_updated_ms(table::Table)::Int64
    if timestamp == -1
        throw(IcebergException("Failed to get table last updated timestamp"))
    end
    return timestamp
end

"""
    table_schema(table::Table)::String

Get the current schema of an Iceberg table as a JSON string.

The returned JSON contains the schema definition including field names, types,
and other metadata in the Iceberg schema format.
"""
function table_schema(table::Table)
    ptr = @ccall rust_lib.iceberg_table_schema(table::Table)::Ptr{Cchar}
    if ptr == C_NULL
        throw(IcebergException("Failed to get table schema"))
    end
    result = unsafe_string(ptr)
    @ccall rust_lib.iceberg_destroy_cstring(ptr::Ptr{Cchar})::Cint
    return result
end

end # module RustyIceberg
