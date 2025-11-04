module RustyIceberg

using Base.Libc.Libdl: dlext
using Base: @kwdef, @lock
using Base.Threads: Atomic
using Libdl
using Arrow
using iceberg_rust_ffi_jll

export Table, Scan, IncrementalScan, ArrowBatch, StaticConfig, ArrowStream
export init_runtime
export IcebergException
export new_incremental_scan, free_incremental_scan!
export table_open, free_table, new_scan, free_scan!
export select_columns!, with_batch_size!, with_data_file_concurrency_limit!, with_manifest_entry_concurrency_limit!
export scan!, next_batch, free_batch, free_stream

const Option{T} = Union{T, Nothing}

const rust_lib = if haskey(ENV, "ICEBERG_RUST_LIB")
    # For development, e.g. run `cargo build --release` and point to `target/release/` dir.
    # Note this is set a precompilation time, as `ccall` needs this to be a `const`,
    # so you need to restart Julia / recompile the package if you change it.
    lib_path = realpath(joinpath(ENV["ICEBERG_RUST_LIB"], "libiceberg_rust_ffi.$(dlext)"))
    @warn """
        Using unreleased iceberg_rust_ffi library:
            $(repr(replace(lib_path, homedir() => "~")))
        This is only intended for local development and should not be used in production.
        """
    lib_path
else
    iceberg_rust_ffi_jll.libiceberg_rust_ffi
end

"""
Runtime configuration for the Iceberg library.
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
    config::StaticConfig=StaticConfig(0);
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

# Include scan modules
include("scan_common.jl")
include("full.jl")
include("incremental.jl")

"""
    TableResponse

Response structure for asynchronous table operations.

# Fields
- `result::Cint`: Result code from the operation (0 for success)
- `table::Table`: The opened table handle
- `error_message::Ptr{Cchar}`: Error message string if operation failed
- `context::Ptr{Cvoid}`: Context pointer for operation cancellation
"""
mutable struct TableResponse
    result::Cint
    table::Table
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    TableResponse() = new(-1, C_NULL, C_NULL, C_NULL)
end

"""
    Response

Generic response structure for asynchronous operations.

# Fields
- `result::Cint`: Result code from the operation (0 for success)
- `error_message::Ptr{Cchar}`: Error message string if operation failed
- `context::Ptr{Cvoid}`: Context pointer for operation cancellation
"""
mutable struct Response
    result::Cint
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    Response() = new(-1, C_NULL, C_NULL)
end

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
    table_open(snapshot_path::String)::IcebergTable

Open an Iceberg table from the given snapshot path.
"""
function table_open(snapshot_path::String)
    response = TableResponse()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)

    preserve_task(ct)
    result = GC.@preserve response event try
        result = @ccall rust_lib.iceberg_table_open(
            snapshot_path::Cstring,
            response::Ref{TableResponse},
            handle::Ptr{Cvoid}
        )::Cint

        wait_or_cancel(event, response)

        result
    finally
        unpreserve_task(ct)
    end

    @throw_on_error(response, "table_open", IcebergException)

    return response.table
end

"""
    free_table(table::Table)

Free the memory associated with an Iceberg table.
"""
function free_table(table::Table)
    @ccall rust_lib.iceberg_table_free(table::Table)::Cvoid
end

end # module RustyIceberg
