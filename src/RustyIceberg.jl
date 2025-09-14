module RustyIceberg

using Base.Libc.Libdl: dlext
using Base: @kwdef, @lock
using Base.Threads: Atomic
using Libdl
using Arrow
using iceberg_rust_ffi_jll

export IcebergTable, IcebergScan, ArrowBatch, IcebergConfig
export IcebergTableIterator, IcebergTableIteratorState
export init_iceberg_runtime, read_iceberg_table
export iceberg_scan_init_stream, iceberg_scan_next_batch
export IcebergException

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
struct IcebergConfig
    n_threads::Culonglong
end

function default_panic_hook()
    println("Rust thread panicked, exiting the process")
    exit(1)
end

const _ICEBERG_STARTED = Atomic{Bool}(false)
const _INIT_LOCK::ReentrantLock = ReentrantLock()
_PANIC_HOOK::Function = default_panic_hook

struct InitException <: Exception
    msg::String
    return_code::Cint
end

Base.@ccallable function panic_hook_wrapper_iceberg()::Cint
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
    init_iceberg_runtime()
    init_iceberg_runtime(config::IcebergConfig)
    init_iceberg_runtime(config::IcebergConfig; on_rust_panic::Function)

Initialize the Iceberg runtime.

This starts a `tokio` runtime for handling Iceberg requests.
It must be called before sending a request.
"""
function init_iceberg_runtime(
    config::IcebergConfig=IcebergConfig(0);
    on_rust_panic::Function=default_panic_hook
)
    global _PANIC_HOOK
    @lock _INIT_LOCK begin
        if _ICEBERG_STARTED[]
            return nothing
        end
        _PANIC_HOOK = on_rust_panic
        panic_fn_ptr = @cfunction(panic_hook_wrapper_iceberg, Cint, ())
        fn_ptr = @cfunction(notify_result_iceberg, Cint, (Ptr{Nothing},))
        res = @ccall rust_lib.iceberg_init_runtime(config::IcebergConfig, panic_fn_ptr::Ptr{Nothing}, fn_ptr::Ptr{Nothing})::Cint
        if res != 0
            throw(InitException("Failed to initialize Iceberg runtime.", res))
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

# Opaque pointer types
const IcebergTable = Ptr{Cvoid}
const IcebergScan = Ptr{Cvoid}

# Arrow batch structure
struct ArrowBatch
    data::Ptr{UInt8}
    length::Csize_t
    rust_ptr::Ptr{Cvoid}
end

# Response structures for async operations
mutable struct IcebergTableResponse
    result::Cint
    table::IcebergTable
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    IcebergTableResponse() = new(-1, C_NULL, C_NULL, C_NULL)
end

mutable struct IcebergScanResponse
    result::Cint
    scan::IcebergScan
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    IcebergScanResponse() = new(-1, C_NULL, C_NULL, C_NULL)
end


mutable struct IcebergResponse
    result::Cint
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    IcebergResponse() = new(-1, C_NULL, C_NULL)
end

mutable struct IcebergBatchResponse
    result::Cint
    batch::Ptr{ArrowBatch}
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    IcebergBatchResponse() = new(-1, C_NULL, C_NULL, C_NULL)
end

# Exception types
abstract type IcebergException <: Exception end

struct TableOpenException <: IcebergException
    msg::String
end

struct ScanException <: IcebergException
    msg::String
end

struct BatchException <: IcebergException
    msg::String
end

# High-level functions using the async API pattern from RustyObjectStore.jl

"""
    iceberg_table_open(table_path::String, metadata_path::String) -> IcebergTable

Open an Iceberg table from the given path and metadata file.
"""
function iceberg_table_open(table_path::String, metadata_path::String)
    response = IcebergTableResponse()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)

    preserve_task(ct)
    result = GC.@preserve response event try
        result = @ccall rust_lib.iceberg_table_open(
            table_path::Cstring,
            metadata_path::Cstring,
            response::Ref{IcebergTableResponse},
            handle::Ptr{Cvoid}
        )::Cint

        wait_or_cancel(event, response)

        result
    finally
        unpreserve_task(ct)
    end

    @throw_on_error(response, "table_open", TableOpenException)

    return response.table
end

"""
    iceberg_table_scan(table::IcebergTable) -> IcebergScan

Create a scan for the given table.
"""
function iceberg_table_scan(table::IcebergTable)
    response = IcebergScanResponse()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)

    preserve_task(ct)
    result = GC.@preserve response event try
        result = @ccall rust_lib.iceberg_table_scan(
            table::IcebergTable,
            response::Ref{IcebergScanResponse},
            handle::Ptr{Cvoid}
        )::Cint

        wait_or_cancel(event, response)

        result
    finally
        unpreserve_task(ct)
    end

    @throw_on_error(response, "table_scan", ScanException)

    return response.scan
end

"""
    iceberg_scan_init_stream(scan::IcebergScan) -> Nothing

Initialize the stream for the scan asynchronously.
"""
function iceberg_scan_init_stream(scan::IcebergScan, batch_size::Csize_t, concurrency_limit::Csize_t)
    response = IcebergResponse()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)

    preserve_task(ct)
    result = GC.@preserve response event try
        result = @ccall rust_lib.iceberg_scan_init_stream(
            scan::IcebergScan,
            batch_size::Csize_t,
            concurrency_limit::Csize_t,
            response::Ref{IcebergResponse},
            handle::Ptr{Cvoid}
        )::Cint

        wait_or_cancel(event, response)

        result
    finally
        unpreserve_task(ct)
    end

    @throw_on_error(response, "scan_init_stream", BatchException)

    # Stream is automatically stored in the scan by the Rust function
    return nothing
end

"""
    _iceberg_scan_next_batch(scan::IcebergScan) -> Ptr{ArrowBatch}

Wait for the next batch from the initialized stream asynchronously and return it directly.
Returns C_NULL if end of stream is reached.
"""
function iceberg_scan_next_batch(scan::IcebergScan)
    response = IcebergBatchResponse()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)

    preserve_task(ct)
    result = GC.@preserve response event try
        result = @ccall rust_lib.iceberg_scan_next_batch(
            scan::IcebergScan,
            response::Ref{IcebergBatchResponse},
            handle::Ptr{Cvoid}
        )::Cint

        wait_or_cancel(event, response)

        result
    finally
        unpreserve_task(ct)
    end

    @throw_on_error(response, "scan_wait_batch", BatchException)

    # Return the batch pointer directly
    return response.batch
end

"""
    iceberg_scan_select_columns(scan::IcebergScan, column_names::Vector{String}) -> Nothing

Select specific columns for the scan.
"""
function iceberg_scan_select_columns(scan::IcebergScan, column_names::Vector{String})
    # Convert String vector to Cstring array
    c_strings = [pointer(col) for col in column_names]
    result = @ccall rust_lib.iceberg_scan_select_columns(
        scan::IcebergScan,
        pointer(c_strings)::Ptr{Cstring},
        length(column_names)::Csize_t
    )::Cint

    if result != 0
        error("Failed to select columns")
    end
    return nothing
end

"""
    iceberg_table_free(table::IcebergTable)

Free the memory associated with an Iceberg table.
"""
function iceberg_table_free(table::IcebergTable)
    @ccall rust_lib.iceberg_table_free(table::IcebergTable)::Cvoid
end

"""
    iceberg_scan_free(scan::IcebergScan)

Free the memory associated with a scan.
"""
function iceberg_scan_free(scan::IcebergScan)
    @ccall rust_lib.iceberg_scan_free(scan::IcebergScan)::Cvoid
end

"""
    iceberg_arrow_batch_free(batch::Ptr{ArrowBatch})

Free the memory associated with an Arrow batch.
"""
function iceberg_arrow_batch_free(batch::Ptr{ArrowBatch})
    @ccall rust_lib.iceberg_arrow_batch_free(batch::Ptr{ArrowBatch})::Cvoid
end

# Iterator type for Arrow batches
struct IcebergTableIterator
    table_path::String
    metadata_path::String
    columns::Vector{String}
    batch_size::UInt
    concurrency_limit::UInt
end

# Iterator state
mutable struct IcebergTableIteratorState
    table::IcebergTable
    scan::IcebergScan
    is_open::Bool
    batch_ptr::Ptr{ArrowBatch}

    function IcebergTableIteratorState(table, scan, is_open)
        state = new(table, scan, is_open, C_NULL)
        # Ensure cleanup happens even if iterator is abandoned
        finalizer(cleanup_iceberg_state, state)
        return state
    end
end

"""
    cleanup_iceberg_state(state::IcebergTableIteratorState)

Cleanup function for IcebergTableIteratorState finalizer.
Ensures Rust resources are freed even if iterator is abandoned.
"""
function cleanup_iceberg_state(state::IcebergTableIteratorState)
    if state.is_open
        try
            # Only free batch if we know we have one pending to prevent double-free
            if state.batch_ptr != C_NULL
                iceberg_arrow_batch_free(state.batch_ptr)
            end
            iceberg_scan_free(state.scan)
            iceberg_table_free(state.table)
        catch e
            # Log but don't throw in finalizer
            @error "Error in IcebergTableIteratorState finalizer: $e"
        finally
            state.is_open = false
            state.batch_ptr = C_NULL
        end
    end
end

"""
    Base.iterate(iter::IcebergTableIterator, state=nothing)

Iterate over Arrow.Table objects from the Iceberg table.
"""
function Base.iterate(iter::IcebergTableIterator, state=nothing)
    local arrow_table
    local should_cleanup_resources = false

    try
        if state === nothing
            # First iteration - ensure runtime is initialized
            if !_ICEBERG_STARTED[]
                init_iceberg_runtime()
            end

            # Open table
            table = iceberg_table_open(iter.table_path, iter.metadata_path)

            # Create scan
            scan = iceberg_table_scan(table)

            # Select columns if specified
            if !isempty(iter.columns)
                iceberg_scan_select_columns(scan, iter.columns)
            end

            iceberg_scan_init_stream(scan, Csize_t(iter.batch_size), Csize_t(iter.concurrency_limit))

            state = IcebergTableIteratorState(table, scan, true)
        elseif state.batch_ptr != C_NULL
            # Always clean up batch if we have one
            iceberg_arrow_batch_free(state.batch_ptr)
        end

        # Wait for next batch asynchronously
        state.batch_ptr = iceberg_scan_next_batch(state.scan)

        if state.batch_ptr == C_NULL
            # End of stream - mark for cleanup and return nothing
            should_cleanup_resources = true
            return nothing
        end

        # Convert ArrowBatch pointer to ArrowBatch struct
        batch = unsafe_load(state.batch_ptr)

        # Read Arrow data
        arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))

        return arrow_table, state

    catch e
        # On exception, mark resources for cleanup before rethrowing
        should_cleanup_resources = true
        rethrow(e)
    finally
        # Clean up scan and table resources only if needed (end of stream or exception)
        if should_cleanup_resources && state !== nothing && state.is_open
            if state.batch_ptr != C_NULL
                # Always clean up batch if we have one
                iceberg_arrow_batch_free(state.batch_ptr)
            end
            iceberg_scan_free(state.scan)
            iceberg_table_free(state.table)
            # Mark as closed to prevent finalizer from double-freeing
            state.is_open = false
        end
    end
end

"""
    Base.eltype(::Type{IcebergTableIterator})

Return the element type of the iterator.
"""
Base.eltype(::Type{IcebergTableIterator}) = Arrow.Table

"""
    Base.IteratorSize(::Type{IcebergTableIterator})

Return the size trait of the iterator.
"""
Base.IteratorSize(::Type{IcebergTableIterator}) = Base.SizeUnknown()

# High-level Julia interface
"""
    read_iceberg_table(table_path::String, metadata_path::String; batch_size::UInt=0, concurrency_limit::UInt=0, columns::Vector{String}=String[]) -> IcebergTableIterator

Read an Iceberg table and return an iterator over Arrow.Table objects.
"""
function read_iceberg_table(
    table_path::String, metadata_path::String;
    columns::Vector{String}=String[], batch_size::UInt=UInt(0), concurrency_limit::UInt=UInt(0),
)
    return IcebergTableIterator(table_path, metadata_path, columns, batch_size, concurrency_limit)
end

end # module RustyIceberg
