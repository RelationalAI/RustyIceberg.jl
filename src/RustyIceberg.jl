module RustyIceberg

using Base.Libc.Libdl: dlext
using Base: @kwdef, @lock
using Libdl
using Arrow
using iceberg_rust_ffi_jll

export IcebergTable, IcebergScan, ArrowBatch, IcebergConfig
export IcebergTableIterator, IcebergTableIteratorState
export init_iceberg_runtime, read_iceberg_table
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

const _ICEBERG_STARTED = Ref(false)
const _INIT_LOCK::ReentrantLock = ReentrantLock()
_PANIC_HOOK::Function = default_panic_hook

struct InitException <: Exception
    msg::String
    return_code::Cint
end

Base.@ccallable function panic_hook_wrapper()::Cint
    global _PANIC_HOOK
    _PANIC_HOOK()
    return 0
end

# This is the callback that Rust calls to notify a Julia task of a completed operation.
Base.@ccallable function notify_result(event_ptr::Ptr{Nothing})::Cint
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
        panic_fn_ptr = @cfunction(panic_hook_wrapper, Cint, ())
        fn_ptr = @cfunction(notify_result, Cint, (Ptr{Nothing},))
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
        # Note: context cancellation not fully implemented in iceberg_rust_ffi yet
        ensure_wait(event)
        if response.error_message != C_NULL
            @ccall rust_lib.iceberg_destroy_cstring(response.error_message::Ptr{Cchar})::Cint
        end
        rethrow(e)
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

mutable struct IcebergBatchResponse
    result::Cint
    batch::Ptr{ArrowBatch}
    end_of_stream::Bool
    new_stream_ptr::Ptr{Cvoid}
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    IcebergBatchResponse() = new(-1, C_NULL, false, C_NULL, C_NULL, C_NULL)
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
    
    while true
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

        if result == -2  # CRESULT_BACKOFF
            sleep(0.01)
            continue
        end

        @throw_on_error(response, "table_open", TableOpenException)

        return response.table
    end
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
    
    while true
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

        if result == -2  # CRESULT_BACKOFF
            sleep(0.01)
            continue
        end

        @throw_on_error(response, "table_scan", ScanException)

        return response.scan
    end
end

"""
    iceberg_scan_wait_batch(scan::IcebergScan) -> Nothing

Wait for the next batch asynchronously and store it in the scan.
"""
function iceberg_scan_wait_batch(scan::IcebergScan)
    response = IcebergBatchResponse()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    
    while true
        preserve_task(ct)
        result = GC.@preserve response event try
            result = @ccall rust_lib.iceberg_scan_wait_batch_with_storage(
                scan::IcebergScan,
                response::Ref{IcebergBatchResponse},
                handle::Ptr{Cvoid}
            )::Cint

            wait_or_cancel(event, response)

            result
        finally
            unpreserve_task(ct)
        end

        if result == -2  # CRESULT_BACKOFF
            sleep(0.01)
            continue
        end

        @throw_on_error(response, "scan_wait_batch", BatchException)

        # Store the batch result in the scan
        result = @ccall rust_lib.iceberg_scan_store_batch_result(
            scan::IcebergScan,
            response::Ref{IcebergBatchResponse}
        )::Cint
        
        if result != 0
            error("Failed to store batch result")
        end

        return nothing
    end
end

"""
    iceberg_scan_next_batch(scan::IcebergScan) -> Tuple{Bool, Union{ArrowBatch, Nothing}}

Get the current batch from the scan (synchronous). Returns (end_of_stream, batch).
Call iceberg_scan_wait_batch first to wait for and store the batch.
"""
function iceberg_scan_next_batch(scan::IcebergScan)
    response = IcebergBatchResponse()
    
    result = @ccall rust_lib.iceberg_scan_next_batch(
        scan::IcebergScan,
        response::Ref{IcebergBatchResponse},
        C_NULL::Ptr{Cvoid}
    )::Cint

    @throw_on_error(response, "scan_next_batch", BatchException)

    return response.end_of_stream, response.batch
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
end

# Iterator state
mutable struct IcebergTableIteratorState
    table::IcebergTable
    scan::IcebergScan
    is_open::Bool
end

"""
    Base.iterate(iter::IcebergTableIterator, state=nothing)

Iterate over Arrow.Table objects from the Iceberg table.
"""
function Base.iterate(iter::IcebergTableIterator, state=nothing)
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

        state = IcebergTableIteratorState(table, scan, true)
    end

    # Wait for next batch asynchronously 
    iceberg_scan_wait_batch(state.scan)
    
    # Get the stored batch synchronously
    end_of_stream, batch_ptr = iceberg_scan_next_batch(state.scan)

    if end_of_stream
        # End of stream - cleanup and return nothing
        iceberg_scan_free(state.scan)
        iceberg_table_free(state.table)
        return nothing
    end

    if batch_ptr == C_NULL
        error("Received NULL batch")
    end

    # Convert ArrowBatch pointer to ArrowBatch struct
    batch = unsafe_load(batch_ptr)

    # Create IOBuffer from the serialized Arrow data
    io = IOBuffer(unsafe_wrap(Array, batch.data, batch.length))

    # Read Arrow data
    arrow_table = Arrow.Table(io)

    # Free the batch
    iceberg_arrow_batch_free(batch_ptr)

    return arrow_table, state
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
    read_iceberg_table(table_path::String, metadata_path::String; columns::Vector{String}=String[]) -> IcebergTableIterator

Read an Iceberg table and return an iterator over Arrow.Table objects.
"""
function read_iceberg_table(table_path::String, metadata_path::String; columns::Vector{String}=String[])
    return IcebergTableIterator(table_path, metadata_path, columns)
end


end # module RustyIceberg