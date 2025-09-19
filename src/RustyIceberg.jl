module RustyIceberg

using Base.Libc.Libdl: dlext
using Base: @kwdef, @lock
using Base.Threads: Atomic
using Libdl
using Arrow
using iceberg_rust_ffi_jll

export Table, Scan, ArrowBatch, StaticConfig, ArrowStream
export TableIterator, TableIteratorState
export init_runtime, read_table
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
        panic_fn_ptr = @cfunction(panic_hook_wrapper_iceberg, Cint, ())
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

# Opaque pointer types
const Table = Ptr{Cvoid}
const Scan = Ptr{Cvoid}
const ScanRef = Ref{Scan}
const ArrowStream = Ptr{Cvoid}

# Arrow batch structure
struct ArrowBatch
    data::Ptr{UInt8}
    length::Csize_t
    rust_ptr::Ptr{Cvoid}
end

# Response structures for async operations
mutable struct TableResponse
    result::Cint
    table::Table
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    TableResponse() = new(-1, C_NULL, C_NULL, C_NULL)
end

mutable struct Response
    result::Cint
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    Response() = new(-1, C_NULL, C_NULL)
end

mutable struct ArrowStreamResponse
    result::Cint
    stream::ArrowStream
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    ArrowStreamResponse() = new(-1, C_NULL, C_NULL, C_NULL)
end

mutable struct BatchResponse
    result::Cint
    batch::Ptr{ArrowBatch}
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    BatchResponse() = new(-1, C_NULL, C_NULL, C_NULL)
end

# Exception types
struct IcebergException <: Exception
    msg::String
    code::Union{Int,Nothing}
end

# High-level functions using the async API pattern from RustyObjectStore.jl

"""
    table_open(table_path::String, metadata_path::String)::IcebergTable

Open an Iceberg table from the given path and metadata file.
"""
function table_open(table_path::String, metadata_path::String)
    response = TableResponse()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)

    preserve_task(ct)
    result = GC.@preserve response event try
        result = @ccall rust_lib.iceberg_table_open(
            table_path::Cstring,
            metadata_path::Cstring,
            response::Ref{TableResponse},
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
    new_scan(table::Table) -> IcebergScan

Create a scan for the given table.
"""
function new_scan(table::Table)
    scan = @ccall rust_lib.iceberg_new_scan(table::Table)::Ptr{Cvoid}
    return Ref(scan)
end

"""
    select_columns!(scan::ScanRef, column_names::Vector{String})::Cint

Select specific columns for the scan.
"""
function select_columns!(scan::ScanRef, column_names::Vector{String})
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
    with_data_file_concurrency_limit!(scan::ScanRef, n::UInt)::Cint

Sets the data file concurrency level for the scan.
"""
function with_data_file_concurrency_limit!(scan::ScanRef, n::UInt)
    return @ccall rust_lib.iceberg_scan_with_data_file_concurrency(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint
end

"""
    with_manifest_entry_concurrency_limit!(scan::ScanRef, n::UInt)::Cint

Sets the manifest entry concurrency level for the scan.
"""
function with_manifest_entry_concurrency_limit!(scan::ScanRef, n::UInt)
    return @ccall rust_lib.iceberg_scan_with_manifest_entry_concurrency(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint
end

"""
    with_batch_size!(scan::ScanRef, n::UInt)::Cint

Sets the batch size for the scan.
"""
function with_batch_size!(scan::ScanRef, n::UInt)
    return @ccall rust_lib.iceberg_scan_with_batch_size(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}},
        n::Csize_t
    )::Cint
end

"""
    build!(scan::ScanRef)::Cint

Build the provided table scan object.
"""
function build!(scan::ScanRef)
    return _build!(convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan)))
end

function _build!(scan::Ptr{Ptr{Cvoid}})
    return @ccall rust_lib.iceberg_scan_build(scan::Ptr{Ptr{Cvoid}})::Cint
end

"""
    scan!(scan::ScanRef) -> Cint

Build the provided table scan object.
"""
function scan!(scan::ScanRef)
    result = build!(scan)
    if result != 0
        throw(IcebergException("Failed to build scan", results))
    end

    return arrow_stream(scan[])
end

"""
    arrow_stream(scan::Scan)::IcebergArrowStream

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
            scan::Scan,
            response::Ref{ArrowStreamResponse},
            handle::Ptr{Cvoid}
        )::Cint

        wait_or_cancel(event, response)

        result
    finally
        unpreserve_task(ct)
    end

    @throw_on_error(response, "iceberg_arrow_stream", BatchException)

    return response.stream
end

"""
    next_batch(scan::Scan)::Ptr{ArrowBatch}

Wait for the next batch from the initialized stream asynchronously and return it directly.
Returns C_NULL if end of stream is reached.
"""
function next_batch(stream::ArrowStream)
    response = BatchResponse()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)

    preserve_task(ct)
    result = GC.@preserve response event try
        result = @ccall rust_lib.iceberg_next_batch(
            stream::ArrowStream,
            response::Ref{BatchResponse},
            handle::Ptr{Cvoid}
        )::Cint

        wait_or_cancel(event, response)

        result
    finally
        unpreserve_task(ct)
    end

    @throw_on_error(response, "iceberg_next_batch", BatchException)

    # Return the batch pointer directly
    return response.batch
end

"""
    free_table(table::IcebergTable)

Free the memory associated with an Iceberg table.
"""
function free_table(table::Table)
    @ccall rust_lib.iceberg_table_free(table::Table)::Cvoid
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

"""
    free_scan!(scan::IcebergScanRef)

Free the memory associated with a scan.
"""
function free_scan!(scan::ScanRef)
    @ccall rust_lib.iceberg_scan_free(
        convert(Ptr{Ptr{Cvoid}}, pointer_from_objref(scan))::Ptr{Ptr{Cvoid}}
    )::Cvoid
end


# Iterator type for Arrow batches
struct TableIterator
    table_path::String
    metadata_path::String
    columns::Vector{String}
    batch_size::Union{UInt,Nothing}
    data_file_concurrency_limit::Union{UInt,Nothing}
    manifest_entry_concurrency_limit::Union{UInt,Nothing}
end

# Iterator state
mutable struct TableIteratorState
    table::Table
    scan::Ref{Ptr{Cvoid}}
    stream::ArrowStream
    is_open::Bool
    batch_ptr::Ptr{ArrowBatch}

    function TableIteratorState(table, scan, stream, is_open)
        state = new(table, scan, stream, is_open, C_NULL)
        # Ensure cleanup happens even if iterator is abandoned
        finalizer(_cleanup_iterator_state, state)
        return state
    end
end

function _cleanup_iterator_state(state::TableIteratorState)
    if state.is_open
        try
            # Only free batch if we know we have one pending to prevent double-free
            state.batch_ptr != C_NULL && free_batch(state.batch_ptr)
            @assert state.stream != C_NULL
            @assert state.scan != C_NULL
            @assert state.table != C_NULL
            free_stream(state.stream)
            free_scan!(state.scan)
            free_table(state.table)
        catch e
            # Log but don't throw in finalizer
            # TODO: should we keep this log or throw?
            @error "Error in IcebergTableIteratorState finalizer: $(e)"
        finally
            state.is_open = false
            state.batch_ptr = C_NULL
            state.stream = C_NULL
            state.scan = C_NULL
            state.table = C_NULL
        end
    end
end

"""
    Base.iterate(iter::IcebergTableIterator, state=nothing)

Iterate over `Arrow.Table` objects from the Iceberg table.
"""
function Base.iterate(iter::TableIterator, state=nothing)
    local arrow_table
    local should_cleanup_resources = false

    try
        if state === nothing
            # First iteration - ensure runtime is initialized
            if !iceberg_started()
                init_runtime()
            end

            # Open table
            table = table_open(iter.table_path, iter.metadata_path)

            # Create scan
            scan = new_scan(table)

            # Select columns if specified
            if !isempty(iter.columns)
                select_columns!(scan, iter.columns)
            end

            if !isnothing(iter.data_file_concurrency_limit)
                with_data_file_concurrency_limit!(scan, iter.data_file_concurrency_limit)
            end

            if !isnothing(iter.manifest_entry_concurrency_limit)
                with_manifest_entry_concurrency_limit!(scan, iter.manifest_entry_concurrency_limit)
            end

            if !isnothing(iter.batch_size)
                with_batch_size!(scan, iter.batch_size)
            end

            stream = scan!(scan)
            state = TableIteratorState(table, scan, stream, true)
        else
            # Clean up the batch from the previous iteration.
            if state.batch_ptr != C_NULL
                free_batch(state.batch_ptr)
                state.batch_ptr = C_NULL
            end
        end

        # Wait for next batch asynchronously
        state.batch_ptr = next_batch(state.stream)

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
            state.batch_ptr != C_NULL && free_batch(state.batch_ptr)
            free_stream(state.stream)
            free_scan!(state.scan)
            free_table(state.table)
            state.stream = C_NULL
            state.scan = C_NULL
            state.table = C_NULL
            # Mark as closed to prevent finalizer from double-freeing
            state.is_open = false
        end
    end
end

"""
    Base.eltype(::Type{IcebergTableIterator})

Return the element type of the iterator.
"""
Base.eltype(::Type{TableIterator}) = Arrow.Table

"""
    Base.IteratorSize(::Type{IcebergTableIterator})

Return the size trait of the iterator.
"""
Base.IteratorSize(::Type{TableIterator}) = Base.SizeUnknown()

# High-level Julia interface
"""
    read_table(table_path::String, metadata_path::String; columns::Vector{String}=String[]) -> IcebergTableIterator

Read an Iceberg table and return an iterator over Arrow.Table objects.
"""
function read_table(
    table_path::String,
    metadata_path::String;
    columns::Vector{String}=String[],
    batch_size::Union{UInt, Nothing}=nothing,
    data_file_concurrency_limit::Union{UInt, Nothing}=nothing,
    manifest_entry_concurrency_limit::Union{UInt, Nothing}=nothing
)
    return TableIterator(
        table_path,
        metadata_path,
        columns,
        batch_size,
        data_file_concurrency_limit,
        manifest_entry_concurrency_limit
    )
end

end # module RustyIceberg
