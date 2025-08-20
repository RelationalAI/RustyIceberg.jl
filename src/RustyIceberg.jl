module RustyIceberg

using Libdl
using Arrow
using iceberg_rust_ffi_jll

export IcebergTable, IcebergScan, ArrowBatch, IcebergResult
export IcebergTableIterator, IcebergTableIteratorState
export iceberg_table_open, iceberg_table_free, iceberg_table_scan
export iceberg_scan_select_columns, iceberg_scan_free, iceberg_scan_next_batch
export iceberg_arrow_batch_free, iceberg_error_message
export load_iceberg_library, unload_iceberg_library, read_iceberg_table

# Constants
const ICEBERG_OK = 0
const ICEBERG_ERROR = -1
const ICEBERG_NULL_POINTER = -2
const ICEBERG_IO_ERROR = -3
const ICEBERG_INVALID_TABLE = -4
const ICEBERG_END_OF_STREAM = -5

# Opaque pointer types
const IcebergTable = Ptr{Cvoid}
const IcebergScan = Ptr{Cvoid}

# Arrow batch structure
struct ArrowBatch
    data::Ptr{UInt8}
    length::Csize_t
    rust_ptr::Ptr{Cvoid}
end

# Result type
const IcebergResult = Cint

# Global variables for dynamic loading
const lib_handle = Ref{Ptr{Cvoid}}(C_NULL)
const function_pointers = Dict{Symbol, Ptr{Cvoid}}()

# Function pointer types
const iceberg_table_open_func_t = Ptr{Cvoid}
const iceberg_table_free_func_t = Ptr{Cvoid}
const iceberg_table_scan_func_t = Ptr{Cvoid}
const iceberg_scan_select_columns_func_t = Ptr{Cvoid}
const iceberg_scan_free_func_t = Ptr{Cvoid}
const iceberg_scan_next_batch_func_t = Ptr{Cvoid}
const iceberg_arrow_batch_free_func_t = Ptr{Cvoid}
const iceberg_error_message_func_t = Ptr{Cvoid}

"""
    load_iceberg_library(lib_path::String=iceberg_rust_ffi_jll.libiceberg_rust_ffi)

Load the Iceberg C API library and resolve all function symbols.
"""
function load_iceberg_library(lib_path::String=iceberg_rust_ffi_jll.libiceberg_rust_ffi)
    println("Loading Iceberg C API library from: $lib_path")

    # Try to open the dynamic library
    lib_handle[] = dlopen(lib_path, RTLD_LAZY)
    if lib_handle[] == C_NULL
        error("Failed to load library: $lib_path")
    end

    println("✅ Library loaded successfully")

    # Function names to resolve
    functions = [
        :iceberg_table_open,
        :iceberg_table_free,
        :iceberg_table_scan,
        :iceberg_scan_select_columns,
        :iceberg_scan_free,
        :iceberg_scan_next_batch,
        :iceberg_arrow_batch_free,
        :iceberg_error_message
    ]

    # Resolve function symbols
    for func_name in functions
        ptr = dlsym(lib_handle[], String(func_name))
        if ptr == C_NULL
            error("Failed to resolve $func_name")
        end
        function_pointers[func_name] = ptr
    end

    println("✅ All function symbols resolved successfully")
    return true
end

"""
    unload_iceberg_library()

Unload the Iceberg C API library.
"""
function unload_iceberg_library()
    if lib_handle[] != C_NULL
        dlclose(lib_handle[])
        lib_handle[] = C_NULL
        empty!(function_pointers)
        println("✅ Library unloaded")
    end
end

# Wrapper functions that call the C API
"""
    iceberg_table_open(table_path::String, metadata_path::String) -> Tuple{IcebergResult, IcebergTable}

Open an Iceberg table from the given path and metadata file.
"""
function iceberg_table_open(table_path::String, metadata_path::String)
    table_ref = Ref{IcebergTable}(C_NULL)
    result = ccall(
        function_pointers[:iceberg_table_open],
        IcebergResult,
        (Cstring, Cstring, Ref{IcebergTable}),
        table_path, metadata_path, table_ref
    )
    return result, table_ref[]
end

"""
    iceberg_table_free(table::IcebergTable)

Free the memory associated with an Iceberg table.
"""
function iceberg_table_free(table::IcebergTable)
    ccall(
        function_pointers[:iceberg_table_free],
        Cvoid,
        (IcebergTable,),
        table
    )
end

"""
    iceberg_table_scan(table::IcebergTable) -> Tuple{IcebergResult, IcebergScan}

Create a scan for the given table.
"""
function iceberg_table_scan(table::IcebergTable)
    scan_ref = Ref{IcebergScan}(C_NULL)
    result = ccall(
        function_pointers[:iceberg_table_scan],
        IcebergResult,
        (IcebergTable, Ref{IcebergScan}),
        table, scan_ref
    )
    return result, scan_ref[]
end

"""
    iceberg_scan_select_columns(scan::IcebergScan, column_names::Vector{String}) -> IcebergResult

Select specific columns for the scan.
"""
function iceberg_scan_select_columns(scan::IcebergScan, column_names::Vector{String})
    # Convert String vector to Cstring array
    c_strings = [pointer(col) for col in column_names]
    result = ccall(
        function_pointers[:iceberg_scan_select_columns],
        IcebergResult,
        (IcebergScan, Ptr{Cstring}, Csize_t),
        scan, pointer(c_strings), length(column_names)
    )
    return result
end

"""
    iceberg_scan_free(scan::IcebergScan)

Free the memory associated with a scan.
"""
function iceberg_scan_free(scan::IcebergScan)
    ccall(
        function_pointers[:iceberg_scan_free],
        Cvoid,
        (IcebergScan,),
        scan
    )
end

"""
    iceberg_scan_next_batch(scan::IcebergScan) -> Tuple{IcebergResult, ArrowBatch}

Get the next Arrow batch from the scan.
"""
function iceberg_scan_next_batch(scan::IcebergScan)
    batch_ref = Ref{Ptr{ArrowBatch}}(C_NULL)
    result = ccall(
        function_pointers[:iceberg_scan_next_batch],
        IcebergResult,
        (IcebergScan, Ref{Ptr{ArrowBatch}}),
        scan, batch_ref
    )
    return result, batch_ref[]
end

"""
    iceberg_arrow_batch_free(batch::Ptr{ArrowBatch})

Free the memory associated with an Arrow batch.
"""
function iceberg_arrow_batch_free(batch::Ptr{ArrowBatch})
    ccall(
        function_pointers[:iceberg_arrow_batch_free],
        Cvoid,
        (Ptr{ArrowBatch},),
        batch
    )
end

"""
    iceberg_error_message() -> String

Get the last error message from the C API.
"""
function iceberg_error_message()
    msg_ptr = ccall(
        function_pointers[:iceberg_error_message],
        Cstring,
        ()
    )
    return msg_ptr == C_NULL ? "" : unsafe_string(msg_ptr)
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
        # First iteration - open table and scan
        if lib_handle[] == C_NULL
            load_iceberg_library()
        end

        # Open table
        result, table = iceberg_table_open(iter.table_path, iter.metadata_path)
        if result != ICEBERG_OK
            error("Failed to open table: $(iceberg_error_message())")
        end

        # Create scan
        result, scan = iceberg_table_scan(table)
        if result != ICEBERG_OK
            # TODO: Is everything exception free? what if we get an exception in Julia
            # between FFI calls to Rust, or between iterations? How do we deallocate objects then?
            iceberg_table_free(table)
            error("Failed to create scan: $(iceberg_error_message())")
        end

        # Select columns if specified
        if !isempty(iter.columns)
            result = iceberg_scan_select_columns(scan, iter.columns)
            if result != ICEBERG_OK
                iceberg_scan_free(scan)
                iceberg_table_free(table)
                error("Failed to select columns: $(iceberg_error_message())")
            end
        end

        state = IcebergTableIteratorState(table, scan, true)
    end

    # Get next batch
    result, batch_ptr = iceberg_scan_next_batch(state.scan)

    if result == ICEBERG_END_OF_STREAM
        # End of stream - cleanup and return nothing
        iceberg_scan_free(state.scan)
        iceberg_table_free(state.table)
        return nothing
    elseif result != ICEBERG_OK
        # Error - cleanup and throw
        iceberg_scan_free(state.scan)
        iceberg_table_free(state.table)
        error("Failed to get next batch: $(iceberg_error_message())")
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

# Cleanup on module unload
function __init__()
    atexit(unload_iceberg_library)
end

end # module Iceberg
