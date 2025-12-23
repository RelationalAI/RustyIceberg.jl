"""
Catalog support for RustyIceberg.jl

This module provides Julia wrappers for the REST catalog FFI functions.
"""

# Token callback function that can be exported as C-callable with token caching support
# This is a static function (no closure) that takes auth_fn and extracts the authenticator
Base.@ccallable function iceberg_token_callback_impl(
    auth_fn::Ptr{Cvoid},
    token_data_ptr::Ptr{Ptr{Cchar}},
    token_len_ptr::Ptr{Csize_t},
    reuse_token_ptr::Ptr{Cint}
)::Cint
    try
        # Check that output pointers are valid
        if token_data_ptr == C_NULL || token_len_ptr == C_NULL || reuse_token_ptr == C_NULL
            return Cint(1)  # Error: invalid output pointers
        end

        # Extract authenticator function from auth_fn pointer
        auth_ref = unsafe_pointer_to_objref(auth_fn)
        auth_fn_call = auth_ref[]

        # Call the authenticator to get the token
        # The authenticator should return either:
        # - A String with the token to use and cache
        # - nothing to signal reuse of the previously cached token
        token_result = auth_fn_call()

        # Handle nothing (signal to reuse cached token)
        if token_result === nothing
            unsafe_store!(reuse_token_ptr, Cint(1))
            return Cint(0)
        end

        # Ensure we got a String
        token_str = token_result::String

        # Signal that this is a new token (not reusing)
        unsafe_store!(reuse_token_ptr, Cint(0))

        # Get token bytes and write data pointer and length to output parameters
        # The data will be kept alive by the token_str variable until the callback returns
        token_bytes = Vector{UInt8}(token_str)
        token_len = length(token_bytes)
        token_data = pointer(token_bytes)

        # Write the pointer and length to the output parameters
        unsafe_store!(token_data_ptr, token_data)
        unsafe_store!(token_len_ptr, Csize_t(token_len))

        return Cint(0)
    catch
        # Return error code on exception
        return Cint(1)
    end
end

"""
    Catalog

Mutable struct holding an Iceberg catalog handle from the Rust FFI layer.

The struct stores both the raw catalog pointer and optionally an authenticator function,
ensuring the authenticator stays alive while the catalog is in use.

Create a catalog using `catalog_create_rest` and free it with `free_catalog` when done.
"""
mutable struct Catalog
    ptr::Ptr{Cvoid}
    authenticator::Union{Nothing, Ref}
end

# Constructor for simple catalogs without authenticator
Catalog(ptr::Ptr{Cvoid}) = Catalog(ptr, nothing)

# Support conversion to Ptr for FFI calls
Base.unsafe_convert(::Type{Ptr{Cvoid}}, catalog::Catalog) = catalog.ptr

"""
    CatalogResponse

Response structure for catalog creation operations.

# Fields
- `result::Cint`: Result code from the operation (0 for success)
- `catalog::Ptr{Cvoid}`: The created catalog handle (raw pointer)
- `error_message::Ptr{Cchar}`: Error message string if operation failed
- `context::Ptr{Cvoid}`: Context pointer for operation cancellation
"""
mutable struct CatalogResponse
    result::Cint
    catalog::Ptr{Cvoid}
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    CatalogResponse() = new(-1, C_NULL, C_NULL, C_NULL)
end

"""
    StringListResponse

Response structure for string list operations (e.g., table names).

# Fields
- `result::Cint`: Result code from the operation (0 for success)
- `items::Ptr{Ptr{Cchar}}`: Pointer to array of string pointers
- `count::Csize_t`: Number of items in the array
- `error_message::Ptr{Cchar}`: Error message string if operation failed
- `context::Ptr{Cvoid}`: Context pointer for operation cancellation
"""
mutable struct StringListResponse
    result::Cint
    items::Ptr{Ptr{Cchar}}
    count::Csize_t
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    StringListResponse() = new(-1, C_NULL, 0, C_NULL, C_NULL)
end

"""
    BoolResponse

Response structure for boolean operations (e.g., table_exists).

# Fields
- `result::Cint`: Result code from the operation (0 for success)
- `value::Bool`: The boolean result
- `error_message::Ptr{Cchar}`: Error message string if operation failed
- `context::Ptr{Cvoid}`: Context pointer for operation cancellation
"""
mutable struct BoolResponse
    result::Cint
    value::Bool
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    BoolResponse() = new(-1, false, C_NULL, C_NULL)
end

"""
    NestedStringListResponse

Response structure for nested string list operations (e.g., namespace lists).

# Fields
- `result::Cint`: Result code from the operation (0 for success)
- `outer_items::Ptr{Ptr{Ptr{Cchar}}}`: Pointer to array of string array pointers
- `outer_count::Csize_t`: Number of outer items
- `inner_counts::Ptr{Csize_t}`: Pointer to array of inner counts
- `error_message::Ptr{Cchar}`: Error message string if operation failed
- `context::Ptr{Cvoid}`: Context pointer for operation cancellation
"""
mutable struct NestedStringListResponse
    result::Cint
    outer_items::Ptr{Ptr{Ptr{Cchar}}}
    outer_count::Csize_t
    inner_counts::Ptr{Csize_t}
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    NestedStringListResponse() = new(-1, C_NULL, 0, C_NULL, C_NULL, C_NULL)
end

"""
    catalog_create_rest(uri::String; properties::Dict{String,String}=Dict{String,String}())::Catalog

Create a REST catalog connection.

# Arguments
- `uri::String`: URI of the Iceberg REST catalog server (e.g., "http://localhost:8181")
- `properties::Dict{String,String}`: Optional key-value properties for catalog configuration.
  By default (empty dict), no additional properties are passed.

# Returns
- A `Catalog` handle for use in other catalog operations

# Example
```julia
catalog = catalog_create_rest("http://polaris:8181")
```
"""
function catalog_create_rest(uri::String; properties::Dict{String,String}=Dict{String,String}())
    # Create an empty catalog (no authenticator)
    catalog_ptr = @ccall rust_lib.iceberg_catalog_init()::Ptr{Cvoid}
    if catalog_ptr == C_NULL
        throw(IcebergException("Failed to create empty catalog"))
    end

    # Initialize the catalog with REST connection
    # Convert properties dict to array of PropertyEntry structs
    property_entries = [PropertyEntry(pointer(k), pointer(v)) for (k, v) in properties]
    properties_len = length(property_entries)

    response = CatalogResponse()

    async_ccall(response, property_entries, properties) do handle
        @ccall rust_lib.iceberg_rest_catalog_create(
            catalog_ptr::Ptr{Cvoid},
            uri::Cstring,
            (properties_len > 0 ? pointer(property_entries) : C_NULL)::Ptr{PropertyEntry},
            properties_len::Csize_t,
            response::Ref{CatalogResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "catalog_create_rest", IcebergException)

    return Catalog(response.catalog, nothing)
end

"""
    catalog_create_rest(authenticator::FunctionWrapper{Union{String,Nothing},Tuple{}}, uri::String; properties::Dict{String,String}=Dict{String,String}())::Catalog

Create a REST catalog connection with custom token authentication and token caching support.

# Arguments
- `authenticator::FunctionWrapper{Union{String,Nothing},Tuple{}}`: A callable that takes no arguments and returns either:
  - A `String` containing the token: Rust will cache it and use it for authentication
  - `nothing`: Signal to reuse the previously cached token (avoids malloc, memory copy, and free operations)

  **Token Caching Optimization**: Returning `nothing` allows efficient token reuse without unnecessary copying.

- `uri::String`: URI of the Iceberg REST catalog server (e.g., "http://localhost:8181")
- `properties::Dict{String,String}`: Optional key-value properties for catalog configuration.
  By default (empty dict), no additional properties are passed.

# Returns
- A `Catalog` handle for use in other catalog operations

# Example with Token Caching
```julia
using FunctionWrappers: FunctionWrapper

function get_token()
    # Check if we need a new token, or if we can reuse the cached one
    if needs_refresh()
        return fetch_new_token()  # Returns String with token
    else
        return nothing  # Signal to reuse cached token (efficient!)
    end
end

catalog = catalog_create_rest(FunctionWrapper{Union{String,Nothing},Tuple{}}(get_token), "http://polaris:8181")
```
"""
function catalog_create_rest(authenticator::FunctionWrapper{Union{String,Nothing},Tuple{}}, uri::String; properties::Dict{String,String}=Dict{String,String}())
    # Step 1: Create an empty catalog
    catalog_ptr = @ccall rust_lib.iceberg_catalog_init()::Ptr{Cvoid}
    if catalog_ptr == C_NULL
        throw(IcebergException("Failed to create empty catalog"))
    end

    # Step 2: Wrap the authenticator in a Ref for stable memory address
    authenticator_ref = Ref(authenticator)

    # Step 3: Create C callback using the static token_callback_impl function
    c_callback = @cfunction(iceberg_token_callback_impl, Cint, (Ptr{Cvoid}, Ptr{Ptr{Cchar}}, Ptr{Csize_t}, Ptr{Cint}))

    # Step 4: Get auth_fn pointer to pass to the authenticator
    auth_fn = pointer_from_objref(authenticator_ref)

    # Step 5: Set the authenticator on the empty catalog BEFORE initializing REST
    result = @ccall rust_lib.iceberg_catalog_set_token_authenticator(
        catalog_ptr::Ptr{Cvoid},
        c_callback::Ptr{Cvoid},
        auth_fn::Ptr{Cvoid}
    )::Cint

    if result != 0
        throw(IcebergException("Failed to set token authenticator"))
    end

    # Step 6: Initialize the catalog with REST connection
    # Convert properties dict to array of PropertyEntry structs
    property_entries = [PropertyEntry(pointer(k), pointer(v)) for (k, v) in properties]
    properties_len = length(property_entries)

    response = CatalogResponse()

    async_ccall(response, property_entries, properties) do handle
        @ccall rust_lib.iceberg_rest_catalog_create(
            catalog_ptr::Ptr{Cvoid},
            uri::Cstring,
            (properties_len > 0 ? pointer(property_entries) : C_NULL)::Ptr{PropertyEntry},
            properties_len::Csize_t,
            response::Ref{CatalogResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "catalog_create_rest", IcebergException)

    # Create a Catalog struct that holds the catalog pointer and keeps authenticator alive
    catalog = Catalog(response.catalog, authenticator_ref)
    return catalog
end

"""
    free_catalog(catalog::Catalog)

Free the memory associated with a catalog.
"""
function free_catalog(catalog::Catalog)
    @ccall rust_lib.iceberg_catalog_free(catalog.ptr::Ptr{Cvoid})::Cvoid
end

"""
    load_table(catalog::Catalog, namespace::Vector{String}, table_name::String)::Table

Load a table from a catalog by namespace and name.

# Arguments
- `catalog::Catalog`: The catalog handle
- `namespace::Vector{String}`: Namespace parts (e.g., ["warehouse", "orders"])
- `table_name::String`: The table name

# Returns
- A `Table` handle for use in scan operations

# Example
```julia
table = load_table(catalog, ["warehouse", "orders"], "customers")
```
"""
function load_table(catalog::Catalog, namespace::Vector{String}, table_name::String)
    response = TableResponse()

    # Convert namespace to array of C strings
    namespace_ptrs = [pointer(part) for part in namespace]
    namespace_len = length(namespace)

    async_ccall(response, namespace, namespace_ptrs) do handle
        @ccall rust_lib.iceberg_catalog_load_table(
            catalog.ptr::Ptr{Cvoid},
            (namespace_len > 0 ? pointer(namespace_ptrs) : C_NULL)::Ptr{Ptr{Cchar}},
            namespace_len::Csize_t,
            table_name::Cstring,
            response::Ref{TableResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "catalog_load_table", IcebergException)

    return response.table
end

"""
    list_tables(catalog::Catalog, namespace::Vector{String})::Vector{String}

List tables in a namespace.

# Arguments
- `catalog::Catalog`: The catalog handle
- `namespace::Vector{String}`: Namespace parts

# Returns
- A vector of table names

# Example
```julia
tables = list_tables(catalog, ["warehouse", "orders"])
```
"""
function list_tables(catalog::Catalog, namespace::Vector{String})
    response = StringListResponse()

    # Convert namespace to array of C strings
    namespace_ptrs = [pointer(part) for part in namespace]
    namespace_len = length(namespace)

    async_ccall(response, namespace, namespace_ptrs) do handle
        @ccall rust_lib.iceberg_catalog_list_tables(
            catalog.ptr::Ptr{Cvoid},
            (namespace_len > 0 ? pointer(namespace_ptrs) : C_NULL)::Ptr{Ptr{Cchar}},
            namespace_len::Csize_t,
            response::Ref{StringListResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "list_tables", IcebergException)

    # Convert C string array to Julia strings
    tables = String[]
    if response.items != C_NULL && response.count > 0
        # response.items is a *mut Vec<*mut c_char>
        # Vec layout: [len: usize, data: *mut *mut c_char, capacity: usize]
        # Data pointer is at offset 8 (index 2 in 1-based Julia indexing)
        items_data_ptr_as_u64 = unsafe_load(Ptr{UInt64}(response.items), 2)
        items_data = reinterpret(Ptr{Ptr{Cchar}}, items_data_ptr_as_u64)

        for i in 1:response.count
            str_ptr = unsafe_load(items_data, i)
            if str_ptr != C_NULL
                push!(tables, unsafe_string(str_ptr))
            end
        end
    end

    return tables
end

"""
    list_namespaces(catalog::Catalog, parent::Vector{String}=String[])::Vector{Vector{String}}

List namespaces.

# Arguments
- `catalog::Catalog`: The catalog handle
- `parent::Vector{String}`: Parent namespace parts (optional, defaults to root)

# Returns
- A vector of namespace paths (each namespace is a vector of strings)

# Example
```julia
namespaces = list_namespaces(catalog)
all_namespaces = list_namespaces(catalog, ["warehouse"])
```
"""
function list_namespaces(catalog::Catalog, parent::Vector{String}=String[])
    response = NestedStringListResponse()

    # Convert parent to array of C strings
    parent_ptrs = [pointer(part) for part in parent]
    parent_len = length(parent)

    async_ccall(response, parent, parent_ptrs) do handle
        @ccall rust_lib.iceberg_catalog_list_namespaces(
            catalog.ptr::Ptr{Cvoid},
            (parent_len > 0 ? pointer(parent_ptrs) : C_NULL)::Ptr{Ptr{Cchar}},
            parent_len::Csize_t,
            response::Ref{NestedStringListResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "list_namespaces", IcebergException)

    # Parse nested C string array using helper function
    return _parse_nested_c_string_list(response.outer_items, response.outer_count, response.inner_counts)
end

"""
    table_exists(catalog::Catalog, namespace::Vector{String}, table_name::String)::Bool

Check if a table exists in a catalog.

# Arguments
- `catalog::Catalog`: The catalog handle
- `namespace::Vector{String}`: Namespace parts
- `table_name::String`: The table name

# Returns
- `true` if the table exists, `false` otherwise

# Example
```julia
exists = table_exists(catalog, ["warehouse", "orders"], "customers")
```
"""
function table_exists(catalog::Catalog, namespace::Vector{String}, table_name::String)
    response = BoolResponse()

    # Convert namespace to array of C strings
    namespace_ptrs = [pointer(part) for part in namespace]
    namespace_len = length(namespace)

    async_ccall(response, namespace, namespace_ptrs) do handle
        @ccall rust_lib.iceberg_catalog_table_exists(
            catalog.ptr::Ptr{Cvoid},
            (namespace_len > 0 ? pointer(namespace_ptrs) : C_NULL)::Ptr{Ptr{Cchar}},
            namespace_len::Csize_t,
            table_name::Cstring,
            response::Ref{BoolResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "table_exists", IcebergException)

    return response.value
end

"""
    _parse_nested_c_string_list(
        outer_items::Ptr{Ptr{Ptr{Cchar}}},
        outer_count::Csize_t,
        inner_counts::Ptr{Csize_t}
    )::Vector{Vector{String}}

Helper function to parse a nested C string array returned by FFI functions.

This converts a Rust-allocated nested string list (outer array of inner string arrays)
into a Julia Vector{Vector{String}}.

# Arguments
- `outer_items`: Pointer to array of pointers to string arrays
- `outer_count`: Number of outer items
- `inner_counts`: Pointer to array of inner counts for each outer item

# Returns
A Vector{Vector{String}} representing the parsed nested list
"""
function _parse_nested_c_string_list(
    outer_items::Ptr{Ptr{Ptr{Cchar}}},
    outer_count::Csize_t,
    inner_counts::Ptr{Csize_t}
)
    result = Vector{String}[]

    if outer_items == C_NULL || outer_count == 0
        return result
    end

    if inner_counts == C_NULL
        return result
    end

    for i in 1:outer_count
        # Load inner_count for this item
        # inner_counts is a *mut Vec<usize>
        # Vec layout: [len: usize, data: *mut usize, capacity: usize]
        # The data pointer is at offset 8 (after the len field)
        # Read as UInt64 array to get the data pointer at index 2 (1-based)
        inner_counts_data_ptr_as_u64 = unsafe_load(Ptr{UInt64}(inner_counts), 2)
        inner_counts_data = reinterpret(Ptr{Csize_t}, inner_counts_data_ptr_as_u64)
        inner_counts_array_ptr = unsafe_load(inner_counts_data, i)

        # Load pointer to inner items array
        # outer_items is a *mut Vec<*mut *mut c_char>
        # Vec layout: [len: usize, data: *mut *mut c_char, capacity: usize]
        # The data pointer is at offset 8 (after the len field)
        outer_items_data_ptr_as_u64 = unsafe_load(Ptr{UInt64}(outer_items), 2)
        outer_items_data = reinterpret(Ptr{Ptr{Cchar}}, outer_items_data_ptr_as_u64)
        vec_data_ptr = unsafe_load(outer_items_data, i)
        inner_items_ptr = vec_data_ptr

        namespace = String[]
        if inner_items_ptr != C_NULL && inner_counts_array_ptr > 0
            # inner_items_ptr is a *mut Vec<*mut c_char>
            # Extract the data pointer from the Vec
            # Vec layout: [len: usize, data: *mut *mut c_char, capacity: usize]
            strings_data_ptr_as_u64 = unsafe_load(Ptr{UInt64}(inner_items_ptr), 2)
            strings_data = reinterpret(Ptr{Ptr{Cchar}}, strings_data_ptr_as_u64)

            # Load each string pointer individually
            for j in 1:inner_counts_array_ptr
                str_ptr = unsafe_load(strings_data, j)
                if str_ptr != C_NULL
                    push!(namespace, unsafe_string(str_ptr))
                end
            end
        end
        push!(result, namespace)
    end

    return result
end