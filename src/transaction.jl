# Transaction API for RustyIceberg
#
# This module provides Julia wrappers for the iceberg-rust Transaction API,
# enabling Julia to create transactions, add data files, and commit changes.

"""
    Transaction

Opaque handle representing an Iceberg transaction.

Create a transaction using `Transaction(table)` and free it with `free_transaction!`
when done. Transactions should be committed using `commit` before being freed.
"""
mutable struct Transaction
    ptr::Ptr{Cvoid}
    table::Table  # Keep reference to table to prevent GC
end

Base.unsafe_convert(::Type{Ptr{Cvoid}}, tx::Transaction) = tx.ptr

"""
    Transaction(table::Table) -> Transaction

Create a new transaction for the given table.

# Arguments
- `table::Table`: The table to create a transaction for

# Returns
A new `Transaction` handle that must be freed with `free_transaction!`.

# Example
```julia
table = load_table(catalog, ["db"], "users")
tx = Transaction(table)
# ... add operations to transaction ...
updated_table = commit(tx, catalog)
free_transaction!(tx)
```
"""
function Transaction(table::Table)
    ptr = @ccall rust_lib.iceberg_transaction_new(table::Table)::Ptr{Cvoid}
    if ptr == C_NULL
        throw(IcebergException("Failed to create transaction: null table pointer"))
    end
    return Transaction(ptr, table)
end

"""
    free_transaction!(tx::Transaction)

Free the memory associated with a transaction.

This should be called after the transaction has been committed or if
the transaction is no longer needed.
"""
function free_transaction!(tx::Transaction)
    if tx.ptr == C_NULL
        return nothing
    end
    @ccall rust_lib.iceberg_transaction_free(tx.ptr::Ptr{Cvoid})::Cvoid
    tx.ptr = C_NULL
    return nothing
end

"""
    fast_append(tx::Transaction, data_files::DataFiles)

Add data files to a transaction using fast append.

This operation:
1. Creates a FastAppendAction
2. Adds the data files from the handle
3. Applies the action to the transaction

The `data_files` handle is consumed by this operation - the data files are
moved into the transaction and the handle becomes empty.

# Arguments
- `tx::Transaction`: The transaction to add files to
- `data_files::DataFiles`: The data files to append

# Throws
- `IcebergException` if the operation fails

# Example
```julia
tx = Transaction(table)
# data_files would come from a writer (not yet implemented)
fast_append(tx, data_files)
updated_table = commit(tx, catalog)
```
"""
function fast_append(tx::Transaction, data_files::DataFiles)
    if tx.ptr == C_NULL
        throw(IcebergException("Transaction has been freed or consumed"))
    end
    if data_files.ptr == C_NULL
        throw(IcebergException("DataFiles has been freed or consumed"))
    end

    error_message_ptr = Ref{Ptr{Cchar}}(C_NULL)

    result = @ccall rust_lib.iceberg_transaction_fast_append(
        tx.ptr::Ptr{Cvoid},
        data_files.ptr::Ptr{Cvoid},
        error_message_ptr::Ref{Ptr{Cchar}}
    )::Cint

    if result != 0
        error_msg = "fast_append failed"
        if error_message_ptr[] != C_NULL
            error_msg = unsafe_string(error_message_ptr[])
            @ccall rust_lib.iceberg_destroy_cstring(error_message_ptr[]::Ptr{Cchar})::Cint
        end
        throw(IcebergException(error_msg))
    end

    return nothing
end

"""
    commit(tx::Transaction, catalog::Catalog) -> Table

Commit a transaction to the catalog.

This consumes the transaction and returns the updated table. After calling this,
the transaction handle should be freed with `free_transaction!` but the
transaction itself has been consumed and cannot be used again.

# Arguments
- `tx::Transaction`: The transaction to commit
- `catalog::Catalog`: The catalog to commit the transaction to

# Returns
The updated `Table` after the transaction has been committed.

# Throws
- `IcebergException` if the commit fails

# Example
```julia
tx = Transaction(table)
fast_append(tx, data_files)
updated_table = commit(tx, catalog)
free_transaction!(tx)
# Now use updated_table for subsequent operations
```
"""
function commit(tx::Transaction, catalog::Catalog)
    if tx.ptr == C_NULL
        throw(IcebergException("Transaction has been freed or consumed"))
    end
    if catalog.ptr == C_NULL
        throw(IcebergException("Catalog has been freed"))
    end

    response = TableResponse()

    async_ccall(response, tx, catalog) do handle
        @ccall rust_lib.iceberg_transaction_commit(
            tx.ptr::Ptr{Cvoid},
            catalog.ptr::Ptr{Cvoid},
            response::Ref{TableResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end

    @throw_on_error(response, "commit", IcebergException)

    return response.value
end
