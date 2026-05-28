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
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "null table pointer passed to Transaction()",
        ))
    end
    return Transaction(ptr, table)
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
        throw(IcebergException(
            STATE_TRANSACTION_CONSUMED,
            "Transaction has already been committed or rolled back",
            "Transaction has been freed or consumed",
        ))
    end
    if catalog.ptr == C_NULL
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "Catalog has been freed",
        ))
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


"""
    with_transaction(f::Function, table::Table, catalog::Catalog) -> Table

Create a transaction, pass it to `f`, then commit and return the updated table.

This provides a convenient way to perform transaction operations with automatic
cleanup and commit, ensuring the transaction is freed even if an error occurs.

# Arguments
- `f`: A function that takes a `Transaction` and performs operations on it
- `table::Table`: The table to create a transaction for
- `catalog::Catalog`: The catalog to commit the transaction to

# Returns
The updated `Table` after the transaction has been committed.

# Example
```julia
updated_table = with_transaction(table, catalog) do tx
    with_fast_append(tx) do action
        add_data_files(action, data_files)
    end
end
```
"""
function with_transaction(f::Function, table::Table, catalog::Catalog)
    tx = Transaction(table)
    try
        f(tx)
        return commit(tx, catalog)
    finally
        free_transaction!(tx)
    end
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
    FastAppendAction

Opaque handle representing a fast append action that can accumulate data files
from multiple writers before being applied to a transaction.

Create using `FastAppendAction()`, add files with `add_data_files`, then apply
with `apply`. Or use the convenient `with_fast_append` helper.
"""
mutable struct FastAppendAction
    ptr::Ptr{Cvoid}
end

"""
    FastAppendAction() -> FastAppendAction

Create a new empty fast append action.

# Returns
A new `FastAppendAction` handle that must be freed with `free_fast_append_action!`.

# Example
```julia
action = FastAppendAction()
add_data_files(action, data_files1)
add_data_files(action, data_files2)
apply(action, tx)
free_fast_append_action!(action)
```
"""
function FastAppendAction()
    ptr = @ccall rust_lib.iceberg_fast_append_action_new()::Ptr{Cvoid}
    if ptr == C_NULL
        throw(IcebergException(
            INTERNAL,
            "Internal error (please report this as a bug)",
            "iceberg_fast_append_action_new returned null",
        ))
    end
    return FastAppendAction(ptr)
end

"""
    free_fast_append_action!(action::FastAppendAction)

Free the memory associated with a fast append action.
"""
function free_fast_append_action!(action::FastAppendAction)
    if action.ptr == C_NULL
        return nothing
    end
    @ccall rust_lib.iceberg_fast_append_action_free(action.ptr::Ptr{Cvoid})::Cvoid
    action.ptr = C_NULL
    return nothing
end

"""
    add_data_files(action::FastAppendAction, data_files::DataFiles)

Add data files to a fast append action.

This can be called multiple times to accumulate data files from multiple writers.
The `data_files` handle is consumed by this operation and marked as such
(`data_files.ptr` is set to `C_NULL`).

# Arguments
- `action::FastAppendAction`: The action to add files to
- `data_files::DataFiles`: The data files to add (consumed by this operation)

# Throws
- `IcebergException` if the operation fails
"""
function add_data_files(action::FastAppendAction, data_files::DataFiles)
    if action.ptr == C_NULL
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "FastAppendAction has been freed",
        ))
    end
    if data_files.ptr == C_NULL
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "DataFiles has been freed or consumed",
        ))
    end

    try
        error_message_ptr = Ref{Ptr{Cchar}}(C_NULL)

        result = @ccall rust_lib.iceberg_fast_append_action_add_data_files(
            action.ptr::Ptr{Cvoid},
            data_files.ptr::Ptr{Cvoid},
            error_message_ptr::Ref{Ptr{Cchar}}
        )::Cint

        if result != 0
            parse_and_throw(error_message_ptr[], "add_data_files / apply")
        end
    finally
        # Free the now-empty DataFiles container and mark as consumed
        # The Rust side took the Vec<DataFile> contents via std::mem::take,
        # but we still need to free the IcebergDataFiles struct itself
        free_data_files!(data_files)
    end

    return nothing
end

"""
    apply(action::FastAppendAction, tx::Transaction)

Apply a fast append action to a transaction.

This applies all accumulated data files as a single FastAppendAction to the transaction.

# Arguments
- `action::FastAppendAction`: The action to apply
- `tx::Transaction`: The transaction to apply the action to

# Throws
- `IcebergException` if the operation fails
"""
function apply(action::FastAppendAction, tx::Transaction)
    if action.ptr == C_NULL
        throw(IcebergException(
            STATE_RESOURCE_FREED,
            "Resource has been freed",
            "FastAppendAction has been freed",
        ))
    end
    if tx.ptr == C_NULL
        throw(IcebergException(
            STATE_TRANSACTION_CONSUMED,
            "Transaction has already been committed or rolled back",
            "Transaction has been freed or consumed",
        ))
    end

    error_message_ptr = Ref{Ptr{Cchar}}(C_NULL)

    result = @ccall rust_lib.iceberg_fast_append_action_apply(
        action.ptr::Ptr{Cvoid},
        tx.ptr::Ptr{Cvoid},
        error_message_ptr::Ref{Ptr{Cchar}}
    )::Cint

    if result != 0
        parse_and_throw(error_message_ptr[], "add_data_files / apply")
    end

    return nothing
end

"""
    with_fast_append(f::Function, tx::Transaction)

Create a fast append action, pass it to `f`, then apply it to the transaction.

This provides a convenient way to accumulate multiple data files and apply them
as a single action, ensuring proper cleanup even if an error occurs.

# Arguments
- `f`: A function that takes a `FastAppendAction` and adds data files to it
- `tx::Transaction`: The transaction to apply the action to

# Example
```julia
tx = Transaction(table)

with_fast_append(tx) do action
    add_data_files(action, data_files1)
    add_data_files(action, data_files2)
end

updated_table = commit(tx, catalog)
```
"""
function with_fast_append(f::Function, tx::Transaction)
    action = FastAppendAction()
    try
        f(action)
        apply(action, tx)
    finally
        free_fast_append_action!(action)
    end
    return nothing
end

# ---------------------------------------------------------------------------
# OverwriteAction
# ---------------------------------------------------------------------------

const DataFilesResponse = Response{Ptr{Cvoid}}

"""
    OverwriteAction

Opaque handle that accumulates files to add and files to delete for an atomic
overwrite snapshot (`Operation::Overwrite`).

Create with `OverwriteAction()`, populate with `add_data_files` and
`delete_data_files`, then commit with `apply`. Use `with_overwrite` for
automatic cleanup.
"""
mutable struct OverwriteAction
    ptr::Ptr{Cvoid}
end

function OverwriteAction()
    ptr = @ccall rust_lib.iceberg_overwrite_action_new()::Ptr{Cvoid}
    if ptr == C_NULL
        throw(IcebergException(STATE_RESOURCE_FREED, "Resource has been freed", "iceberg_overwrite_action_new returned null"))
    end
    return OverwriteAction(ptr)
end

function free_overwrite_action!(action::OverwriteAction)
    if action.ptr == C_NULL
        return nothing
    end
    @ccall rust_lib.iceberg_overwrite_action_free(action.ptr::Ptr{Cvoid})::Cvoid
    action.ptr = C_NULL
    return nothing
end

"""
    add_data_files(action::OverwriteAction, data_files::DataFiles)

Add new data files to be written in the overwrite snapshot.
The `data_files` handle is consumed by this call.
"""
function add_data_files(action::OverwriteAction, data_files::DataFiles)
    if action.ptr == C_NULL
        throw(IcebergException(STATE_RESOURCE_FREED, "Resource has been freed", "OverwriteAction has been freed"))
    end
    if data_files.ptr == C_NULL
        throw(IcebergException(STATE_RESOURCE_FREED, "Resource has been freed", "DataFiles has been freed or consumed"))
    end
    err_ptr = Ref{Ptr{Cchar}}(C_NULL)
    ret = @ccall rust_lib.iceberg_overwrite_action_add_data_files(
        action.ptr::Ptr{Cvoid},
        data_files.ptr::Ptr{Cvoid},
        err_ptr::Ptr{Ptr{Cchar}}
    )::Cint
    if ret != 0
        msg = err_ptr[] != C_NULL ? unsafe_string(err_ptr[]) : "unknown error"
        throw(IcebergException(UNEXPECTED, "Failed to add data files to overwrite action", msg))
    end
    data_files.ptr = C_NULL
    return nothing
end

"""
    delete_data_files(action::OverwriteAction, data_files::DataFiles)

Mark existing data files for deletion in the overwrite snapshot.
The `data_files` handle is consumed by this call.
"""
function delete_data_files(action::OverwriteAction, data_files::DataFiles)
    if action.ptr == C_NULL
        throw(IcebergException(STATE_RESOURCE_FREED, "Resource has been freed", "OverwriteAction has been freed"))
    end
    if data_files.ptr == C_NULL
        throw(IcebergException(STATE_RESOURCE_FREED, "Resource has been freed", "DataFiles has been freed or consumed"))
    end
    err_ptr = Ref{Ptr{Cchar}}(C_NULL)
    ret = @ccall rust_lib.iceberg_overwrite_action_delete_data_files(
        action.ptr::Ptr{Cvoid},
        data_files.ptr::Ptr{Cvoid},
        err_ptr::Ptr{Ptr{Cchar}}
    )::Cint
    if ret != 0
        msg = err_ptr[] != C_NULL ? unsafe_string(err_ptr[]) : "unknown error"
        throw(IcebergException(UNEXPECTED, "Failed to add delete files to overwrite action", msg))
    end
    data_files.ptr = C_NULL
    return nothing
end

"""
    apply(action::OverwriteAction, tx::Transaction)

Apply the overwrite action to the transaction.
"""
function apply(action::OverwriteAction, tx::Transaction)
    if action.ptr == C_NULL
        throw(IcebergException(STATE_RESOURCE_FREED, "Resource has been freed", "OverwriteAction has been freed"))
    end
    if tx.ptr == C_NULL
        throw(IcebergException(STATE_TRANSACTION_CONSUMED, "Transaction has already been committed or rolled back", "Transaction has been freed or consumed"))
    end
    err_ptr = Ref{Ptr{Cchar}}(C_NULL)
    ret = @ccall rust_lib.iceberg_overwrite_action_apply(
        action.ptr::Ptr{Cvoid},
        tx.ptr::Ptr{Cvoid},
        err_ptr::Ptr{Ptr{Cchar}}
    )::Cint
    if ret != 0
        msg = err_ptr[] != C_NULL ? unsafe_string(err_ptr[]) : "unknown error"
        throw(IcebergException(UNEXPECTED, "Failed to apply overwrite action", msg))
    end
    return nothing
end

"""
    with_overwrite(f::Function, tx::Transaction)

Create an `OverwriteAction`, pass it to `f`, then apply it to `tx`.
Frees the action automatically even on error.

# Example
```julia
updated_table = with_transaction(table, catalog) do tx
    with_overwrite(tx) do action
        add_data_files(action, new_files)
        delete_data_files(action, old_files)
    end
end
```
"""
function with_overwrite(f::Function, tx::Transaction)
    action = OverwriteAction()
    try
        f(action)
        apply(action, tx)
    finally
        free_overwrite_action!(action)
    end
    return nothing
end

"""
    list_data_files(table::Table) -> DataFiles

Return a `DataFiles` handle containing all live data files in the current
snapshot of `table`. Returns an empty handle if the table has no snapshot.

The returned `DataFiles` must be freed with `free_data_files!` if not passed
to `delete_data_files`.
"""
function list_data_files(table::Table)
    response = DataFilesResponse()
    async_ccall(response, table) do handle
        @ccall rust_lib.iceberg_table_list_data_files(
            table::Table,
            response::Ref{DataFilesResponse},
            handle::Ptr{Cvoid}
        )::Cint
    end
    @throw_on_error(response, "list_data_files", IcebergException)
    return DataFiles(response.value)
end
