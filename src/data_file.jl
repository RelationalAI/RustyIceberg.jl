# DataFiles API for RustyIceberg
#
# This module provides Julia wrappers for Iceberg DataFiles,
# which represent the metadata for data files written to storage.

"""
    DataFiles

Opaque handle representing a collection of data files produced by a writer.

This handle is consumed when passed to `add_data_files`. DataFiles are
automatically freed when the associated `DataFileWriter` is freed.
"""
mutable struct DataFiles
    ptr::Ptr{Cvoid}
end

"""
    free_data_files!(df::DataFiles)

Free the memory associated with a data files handle.

This is called automatically by `add_data_files` after consuming the data files,
but can also be called manually if the data files won't be used in a transaction.
"""
function free_data_files!(df::DataFiles)
    if df.ptr == C_NULL
        return nothing
    end
    @ccall rust_lib.iceberg_data_files_free(df.ptr::Ptr{Cvoid})::Cvoid
    df.ptr = C_NULL
    return nothing
end
