# DataFiles API for RustyIceberg
#
# This module provides Julia wrappers for Iceberg DataFiles,
# which represent the metadata for data files written to storage.

"""
    DataFiles

Opaque handle representing a collection of data files produced by a writer.

This handle is consumed when passed to `add_data_files` and should be freed
with `free_data_files!` if not used.
"""
mutable struct DataFiles
    ptr::Ptr{Cvoid}
end

"""
    free_data_files!(df::DataFiles)

Free the memory associated with a data files handle.

This should be called if the data files are not going to be used in a transaction.
Note: After calling `add_data_files`, the data files are consumed and this function
should still be called to free the handle (though it will be empty).
"""
function free_data_files!(df::DataFiles)
    if df.ptr == C_NULL
        return nothing
    end
    @ccall rust_lib.iceberg_data_files_free(df.ptr::Ptr{Cvoid})::Cvoid
    df.ptr = C_NULL
    return nothing
end
