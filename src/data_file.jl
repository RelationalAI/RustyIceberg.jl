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

# Internal: free memory for data files handle
function _free_data_files!(df::DataFiles)
    if df.ptr == C_NULL
        return nothing
    end
    @ccall rust_lib.iceberg_data_files_free(df.ptr::Ptr{Cvoid})::Cvoid
    df.ptr = C_NULL
    return nothing
end
