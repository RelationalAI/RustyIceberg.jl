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

"""
    Base.length(df::DataFiles) -> Int

Return the number of data files in the handle (0 if the handle is null/freed).
"""
function Base.length(df::DataFiles)
    df.ptr == C_NULL && return 0
    return Int(@ccall rust_lib.iceberg_data_files_len(df.ptr::Ptr{Cvoid})::Csize_t)
end

"""
    data_file_info(df::DataFiles) -> Vector{Dict{String,Any}}

Return a `Vector` of `Dict`s, one per data file, containing all Iceberg
`DataFile` metadata fields:

- `content` — `"data"`, `"position_deletes"`, or `"equality_deletes"`
- `file_path` — full URI of the file
- `file_format` — `"parquet"`, `"avro"`, `"orc"`, etc.
- `record_count` — number of records in the file
- `file_size_in_bytes` — total file size
- `column_sizes`, `value_counts`, `null_value_counts`, `nan_value_counts` — per-column stats maps (keys are field-id strings)
- `lower_bounds`, `upper_bounds` — per-column min/max bounds
- `split_offsets` — row-group offsets (Parquet), or `nothing`
- `sort_order_id`, `equality_ids`, `first_row_id`, `referenced_data_file`, `content_offset`, `content_size_in_bytes` — optional fields

Returns an empty vector if the handle is null/freed.
"""
function data_file_info(df::DataFiles)
    df.ptr == C_NULL && return Dict{String,Any}[]
    ptr = @ccall rust_lib.iceberg_data_files_to_json(df.ptr::Ptr{Cvoid})::Ptr{Cchar}
    if ptr == C_NULL
        throw(IcebergException(
            INTERNAL,
            "Internal error (please report this as a bug)",
            "iceberg_data_files_to_json returned null",
        ))
    end
    json_str = unsafe_string(ptr)
    @ccall rust_lib.iceberg_destroy_cstring(ptr::Ptr{Cchar})::Cint
    return JSON.parse(json_str)
end
