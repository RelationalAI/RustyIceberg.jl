# Test helper functions
#
# This module provides utility functions for tests.

"""
    read_table_data(table) -> NamedTuple

Read all data from an Iceberg table and return it as a NamedTuple of vectors.
This is a convenience function for tests that need to verify written data.

# Example
```julia
tbl = read_table_data(table)
@test length(tbl.id) == 5
@test tbl.value[1] == 1.5
```
"""
function read_table_data(table)
    scan = RustyIceberg.new_scan(table, RustyIceberg.IcebergPerfConfig(batch_size=1024))
    stream = RustyIceberg.scan!(scan)

    # Collect column data across batches as plain Julia vectors.
    # Each batch is imported zero-copy via Arrow C Data Interface, then
    # materialised into a DataFrame before free_batch releases Rust memory.
    all_dfs = DataFrame[]
    batch_ptr = RustyIceberg.next_batch(stream)
    while batch_ptr != C_NULL
        batch = unsafe_load(batch_ptr)
        if batch.schema != C_NULL && batch.array != C_NULL
            imported = Arrow.from_c_data(batch.schema, batch.array)
            push!(all_dfs, DataFrame(imported))
            Arrow.release_c_data(imported)
        end
        RustyIceberg.free_batch(batch_ptr)
        batch_ptr = RustyIceberg.next_batch(stream)
    end
    RustyIceberg.free_stream(stream)
    RustyIceberg.free_scan!(scan)

    isempty(all_dfs) && return nothing

    combined = vcat(all_dfs...)
    names = Tuple(propertynames(combined))
    return NamedTuple{names}(Tuple(collect(combined[!, n]) for n in names))
end
