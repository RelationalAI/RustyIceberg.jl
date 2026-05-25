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
    scan = RustyIceberg.new_scan(table)
    stream = RustyIceberg.scan!(scan)

    all_batches = Arrow.Table[]
    batch_ptr = RustyIceberg.next_batch(stream)
    while batch_ptr != C_NULL
        batch = unsafe_load(batch_ptr)
        if batch.length > 0
            # Copy the arrow data before freeing the batch
            arrow_data = unsafe_wrap(Array, batch.data, batch.length)
            arrow_data_copy = copy(arrow_data)
            push!(all_batches, Arrow.Table(arrow_data_copy))
        end
        RustyIceberg.free_batch(batch_ptr)
        batch_ptr = RustyIceberg.next_batch(stream)
    end
    RustyIceberg.free_stream(stream)
    RustyIceberg.free_scan!(scan)

    if isempty(all_batches)
        return nothing
    end

    # Get column names from first batch
    first_tbl = all_batches[1]
    cols = Tables.columns(first_tbl)
    names = Tables.columnnames(cols)

    # Collect all data from all batches into plain Julia vectors
    result = Dict{Symbol, Vector}()
    for name in names
        # Collect from all batches into a single vector
        all_values = []
        for tbl in all_batches
            col = Tables.getcolumn(Tables.columns(tbl), name)
            # Use collect to convert Arrow array to plain Julia array
            append!(all_values, collect(col))
        end
        result[name] = all_values
    end

    return NamedTuple{Tuple(names)}(Tuple(result[n] for n in names))
end
