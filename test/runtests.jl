using Test
using RustyIceberg
using DataFrames
using Arrow

@testset "RustyIceberg.jl" begin
    @testset "Runtime Initialization" begin
        # Test runtime initialization - this should work
        @test_nowarn init_runtime()

        # Test that we can initialize multiple times safely
        @test_nowarn init_runtime()

        println("✅ Runtime initialization successful")
    end

    @testset "High-level API" begin
        # Test with the actual customer table that we know works
        snapshot_path = "s3://warehouse/tpch.sf01/customer/metadata/00001-76f6e7e4-b34f-492f-b6a1-cc9f8c8f4975.metadata.json"

        println("Testing high-level API...")
        println("  Snapshot path: $snapshot_path")

        # Test creating table iterator
        table_iterator = read_table(snapshot_path)
        @test table_iterator isa TableIterator
        println("✅ Table iterator created successfully")

        # Test iteration over Arrow.Table objects
        arrow_tables = Arrow.Table[]
        batch_count = 0
        total_rows = 0

        for arrow_table in table_iterator
            batch_count += 1
            push!(arrow_tables, arrow_table)

            # Convert to DataFrame for testing
            df = DataFrame(arrow_table)
            @test !isempty(df)
            total_rows += nrow(df)

            # Only print details for first few batches to avoid spam
            if batch_count <= 3
                println("📦 Batch $batch_count: $(size(df)) rows × $(length(names(df))) columns")
                println("   → Columns: $(names(df))")
            end

            # Stop after a few batches for testing to avoid long test times
            if batch_count >= 5
                println("   ... stopping after $batch_count batches for testing")
                break
            end
        end

        @test batch_count > 0
        @test total_rows > 0
        @test !isempty(arrow_tables)
        println("✅ High-level API iteration test successful")
        println("   - Total batches processed: $batch_count")
        println("   - Total rows processed: $total_rows")
        println("   - Total Arrow tables: $(length(arrow_tables))")

        # Test reading with column selection
        if !isempty(arrow_tables)
            # Get column names from first batch
            first_df = DataFrame(arrow_tables[1])
            if !isempty(names(first_df))
                # Select first two columns for testing
                selected_columns = names(first_df)[1:min(2, length(names(first_df)))]
                selected_iterator = read_table(
                    snapshot_path; columns=selected_columns, batch_size=UInt(8)
                )
                @test selected_iterator isa TableIterator

                selected_arrow_tables = Arrow.Table[]
                selected_batch_count = 0
                for arrow_table in selected_iterator
                    @test length(arrow_table) <= 8
                    selected_batch_count += 1
                    push!(selected_arrow_tables, arrow_table)

                    # Only process first batch for column selection test
                    if selected_batch_count >= 1
                        break
                    end
                end

                @test !isempty(selected_arrow_tables)

                # Check that selected columns match
                if !isempty(selected_arrow_tables)
                    selected_df = DataFrame(selected_arrow_tables[1])
                    @test names(selected_df) == selected_columns
                    println("✅ Column selection test successful")
                    println("   - Selected columns: $(names(selected_df))")
                end
            end
        end
    end

    @testset "Iterator Properties" begin
        # Test iterator type properties
        snapshot_path = "s3://warehouse/tpch.sf01/customer/metadata/00001-76f6e7e4-b34f-492f-b6a1-cc9f8c8f4975.metadata.json"

        table_iterator = read_table(snapshot_path)

        # Test eltype
        @test Base.eltype(table_iterator) == Arrow.Table

        # Test IteratorSize
        @test Base.IteratorSize(table_iterator) == Base.SizeUnknown()

        println("✅ Iterator properties test successful")
    end

    @testset "Error Handling" begin
        # Test with invalid paths - this should throw an exception in our async API
        try
            invalid_iterator = read_table("invalid/path", "invalid/metadata.json")
            # Try to iterate - this should fail
            for arrow_table in invalid_iterator
                @test false  # Should not reach here
                break
            end
        catch e
            @test e isa Exception
            println("✅ Error handling test successful: caught expected exception")
        end
    end
end

println("\n🎉 All tests completed!")
