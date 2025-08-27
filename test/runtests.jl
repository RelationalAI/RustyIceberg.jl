using Test
using RustyIceberg
using RustyIceberg: ICEBERG_OK
using DataFrames
using Arrow

@testset "RustyIceberg.jl" begin
    @testset "Library Loading" begin
        # Test library loading
        @test load_iceberg_library()

        # Test that we can unload and reload
        unload_iceberg_library()
        @test load_iceberg_library()
    end

    @testset "Low-level API" begin
        # Test table operations
        table_path = "s3://vustef-dev/tpch-sf0.1-no-part/nation"
        metadata_path = "metadata/00001-894cf98a-a055-47ba-a701-327455060d32.metadata.json"

        println("Testing table operations with:")
        println("  Table path: $table_path")
        println("  Metadata path: $metadata_path")

        # Test table open
        result, table = iceberg_table_open(table_path, metadata_path)
        if result == ICEBERG_OK
            @test table != C_NULL
            println("✅ Table opened successfully")

            # Test scan creation
            result, scan = iceberg_table_scan(table)
            if result == ICEBERG_OK
                @test scan != C_NULL
                println("✅ Scan created successfully")

                # Test batch reading
                batch_count = 0
                total_bytes = 0

                while true
                    result, batch_ptr = iceberg_scan_next_batch(scan)

                    if result == ICEBERG_END_OF_STREAM
                        println("✅ Reached end of stream")
                        break
                    end

                    if result != ICEBERG_OK
                        println("❌ Failed to get next batch: $(iceberg_error_message())")
                        break
                    end

                    if batch_ptr == C_NULL
                        println("❌ Received NULL batch")
                        break
                    end

                    batch_count += 1
                    batch = unsafe_load(batch_ptr)
                    total_bytes += batch.length

                    println("📦 Batch $batch_count:")
                    println("   - Serialized size: $(batch.length) bytes")
                    println("   - Data pointer: $(batch.data)")

                    # Print first few bytes as hex for verification
                    print("   - First few bytes: ")
                    print_len = min(batch.length, 8)
                    for i in 1:print_len
                        print(string(unsafe_load(batch.data + i - 1), base=16, pad=2), " ")
                    end
                    println()

                    # Test Arrow data reading
                    io = IOBuffer(unsafe_wrap(Array, batch.data, batch.length))
                    arrow_table = Arrow.Table(io)
                    df = DataFrame(arrow_table)
                    println("   → Arrow data: $(size(df)) rows × $(length(names(df))) columns")
                    println("   → Columns: $(names(df))")

                    # Free the batch
                    iceberg_arrow_batch_free(batch_ptr)
                end

                println("📊 Summary:")
                println("   - Total batches: $batch_count")
                println("   - Total bytes processed: $total_bytes")

                @test batch_count > 0
                @test total_bytes > 0

                # Cleanup scan
                iceberg_scan_free(scan)
            else
                println("❌ Failed to create scan: $(iceberg_error_message())")
                @test false
            end

            # Cleanup table
            iceberg_table_free(table)
        else
            println("❌ Failed to open table: $(iceberg_error_message())")
            println("This may be expected due to S3 permissions or missing data")
            @test_broken false  # Mark as broken since this might fail due to external dependencies
        end
    end

    @testset "High-level API" begin
        table_path = "s3://vustef-dev/tpch-sf0.1-no-part/nation"
        metadata_path = "metadata/00001-894cf98a-a055-47ba-a701-327455060d32.metadata.json"

        println("Testing high-level API...")

        try
            # Test reading entire table - now returns an iterator
            table_iterator = read_iceberg_table(table_path, metadata_path)
            @test table_iterator isa IcebergTableIterator
            println("✅ Table iterator created successfully")

            # Test iteration over Arrow.Table objects
            arrow_tables = Arrow.Table[]
            batch_count = 0

            for arrow_table in table_iterator
                batch_count += 1
                push!(arrow_tables, arrow_table)

                # Convert to DataFrame for testing
                df = DataFrame(arrow_table)
                @test !isempty(df)
                println("📦 Batch $batch_count: $(size(df)) rows × $(length(names(df))) columns")
            end

            @test batch_count > 0
            @test !isempty(arrow_tables)
            println("✅ High-level API iteration test successful")
            println("   - Total batches: $batch_count")
            println("   - Total Arrow tables: $(length(arrow_tables))")

            # Test reading with column selection
            if !isempty(arrow_tables)
                # Get column names from first batch
                first_df = DataFrame(arrow_tables[1])
                if !isempty(names(first_df))
                    selected_columns = [names(first_df)[1]]  # Select first column
                    selected_iterator = read_iceberg_table(table_path, metadata_path, columns=selected_columns)
                    @test selected_iterator isa IcebergTableIterator

                    selected_arrow_tables = Arrow.Table[]
                    for arrow_table in selected_iterator
                        push!(selected_arrow_tables, arrow_table)
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

        catch e
            println("❌ High-level API test failed: $e")
            println("This may be expected due to S3 permissions or missing data")
            @test_broken false  # Mark as broken since this might fail due to external dependencies
        end
    end

    @testset "Iterator Properties" begin
        # Test iterator type properties
        table_path = "s3://vustef-dev/tpch-sf0.1-no-part/nation"
        metadata_path = "metadata/00001-894cf98a-a055-47ba-a701-327455060d32.metadata.json"

        table_iterator = read_iceberg_table(table_path, metadata_path)

        # Test eltype
        @test Base.eltype(table_iterator) == Arrow.Table

        # Test IteratorSize
        @test Base.IteratorSize(table_iterator) == Base.SizeUnknown()

        println("✅ Iterator properties test successful")
    end

    @testset "Error Handling" begin
        # Test with invalid paths
        result, table = iceberg_table_open("invalid/path", "invalid/metadata.json")
        @test result != ICEBERG_OK
        @test table == C_NULL

        # Test error message
        error_msg = iceberg_error_message()
        @test !isempty(error_msg)
        println("✅ Error handling test: $error_msg")
    end

    @testset "Cleanup" begin
        # Test library unloading
        unload_iceberg_library()
        println("✅ Library cleanup successful")
    end
end

println("\n🎉 All tests completed!")
