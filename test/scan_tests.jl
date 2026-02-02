using RustyIceberg
using Test
using DataFrames
using Arrow

@testset "Low-level API" begin
    # Test with the actual customer table that we know works
    snapshot_path = "s3://warehouse/tpch.sf01/customer/metadata/00001-76f6e7e4-b34f-492f-b6a1-cc9f8c8f4975.metadata.json"

    println("Testing low-level API...")
    println("  Snapshot path: $snapshot_path")

    # Open table
    table = RustyIceberg.table_open(snapshot_path)
    @test table != C_NULL
    println("✅ Table opened successfully")

    # Create scan
    scan = RustyIceberg.new_scan(table)
    @test scan isa RustyIceberg.Scan
    @test scan.ptr != C_NULL
    println("✅ Scan created successfully")

    # Build and get stream
    stream = RustyIceberg.scan!(scan)
    @test stream != C_NULL
    println("✅ Stream obtained successfully")

    # Test iteration over batches
    arrow_tables = Arrow.Table[]
    batch_count = 0
    total_rows = 0

    try
        while true
            batch_ptr = RustyIceberg.next_batch(stream)
            if batch_ptr == C_NULL
                break
            end

            batch_count += 1
            batch = unsafe_load(batch_ptr)
            arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
            @test arrow_table isa Arrow.Table
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

            RustyIceberg.free_batch(batch_ptr)

            # Stop after a few batches for testing to avoid long test times
            if batch_count >= 5
                println("   ... stopping after $batch_count batches for testing")
                break
            end
        end

        @test batch_count > 0
        @test total_rows > 0
        @test !isempty(arrow_tables)
        println("✅ Low-level API iteration test successful")
        println("   - Total batches processed: $batch_count")
        println("   - Total rows processed: $total_rows")
        println("   - Total Arrow tables: $(length(arrow_tables))")
    finally
        # Clean up
        RustyIceberg.free_stream(stream)
        RustyIceberg.free_scan!(scan)
        RustyIceberg.free_table(table)
        println("✅ Resources cleaned up")
    end

    # Test reading with column selection
    if !isempty(arrow_tables)
        # Get column names from first batch
        first_df = DataFrame(arrow_tables[1])
        if !isempty(names(first_df))
            # Select first two columns for testing
            selected_columns = names(first_df)[1:min(2, length(names(first_df)))]

            table2 = RustyIceberg.table_open(snapshot_path)
            scan2 = RustyIceberg.new_scan(table2)
            RustyIceberg.select_columns!(scan2, selected_columns)
            RustyIceberg.with_batch_size!(scan2, UInt(8))
            stream2 = RustyIceberg.scan!(scan2)

            try
                selected_arrow_tables = Arrow.Table[]
                selected_batch_count = 0

                batch_ptr = RustyIceberg.next_batch(stream2)
                while batch_ptr != C_NULL
                    batch = unsafe_load(batch_ptr)
                    arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                    @test arrow_table isa Arrow.Table
                    @test length(arrow_table) <= 8
                    push!(selected_arrow_tables, arrow_table)
                    RustyIceberg.free_batch(batch_ptr)
                    batch_ptr = RustyIceberg.next_batch(stream2)
                end

                @test !isempty(selected_arrow_tables)

                # Check that selected columns match
                selected_df = DataFrame(selected_arrow_tables[1])
                @test names(selected_df) == selected_columns
                println("✅ Column selection test successful")
                println("   - Selected columns: $(names(selected_df))")
            finally
                # Clean up
                RustyIceberg.free_stream(stream2)
                RustyIceberg.free_scan!(scan2)
                RustyIceberg.free_table(table2)
            end
        end
    end
end

@testset "Error Handling" begin
    # Test with invalid paths - this should throw an exception in our async API
    try
        table = RustyIceberg.table_open("invalid/path/metadata.json")
        @test false  # Should not reach here
    catch e
        @test e isa Exception
        println("✅ Error handling test successful: caught expected exception")
    end
end

@testset "Read and verify nations table" begin
    # Test reading the nations table and verify contents
    nations_snapshot_path = "s3://warehouse/tpch.sf01/nation/metadata/00001-44f668fe-3688-49d5-851f-36e75d143321.metadata.json"

    println("Testing reading nations table...")

    table = RustyIceberg.table_open(nations_snapshot_path)
    scan = RustyIceberg.new_scan(table)
    RustyIceberg.with_batch_size!(scan, UInt(5))
    stream = RustyIceberg.scan!(scan)

    rows = Tuple[]
    batch_ptr = C_NULL
    try
        while true
            batch_ptr = RustyIceberg.next_batch(stream)
            if batch_ptr == C_NULL
                break
            end

            batch = unsafe_load(batch_ptr)
            arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
            df = DataFrame(arrow_table)
            expected_columns = ["n_nationkey", "n_name", "n_regionkey", "n_comment"]
            @test names(df) == expected_columns
            # Collect rows as tuples for easier verification
            for row in eachrow(df)
                push!(rows, Tuple(row))
            end

            RustyIceberg.free_batch(batch_ptr)
            batch_ptr = C_NULL
        end

        sort!(rows, by = x -> x[1])

        @test rows == Tuple[
            (0, "ALGERIA", 0, "furiously regular requests. platelets affix furious"),
            (1, "ARGENTINA", 1, "instructions wake quickly. final deposits haggle. final, silent theodolites "),
            (2, "BRAZIL", 1, "asymptotes use fluffily quickly bold instructions. slyly bold dependencies sleep carefully pending accounts"),
            (3, "CANADA", 1, "ss deposits wake across the pending foxes. packages after the carefully bold requests integrate caref"),
            (4, "EGYPT", 4, "usly ironic, pending foxes. even, special instructions nag. sly, final foxes detect slyly fluffily "),
            (5, "ETHIOPIA", 0, "regular requests sleep carefull"),
            (6, "FRANCE", 3, "oggedly. regular packages solve across"),
            (7, "GERMANY", 3, "ong the regular requests: blithely silent pinto beans hagg"),
            (8, "INDIA", 2, "uriously unusual deposits about the slyly final pinto beans could"),
            (9, "INDONESIA", 2, "d deposits sleep quickly according to the dogged, regular dolphins. special excuses haggle furiously special reque"),
            (10, "IRAN", 4, "furiously idle platelets nag. express asymptotes s"),
            (11, "IRAQ", 4, "pendencies; slyly express foxes integrate carefully across the reg"),
            (12, "JAPAN", 2, " quickly final packages. furiously i"),
            (13, "JORDAN", 4, "the slyly regular ideas. silent Tiresias affix slyly fu"),
            (14, "KENYA", 0, "lyly special foxes. slyly regular deposits sleep carefully. carefully permanent accounts slee"),
            (15, "MOROCCO", 0, "ct blithely: blithely express accounts nag carefully. silent packages haggle carefully abo"),
            (16, "MOZAMBIQUE", 0, " beans after the carefully regular accounts r"),
            (17, "PERU", 1, "ly final foxes. blithely ironic accounts haggle. regular foxes about the regular deposits are furiously ir"),
            (18, "CHINA", 2, "ckly special packages cajole slyly. unusual, unusual theodolites mold furiously. slyly sile"),
            (19, "ROMANIA", 3, "sly blithe requests. thinly bold deposits above the blithely regular accounts nag special, final requests. care"),
            (20, "SAUDI ARABIA", 4, "se slyly across the blithely regular deposits. deposits use carefully regular "),
            (21, "VIETNAM", 2, "lly across the quickly even pinto beans. caref"),
            (22, "RUSSIA", 3, "uctions. furiously unusual instructions sleep furiously ironic packages. slyly "),
            (23, "UNITED KINGDOM", 3, "carefully pending courts sleep above the ironic, regular theo"),
            (24, "UNITED STATES", 1, "ly ironic requests along the slyly bold ideas hang after the blithely special notornis; blithely even accounts")
        ]
    finally
        if batch_ptr != C_NULL
            RustyIceberg.free_batch(batch_ptr)
        end
        RustyIceberg.free_stream(stream)
        RustyIceberg.free_scan!(scan)
        RustyIceberg.free_table(table)
    end
    println("✅ Nations table read and verified successfully")
end

@testset "Incremental Scan API" begin
    println("Testing incremental scan API...")

    # Use the test table created specifically for incremental scan testing
    test_snapshot_path = "s3://warehouse/incremental/test1/metadata/00003-359e8bb8-1e5d-46d2-bcde-fdaeaa41114f.metadata.json"

    # Open the table
    table = RustyIceberg.table_open(test_snapshot_path)
    @test table != C_NULL
    println("✅ Table opened successfully")

    # Use real snapshot IDs from the test table
    from_snapshot_id = Int64(6540713100348352610)
    to_snapshot_id = Int64(6832180054960511692)

    @testset "Incremental Scan E2E Test" begin
        scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)
        @test scan isa RustyIceberg.IncrementalScan
        @test scan.ptr != C_NULL
        println("✅ Incremental scan created (from snapshot $from_snapshot_id to $to_snapshot_id)")

        # Test builder methods
        @test_nowarn RustyIceberg.with_batch_size!(scan, UInt(50))
        println("✅ Batch size configured")

        # Build and get streams
        inserts_stream, deletes_stream = RustyIceberg.scan!(scan)
        @test inserts_stream != C_NULL
        @test deletes_stream != C_NULL
        println("✅ Streams obtained successfully")

        try
            # Read and validate from both streams
            inserts_values = Int64[]
            deletes_values = Int64[]
            inserts_batches = 0
            deletes_batches = 0

            # Read from inserts stream
            while true
                batch_ptr = RustyIceberg.next_batch(inserts_stream)
                if batch_ptr == C_NULL
                    break
                end
                inserts_batches += 1
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                @test arrow_table isa Arrow.Table

                # Convert to DataFrame and collect values from column "n"
                df = DataFrame(arrow_table)
                @test "n" in names(df)
                append!(inserts_values, df.n)

                RustyIceberg.free_batch(batch_ptr)
            end

            # Read from deletes stream
            # Position deletes return metadata (pos, file_path) not actual row data
            deletes_values = Tuple{String, Int64}[]
            while true
                batch_ptr = RustyIceberg.next_batch(deletes_stream)
                if batch_ptr == C_NULL
                    break
                end
                deletes_batches += 1
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                @test arrow_table isa Arrow.Table

                # Convert to DataFrame and extract position delete metadata
                df = DataFrame(arrow_table)
                for row in eachrow(df)
                    push!(deletes_values, (row.file_path, row.pos))
                end

                RustyIceberg.free_batch(batch_ptr)
            end

            println("✅ Successfully read from incremental scan streams")
            println("   - Inserts batches: $inserts_batches")
            println("   - Deletes batches: $deletes_batches")
            println("   - Inserts rows: $(length(inserts_values))")
            println("   - Delete records: $(length(deletes_values))")

            # Validate deletes: should have 1 delete record for row 150
            # (row 250 was added and deleted in the same incremental range, so it's filtered out)
            @test length(deletes_values) == 1

            # Sort by position for consistent ordering
            sort!(deletes_values, by = x -> x[2])

            # Extract positions and file paths
            positions = [x[2] for x in deletes_values]
            file_paths = [x[1] for x in deletes_values]

            # Validate the position values and file paths
            @test positions == [10]  # position for row 150 in corresponding data file
            @test all(endswith.(file_paths, ".parquet"))

            # Validate inserts: should have n from 201 to 299 inclusive, except 250
            # That's 98 rows: 201-249 (49) + 251-299 (49)
            @test length(inserts_values) == 98

            # Sort for easier validation
            sort!(inserts_values)

            # Check range and missing 250
            @test minimum(inserts_values) == 201
            @test maximum(inserts_values) == 299
            @test 250 ∉ inserts_values

            # Verify exact expected set
            expected_inserts = vcat(201:249, 251:299)
            @test inserts_values == expected_inserts
            println("✅ Inserts validated: n from 201-299 (excluding 250), total $(length(inserts_values)) rows")
        finally
            # Clean up
            RustyIceberg.free_stream(inserts_stream)
            RustyIceberg.free_stream(deletes_stream)
            RustyIceberg.free_incremental_scan!(scan)
            println("✅ Resources cleaned up")
        end
    end

    @testset "Incremental Scan with Column Selection" begin
        scan2 = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)

        # Select only the "n" column
        RustyIceberg.select_columns!(scan2, ["n"])

        inserts_stream2, deletes_stream2 = RustyIceberg.scan!(scan2)

        try
            # Read all batches from inserts to verify column selection
            batch_ptr = RustyIceberg.next_batch(inserts_stream2)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                # Should only have the "n" column
                @test names(df) == ["n"]
                @test !isempty(df)

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(inserts_stream2)
            end
            println("✅ Incremental scan column selection test successful")
        finally
            RustyIceberg.free_stream(inserts_stream2)
            RustyIceberg.free_stream(deletes_stream2)
            RustyIceberg.free_incremental_scan!(scan2)
        end
    end

    @testset "Incremental Scan with nothing for both snapshot IDs" begin
        # Test scanning from root (nothing) to current (nothing) - full history
        scan3 = new_incremental_scan(table, nothing, nothing)
        @test scan3 isa RustyIceberg.IncrementalScan
        @test scan3.ptr != C_NULL
        println("✅ Incremental scan created with nothing for both snapshot IDs")

        RustyIceberg.with_manifest_file_concurrency_limit!(scan3, UInt(2))
        RustyIceberg.with_manifest_entry_concurrency_limit!(scan3, UInt(256))
        RustyIceberg.with_data_file_concurrency_limit!(scan3, UInt(1024))
        RustyIceberg.with_batch_size!(scan3, UInt(50))

        inserts_stream3, deletes_stream3 = RustyIceberg.scan!(scan3)
        @test inserts_stream3 != C_NULL
        @test deletes_stream3 != C_NULL
        println("✅ Streams obtained successfully for full history scan")

        try
            # Read and validate from DELETES stream FIRST (to test the deadlock fix)
            # This is the scenario that previously caused indefinite blocking
            deletes_values = Tuple{String, Int64}[]
            deletes_batches = 0

            while true
                batch_ptr = RustyIceberg.next_batch(deletes_stream3)
                if batch_ptr == C_NULL
                    break
                end
                deletes_batches += 1
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                @test arrow_table isa Arrow.Table

                # Convert to DataFrame and extract position delete metadata
                df = DataFrame(arrow_table)
                for row in eachrow(df)
                    push!(deletes_values, (row.file_path, row.pos))
                end

                RustyIceberg.free_batch(batch_ptr)
            end

            # Verify we have no delete records (since this is a full history scan)
            @test isempty(deletes_values)

            println("✅ Full history deletes stream validated successfully")
            println("   - Total deletes batches: $deletes_batches")
            println("   - Total delete records: $(length(deletes_values))")

            # Now read and validate from inserts stream AFTER deletes
            inserts_values = Int64[]
            inserts_batches = 0

            while true
                batch_ptr = RustyIceberg.next_batch(inserts_stream3)
                if batch_ptr == C_NULL
                    break
                end
                inserts_batches += 1
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))

                # Convert to DataFrame and collect values from column "n"
                df = DataFrame(arrow_table)
                @test "n" in names(df)
                append!(inserts_values, df.n)

                RustyIceberg.free_batch(batch_ptr)
            end

            # When scanning full history (nothing to nothing), we should get rows from all transactions
            # Verify we have some rows
            @test length(inserts_values) > 0
            sort!(inserts_values)

            # Verify we have expected values from the test table
            # The test table was created with: range(1, 11), range(101, 200), range(201, 300)
            # And row 150 and 250 were deleted
            @test 1 in inserts_values       # From first insert
            @test 101 in inserts_values     # From second insert
            @test 201 in inserts_values     # From third insert
            @test 150 ∉ inserts_values      # Was deleted
            @test 250 ∉ inserts_values      # Was deleted

            println("✅ Full history inserts stream validated successfully")
            println("   - Total inserts batches: $inserts_batches")
            println("   - Total inserts rows: $(length(inserts_values))")
        finally
            RustyIceberg.free_stream(inserts_stream3)
            RustyIceberg.free_stream(deletes_stream3)
            RustyIceberg.free_incremental_scan!(scan3)
        end
    end

    # Clean up table
    RustyIceberg.free_table(table)
    println("✅ Incremental scan test completed successfully!")
end

@testset "Builder API Tests" begin
    println("Testing builder API methods...")

    customer_path = "s3://warehouse/tpch.sf01/customer/metadata/00001-76f6e7e4-b34f-492f-b6a1-cc9f8c8f4975.metadata.json"
    incremental_path = "s3://warehouse/incremental/test1/metadata/00003-359e8bb8-1e5d-46d2-bcde-fdaeaa41114f.metadata.json"
    from_snapshot_id = Int64(6540713100348352610)
    to_snapshot_id = Int64(6832180054960511692)

    @testset "select_columns! - Full Scan" begin
        table = RustyIceberg.table_open(customer_path)
        scan = RustyIceberg.new_scan(table)

        # Select specific columns
        RustyIceberg.select_columns!(scan, ["c_custkey", "c_name"])
        stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                @test names(df) == ["c_custkey", "c_name"]
                @test !isempty(df)

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(stream)
            end
            println("✅ select_columns! test passed for full scan")
        finally
            RustyIceberg.free_stream(stream)
            RustyIceberg.free_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "select_columns! - Incremental Scan" begin
        table = RustyIceberg.table_open(incremental_path)
        scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)

        RustyIceberg.select_columns!(scan, ["n"])
        inserts_stream, deletes_stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(inserts_stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                @test names(df) == ["n"]
                @test !isempty(df)

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(inserts_stream)
            end
            println("✅ select_columns! test passed for incremental scan")
        finally
            RustyIceberg.free_stream(inserts_stream)
            RustyIceberg.free_stream(deletes_stream)
            RustyIceberg.free_incremental_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "with_batch_size! - Full Scan" begin
        table = RustyIceberg.table_open(customer_path)
        scan = RustyIceberg.new_scan(table)

        # Set small batch size
        RustyIceberg.with_batch_size!(scan, UInt(10))
        stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))

                # Batch size should be respected (at most 10 rows)
                @test length(arrow_table) <= 10

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(stream)
            end
            println("✅ with_batch_size! test passed for full scan")
        finally
            RustyIceberg.free_stream(stream)
            RustyIceberg.free_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "with_batch_size! - Incremental Scan" begin
        table = RustyIceberg.table_open(incremental_path)
        scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)

        RustyIceberg.with_batch_size!(scan, UInt(10))
        inserts_stream, deletes_stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(inserts_stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))

                @test length(arrow_table) <= 10

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(inserts_stream)
            end
            println("✅ with_batch_size! test passed for incremental scan")
        finally
            RustyIceberg.free_stream(inserts_stream)
            RustyIceberg.free_stream(deletes_stream)
            RustyIceberg.free_incremental_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "with_data_file_concurrency_limit! - Full Scan" begin
        table = RustyIceberg.table_open(customer_path)
        scan = RustyIceberg.new_scan(table)

        # Set concurrency limit (should not error)
        @test_nowarn RustyIceberg.with_data_file_concurrency_limit!(scan, UInt(4))
        stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(stream)
            while batch_ptr != C_NULL
                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(stream)
            end
            println("✅ with_data_file_concurrency_limit! test passed for full scan")
        finally
            RustyIceberg.free_stream(stream)
            RustyIceberg.free_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "with_data_file_concurrency_limit! - Incremental Scan" begin
        table = RustyIceberg.table_open(incremental_path)
        scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)

        @test_nowarn RustyIceberg.with_data_file_concurrency_limit!(scan, UInt(4))
        inserts_stream, deletes_stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(inserts_stream)
            while batch_ptr != C_NULL
                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(inserts_stream)
            end
            println("✅ with_data_file_concurrency_limit! test passed for incremental scan")
        finally
            RustyIceberg.free_stream(inserts_stream)
            RustyIceberg.free_stream(deletes_stream)
            RustyIceberg.free_incremental_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "with_manifest_entry_concurrency_limit! - Full Scan" begin
        table = RustyIceberg.table_open(customer_path)
        scan = RustyIceberg.new_scan(table)

        # Set concurrency limit (should not error)
        @test_nowarn RustyIceberg.with_manifest_entry_concurrency_limit!(scan, UInt(4))
        stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(stream)
            while batch_ptr != C_NULL
                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(stream)
            end
            println("✅ with_manifest_entry_concurrency_limit! test passed for full scan")
        finally
            RustyIceberg.free_stream(stream)
            RustyIceberg.free_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "with_manifest_file_concurrency_limit! - Full Scan" begin
        table = RustyIceberg.table_open(customer_path)
        scan = RustyIceberg.new_scan(table)

        # Set concurrency limit (should not error)
        @test_nowarn RustyIceberg.with_manifest_file_concurrency_limit!(scan, UInt(4))
        stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(stream)
            while batch_ptr != C_NULL
                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(stream)
            end
            println("✅ with_manifest_file_concurrency_limit! test passed for full scan")
        finally
            RustyIceberg.free_stream(stream)
            RustyIceberg.free_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "with_manifest_entry_concurrency_limit! - Incremental Scan" begin
        table = RustyIceberg.table_open(incremental_path)
        scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)

        @test_nowarn RustyIceberg.with_manifest_entry_concurrency_limit!(scan, UInt(4))
        inserts_stream, deletes_stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(inserts_stream)
            while batch_ptr != C_NULL
                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(inserts_stream)
            end
            println("✅ with_manifest_entry_concurrency_limit! test passed for incremental scan")
        finally
            RustyIceberg.free_stream(inserts_stream)
            RustyIceberg.free_stream(deletes_stream)
            RustyIceberg.free_incremental_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "Combined Builder Methods - Full Scan" begin
        table = RustyIceberg.table_open(customer_path)
        scan = RustyIceberg.new_scan(table)

        # Combine multiple builder methods
        RustyIceberg.select_columns!(scan, ["c_custkey", "c_name", "c_address"])
        RustyIceberg.with_batch_size!(scan, UInt(5))
        RustyIceberg.with_data_file_concurrency_limit!(scan, UInt(2))
        RustyIceberg.with_manifest_entry_concurrency_limit!(scan, UInt(2))
        RustyIceberg.with_serialization_concurrency_limit!(scan, UInt(2))

        stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                # Verify all configurations
                @test names(df) == ["c_custkey", "c_name", "c_address"]
                @test length(arrow_table) <= 5
                @test !isempty(df)

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(stream)
            end
            println("✅ Combined builder methods test passed for full scan")
        finally
            RustyIceberg.free_stream(stream)
            RustyIceberg.free_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "Combined Builder Methods - Incremental Scan" begin
        table = RustyIceberg.table_open(incremental_path)
        scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)

        # Combine multiple builder methods
        RustyIceberg.select_columns!(scan, ["n"])
        RustyIceberg.with_batch_size!(scan, UInt(5))
        RustyIceberg.with_data_file_concurrency_limit!(scan, UInt(2))
        RustyIceberg.with_manifest_file_concurrency_limit!(scan, UInt(2))
        RustyIceberg.with_manifest_entry_concurrency_limit!(scan, UInt(2))
        RustyIceberg.with_serialization_concurrency_limit!(scan, UInt(2))

        inserts_stream, deletes_stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(inserts_stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                @test names(df) == ["n"]
                @test length(arrow_table) <= 5
                @test !isempty(df)

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(inserts_stream)
            end
            println("✅ Combined builder methods test passed for incremental scan")
        finally
            RustyIceberg.free_stream(inserts_stream)
            RustyIceberg.free_stream(deletes_stream)
            RustyIceberg.free_incremental_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "select_columns! with with_file_column! - Full Scan" begin
        table = RustyIceberg.table_open(customer_path)
        scan = RustyIceberg.new_scan(table)

        # Select specific columns AND include file metadata
        RustyIceberg.select_columns!(scan, ["c_custkey", "c_name"])
        RustyIceberg.with_file_column!(scan)
        stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                # Should have selected columns plus file column
                @test "c_custkey" in names(df)
                @test "c_name" in names(df)
                @test "_file" in names(df)
                @test !isempty(df)

                # Verify file column contains file paths (strings ending in .parquet)
                file_paths = df._file
                @test all(endswith.(file_paths, ".parquet"))
                # Verify file paths are non-empty strings with proper structure
                @test all(length.(file_paths) .> 0)
                # Should be full S3 paths like "s3://warehouse/tpch.sf01/customer/data/data_customer-00000.parquet"
                @test all(startswith.(file_paths, "s3://warehouse/tpch.sf01/customer/data/data_customer-"))
                @test eltype(file_paths) <: AbstractString

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(stream)
            end
            println("✅ select_columns! with with_file_column! test passed for full scan")
        finally
            RustyIceberg.free_stream(stream)
            RustyIceberg.free_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "select_columns! with with_file_column! - Incremental Scan" begin
        table = RustyIceberg.table_open(incremental_path)
        scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)

        # Select specific column AND include file metadata for incremental scan
        RustyIceberg.select_columns!(scan, ["n"])
        RustyIceberg.with_file_column!(scan)
        inserts_stream, deletes_stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(inserts_stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                # Should have the selected column "n" plus file column
                @test "n" in names(df)
                @test "_file" in names(df)
                @test !isempty(df)

                # Verify file column contains file paths
                file_paths = df._file
                @test all(endswith.(file_paths, ".parquet"))
                @test all(length.(file_paths) .> 0)
                @test all(startswith.(file_paths, "s3://warehouse/incremental/"))
                @test eltype(file_paths) <: AbstractString

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(inserts_stream)
            end
            println("✅ select_columns! with with_file_column! test passed for incremental scan")
        finally
            RustyIceberg.free_stream(inserts_stream)
            RustyIceberg.free_stream(deletes_stream)
            RustyIceberg.free_incremental_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "select_columns! with FILE_COLUMN constant" begin
        table = RustyIceberg.table_open(customer_path)
        scan = RustyIceberg.new_scan(table)

        # Select columns including FILE_COLUMN constant
        RustyIceberg.select_columns!(scan, ["c_custkey", "c_name", RustyIceberg.FILE_COLUMN])
        stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                # Should have selected columns
                @test "c_custkey" in names(df)
                @test "c_name" in names(df)
                # FILE_COLUMN should be "_file"
                @test "_file" in names(df)
                @test !isempty(df)

                # Verify file column contains file paths
                file_paths = df._file
                @test all(endswith.(file_paths, ".parquet"))
                @test all(length.(file_paths) .> 0)
                @test all(startswith.(file_paths, "s3://warehouse/tpch.sf01/customer/data/data_customer-"))
                @test eltype(file_paths) <: AbstractString

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(stream)
            end
            println("✅ select_columns! with FILE_COLUMN constant test passed")
        finally
            RustyIceberg.free_stream(stream)
            RustyIceberg.free_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "select_columns! with with_pos_column! - Full Scan" begin
        table = RustyIceberg.table_open(customer_path)
        scan = RustyIceberg.new_scan(table)

        # Select specific columns AND include pos metadata
        RustyIceberg.select_columns!(scan, ["c_custkey", "c_name"])
        RustyIceberg.with_pos_column!(scan)
        stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                # Should have selected columns plus pos column
                @test "c_custkey" in names(df)
                @test "c_name" in names(df)
                @test "_pos" in names(df)
                @test !isempty(df)

                # Verify pos column contains non-negative integers
                positions = df._pos
                @test all(positions .>= 0)
                @test eltype(positions) <: Integer

                # Verify positions - represent row position within the source file
                # Positions should be unique within the batch
                @test length(unique(positions)) == length(positions)
                # Positions should be non-negative and sequential (form a contiguous range)
                sorted_pos = sort(positions)
                @test all(diff(sorted_pos) .== 1)  # Sequential with no gaps

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(stream)
            end
            println("✅ select_columns! with with_pos_column! test passed for full scan")
        finally
            RustyIceberg.free_stream(stream)
            RustyIceberg.free_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "select_columns! with with_pos_column! - Incremental Scan" begin
        table = RustyIceberg.table_open(incremental_path)
        scan = new_incremental_scan(table, from_snapshot_id, to_snapshot_id)

        # Select specific column AND include pos metadata for incremental scan
        RustyIceberg.select_columns!(scan, ["n"])
        RustyIceberg.with_pos_column!(scan)
        inserts_stream, deletes_stream = RustyIceberg.scan!(scan)

        try
            all_positions = Int64[]
            all_n_values = Int64[]

            batch_ptr = RustyIceberg.next_batch(inserts_stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                # Should have the selected column "n" plus pos column
                @test "n" in names(df)
                @test "_pos" in names(df)
                @test !isempty(df)

                # Gather positions and values
                positions = df._pos
                @test all(positions .>= 0)
                @test eltype(positions) <: Integer
                @test length(unique(positions)) == length(positions)  # No duplicates within batch

                append!(all_positions, positions)
                append!(all_n_values, df.n)

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(inserts_stream)
            end

            # Verify positions across all batches
            # Positions represent row numbers within individual Parquet files (0-indexed)
            # The incremental scan reads from multiple Parquet files, each with ~20 rows

            @test length(all_positions) == 98  # 98 total records (n=201-299 excluding deleted n=250)
            @test all(all_positions .>= 0)
            @test eltype(all_positions) <: Integer

            # Count occurrences of each position across all files
            position_counts = Dict{Int64, Int}()
            for pos in all_positions
                position_counts[pos] = get(position_counts, pos, 0) + 1
            end

            # Baseline expectations:
            # - Positions 0-19 represent rows within each Parquet file
            # - Most positions appear multiple times (once per file)
            # - The deleted record (n=250) creates one missing occurrence

            # Verify all positions are in expected range (0-19 per file)
            @test minimum(all_positions) == 0
            @test maximum(all_positions) == 19
            @test length(unique(all_positions)) == 20  # All positions 0-19 are present

            # Expected baseline: positions appear with these frequencies
            # (derived from actual data structure where n=250 is deleted)
            expected_counts = Dict{Int64, Int}()
            for pos in 0:19
                if pos == 10
                    expected_counts[pos] = 4  # Missing one occurrence (deleted record n=250 at position 10)
                elseif pos == 19
                    expected_counts[pos] = 4  # Some files have only 19 rows
                else
                    expected_counts[pos] = 5  # Most positions appear 5 times (once per file)
                end
            end

            @test position_counts == expected_counts

            # Verify no gaps in the position range
            full_range = 0:19
            @test Set(keys(position_counts)) == Set(full_range)

            println("✅ select_columns! with with_pos_column! test passed for incremental scan")
        finally
            RustyIceberg.free_stream(inserts_stream)
            RustyIceberg.free_stream(deletes_stream)
            RustyIceberg.free_incremental_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "select_columns! with POS_COLUMN constant" begin
        table = RustyIceberg.table_open(customer_path)
        scan = RustyIceberg.new_scan(table)

        # Select columns including POS_COLUMN constant
        RustyIceberg.select_columns!(scan, ["c_custkey", "c_name", RustyIceberg.POS_COLUMN])
        stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                # Should have selected columns
                @test "c_custkey" in names(df)
                @test "c_name" in names(df)
                # POS_COLUMN should be "_pos"
                @test "_pos" in names(df)
                @test !isempty(df)

                # Verify pos column contains non-negative integers
                positions = df._pos
                @test all(positions .>= 0)
                @test eltype(positions) <: Integer

                # Verify positions - represent row position within the source file
                @test length(unique(positions)) == length(positions)
                sorted_pos = sort(positions)
                @test all(diff(sorted_pos) .== 1)  # Sequential with no gaps

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(stream)
            end
            println("✅ select_columns! with POS_COLUMN constant test passed")
        finally
            RustyIceberg.free_stream(stream)
            RustyIceberg.free_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end

    @testset "with_file_column! and with_pos_column! combined" begin
        table = RustyIceberg.table_open(customer_path)
        scan = RustyIceberg.new_scan(table)

        # Select columns and include both file and pos metadata
        RustyIceberg.select_columns!(scan, ["c_custkey", "c_name"])
        RustyIceberg.with_file_column!(scan)
        RustyIceberg.with_pos_column!(scan)
        stream = RustyIceberg.scan!(scan)

        try
            batch_ptr = RustyIceberg.next_batch(stream)
            while batch_ptr != C_NULL
                batch = unsafe_load(batch_ptr)
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)

                # Should have selected columns plus both metadata columns
                @test "c_custkey" in names(df)
                @test "c_name" in names(df)
                @test "_file" in names(df)
                @test "_pos" in names(df)
                @test !isempty(df)

                # Verify both metadata columns
                file_paths = df._file
                @test all(endswith.(file_paths, ".parquet"))
                @test all(startswith.(file_paths, "s3://warehouse/tpch.sf01/customer/data/data_customer-"))
                @test all(df._pos .>= 0)
                @test eltype(df._pos) <: Integer

                # Verify positions - represent row position within the source file
                positions = df._pos
                @test length(unique(positions)) == length(positions)
                sorted_pos = sort(positions)
                @test all(diff(sorted_pos) .== 1)  # Sequential with no gaps

                RustyIceberg.free_batch(batch_ptr)
                batch_ptr = RustyIceberg.next_batch(stream)
            end
            println("✅ with_file_column! and with_pos_column! combined test passed")
        finally
            RustyIceberg.free_stream(stream)
            RustyIceberg.free_scan!(scan)
            RustyIceberg.free_table(table)
        end
    end
end
