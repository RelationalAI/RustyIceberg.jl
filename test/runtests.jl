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
                if batch_ptr != C_NULL
                    batch = unsafe_load(batch_ptr)
                    arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                    @test arrow_table isa Arrow.Table
                    @test length(arrow_table) <= 8
                    push!(selected_arrow_tables, arrow_table)
                    RustyIceberg.free_batch(batch_ptr)
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

    # The table is created with these transactions. Above snapshot IDs are for the 2nd and the 4th txn below:
    #=
        CREATE TABLE demo.incremental.test1 using iceberg
        TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')
        AS (SELECT n FROM range(1, 11) r(n));

        INSERT INTO incremental.test1
        select n from range(101, 200) r(n);

        INSERT INTO incremental.test1
        select n from range(201, 300) r(n);

        delete from incremental.test1 where n = 150 or n = 250;
    =#

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

    # Clean up table
    RustyIceberg.free_table(table)
    println("✅ Incremental scan test completed successfully!")
end

end # End of testset

println("\n🎉 All tests completed!")
