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

@testset "Read and verify nations table" begin
    # Test reading the nations table and verify contents
    nations_snapshot_path = "s3://warehouse/tpch.sf01/nation/metadata/00001-44f668fe-3688-49d5-851f-36e75d143321.metadata.json"

    println("Testing reading nations table...")
    nations_iterator = read_table(nations_snapshot_path)
    @test nations_iterator isa TableIterator

    rows = Tuple[]
    for arrow_table in nations_iterator
        df = DataFrame(arrow_table)
        expected_columns = ["n_nationkey", "n_name", "n_regionkey", "n_comment"]
        @test names(df) == expected_columns
        # Collect rows as tuples for easier verification
        for row in eachrow(df)
            push!(rows, Tuple(row))
        end
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

    println("✅ Nations table read and verified successfully")
end

end # End of testset

println("\n🎉 All tests completed!")
