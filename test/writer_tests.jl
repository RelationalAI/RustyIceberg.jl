using RustyIceberg
using Test
using Tables

@testset "Writer API" begin
    println("Testing writer API...")

    catalog_uri = get_catalog_uri()
    props = get_catalog_properties()

    catalog = nothing
    table = C_NULL
    writer = nothing
    data_files = nothing
    test_namespace = nothing
    table_name = nothing

    try
        # Create catalog connection
        catalog = RustyIceberg.catalog_create_rest(catalog_uri; properties=props)
        @test catalog !== nothing
        println("✅ Catalog created successfully")

        # Create a test namespace for table creation
        test_namespace = ["test_writer_$(round(Int, time() * 1000))"]
        RustyIceberg.create_namespace(catalog, test_namespace)
        println("✅ Test namespace created: $test_namespace")

        # Create a schema for test table
        schema = Schema([
            Field(Int32(1), "id", "long"; required=true),
            Field(Int32(2), "name", "string"; required=false),
            Field(Int32(3), "value", "double"; required=false),
        ])

        # Create test table
        table_name = "writer_test_$(round(Int, time() * 1000))"
        table = RustyIceberg.create_table(
            catalog,
            test_namespace,
            table_name,
            schema
        )
        @test table != C_NULL
        println("✅ Test table created: $table_name")

        # Test 1: Create a writer
        println("\nTest 1: Creating writer...")
        writer = RustyIceberg.DataFileWriter(table)
        @test writer !== nothing
        @test writer.ptr != C_NULL
        println("✅ Writer created successfully")

        # Test 2: Write data to the writer
        println("\nTest 2: Writing data...")
        test_data = (
            id = Int64[1, 2, 3, 4, 5],
            name = ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            value = [1.1, 2.2, 3.3, 4.4, 5.5]
        )
        write(writer, test_data)
        println("✅ Data written successfully")

        # Test 3: Write more data
        println("\nTest 3: Writing more data...")
        more_data = (
            id = Int64[6, 7, 8],
            name = ["Frank", "Grace", "Henry"],
            value = [6.6, 7.7, 8.8]
        )
        write(writer, more_data)
        println("✅ More data written successfully")

        # Test 4: Close writer and get data files
        println("\nTest 4: Closing writer...")
        data_files = RustyIceberg.close_writer!(writer)
        @test data_files !== nothing
        @test data_files.ptr != C_NULL
        println("✅ Writer closed successfully, got DataFiles handle")

        # Test 5: Create transaction and append data files
        println("\nTest 5: Committing data files via transaction...")
        tx = RustyIceberg.Transaction(table)
        RustyIceberg.fast_append!(tx, data_files)
        updated_table = RustyIceberg.commit!(tx, catalog)
        @test updated_table != C_NULL
        println("✅ Transaction committed successfully")

        # Test 6: Verify table exists in catalog by loading it fresh
        println("\nTest 6: Verifying table exists in catalog...")
        reloaded_table = RustyIceberg.load_table(catalog, test_namespace, table_name)
        @test reloaded_table != C_NULL
        println("✅ Table exists in catalog and can be loaded")
        RustyIceberg.free_table(reloaded_table)

        # Test 7: Verify data was written by scanning the table
        println("\nTest 7: Verifying written data...")
        scan = RustyIceberg.new_scan(updated_table)
        stream = RustyIceberg.scan!(scan)

        # Collect all data from the scan
        all_ids = Int64[]
        all_names = String[]
        all_values = Float64[]

        batch_ptr = RustyIceberg.next_batch(stream)
        while batch_ptr != C_NULL
            batch = unsafe_load(batch_ptr)
            # Convert batch to Arrow table
            data = batch.data
            len = batch.length
            if len > 0
                arrow_data = unsafe_wrap(Array, data, len)
                tbl = Arrow.Table(arrow_data)
                cols = Tables.columns(tbl)
                append!(all_ids, cols.id)
                append!(all_names, cols.name)
                append!(all_values, cols.value)
            end
            RustyIceberg.free_batch(batch_ptr)
            batch_ptr = RustyIceberg.next_batch(stream)
        end
        RustyIceberg.free_stream(stream)
        RustyIceberg.free_scan!(scan)

        # Verify row count
        @test length(all_ids) == 8  # 5 + 3 rows written
        println("✅ Verified $(length(all_ids)) rows in table")

        # Sort by id for consistent comparison
        perm = sortperm(all_ids)
        sorted_ids = all_ids[perm]
        sorted_names = all_names[perm]
        sorted_values = all_values[perm]

        # Verify exact data matches what we wrote
        expected_ids = Int64[1, 2, 3, 4, 5, 6, 7, 8]
        expected_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
        expected_values = [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8]

        @test sorted_ids == expected_ids
        @test sorted_names == expected_names
        @test sorted_values == expected_values
        println("✅ Verified data content matches exactly")

        # Clean up updated table
        RustyIceberg.free_table(updated_table)
        RustyIceberg.free_transaction!(tx)

    finally
        # Clean up all resources in reverse order
        if data_files !== nothing && data_files.ptr != C_NULL
            RustyIceberg.free_data_files!(data_files)
            println("✅ DataFiles cleaned up")
        end
        if writer !== nothing && writer.ptr != C_NULL
            RustyIceberg.free_writer!(writer)
            println("✅ Writer cleaned up")
        end
        if table != C_NULL
            RustyIceberg.free_table(table)
            println("✅ Table cleaned up")
        end
        # Drop table and namespace
        if table_name !== nothing && test_namespace !== nothing && catalog !== nothing
            RustyIceberg.drop_table(catalog, test_namespace, table_name)
            println("✅ Test table dropped")
        end
        if test_namespace !== nothing && catalog !== nothing
            RustyIceberg.drop_namespace(catalog, test_namespace)
            println("✅ Test namespace dropped")
        end
        if catalog !== nothing
            RustyIceberg.free_catalog!(catalog)
            println("✅ Catalog cleaned up")
        end
    end

    println("\n✅ Writer API tests completed!")
end

@testset "Writer Error Handling" begin
    println("Testing writer error handling...")

    catalog_uri = get_catalog_uri()
    props = get_catalog_properties()

    catalog = nothing
    table = C_NULL
    writer = nothing
    test_namespace = nothing
    table_name = nothing

    try
        # Create catalog and table
        catalog = RustyIceberg.catalog_create_rest(catalog_uri; properties=props)

        test_namespace = ["test_writer_err_$(round(Int, time() * 1000))"]
        RustyIceberg.create_namespace(catalog, test_namespace)

        schema = Schema([
            Field(Int32(1), "id", "long"; required=true),
            Field(Int32(2), "name", "string"; required=false),
        ])

        table_name = "writer_err_test_$(round(Int, time() * 1000))"
        table = RustyIceberg.create_table(catalog, test_namespace, table_name, schema)

        # Test 1: Write to freed writer should fail
        println("\nTest 1: Testing write to freed writer...")
        writer = RustyIceberg.DataFileWriter(table)
        RustyIceberg.free_writer!(writer)

        error_caught = false
        try
            write(writer, (id = Int64[1], name = ["test"]))
        catch e
            error_caught = true
            @test e isa RustyIceberg.IcebergException
            println("✅ Correctly caught exception for freed writer: $(e.msg)")
        end
        @test error_caught
        writer = nothing  # Already freed

        # Test 2: Close already closed writer should fail
        println("\nTest 2: Testing double close...")
        writer = RustyIceberg.DataFileWriter(table)
        write(writer, (id = Int64[1], name = ["test"]))
        data_files = RustyIceberg.close_writer!(writer)
        @test data_files !== nothing
        RustyIceberg.free_data_files!(data_files)

        error_caught = false
        try
            RustyIceberg.close_writer!(writer)
        catch e
            error_caught = true
            @test e isa RustyIceberg.IcebergException
            println("✅ Correctly caught exception for double close: $(e.msg)")
        end
        @test error_caught

        # Test 3: Free writer safety (should not crash)
        println("\nTest 3: Testing free safety...")
        RustyIceberg.free_writer!(writer)  # Should be safe after close
        RustyIceberg.free_writer!(writer)  # Double free should also be safe
        @test writer.ptr == C_NULL
        println("✅ Free operations are safe")

    finally
        if writer !== nothing && writer.ptr != C_NULL
            RustyIceberg.free_writer!(writer)
            println("✅ Writer cleaned up")
        end
        if table != C_NULL
            RustyIceberg.free_table(table)
            println("✅ Table cleaned up")
        end
        # Drop table and namespace
        if table_name !== nothing && test_namespace !== nothing && catalog !== nothing
            RustyIceberg.drop_table(catalog, test_namespace, table_name)
            println("✅ Test table dropped")
        end
        if test_namespace !== nothing && catalog !== nothing
            RustyIceberg.drop_namespace(catalog, test_namespace)
            println("✅ Test namespace dropped")
        end
        if catalog !== nothing
            RustyIceberg.free_catalog!(catalog)
            println("✅ Catalog cleaned up")
        end
    end

    println("\n✅ Writer error handling tests completed!")
end
