using RustyIceberg
using Test

@testset "Transaction API" begin
    println("Testing transaction API...")

    catalog_uri = get_catalog_uri()
    props = get_catalog_properties()

    catalog = nothing
    table = C_NULL
    tx = nothing

    try
        # Create catalog connection
        catalog = RustyIceberg.catalog_create_rest(catalog_uri; properties=props)
        @test catalog !== nothing
        println("✅ Catalog created successfully")

        # Load an existing table
        println("Loading customer table from tpch.sf01...")
        table = RustyIceberg.load_table(catalog, ["tpch.sf01"], "customer")
        @test table != C_NULL
        println("✅ Table loaded successfully")

        # Test 1: Create a transaction
        println("\nTest 1: Creating transaction...")
        tx = RustyIceberg.Transaction(table)
        @test tx !== nothing
        @test tx.ptr != C_NULL
        println("✅ Transaction created successfully")

        # Test 2: Free the transaction
        println("\nTest 2: Freeing transaction...")
        RustyIceberg.free_transaction!(tx)
        @test tx.ptr == C_NULL
        println("✅ Transaction freed successfully")

        # Test 3: Double-free safety
        println("\nTest 3: Testing double-free safety...")
        RustyIceberg.free_transaction!(tx)  # Should not crash
        @test tx.ptr == C_NULL
        println("✅ Double-free is safe")

        # Test 4: Create another transaction for commit test
        println("\nTest 4: Creating transaction for empty commit test...")
        tx = RustyIceberg.Transaction(table)
        @test tx !== nothing
        @test tx.ptr != C_NULL
        println("✅ Transaction created successfully")

        # Test 5: Commit empty transaction (no data files added)
        # This should work - an empty transaction just returns the same table state
        println("\nTest 5: Committing empty transaction...")
        updated_table = RustyIceberg.commit(tx, catalog)
        @test updated_table != C_NULL
        println("✅ Empty transaction committed successfully")

        # Free the updated table
        RustyIceberg.free_table(updated_table)
        println("✅ Updated table freed successfully")

    finally
        # Clean up all resources in reverse order
        if tx !== nothing && tx.ptr != C_NULL
            RustyIceberg.free_transaction!(tx)
            println("✅ Transaction cleaned up")
        end
        if table != C_NULL
            RustyIceberg.free_table(table)
            println("✅ Table cleaned up")
        end
        if catalog !== nothing
            RustyIceberg.free_catalog!(catalog)
            println("✅ Catalog cleaned up")
        end
    end

    println("\n✅ Transaction API tests completed!")
end

@testset "Transaction Error Handling" begin
    println("Testing transaction error handling...")

    catalog_uri = get_catalog_uri()
    props = get_catalog_properties()

    catalog = nothing
    table = C_NULL
    tx = nothing

    try
        # Create catalog and load table
        catalog = RustyIceberg.catalog_create_rest(catalog_uri; properties=props)
        table = RustyIceberg.load_table(catalog, ["tpch.sf01"], "customer")

        # Test 1: Commit on freed transaction should fail
        println("\nTest 1: Testing commit on freed transaction...")
        tx = RustyIceberg.Transaction(table)
        RustyIceberg.free_transaction!(tx)

        error_caught = false
        try
            RustyIceberg.commit(tx, catalog)
        catch e
            error_caught = true
            @test e isa RustyIceberg.IcebergException
            println("✅ Correctly caught exception for freed transaction: $(e.msg)")
        end
        @test error_caught
        tx = nothing  # Already freed

        # Test 2: Create transaction, commit, then try to commit again
        println("\nTest 2: Testing double commit...")
        tx = RustyIceberg.Transaction(table)
        updated_table = RustyIceberg.commit(tx, catalog)
        @test updated_table != C_NULL
        RustyIceberg.free_table(updated_table)

        # Transaction is consumed after commit, second commit should fail
        error_caught = false
        try
            RustyIceberg.commit(tx, catalog)
        catch e
            error_caught = true
            @test e isa RustyIceberg.IcebergException
            println("✅ Correctly caught exception for consumed transaction: $(e.msg)")
        end
        @test error_caught

    finally
        if tx !== nothing && tx.ptr != C_NULL
            RustyIceberg.free_transaction!(tx)
        end
        if table != C_NULL
            RustyIceberg.free_table(table)
        end
        if catalog !== nothing
            RustyIceberg.free_catalog!(catalog)
        end
    end

    println("\n✅ Transaction error handling tests completed!")
end
