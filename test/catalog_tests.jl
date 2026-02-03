using RustyIceberg
using Test
using DataFrames
using Arrow
using HTTP
using JSON
using Base64
using RustyIceberg: Field, Schema, PartitionSpec, SortOrder, PartitionField

@testset "Catalog API" begin
    println("Testing catalog API...")

    # Test connecting to Polaris REST catalog running at localhost:8181
    # This requires the catalog to be running via docker-compose
    catalog_uri = "http://localhost:8181/api/catalog"

    catalog = nothing
    try
        # Test catalog creation with REST API and Polaris credentials
        props = Dict(
            "credential" => "root:s3cr3t",
            "scope" => "PRINCIPAL_ROLE:ALL",
            "warehouse" => "warehouse"
        )
        catalog = RustyIceberg.catalog_create_rest(catalog_uri; properties=props)
        @test catalog !== nothing
        println("✅ Catalog created successfully at $catalog_uri with authentication")

        # Test listing namespaces
        println("Attempting to list namespaces...")
        root_namespaces = RustyIceberg.list_namespaces(catalog)
        @test isa(root_namespaces, Vector{Vector{String}})
        @test length(root_namespaces) >= 2
        println("✅ Root namespaces listed: $root_namespaces")

        # Verify expected namespaces exist in the Polaris test catalog
        @test ["incremental"] in root_namespaces
        @test ["tpch.sf01"] in root_namespaces
        println("✅ Expected namespaces verified: 'incremental' and 'tpch.sf01'")

        # Verify the exact namespaces match what we expect
        expected_namespaces = [["incremental"], ["tpch.sf01"]]
        @test sort(root_namespaces) == sort(expected_namespaces)
        println("✅ Namespace list matches exactly: $expected_namespaces")

        # Test listing tables in tpch.sf01 namespace
        println("Attempting to list tables in tpch.sf01...")
        tpch_tables = RustyIceberg.list_tables(catalog, ["tpch.sf01"])
        @test isa(tpch_tables, Vector{String})
        @test length(tpch_tables) > 0
        println("✅ Tables in tpch.sf01: $tpch_tables")

        # Verify expected TPCH tables exist
        expected_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
        for table in expected_tables
            @test table in tpch_tables
        end
        println("✅ All expected TPCH tables found: $expected_tables")

        # Test table existence check for TPCH tables
        println("Verifying table existence for TPCH tables...")
        for table in expected_tables
            exists = RustyIceberg.table_exists(catalog, ["tpch.sf01"], table)
            @test exists == true
        end
        println("✅ All TPCH tables verified to exist: $expected_tables")

        # Test that a non-existent table returns false
        nonexistent_exists = RustyIceberg.table_exists(catalog, ["tpch.sf01"], "nonexistent_table")
        @test nonexistent_exists == false
        println("✅ Non-existent table correctly returns false")
    finally
        # Clean up
        if catalog !== nothing
            RustyIceberg.free_catalog!(catalog)
            println("✅ Catalog cleaned up successfully")
            RustyIceberg.free_catalog!(catalog) # Double free to test safety
        end
    end

    println("✅ Catalog API tests completed!")
end

@testset "Catalog API with Token Authentication" begin
    println("Testing catalog API with token-based authentication...")

    # Token endpoint
    token_endpoint = "http://localhost:8181/api/catalog/v1/oauth/tokens"
    catalog_uri = "http://localhost:8181/api/catalog"

    catalog = nothing
    try
        # Step 1: Fetch access token using client credentials
        println("Fetching access token...")

        client_id = "root"
        client_secret = "s3cr3t"
        realm = "POLARIS"

        # Make HTTP request to token endpoint with basic auth
        # Construct basic auth header manually
        credentials = base64encode("$client_id:$client_secret")
        auth_header = "Basic $credentials"

        # Send form-encoded body (application/x-www-form-urlencoded)
        body = "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL"

        token_response = HTTP.post(
            token_endpoint;
            headers=[
                "Authorization" => auth_header,
                "Polaris-Realm" => realm,
                "Content-Type" => "application/x-www-form-urlencoded"
            ],
            body=body,
            status_exception=false
        )

        if token_response.status != 200
            error("Failed to fetch token: $(String(token_response.body))")
        end

        token_data = JSON.parse(String(token_response.body))
        access_token = token_data["access_token"]
        @test !isempty(access_token)
        println("✅ Access token obtained successfully")

        # Step 2: Create catalog using token-based authentication
        println("Creating catalog with token authentication...")
        props = Dict(
            "token" => access_token,
            "warehouse" => "warehouse"
        )
        catalog = RustyIceberg.catalog_create_rest(catalog_uri; properties=props)
        @test catalog !== nothing
        println("✅ Catalog created successfully with token authentication")

        # Step 3: List namespaces to verify authentication works
        println("Listing namespaces with token authentication...")
        root_namespaces = RustyIceberg.list_namespaces(catalog)
        @test isa(root_namespaces, Vector{Vector{String}})
        @test length(root_namespaces) >= 2
        println("✅ Namespaces listed: $root_namespaces")

        # Step 4: Verify expected namespaces exist
        @test ["tpch.sf01"] in root_namespaces
        println("✅ Expected namespace 'tpch.sf01' verified")

        # Step 5: List tables in tpch.sf01
        println("Listing tables in tpch.sf01...")
        tpch_tables = RustyIceberg.list_tables(catalog, ["tpch.sf01"])
        @test isa(tpch_tables, Vector{String})
        @test length(tpch_tables) > 0
        println("✅ Tables in tpch.sf01: $tpch_tables")

        # Verify expected TPCH tables exist
        expected_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
        for table in expected_tables
            @test table in tpch_tables
        end
        println("✅ All expected TPCH tables found: $expected_tables")
    finally
        # Clean up
        if catalog !== nothing
            RustyIceberg.free_catalog!(catalog)
            println("✅ Catalog cleaned up successfully")
        end
    end

    println("✅ Catalog API with token authentication tests completed!")
end

@testset "Catalog API with Custom Authenticator Function" begin
    println("Testing catalog API with custom authenticator function...")

    # Token endpoint
    token_endpoint = "http://localhost:8181/api/catalog/v1/oauth/tokens"
    catalog_uri = "http://localhost:8181/api/catalog"

    # Client credentials
    client_id = "root"
    client_secret = "s3cr3t"
    realm = "POLARIS"

    # Create a custom authenticator function that fetches tokens on demand
    function authenticator()
        # Track number of actual token fetches
        fetch_count = Threads.Atomic{Int}(0)
        # Cache the token - use Ref to store it safely
        cached_token = Ref{Union{Nothing, String}}(nothing)
        token_lock = Threads.ReentrantLock()

        function get_token_impl()
            # Spawn on a Julia thread to avoid FFI thread issues
            task = Threads.@spawn begin
                lock(token_lock) do
                    # Check if we have a cached token
                    if cached_token[] !== nothing
                        return cached_token[]::String
                    end

                    # Fetch access token using client credentials
                    credentials = base64encode("$client_id:$client_secret")
                    auth_header = "Basic $credentials"
                    body = "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL"

                    token_response = HTTP.post(
                        token_endpoint;
                        headers=[
                            "Authorization" => auth_header,
                            "Polaris-Realm" => realm,
                            "Content-Type" => "application/x-www-form-urlencoded"
                        ],
                        body=body,
                        status_exception=false
                    )

                    if token_response.status != 200
                        error("Failed to fetch token: $(String(token_response.body))")
                    end

                    token_data = JSON.parse(String(token_response.body))
                    token = token_data["access_token"]

                    # Increment fetch count ONLY when actually fetching from server
                    Threads.atomic_add!(fetch_count, 1)

                    # Cache the token for future use
                    cached_token[] = token

                    return token
                end
            end
            return fetch(task)
        end

        return get_token_impl, fetch_count, cached_token
    end

    auth_fn, fetch_counter, cached_token_ref = authenticator()

    catalog = nothing
    try
        # Test catalog creation with custom authenticator function
        println("Creating catalog with custom authenticator function...")
        props = Dict(
            "warehouse" => "warehouse"
        )
        catalog = RustyIceberg.catalog_create_rest(FunctionWrapper{Union{String,Nothing},Tuple{}}(auth_fn), catalog_uri; properties=props)
        @test catalog !== nothing
        println("✅ Catalog created successfully with custom authenticator function")

        # Test listing namespaces to verify authentication works
        println("Listing namespaces with custom authenticator...")
        root_namespaces = RustyIceberg.list_namespaces(catalog)
        @test isa(root_namespaces, Vector{Vector{String}})
        @test length(root_namespaces) >= 2
        println("✅ Namespaces listed: $root_namespaces")

        # Verify expected namespaces exist
        @test ["tpch.sf01"] in root_namespaces
        println("✅ Expected namespace 'tpch.sf01' verified")

        # Test listing tables in tpch.sf01
        println("Listing tables in tpch.sf01...")
        tpch_tables = RustyIceberg.list_tables(catalog, ["tpch.sf01"])
        @test isa(tpch_tables, Vector{String})
        @test length(tpch_tables) > 0
        println("✅ Tables in tpch.sf01: $tpch_tables")

        # Verify expected TPCH tables exist
        expected_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
        for table in expected_tables
            @test table in tpch_tables
        end
        println("✅ All expected TPCH tables found: $expected_tables")

        # Test table existence check
        println("Verifying table existence for TPCH tables...")
        for table in expected_tables
            exists = RustyIceberg.table_exists(catalog, ["tpch.sf01"], table)
            @test exists == true
        end
        println("✅ All TPCH tables verified to exist: $expected_tables")

        # Verify that authenticator fetched the token exactly once (subsequent calls use cached token)
        final_fetch_count = fetch_counter[]
        println("✅ Authenticator fetched token $final_fetch_count time(s)")
        @test final_fetch_count == 1

        # Test token invalidation and re-fetch
        println("\nTesting token invalidation and re-fetch...")

        # Invalidate the cached token by setting it to an invalid value
        cached_token_ref[] = "invalid_token_foo"
        println("✅ Invalidated cached token")

        # Attempt to list namespaces with invalid token - should fail with 401
        println("Attempting to list namespaces with invalid token...")
        error_occurred = false
        error_msg = ""
        try
            root_namespaces = RustyIceberg.list_namespaces(catalog)
            # If we get here, the test should fail
        catch err
            error_occurred = true
            error_msg = string(err)
            println("✅ Got expected error with invalid token: $(typeof(err))")
            println("   Error message: $error_msg")
        end

        # Verify that an error occurred when using invalid token
        @test error_occurred

        # Check if error message contains indication of 401 or authentication failure
        has_401 = contains(error_msg, "401")
        has_unauthorized = contains(error_msg, "Unauthorized") || contains(error_msg, "unauthorized")
        has_unexpected = contains(error_msg, "unexpected status code")
        has_auth_error = has_401 || has_unauthorized || has_unexpected

        @test has_auth_error

        println("✅ Token invalidation test passed")
    finally
        # Clean up
        if catalog !== nothing
            RustyIceberg.free_catalog!(catalog)
            println("✅ Catalog cleaned up successfully")
        end
    end

    println("✅ Catalog API with custom authenticator function tests completed!")
end

@testset "Catalog Table Loading" begin
    println("Testing catalog table loading...")

    catalog_uri = "http://localhost:8181/api/catalog"

    catalog = nothing
    table = C_NULL
    scan = C_NULL
    stream = C_NULL
    batch = nothing

    try
        # Create catalog connection with MinIO S3 configuration
        # Note: Use localhost since we're running from the host, not from within the Docker network
        props = Dict(
            "credential" => "root:s3cr3t",
            "scope" => "PRINCIPAL_ROLE:ALL",
            "warehouse" => "warehouse",
            "s3.endpoint" => "http://localhost:9000",
            "s3.access-key-id" => "root",
            "s3.secret-access-key" => "password",
            "s3.region" => "us-east-1"
        )
        catalog = RustyIceberg.catalog_create_rest(catalog_uri; properties=props)
        @test catalog !== nothing
        println("✅ Catalog created successfully")

        # Load the customer table from tpch.sf01 namespace
        println("Attempting to load customer table from tpch.sf01...")
        table = RustyIceberg.load_table(catalog, ["tpch.sf01"], "customer")
        @test table != C_NULL
        println("✅ Customer table loaded successfully from catalog")

        # Create a scan on the loaded table
        println("Creating scan on loaded customer table...")
        scan = RustyIceberg.new_scan(table)
        @test scan != C_NULL
        println("✅ Scan created successfully on loaded table")

        # Select specific columns to verify table structure
        println("Selecting specific columns from customer table...")
        RustyIceberg.select_columns!(scan, ["c_custkey", "c_name", "c_nationkey"])
        println("✅ Column selection completed")

        # Execute the scan
        println("Executing scan on loaded customer table...")
        stream = RustyIceberg.scan!(scan)
        @test stream != C_NULL
        println("✅ Scan executed successfully")

        # Read the first batch to verify data
        println("Reading first batch from loaded customer table...")
        batch_ptr = RustyIceberg.next_batch(stream)

        if batch_ptr != C_NULL
            batch = unsafe_load(batch_ptr)
            if batch.data != C_NULL && batch.length > 0
                println("✅ Successfully read first batch with $(batch.length) bytes of Arrow IPC data")

                # Verify we got actual data from the customer table
                @test batch.length > 0
                println("✅ Batch contains valid Arrow data from catalog-loaded customer table")

                # Clean up the batch
                RustyIceberg.free_batch(batch_ptr)
            end
        end
    finally
        # Clean up all resources in reverse order
        if stream != C_NULL
            RustyIceberg.free_stream(stream)
        end
        if scan != C_NULL
            RustyIceberg.free_scan!(scan)
        end
        if table != C_NULL
            RustyIceberg.free_table(table)
        end
        if catalog !== nothing
            RustyIceberg.free_catalog!(catalog)
        end
        println("✅ All resources cleaned up successfully")
    end

    println("✅ Catalog table loading tests completed!")
end

@testset "Catalog Table Loading with Credentials" begin
    println("Testing catalog table loading with vended credentials...")

    catalog_uri = "http://localhost:8181/api/catalog"

    catalog = nothing
    table = C_NULL
    scan = C_NULL
    stream = C_NULL
    batch = nothing

    try
        # Create catalog connection WITHOUT S3 credentials
        # When using load_credentials=true, the catalog will provide vended credentials
        props = Dict(
            "credential" => "root:s3cr3t",
            "scope" => "PRINCIPAL_ROLE:ALL",
            "warehouse" => "warehouse",
            # Note: We include s3.endpoint as we would get the domain name `minio` otherwise.
            "s3.endpoint" => "http://localhost:9000",
            "s3.region" => "us-east-1"
        )
        catalog = RustyIceberg.catalog_create_rest(catalog_uri; properties=props)
        @test catalog !== nothing
        println("✅ Catalog created successfully")

        # Load the customer table with vended credentials
        # The catalog will provide short-lived S3 credentials
        println("Attempting to load customer table from tpch.sf01 with vended credentials...")
        table = RustyIceberg.load_table(catalog, ["tpch.sf01"], "customer"; load_credentials=true)
        @test table != C_NULL
        println("✅ Customer table loaded successfully with vended credentials")

        # Create a scan on the loaded table
        println("Creating scan on loaded customer table...")
        scan = RustyIceberg.new_scan(table)
        @test scan != C_NULL
        println("✅ Scan created successfully on loaded table")

        # Select specific columns
        println("Selecting specific columns from customer table...")
        RustyIceberg.select_columns!(scan, ["c_custkey", "c_name", "c_nationkey"])
        println("✅ Column selection completed")

        # Execute the scan
        println("Executing scan on loaded customer table...")
        stream = RustyIceberg.scan!(scan)
        @test stream != C_NULL
        println("✅ Scan executed successfully")

        # Read the first batch to verify data
        println("Reading first batch from loaded customer table...")
        batch_ptr = RustyIceberg.next_batch(stream)

        if batch_ptr != C_NULL
            batch = unsafe_load(batch_ptr)
            if batch.data != C_NULL && batch.length > 0
                println("✅ Successfully read first batch with $(batch.length) bytes of Arrow IPC data")

                # Verify we got actual data
                @test batch.length > 0
                println("✅ Batch contains valid Arrow data from catalog-loaded customer table with vended credentials")

                # Clean up the batch
                RustyIceberg.free_batch(batch_ptr)
            end
        end
    finally
        # Clean up all resources in reverse order
        if stream != C_NULL
            RustyIceberg.free_stream(stream)
        end
        if scan != C_NULL
            RustyIceberg.free_scan!(scan)
        end
        if table != C_NULL
            RustyIceberg.free_table(table)
        end
        if catalog !== nothing
            RustyIceberg.free_catalog!(catalog)
        end
        println("✅ All resources cleaned up successfully")
    end

    println("✅ Catalog table loading with credentials tests completed!")
end

@testset "Catalog Incremental Scan" begin
    println("Testing catalog incremental scan...")

    catalog_uri = "http://localhost:8181/api/catalog"

    catalog = nothing
    table = C_NULL
    scan = C_NULL
    inserts_stream = C_NULL
    deletes_stream = C_NULL

    try
        # Create catalog connection with MinIO S3 configuration
        props = Dict(
            "credential" => "root:s3cr3t",
            "scope" => "PRINCIPAL_ROLE:ALL",
            "warehouse" => "warehouse",
            "s3.endpoint" => "http://localhost:9000",
            "s3.access-key-id" => "root",
            "s3.secret-access-key" => "password",
            "s3.region" => "us-east-1"
        )
        catalog = RustyIceberg.catalog_create_rest(catalog_uri; properties=props)
        @test catalog !== nothing
        println("✅ Catalog created successfully")

        # Load the incremental test table from catalog
        println("Attempting to load incremental test table from catalog...")
        table = RustyIceberg.load_table(catalog, ["incremental"], "test1")
        @test table != C_NULL
        println("✅ Incremental test table loaded successfully from catalog")

        # Create an incremental scan with specific snapshot IDs
        from_snapshot_id = Int64(6540713100348352610)
        to_snapshot_id = Int64(6832180054960511692)

        println("Creating incremental scan on catalog-loaded table...")
        scan = RustyIceberg.new_incremental_scan(table, from_snapshot_id, to_snapshot_id)
        @test scan isa RustyIceberg.IncrementalScan
        @test scan.ptr != C_NULL
        println("✅ Incremental scan created successfully on catalog-loaded table")

        # Configure scan with batch size
        println("Configuring incremental scan...")
        RustyIceberg.with_batch_size!(scan, UInt(10))
        println("✅ Batch size configured")

        # Execute the scan to get streams
        println("Executing incremental scan on catalog-loaded table...")
        inserts_stream, deletes_stream = RustyIceberg.scan!(scan)
        @test inserts_stream != C_NULL
        @test deletes_stream != C_NULL
        println("✅ Incremental scan streams obtained successfully")

        # Read from inserts stream
        println("Reading from inserts stream...")
        inserts_count = 0
        total_inserts = 0

        batch_ptr = RustyIceberg.next_batch(inserts_stream)
        while batch_ptr != C_NULL
            inserts_count += 1
            batch = unsafe_load(batch_ptr)
            if batch.data != C_NULL && batch.length > 0
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)
                @test "n" in names(df)
                total_inserts += nrow(df)

                if inserts_count <= 2
                    println("  Batch $inserts_count: $(nrow(df)) rows")
                end
            end
            RustyIceberg.free_batch(batch_ptr)
            batch_ptr = RustyIceberg.next_batch(inserts_stream)
        end

        # Read from deletes stream
        println("Reading from deletes stream...")
        deletes_count = 0
        total_deletes = 0

        batch_ptr = RustyIceberg.next_batch(deletes_stream)
        while batch_ptr != C_NULL
            deletes_count += 1
            batch = unsafe_load(batch_ptr)
            if batch.data != C_NULL && batch.length > 0
                arrow_table = Arrow.Table(unsafe_wrap(Array, batch.data, batch.length))
                df = DataFrame(arrow_table)
                @test "file_path" in names(df) || "pos" in names(df)
                total_deletes += nrow(df)

                if deletes_count <= 2
                    println("  Batch $deletes_count: $(nrow(df)) rows")
                end
            end
            RustyIceberg.free_batch(batch_ptr)
            batch_ptr = RustyIceberg.next_batch(deletes_stream)
        end

        @test inserts_count > 0
        @test total_inserts == 98  # Expected rows from incremental range
        @test deletes_count > 0
        @test total_deletes > 0

        println("✅ Successfully read from catalog-loaded incremental scan")
        println("   - Inserts batches: $inserts_count, total rows: $total_inserts")
        println("   - Deletes batches: $deletes_count, total rows: $total_deletes")
    finally
        # Clean up all resources in reverse order
        if inserts_stream != C_NULL
            RustyIceberg.free_stream(inserts_stream)
        end
        if deletes_stream != C_NULL
            RustyIceberg.free_stream(deletes_stream)
        end
        if scan != C_NULL
            RustyIceberg.free_incremental_scan!(scan)
        end
        if table != C_NULL
            RustyIceberg.free_table(table)
        end
        if catalog !== nothing
            RustyIceberg.free_catalog!(catalog)
        end
        println("✅ All resources cleaned up successfully")
    end

    println("✅ Catalog incremental scan tests completed!")
end

@testset "Catalog Table Creation" begin
    println("Testing catalog table creation...")

    catalog_uri = "http://localhost:8181/api/catalog"

    catalog = nothing

    try
        # Create catalog connection
        props = Dict(
            "credential" => "root:s3cr3t",
            "scope" => "PRINCIPAL_ROLE:ALL",
            "warehouse" => "warehouse",
            "s3.endpoint" => "http://localhost:9000",
            "s3.access-key-id" => "root",
            "s3.secret-access-key" => "password",
            "s3.region" => "us-east-1"
        )
        catalog = RustyIceberg.catalog_create_rest(catalog_uri; properties=props)
        @test catalog !== nothing
        println("✅ Catalog created successfully")

        # Define a test namespace for table creation
        test_namespace = ["test_create_table_$(round(Int, time() * 1000))"]

        # Create a new namespace for testing
        try
            RustyIceberg.create_namespace(catalog, test_namespace)
            println("✅ Test namespace created: $test_namespace")
        catch e
            # If namespace already exists, that's acceptable for retry scenarios
            # But we should fail for other errors
            if !contains(string(e), "already exists")
                rethrow()
            end
            println("ℹ️  Test namespace already exists: $test_namespace")
        end

        # Create a simple schema for the test table
        schema = Schema([
            Field(Int32(1), "id", "long"; required=true),
            Field(Int32(2), "name", "string"; required=false),
            Field(Int32(3), "age", "int"; required=false),
            Field(Int32(4), "salary", "double"; required=false),
        ])
        println("✅ Schema created with 4 fields")

        # Test 1: Create table without partition spec or sort order
        println("\nTest 1: Creating table without optional parameters...")
        table_name_1 = "simple_table_$(round(Int, time() * 1000))"

        table_1 = RustyIceberg.create_table(
            catalog,
            test_namespace,
            table_name_1,
            schema
        )
        @test table_1 !== nothing
        @test table_1 isa RustyIceberg.Table
        println("✅ Simple table created: $table_name_1")

        # Verify table exists
        exists = RustyIceberg.table_exists(catalog, test_namespace, table_name_1)
        @test exists == true
        println("✅ Table existence verified: $table_name_1")

        # Clean up first table
        RustyIceberg.free_table(table_1)

        # Test 2: Create table with custom properties
        println("\nTest 2: Creating table with custom properties...")
        table_name_2 = "table_with_props_$(round(Int, time() * 1000))"
        custom_properties = Dict(
            "description" => "Test table with properties",
            "owner" => "test_user",
            "custom_prop" => "custom_value"
        )

        table_2 = RustyIceberg.create_table(
            catalog,
            test_namespace,
            table_name_2,
            schema;
            properties=custom_properties
        )
        @test table_2 !== nothing
        @test table_2 isa RustyIceberg.Table
        println("✅ Table with properties created: $table_name_2")

        # Verify table exists
        exists = RustyIceberg.table_exists(catalog, test_namespace, table_name_2)
        @test exists == true
        println("✅ Table existence verified: $table_name_2")

        # Clean up second table
        RustyIceberg.free_table(table_2)

        # Test 3: Create table with partition spec
        println("\nTest 3: Creating table with partition spec...")
        table_name_3 = "partitioned_table_$(round(Int, time() * 1000))"

        partition_spec = PartitionSpec([
            PartitionField(Int32(1), "id", "identity")
        ])
        println("✅ Partition spec created")

        table_3 = RustyIceberg.create_table(
            catalog,
            test_namespace,
            table_name_3,
            schema;
            partition_spec=partition_spec
        )
        @test table_3 !== nothing
        @test table_3 isa RustyIceberg.Table
        println("✅ Partitioned table created: $table_name_3")

        # Verify table exists
        exists = RustyIceberg.table_exists(catalog, test_namespace, table_name_3)
        @test exists == true
        println("✅ Table existence verified: $table_name_3")

        # Clean up third table
        RustyIceberg.free_table(table_3)

        # Test 4: List tables in test namespace to verify all created tables
        println("\nTest 4: Verifying all created tables in namespace...")
        tables = RustyIceberg.list_tables(catalog, test_namespace)
        println("✅ Tables in test_namespace: $tables")

        # All three tables should be in the list
        @test table_name_1 in tables
        @test table_name_2 in tables
        @test table_name_3 in tables
        println("✅ All created tables found in namespace listing")

        # Test 5: Drop operations
        println("\nTest 5: Testing drop operations...")

        # Drop the first table successfully
        RustyIceberg.drop_table(catalog, test_namespace, table_name_1)
        @test !RustyIceberg.table_exists(catalog, test_namespace, table_name_1)
        println("✅ Successfully dropped table: $table_name_1")

        # Verify that dropping a non-existent table throws an error
        try
            RustyIceberg.drop_table(catalog, test_namespace, table_name_1)
            error("Expected IcebergException for non-existent table")
        catch e
            @test e isa RustyIceberg.IcebergException
            println("✅ Correctly threw exception for non-existent table: $table_name_1")
        end

        # Drop the second and third tables
        RustyIceberg.drop_table(catalog, test_namespace, table_name_2)
        println("✅ Successfully dropped table: $table_name_2")
        RustyIceberg.drop_table(catalog, test_namespace, table_name_3)
        println("✅ Successfully dropped table: $table_name_3")

        # Drop the namespace successfully
        RustyIceberg.drop_namespace(catalog, test_namespace)
        println("✅ Successfully dropped namespace: $test_namespace")

        # Verify that dropping a non-existent namespace throws an error
        try
            RustyIceberg.drop_namespace(catalog, test_namespace)
            error("Expected IcebergException for non-existent namespace")
        catch e
            @test e isa RustyIceberg.IcebergException
            println("✅ Correctly threw exception for non-existent namespace: $test_namespace")
        end

    finally
        # Clean up - tables and namespace should already be dropped in the test
        if catalog !== nothing
            RustyIceberg.free_catalog!(catalog)
            println("✅ Catalog cleaned up successfully")
        end
    end

    println("✅ Catalog table creation and drop tests completed!")
end
