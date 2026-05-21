using RustyIceberg
using Test

function _err_schema()
    Schema([
        Field(Int32(1), "id",   IcebergLong();   required=true),
        Field(Int32(2), "name", IcebergString(); required=false),
    ])
end

# Strip "file://" prefix from table_location for local-FS memory catalog tables.
function _loc_to_path(loc::String)
    startswith(loc, "file://") ? loc[8:end] : loc
end

@testset "table_open — invalid and missing metadata" begin

    @testset "non-existent local path" begin
        @test_throws RustyIceberg.IcebergException table_open(
            "/no/such/path/$(rand(UInt32)).metadata.json"
        )
    end

    @testset "invalid JSON in metadata file" begin
        mktempdir() do dir
            f = joinpath(dir, "00000-bad.metadata.json")
            open(f, "w") do io; Base.write(io, "this is not valid json {{{"); end
            @test_throws RustyIceberg.IcebergException table_open(f)
        end
    end

    @testset "empty metadata file" begin
        mktempdir() do dir
            f = joinpath(dir, "00000-empty.metadata.json")
            touch(f)
            @test_throws RustyIceberg.IcebergException table_open(f)
        end
    end

    @testset "valid JSON but not Iceberg metadata" begin
        mktempdir() do dir
            f = joinpath(dir, "00000-notmeta.metadata.json")
            open(f, "w") do io; Base.write(io, """{"not_iceberg": true, "random_key": 42}"""); end
            @test_throws RustyIceberg.IcebergException table_open(f)
        end
    end

    @testset "non-existent S3 path" begin
        without_aws_env() do
            s3 = get_s3_config()
            @test_throws RustyIceberg.IcebergException table_open(
                "s3://warehouse/no/such/table/metadata/00000-nonexistent.metadata.json";
                properties=Dict(
                    "s3.endpoint"          => s3["endpoint"],
                    "s3.access-key-id"     => s3["access_key_id"],
                    "s3.secret-access-key" => s3["secret_access_key"],
                    "s3.region"            => s3["region"],
                    "s3.path-style-access" => "true",
                )
            )
        end
    end

    @testset "wrong S3 credentials" begin
        without_aws_env() do
            s3 = get_s3_config()
            nation_meta = "s3://warehouse/tpch.sf01/nation/metadata/" *
                          "00001-44f668fe-3688-49d5-851f-36e75d143321.metadata.json"
            @test_throws RustyIceberg.IcebergException table_open(
                nation_meta;
                properties=Dict(
                    "s3.endpoint"          => s3["endpoint"],
                    "s3.access-key-id"     => "WRONG_ACCESS_KEY",
                    "s3.secret-access-key" => "WRONG_SECRET_KEY",
                    "s3.region"            => s3["region"],
                    "s3.path-style-access" => "true",
                )
            )
        end
    end

end

@testset "Scan — missing and corrupted Parquet files" begin

    @testset "missing Parquet data file" begin
        mktempdir() do warehouse
            cat = catalog_create_memory(warehouse)
            table = C_NULL
            updated_table = C_NULL
            try
                create_namespace(cat, ["ns"])
                table = create_table(cat, ["ns"], "missing_parquet", _err_schema())

                data_files = RustyIceberg.with_data_file_writer(table) do writer
                    write(writer, (id = Int64[1, 2, 3], name = ["a", "b", "c"]))
                end
                updated_table = RustyIceberg.with_transaction(table, cat) do tx
                    with_fast_append(tx) do action
                        add_data_files(action, data_files)
                    end
                end
                @test updated_table != C_NULL

                loc_path = _loc_to_path(table_location(updated_table))
                data_dir = joinpath(loc_path, "data")
                @test isdir(data_dir)
                parquet_files = filter(f -> endswith(f, ".parquet"), readdir(data_dir; join=true))
                @test !isempty(parquet_files)
                for f in parquet_files
                    rm(f)
                end

                scan = new_scan(updated_table)
                stream = C_NULL
                error_caught = false
                try
                    stream = scan!(scan)
                    batch_ptr = next_batch(stream)
                    while batch_ptr != C_NULL
                        free_batch(batch_ptr)
                        batch_ptr = next_batch(stream)
                    end
                catch e
                    error_caught = true
                    @test e isa RustyIceberg.IcebergException
                finally
                    if stream != C_NULL
                        free_stream(stream)
                    end
                    free_scan!(scan)
                end
                @test error_caught

            finally
                if updated_table != C_NULL
                    free_table(updated_table)
                end
                if table != C_NULL
                    free_table(table)
                end
                free_catalog!(cat)
            end
        end
    end

    @testset "corrupted Parquet data file" begin
        mktempdir() do warehouse
            cat = catalog_create_memory(warehouse)
            table = C_NULL
            updated_table = C_NULL
            try
                create_namespace(cat, ["ns"])
                table = create_table(cat, ["ns"], "corrupted_parquet", _err_schema())

                data_files = RustyIceberg.with_data_file_writer(table) do writer
                    write(writer, (id = Int64[1, 2, 3], name = ["x", "y", "z"]))
                end
                updated_table = RustyIceberg.with_transaction(table, cat) do tx
                    with_fast_append(tx) do action
                        add_data_files(action, data_files)
                    end
                end
                @test updated_table != C_NULL

                loc_path = _loc_to_path(table_location(updated_table))
                data_dir = joinpath(loc_path, "data")
                @test isdir(data_dir)
                parquet_files = filter(f -> endswith(f, ".parquet"), readdir(data_dir; join=true))
                @test !isempty(parquet_files)
                for f in parquet_files
                    open(f, "w") do io
                        Base.write(io, "this is not a valid parquet file - corrupted content")
                    end
                end

                scan = new_scan(updated_table)
                stream = C_NULL
                error_caught = false
                try
                    stream = scan!(scan)
                    batch_ptr = next_batch(stream)
                    while batch_ptr != C_NULL
                        free_batch(batch_ptr)
                        batch_ptr = next_batch(stream)
                    end
                catch e
                    error_caught = true
                    @test e isa RustyIceberg.IcebergException
                finally
                    if stream != C_NULL
                        free_stream(stream)
                    end
                    free_scan!(scan)
                end
                @test error_caught

            finally
                if updated_table != C_NULL
                    free_table(updated_table)
                end
                if table != C_NULL
                    free_table(table)
                end
                free_catalog!(cat)
            end
        end
    end

end

@testset "Catalog — error conditions" begin

    @testset "load non-existent table" begin
        cat = catalog_create_memory("memory")
        try
            create_namespace(cat, ["ns"])
            @test_throws RustyIceberg.IcebergException load_table(cat, ["ns"], "does_not_exist")
        finally
            free_catalog!(cat)
        end
    end

    @testset "load table in non-existent namespace" begin
        cat = catalog_create_memory("memory")
        try
            @test_throws RustyIceberg.IcebergException load_table(cat, ["no_such_ns"], "any_table")
        finally
            free_catalog!(cat)
        end
    end

    @testset "create table in non-existent namespace" begin
        cat = catalog_create_memory("memory")
        try
            @test_throws RustyIceberg.IcebergException create_table(
                cat, ["no_such_ns"], "t", _err_schema()
            )
        finally
            free_catalog!(cat)
        end
    end

    @testset "create duplicate namespace" begin
        cat = catalog_create_memory("memory")
        try
            create_namespace(cat, ["ns"])
            @test_throws RustyIceberg.IcebergException create_namespace(cat, ["ns"])
        finally
            free_catalog!(cat)
        end
    end

    @testset "create duplicate table" begin
        cat = catalog_create_memory("memory")
        try
            create_namespace(cat, ["ns"])
            tbl = create_table(cat, ["ns"], "t", _err_schema())
            free_table(tbl)
            @test_throws RustyIceberg.IcebergException create_table(cat, ["ns"], "t", _err_schema())
        finally
            free_catalog!(cat)
        end
    end

    @testset "drop non-existent table" begin
        cat = catalog_create_memory("memory")
        try
            create_namespace(cat, ["ns"])
            @test_throws RustyIceberg.IcebergException drop_table(cat, ["ns"], "nonexistent")
        finally
            free_catalog!(cat)
        end
    end

    @testset "drop non-existent namespace" begin
        cat = catalog_create_memory("memory")
        try
            @test_throws RustyIceberg.IcebergException drop_namespace(cat, ["no_such_ns"])
        finally
            free_catalog!(cat)
        end
    end

    @testset "drop non-empty namespace" begin
        # The memory catalog allows this (cascade drop). If it throws, it must be IcebergException.
        # If it succeeds, the namespace must no longer exist.
        cat = catalog_create_memory("memory")
        try
            create_namespace(cat, ["ns"])
            tbl = create_table(cat, ["ns"], "t", _err_schema())
            free_table(tbl)
            try
                drop_namespace(cat, ["ns"])
                @test !any(==([["ns"]]), list_namespaces(cat))
            catch e
                @test e isa RustyIceberg.IcebergException
            end
        finally
            free_catalog!(cat)
        end
    end

end

@testset "Writer — schema mismatch" begin

    @testset "write with missing required column" begin
        mktempdir() do warehouse
            cat = catalog_create_memory(warehouse)
            table = C_NULL
            try
                create_namespace(cat, ["ns"])
                table = create_table(cat, ["ns"], "missing_col", _err_schema())

                error_caught = false
                writer = RustyIceberg.DataFileWriter(table)
                try
                    write(writer, (name = ["alice"],))
                    RustyIceberg.close_writer(writer)
                catch e
                    error_caught = true
                    @test e isa RustyIceberg.IcebergException
                finally
                    RustyIceberg.free_writer!(writer)
                end
                @test error_caught

            finally
                if table != C_NULL
                    free_table(table)
                end
                free_catalog!(cat)
            end
        end
    end

    @testset "write with no matching columns" begin
        mktempdir() do warehouse
            cat = catalog_create_memory(warehouse)
            table = C_NULL
            try
                create_namespace(cat, ["ns"])
                table = create_table(cat, ["ns"], "no_match", _err_schema())

                error_caught = false
                writer = RustyIceberg.DataFileWriter(table)
                try
                    write(writer, (foo = Int64[1, 2], bar = ["x", "y"]))
                    RustyIceberg.close_writer(writer)
                catch e
                    error_caught = true
                    @test e isa RustyIceberg.IcebergException
                finally
                    RustyIceberg.free_writer!(writer)
                end
                @test error_caught

            finally
                if table != C_NULL
                    free_table(table)
                end
                free_catalog!(cat)
            end
        end
    end

end

# ── Source location in detail ─────────────────────────────────────────────────
#
# Every classified_error() / classify() / classify_iceberg() call is annotated
# with #[track_caller] and appends "[src/file.rs:line]" to the detail field.
# All map_err(fn) call sites use closure form so that the closure-body location
# (our source line) is captured rather than a location inside map_err itself.

@testset "IcebergException.detail includes Rust source location" begin

    loc_re = r"\[src/[\w./]+\.rs:\d+\]"

    # classify_iceberg path: table_open on a non-existent local path goes through
    # classify_iceberg() in table.rs and should produce a detail like
    # "... [src/table.rs:291]".
    @testset "classify_iceberg — table_open non-existent path" begin
        exc = try
            table_open("/no/such/path/$(rand(UInt32)).metadata.json")
            nothing
        catch e
            e isa RustyIceberg.IcebergException ? e : rethrow()
        end
        @test !isnothing(exc)
        @test occursin(loc_re, exc.detail)
    end

    # classify path: load_table for a table that does not exist hits
    # classify() in catalog.rs.
    @testset "classify — load_table non-existent table (memory catalog)" begin
        mktempdir() do warehouse
            cat = catalog_create_memory(warehouse)
            try
                create_namespace(cat, ["ns"])
                exc = try
                    load_table(cat, ["ns"], "nonexistent")
                    nothing
                catch e
                    e isa RustyIceberg.IcebergException ? e : rethrow()
                end
                @test !isnothing(exc)
                @test occursin(loc_re, exc.detail)
            finally
                free_catalog!(cat)
            end
        end
    end

    # classified_error path: the scan builder has not been built (scan.scan is None).
    # Calling arrow_stream triggers the "Scan not initialized" guard inside
    # export_runtime_op!, which fires classified_error() — no iceberg::Error involved.
    @testset "classified_error — scan not built before arrow_stream" begin
        mktempdir() do warehouse
            cat = catalog_create_memory(warehouse)
            tbl = C_NULL
            scan = nothing
            try
                create_namespace(cat, ["ns"])
                tbl = create_table(cat, ["ns"], "loc_scan", _err_schema())
                scan = new_scan(tbl)
                with_batch_size!(scan, UInt(1024))
                # Do NOT call build!(scan) — leaves scan.scan as None on the Rust side.
                exc = try
                    RustyIceberg.arrow_stream(scan)  # "Scan not initialized" guard
                    nothing
                catch e
                    e isa RustyIceberg.IcebergException ? e : rethrow()
                end
                @test !isnothing(exc)
                @test exc.code == RustyIceberg.STATE_RESOURCE_FREED
                @test occursin(loc_re, exc.detail)
            finally
                if !isnothing(scan)
                    free_scan!(scan)
                end
                if tbl != C_NULL
                    free_table(tbl)
                end
                free_catalog!(cat)
            end
        end
    end

end

