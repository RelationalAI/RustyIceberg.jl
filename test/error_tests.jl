using RustyIceberg
using Test

# ── Intentionally untested codes ─────────────────────────────────────────────
#
# The following error codes are defined but not exercised by these tests because
# they require infrastructure absent from the local/CI environment:
#   AUTH_TOKEN_EXPIRED      — needs an active expiring auth token
#   AUTH_INSUFFICIENT       — needs IAM-level permission testing
#   DATA_TYPE_MISMATCH      — not currently reachable via the public Julia API
#   CATALOG_REST_ONLY       — needs a REST catalog (separate container)
#   CATALOG_COMMIT_CONFLICT — needs concurrent writers
#   IO_NETWORK / IO_S3 / IO_LOCAL — needs network-failure / filesystem-error injection
#   INTERNAL                — fallback for truly unexpected errors

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

# ── Programmatic Rust ↔ Julia enum sync ───────────────────────────────────────

@testset "IcebergError enum sync (Rust ↔ Julia)" begin
    ptr = @ccall RustyIceberg.rust_lib.iceberg_all_error_codes()::Ptr{Cchar}
    raw = unsafe_string(ptr)
    @ccall RustyIceberg.rust_lib.iceberg_destroy_cstring(ptr::Ptr{Cchar})::Cint

    rust_codes = Dict(
        name => parse(UInt32, code_str)
        for line in split(raw, '\n')
        for (code_str, name) in (split(line, '\t'),)
    )

    # Every Julia variant must appear in Rust with the same numeric value
    for variant in instances(RustyIceberg.IcebergError)
        name = string(variant)
        @test haskey(rust_codes, name)
        if haskey(rust_codes, name)
            @test rust_codes[name] == UInt32(variant)
        end
    end

    # Every Rust constant must appear in the Julia enum
    julia_names = Set(string(v) for v in instances(RustyIceberg.IcebergError))
    for name in keys(rust_codes)
        @test name ∈ julia_names
    end
end

# ── table_open — invalid and missing metadata ─────────────────────────────────

@testset "table_open — invalid and missing metadata" begin

    @testset "non-existent local path" begin
        exc = try
            table_open("/no/such/path/$(rand(UInt32)).metadata.json")
            nothing
        catch e
            e isa RustyIceberg.IcebergException ? e : rethrow()
        end
        @test !isnothing(exc)
        @test exc.code == RustyIceberg.NOT_FOUND_METADATA
    end

    @testset "invalid JSON in metadata file" begin
        mktempdir() do dir
            f = joinpath(dir, "00000-bad.metadata.json")
            open(f, "w") do io; Base.write(io, "this is not valid json {{{"); end
            exc = try
                table_open(f)
                nothing
            catch e
                e isa RustyIceberg.IcebergException ? e : rethrow()
            end
            @test !isnothing(exc)
            @test exc.code == RustyIceberg.DATA_METADATA_INVALID
        end
    end

    @testset "empty metadata file" begin
        mktempdir() do dir
            f = joinpath(dir, "00000-empty.metadata.json")
            touch(f)
            exc = try
                table_open(f)
                nothing
            catch e
                e isa RustyIceberg.IcebergException ? e : rethrow()
            end
            @test !isnothing(exc)
            @test exc.code == RustyIceberg.DATA_METADATA_INVALID
        end
    end

    @testset "valid JSON but not Iceberg metadata" begin
        mktempdir() do dir
            f = joinpath(dir, "00000-notmeta.metadata.json")
            open(f, "w") do io; Base.write(io, """{"not_iceberg": true, "random_key": 42}"""); end
            exc = try
                table_open(f)
                nothing
            catch e
                e isa RustyIceberg.IcebergException ? e : rethrow()
            end
            @test !isnothing(exc)
            @test exc.code == RustyIceberg.DATA_METADATA_INVALID
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
            exc = try
                table_open(
                    nation_meta;
                    properties=Dict(
                        "s3.endpoint"          => s3["endpoint"],
                        "s3.access-key-id"     => "WRONG_ACCESS_KEY",
                        "s3.secret-access-key" => "WRONG_SECRET_KEY",
                        "s3.region"            => s3["region"],
                        "s3.path-style-access" => "true",
                    )
                )
                nothing
            catch e
                e isa RustyIceberg.IcebergException ? e : rethrow()
            end
            @test !isnothing(exc)
            @test exc.code == RustyIceberg.AUTH_FAILED
        end
    end

end

# ── Scan — missing / corrupted Parquet files ──────────────────────────────────

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

                scan = new_scan(updated_table, IcebergPerfConfig(batch_size=1024))
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
                    @test e.code == RustyIceberg.NOT_FOUND_DATA_FILE
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

                scan = new_scan(updated_table, IcebergPerfConfig(batch_size=1024))
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
                    @test e.code == RustyIceberg.DATA_FILE_CORRUPT
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

    @testset "non-existent snapshot id" begin
        mktempdir() do warehouse
            cat = catalog_create_memory(warehouse)
            table = C_NULL
            updated_table = C_NULL
            try
                create_namespace(cat, ["ns"])
                table = create_table(cat, ["ns"], "snap_missing", _err_schema())

                # Commit one snapshot so the table is valid
                data_files = RustyIceberg.with_data_file_writer(table) do writer
                    write(writer, (id = Int64[1], name = ["a"]))
                end
                updated_table = RustyIceberg.with_transaction(table, cat) do tx
                    with_fast_append(tx) do action
                        add_data_files(action, data_files)
                    end
                end

                scan = new_scan(updated_table, IcebergPerfConfig(batch_size=1024))
                with_snapshot_id!(scan, Int64(999_999_999_999))  # non-existent
                stream = C_NULL
                exc = try
                    stream = scan!(scan)
                    nothing
                catch e
                    e isa RustyIceberg.IcebergException ? e : rethrow()
                finally
                    if stream != C_NULL; free_stream(stream); end
                    free_scan!(scan)
                end
                @test !isnothing(exc)
                @test exc.code == RustyIceberg.NOT_FOUND_SNAPSHOT
            finally
                if updated_table != C_NULL; free_table(updated_table); end
                if table != C_NULL;         free_table(table); end
                free_catalog!(cat)
            end
        end
    end

end

# ── Catalog — error conditions ────────────────────────────────────────────────

@testset "Catalog — error conditions" begin

    @testset "load non-existent table" begin
        cat = catalog_create_memory("memory")
        try
            create_namespace(cat, ["ns"])
            exc = try
                load_table(cat, ["ns"], "does_not_exist")
                nothing
            catch e
                e isa RustyIceberg.IcebergException ? e : rethrow()
            end
            @test !isnothing(exc)
            @test exc.code == RustyIceberg.NOT_FOUND_TABLE
        finally
            free_catalog!(cat)
        end
    end

    @testset "load table in non-existent namespace" begin
        cat = catalog_create_memory("memory")
        try
            exc = try
                load_table(cat, ["no_such_ns"], "any_table")
                nothing
            catch e
                e isa RustyIceberg.IcebergException ? e : rethrow()
            end
            @test !isnothing(exc)
            @test exc.code == RustyIceberg.NOT_FOUND_NAMESPACE
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
            exc = try
                create_namespace(cat, ["ns"])
                nothing
            catch e
                e isa RustyIceberg.IcebergException ? e : rethrow()
            end
            @test !isnothing(exc)
            @test exc.code == RustyIceberg.CATALOG_NAMESPACE_EXISTS
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
            exc = try
                create_table(cat, ["ns"], "t", _err_schema())
                nothing
            catch e
                e isa RustyIceberg.IcebergException ? e : rethrow()
            end
            @test !isnothing(exc)
            @test exc.code == RustyIceberg.CATALOG_TABLE_EXISTS
        finally
            free_catalog!(cat)
        end
    end

    @testset "drop non-existent table" begin
        cat = catalog_create_memory("memory")
        try
            create_namespace(cat, ["ns"])
            exc = try
                drop_table(cat, ["ns"], "nonexistent")
                nothing
            catch e
                e isa RustyIceberg.IcebergException ? e : rethrow()
            end
            @test !isnothing(exc)
            @test exc.code == RustyIceberg.NOT_FOUND_TABLE
        finally
            free_catalog!(cat)
        end
    end

    @testset "drop non-existent namespace" begin
        cat = catalog_create_memory("memory")
        try
            exc = try
                drop_namespace(cat, ["no_such_ns"])
                nothing
            catch e
                e isa RustyIceberg.IcebergException ? e : rethrow()
            end
            @test !isnothing(exc)
            @test exc.code == RustyIceberg.NOT_FOUND_NAMESPACE
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

# ── Writer — schema mismatch ──────────────────────────────────────────────────

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
                    @test e.code == RustyIceberg.DATA_SCHEMA_MISMATCH
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
                    @test e.code == RustyIceberg.DATA_SCHEMA_MISMATCH
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

    @testset "double close yields STATE_WRITER_CLOSED" begin
        mktempdir() do warehouse
            cat = catalog_create_memory(warehouse)
            table = C_NULL
            writer = nothing
            try
                create_namespace(cat, ["ns"])
                table = create_table(cat, ["ns"], "double_close", _err_schema())
                writer = RustyIceberg.DataFileWriter(table)

                # First close: succeeds (no data written → empty DataFiles)
                data_files = RustyIceberg.close_writer(writer)
                writer.data_files = nothing       # detach so free_writer! won't double-free
                RustyIceberg.free_data_files!(data_files)

                # Second close: should fail with STATE_WRITER_CLOSED
                exc = try
                    RustyIceberg.close_writer(writer)
                    nothing
                catch e
                    e isa RustyIceberg.IcebergException ? e : rethrow()
                end
                @test !isnothing(exc)
                @test exc.code == RustyIceberg.STATE_WRITER_CLOSED
            finally
                if !isnothing(writer); RustyIceberg.free_writer!(writer); end
                if table != C_NULL;    free_table(table); end
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
                scan = new_scan(tbl, IcebergPerfConfig(batch_size=1024))
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
