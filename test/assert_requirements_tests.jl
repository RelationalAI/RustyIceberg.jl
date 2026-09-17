using RustyIceberg
using Test

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

function _ar_schema()
    Schema([
        Field(Int32(1), "id",    "long";   required=true),
        Field(Int32(2), "value", "double"; required=false),
    ])
end

# ---------------------------------------------------------------------------
# assert_requirements: overriding an auto-derived requirement
# ---------------------------------------------------------------------------

@testset "assert_requirements overrides ref-snapshot-id-match end-to-end" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL
        updated1 = C_NULL
        updated2 = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ar_schema())

            files1 = RustyIceberg.with_data_file_writer(table) do w
                write(w, (id=Int64[1], value=[1.0]))
            end
            updated1 = RustyIceberg.with_transaction(table, cat) do tx
                with_overwrite(tx) do action
                    add_data_files(action, files1)
                end
            end
            base_id = table_current_snapshot_id(updated1)

            # A second commit, asserting the *same* base explicitly via assert_requirements
            # (rather than letting the action auto-derive it). Since the table hasn't
            # moved since `updated1` was loaded, this should succeed exactly like a normal
            # commit would.
            files2 = RustyIceberg.with_data_file_writer(updated1; prefix="d2") do w
                write(w, (id=Int64[2], value=[2.0]))
            end
            updated2 = RustyIceberg.with_transaction(updated1, cat) do tx
                with_overwrite(tx) do action
                    add_data_files(action, files2)
                    assert_requirements(action, [AssertRefSnapshotId("main", base_id)])
                end
            end
            @test table_current_snapshot_id(updated2) != base_id
            data = read_table_data(updated2)
            @test sort(data.id) == [1, 2]
        finally
            table    != C_NULL && free_table(table)
            updated1 != C_NULL && free_table(updated1)
            updated2 != C_NULL && free_table(updated2)
            free_catalog!(cat)
        end
    end
    println("✅ assert_requirements overrides ref-snapshot-id-match end-to-end")
end

@testset "assert_requirements override is rejected when it no longer matches the catalog" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL
        updated1 = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ar_schema())

            files1 = RustyIceberg.with_data_file_writer(table) do w
                write(w, (id=Int64[1], value=[1.0]))
            end
            updated1 = RustyIceberg.with_transaction(table, cat) do tx
                with_overwrite(tx) do action
                    add_data_files(action, files1)
                end
            end
            base_id = table_current_snapshot_id(updated1)

            # Assert a base snapshot id that is deliberately wrong (doesn't match what the
            # catalog actually has). Since assert_requirements-supplied requirements are not
            # locally pre-validated (that's the whole point -- see the PR this is testing),
            # this must reach the catalog and be rejected there, not fail earlier for the
            # wrong reason.
            files2 = RustyIceberg.with_data_file_writer(updated1; prefix="d2") do w
                write(w, (id=Int64[2], value=[2.0]))
            end
            bogus_base = base_id + 12345
            @test_throws RustyIceberg.IcebergException begin
                RustyIceberg.with_transaction(updated1, cat) do tx
                    with_overwrite(tx) do action
                        add_data_files(action, files2)
                        assert_requirements(action, [AssertRefSnapshotId("main", bogus_base)])
                    end
                end
            end

            # And the catalog must genuinely be untouched by the rejected attempt.
            reloaded = load_table(cat, ["ns"], "t")
            try
                @test table_current_snapshot_id(reloaded) == base_id
            finally
                free_table(reloaded)
            end
        finally
            table    != C_NULL && free_table(table)
            updated1 != C_NULL && free_table(updated1)
            free_catalog!(cat)
        end
    end
    println("✅ assert_requirements override is rejected when it no longer matches the catalog")
end

@testset "assert_requirements adds a new requirement kind alongside derived ones" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL
        updated = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ar_schema())

            files = RustyIceberg.with_data_file_writer(table) do w
                write(w, (id=Int64[1], value=[1.0]))
            end

            # CurrentSchemaIdMatch isn't a kind this action derives on its own, so it
            # should ride alongside the auto-derived UuidMatch/RefSnapshotIdMatch, not
            # replace anything -- and since it's correct, the commit still succeeds.
            updated = RustyIceberg.with_transaction(table, cat) do tx
                with_overwrite(tx) do action
                    add_data_files(action, files)
                    assert_requirements(action, [AssertCurrentSchemaId(0)])
                end
            end
            @test !isnothing(table_current_snapshot_id(updated))
        finally
            table   != C_NULL && free_table(table)
            updated != C_NULL && free_table(updated)
            free_catalog!(cat)
        end
    end
    println("✅ assert_requirements adds a new requirement kind alongside derived ones")
end

@testset "assert_requirements: wrong extra requirement kind is rejected by the catalog" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ar_schema())

            files = RustyIceberg.with_data_file_writer(table) do w
                write(w, (id=Int64[1], value=[1.0]))
            end

            @test_throws RustyIceberg.IcebergException begin
                RustyIceberg.with_transaction(table, cat) do tx
                    with_overwrite(tx) do action
                        add_data_files(action, files)
                        assert_requirements(action, [AssertCurrentSchemaId(999)])
                    end
                end
            end
        finally
            table != C_NULL && free_table(table)
            free_catalog!(cat)
        end
    end
    println("✅ assert_requirements: wrong extra requirement kind is rejected by the catalog")
end

@testset "assert_requirements accumulates across multiple calls" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL
        updated = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ar_schema())

            files = RustyIceberg.with_data_file_writer(table) do w
                write(w, (id=Int64[1], value=[1.0]))
            end

            updated = RustyIceberg.with_transaction(table, cat) do tx
                with_overwrite(tx) do action
                    add_data_files(action, files)
                    assert_requirements(action, [AssertCurrentSchemaId(0)])
                    assert_requirements(action, [AssertLastAssignedFieldId(2)])
                end
            end
            @test !isnothing(table_current_snapshot_id(updated))
        finally
            table   != C_NULL && free_table(table)
            updated != C_NULL && free_table(updated)
            free_catalog!(cat)
        end
    end
    println("✅ assert_requirements accumulates across multiple calls")
end

# ---------------------------------------------------------------------------
# error handling
# ---------------------------------------------------------------------------

@testset "assert_requirements on a freed action throws" begin
    action = RustyIceberg.OverwriteAction()
    free_overwrite_action!(action)
    @test_throws RustyIceberg.IcebergException assert_requirements(action, [AssertTableUuid("00000000-0000-0000-0000-000000000000")])
    println("✅ assert_requirements on a freed action throws")
end

@testset "assert_requirements with an empty vector is a no-op" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL
        updated = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ar_schema())

            files = RustyIceberg.with_data_file_writer(table) do w
                write(w, (id=Int64[1], value=[1.0]))
            end

            updated = RustyIceberg.with_transaction(table, cat) do tx
                with_overwrite(tx) do action
                    add_data_files(action, files)
                    assert_requirements(action, TableRequirement[])
                end
            end
            @test !isnothing(table_current_snapshot_id(updated))
        finally
            table   != C_NULL && free_table(table)
            updated != C_NULL && free_table(updated)
            free_catalog!(cat)
        end
    end
    println("✅ assert_requirements with an empty vector is a no-op")
end
