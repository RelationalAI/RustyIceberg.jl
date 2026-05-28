using RustyIceberg
using Test

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

function _ow_schema()
    Schema([
        Field(Int32(1), "id",    "long";   required=true),
        Field(Int32(2), "value", "double"; required=false),
    ])
end

function _write_and_append(table, catalog, data; prefix="data")
    files = RustyIceberg.with_data_file_writer(table; prefix) do w
        write(w, data)
    end
    with_transaction(table, catalog) do tx
        with_fast_append(tx) do action
            add_data_files(action, files)
        end
    end
end

# ---------------------------------------------------------------------------
# OverwriteAction lifecycle
# ---------------------------------------------------------------------------

@testset "OverwriteAction lifecycle" begin
    action = RustyIceberg.OverwriteAction()
    @test action.ptr != C_NULL

    free_overwrite_action!(action)
    @test action.ptr == C_NULL

    # double-free must be a no-op
    free_overwrite_action!(action)
    @test action.ptr == C_NULL
    println("✅ OverwriteAction lifecycle")
end

# ---------------------------------------------------------------------------
# list_data_files
# ---------------------------------------------------------------------------

@testset "list_data_files on empty table" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ow_schema())

            df = list_data_files(table)
            @test df.ptr != C_NULL
            free_data_files!(df)
            @test df.ptr == C_NULL
        finally
            table != C_NULL && free_table(table)
            free_catalog!(cat)
        end
    end
    println("✅ list_data_files on empty table returns valid empty handle")
end

@testset "list_data_files after append" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL
        updated = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ow_schema())
            updated = _write_and_append(table, cat,
                (id=Int64[1,2,3], value=[1.1,2.2,3.3]))

            listed = list_data_files(updated)
            @test listed.ptr != C_NULL
            free_data_files!(listed)
        finally
            table   != C_NULL && free_table(table)
            updated != C_NULL && free_table(updated)
            free_catalog!(cat)
        end
    end
    println("✅ list_data_files after append returns non-null handle")
end

# ---------------------------------------------------------------------------
# full overwrite — replace all files
# ---------------------------------------------------------------------------

@testset "Overwrite replaces all existing files" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL; v1 = C_NULL; v2 = C_NULL; v3 = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ow_schema())

            # two appends so we have two data files to delete
            v1 = _write_and_append(table, cat,
                (id=Int64[1,2,3], value=[1.1,2.2,3.3]); prefix="a")
            v2 = _write_and_append(v1, cat,
                (id=Int64[4,5], value=[4.4,5.5]); prefix="b")

            before = read_table_data(v2)
            @test length(before.id) == 5

            snap_before = table_current_snapshot_id(v2)
            @test !isnothing(snap_before)

            old_files = list_data_files(v2)
            new_files = RustyIceberg.with_data_file_writer(v2; prefix="new") do w
                write(w, (id=Int64[10,20], value=[10.0,20.0]))
            end

            v3 = with_transaction(v2, cat) do tx
                with_overwrite(tx) do action
                    add_data_files(action, new_files)
                    delete_data_files(action, old_files)
                end
            end

            snap_after = table_current_snapshot_id(v3)
            @test !isnothing(snap_after)
            @test snap_after != snap_before

            after = read_table_data(v3)
            @test !isnothing(after)
            @test length(after.id) == 2
            @test sort(after.id) == [10, 20]
        finally
            table != C_NULL && free_table(table)
            v1    != C_NULL && free_table(v1)
            v2    != C_NULL && free_table(v2)
            v3    != C_NULL && free_table(v3)
            free_catalog!(cat)
        end
    end
    println("✅ Overwrite replaces all existing files")
end

# ---------------------------------------------------------------------------
# overwrite with no deletes (add-only via Overwrite snapshot kind)
# ---------------------------------------------------------------------------

@testset "Overwrite add-only produces a new snapshot" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL; updated = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ow_schema())

            new_files = RustyIceberg.with_data_file_writer(table) do w
                write(w, (id=Int64[1], value=[1.0]))
            end

            updated = with_transaction(table, cat) do tx
                with_overwrite(tx) do action
                    add_data_files(action, new_files)
                end
            end

            @test !isnothing(table_current_snapshot_id(updated))
            data = read_table_data(updated)
            @test length(data.id) == 1
        finally
            table   != C_NULL && free_table(table)
            updated != C_NULL && free_table(updated)
            free_catalog!(cat)
        end
    end
    println("✅ Overwrite add-only produces a new snapshot")
end

# ---------------------------------------------------------------------------
# two sequential overwrites
# ---------------------------------------------------------------------------

@testset "Two sequential overwrites" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL; v1 = C_NULL; v2 = C_NULL; v3 = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ow_schema())

            v1 = _write_and_append(table, cat,
                (id=Int64[1,2,3], value=[1.0,2.0,3.0]))

            # first overwrite
            old1   = list_data_files(v1)
            files2 = RustyIceberg.with_data_file_writer(v1; prefix="r1") do w
                write(w, (id=Int64[10,11], value=[10.0,11.0]))
            end
            v2 = with_transaction(v1, cat) do tx
                with_overwrite(tx) do action
                    add_data_files(action, files2)
                    delete_data_files(action, old1)
                end
            end
            @test length(read_table_data(v2).id) == 2

            # second overwrite
            old2   = list_data_files(v2)
            files3 = RustyIceberg.with_data_file_writer(v2; prefix="r2") do w
                write(w, (id=Int64[99], value=[99.0]))
            end
            v3 = with_transaction(v2, cat) do tx
                with_overwrite(tx) do action
                    add_data_files(action, files3)
                    delete_data_files(action, old2)
                end
            end

            data = read_table_data(v3)
            @test length(data.id) == 1
            @test data.id[1] == 99
        finally
            table != C_NULL && free_table(table)
            v1    != C_NULL && free_table(v1)
            v2    != C_NULL && free_table(v2)
            v3    != C_NULL && free_table(v3)
            free_catalog!(cat)
        end
    end
    println("✅ Two sequential overwrites converge correctly")
end

# ---------------------------------------------------------------------------
# error handling
# ---------------------------------------------------------------------------

@testset "OverwriteAction error handling" begin
    mktempdir() do warehouse
        cat = catalog_create_memory(warehouse)
        table = C_NULL
        try
            create_namespace(cat, ["ns"])
            table = create_table(cat, ["ns"], "t", _ow_schema())

            # apply on freed action must throw
            action = RustyIceberg.OverwriteAction()
            free_overwrite_action!(action)
            tx = RustyIceberg.Transaction(table)
            @test_throws RustyIceberg.IcebergException apply(action, tx)
            free_transaction!(tx)
            println("✅ apply on freed action throws")

            # add_data_files / delete_data_files with null DataFiles must throw
            action2  = RustyIceberg.OverwriteAction()
            null_df  = RustyIceberg.DataFiles(C_NULL)
            @test_throws RustyIceberg.IcebergException add_data_files(action2, null_df)
            @test_throws RustyIceberg.IcebergException delete_data_files(action2, null_df)
            free_overwrite_action!(action2)
            println("✅ add/delete_data_files with null DataFiles throw")

            # apply on a consumed transaction must throw
            files1  = RustyIceberg.with_data_file_writer(table) do w
                write(w, (id=Int64[1], value=[1.0]))
            end
            action3 = RustyIceberg.OverwriteAction()
            tx2     = RustyIceberg.Transaction(table)
            add_data_files(action3, files1)
            apply(action3, tx2)          # consumes tx2
            free_overwrite_action!(action3)

            action4 = RustyIceberg.OverwriteAction()
            files2  = RustyIceberg.with_data_file_writer(table; prefix="e2") do w
                write(w, (id=Int64[2], value=[2.0]))
            end
            add_data_files(action4, files2)
            @test_throws RustyIceberg.IcebergException apply(action4, tx2)
            free_overwrite_action!(action4)
            free_transaction!(tx2)
            println("✅ apply on consumed transaction throws")
        finally
            table != C_NULL && free_table(table)
            free_catalog!(cat)
        end
    end
    println("✅ OverwriteAction error handling tests passed")
end
