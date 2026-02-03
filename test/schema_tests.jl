using RustyIceberg
using RustyIceberg: IcebergType, BOOLEAN, INT, LONG, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, TIMESTAMPTZ, STRING, UUID, BINARY
using RustyIceberg: SortDirection, ASC, DESC
using RustyIceberg: NullOrder, NULLS_FIRST, NULLS_LAST
using Test
using JSON

@testset "Schema Types" begin
    @testset "Field Creation" begin
        # Create field with string type
        field1 = Field(Int32(1), "id", "long"; required=true)
        @test field1.id == Int32(1)
        @test field1.name == "id"
        @test field1.type == "long"
        @test field1.required == true
        @test field1.doc === nothing

        # Create field with IcebergType enum
        field2 = Field(Int32(2), "name", STRING)
        @test field2.type == "string"
        @test field2.required == false

        # Create field with doc
        field3 = Field(Int32(3), "timestamp", TIMESTAMP; doc="Creation time")
        @test field3.doc == "Creation time"
    end

    @testset "Schema Creation" begin
        fields = [
            Field(Int32(1), "id", LONG; required=true),
            Field(Int32(2), "name", STRING),
            Field(Int32(3), "age", INT),
        ]
        schema = Schema(fields)
        @test length(schema.fields) == 3
        @test schema.fields[1].name == "id"

        # Schema with identifier field IDs
        schema_with_id = Schema(fields; identifier_field_ids=Int32[1])
        @test schema_with_id.identifier_field_ids == Int32[1]

        # Empty schema should error
        @test_throws ArgumentError Schema(Field[])
    end

    @testset "Schema JSON Serialization" begin
        schema = Schema([
            Field(Int32(1), "id", LONG; required=true),
            Field(Int32(2), "name", STRING),
        ])

        json_str = schema_to_json(schema)
        parsed = JSON.parse(json_str)

        @test parsed["type"] == "struct"
        @test length(parsed["fields"]) == 2
        @test parsed["fields"][1]["id"] == 1
        @test parsed["fields"][1]["name"] == "id"
        @test parsed["fields"][1]["type"] == "long"
        @test parsed["fields"][1]["required"] == true
        @test parsed["fields"][2]["required"] == false
        @test !haskey(parsed, "identifier-field-ids")
    end

    @testset "Schema with Identifier Fields JSON" begin
        schema = Schema(
            [Field(Int32(1), "id", LONG; required=true)],
            identifier_field_ids=Int32[1]
        )

        json_str = schema_to_json(schema)
        parsed = JSON.parse(json_str)

        @test parsed["identifier-field-ids"] == [1]
    end

    @testset "PartitionField Creation" begin
        pf = PartitionField(Int32(1), "created_day", "day")
        @test pf.source_id == Int32(1)
        @test pf.field_id == Int32(1)
        @test pf.name == "created_day"
        @test pf.transform == "day"

        # With explicit field_id
        pf2 = PartitionField(Int32(1), "created_day", "day"; field_id=Int32(1000))
        @test pf2.field_id == Int32(1000)
    end

    @testset "PartitionSpec Creation" begin
        fields = [
            PartitionField(Int32(1), "created_day", "day"),
            PartitionField(Int32(2), "region_bucket", "bucket[16]"),
        ]
        spec = PartitionSpec(fields)
        @test length(spec.fields) == 2
        @test spec.spec_id == Int32(0)

        # With custom spec_id
        spec2 = PartitionSpec(fields; spec_id=Int32(5))
        @test spec2.spec_id == Int32(5)
    end

    @testset "PartitionSpec JSON Serialization" begin
        spec = PartitionSpec([
            PartitionField(Int32(1), "day", "day"),
        ])

        json_str = partition_spec_to_json(spec)
        parsed = JSON.parse(json_str)

        @test parsed["spec-id"] == 0
        @test length(parsed["fields"]) == 1
        @test parsed["fields"][1]["source-id"] == 1
        @test parsed["fields"][1]["field-id"] == 1
        @test parsed["fields"][1]["name"] == "day"
        @test parsed["fields"][1]["transform"] == "day"
    end

    @testset "Empty PartitionSpec JSON" begin
        spec = PartitionSpec(Vector{PartitionField}())
        json_str = partition_spec_to_json(spec)
        @test json_str == "{}"
    end

    @testset "SortField Creation" begin
        sf = SortField(Int32(1))
        @test sf.source_id == Int32(1)
        @test sf.transform == "identity"
        @test sf.direction == ASC
        @test sf.null_order == NULLS_LAST

        # With custom options
        sf2 = SortField(Int32(2); direction=DESC, null_order=NULLS_FIRST)
        @test sf2.direction == DESC
        @test sf2.null_order == NULLS_FIRST
    end

    @testset "SortOrder Creation" begin
        fields = [
            SortField(Int32(1); direction=ASC),
            SortField(Int32(2); direction=DESC, null_order=NULLS_FIRST),
        ]
        order = SortOrder(fields)
        @test length(order.fields) == 2
        @test order.order_id == Int64(0)

        # With custom order_id
        order2 = SortOrder(fields; order_id=Int64(10))
        @test order2.order_id == Int64(10)

        # Empty should error
        @test_throws ArgumentError SortOrder(Vector{SortField}())
    end

    @testset "SortOrder JSON Serialization" begin
        order = SortOrder([
            SortField(Int32(1); direction=ASC, null_order=NULLS_LAST),
            SortField(Int32(2); direction=DESC, null_order=NULLS_FIRST),
        ])

        json_str = sort_order_to_json(order)
        parsed = JSON.parse(json_str)

        @test parsed["order-id"] == 0
        @test length(parsed["fields"]) == 2
        @test parsed["fields"][1]["source-id"] == 1
        @test parsed["fields"][1]["direction"] == "asc"
        @test parsed["fields"][1]["null-order"] == "nulls-last"
        @test parsed["fields"][2]["direction"] == "desc"
        @test parsed["fields"][2]["null-order"] == "nulls-first"
    end

end

@testset "SchemaBuilder" begin
    @testset "Basic Builder" begin
        schema = SchemaBuilder() |>
            s -> add_field(s, "id", LONG; required=true) |>
            s -> add_field(s, "name", STRING) |>
            build

        @test length(schema.fields) == 2
        @test schema.fields[1].id == Int32(1)
        @test schema.fields[1].name == "id"
        @test schema.fields[2].id == Int32(2)
        @test schema.fields[2].name == "name"
    end

    @testset "Builder with Explicit IDs" begin
        schema = SchemaBuilder() |>
            s -> add_field(s, Int32(10), "id", LONG; required=true) |>
            s -> add_field(s, Int32(20), "name", STRING) |>
            build

        @test schema.fields[1].id == Int32(10)
        @test schema.fields[2].id == Int32(20)
    end

    @testset "Builder with Identifiers" begin
        schema = SchemaBuilder() |>
            s -> add_field(s, "id", LONG; required=true) |>
            s -> add_field(s, "name", STRING) |>
            s -> with_identifier(s, Int32[1]) |>
            build

        @test schema.identifier_field_ids == Int32[1]
    end

    @testset "Builder with Docs" begin
        schema = SchemaBuilder() |>
            s -> add_field(s, "created_at", TIMESTAMP; doc="When row was created") |>
            build

        @test schema.fields[1].doc == "When row was created"
    end

    @testset "Mixed Type Specifications" begin
        # Using IcebergType enums
        schema1 = SchemaBuilder() |>
            s -> add_field(s, "id", LONG) |>
            build

        # Using string types
        schema2 = SchemaBuilder() |>
            s -> add_field(s, "id", "long") |>
            build

        # Should produce same JSON
        @test schema_to_json(schema1) == schema_to_json(schema2)
    end
end

@testset "IcebergType Enum Conversion" begin
    test_cases = [
        (BOOLEAN, "boolean"),
        (INT, "int"),
        (LONG, "long"),
        (FLOAT, "float"),
        (DOUBLE, "double"),
        (DATE, "date"),
        (TIME, "time"),
        (TIMESTAMP, "timestamp"),
        (TIMESTAMPTZ, "timestamptz"),
        (STRING, "string"),
        (UUID, "uuid"),
        (BINARY, "binary"),
    ]

    for (enum_val, str_val) in test_cases
        field = Field(Int32(1), "test", enum_val)
        @test field.type == str_val

        json_str = schema_to_json(Schema([field]))
        parsed = JSON.parse(json_str)
        @test parsed["fields"][1]["type"] == str_val
    end
end

println("All schema tests passed!")
