using RustyIceberg
using RustyIceberg: SortDirection, ASC, DESC
using RustyIceberg: NullOrder, NULLS_FIRST, NULLS_LAST
using Test
using JSON
using Arrow
using Dates

@testset "Schema Types" begin
    @testset "Field Creation" begin
        # Create field with string type (backwards compatibility)
        field1 = Field(Int32(1), "id", "long"; required=true)
        @test field1.id == Int32(1)
        @test field1.name == "id"
        @test field1.type isa IcebergLong
        @test field1.required == true
        @test field1.doc === nothing

        # Create field with Iceberg type struct
        field2 = Field(Int32(2), "name", IcebergString())
        @test field2.type isa IcebergString
        @test field2.required == false

        # Create field with doc
        field3 = Field(Int32(3), "timestamp", IcebergTimestamp(); doc="Creation time")
        @test field3.doc == "Creation time"

        # Create field with decimal type
        field4 = Field(Int32(4), "amount", IcebergDecimal(38, 18))
        @test field4.type isa IcebergDecimal
        @test field4.type.precision == 38
        @test field4.type.scale == 18
    end

    @testset "Schema Creation" begin
        fields = [
            Field(Int32(1), "id", IcebergLong(); required=true),
            Field(Int32(2), "name", IcebergString()),
            Field(Int32(3), "age", IcebergInt()),
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
            Field(Int32(1), "id", IcebergLong(); required=true),
            Field(Int32(2), "name", IcebergString()),
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
            [Field(Int32(1), "id", IcebergLong(); required=true)],
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
            s -> add_field(s, "id", IcebergLong(); required=true) |>
            s -> add_field(s, "name", IcebergString()) |>
            build

        @test length(schema.fields) == 2
        @test schema.fields[1].id == Int32(1)
        @test schema.fields[1].name == "id"
        @test schema.fields[2].id == Int32(2)
        @test schema.fields[2].name == "name"
    end

    @testset "Builder with Explicit IDs" begin
        schema = SchemaBuilder() |>
            s -> add_field(s, Int32(10), "id", IcebergLong(); required=true) |>
            s -> add_field(s, Int32(20), "name", IcebergString()) |>
            build

        @test schema.fields[1].id == Int32(10)
        @test schema.fields[2].id == Int32(20)
    end

    @testset "Builder with Identifiers" begin
        schema = SchemaBuilder() |>
            s -> add_field(s, "id", IcebergLong(); required=true) |>
            s -> add_field(s, "name", IcebergString()) |>
            s -> with_identifier(s, Int32[1]) |>
            build

        @test schema.identifier_field_ids == Int32[1]
    end

    @testset "Builder with Docs" begin
        schema = SchemaBuilder() |>
            s -> add_field(s, "created_at", IcebergTimestamp(); doc="When row was created") |>
            build

        @test schema.fields[1].doc == "When row was created"
    end

    @testset "Mixed Type Specifications" begin
        # Using Iceberg type structs
        schema1 = SchemaBuilder() |>
            s -> add_field(s, "id", IcebergLong()) |>
            build

        # Using string types (backwards compatibility)
        schema2 = SchemaBuilder() |>
            s -> add_field(s, "id", "long") |>
            build

        # Should produce same JSON
        @test schema_to_json(schema1) == schema_to_json(schema2)
    end
end

@testset "IcebergType Conversion" begin
    test_cases = [
        (IcebergBoolean(), "boolean"),
        (IcebergInt(), "int"),
        (IcebergLong(), "long"),
        (IcebergFloat(), "float"),
        (IcebergDouble(), "double"),
        (IcebergDate(), "date"),
        (IcebergTime(), "time"),
        (IcebergTimestamp(), "timestamp"),
        (IcebergTimestamptz(), "timestamptz"),
        (IcebergTimestampNs(), "timestamp_ns"),
        (IcebergTimestamptzNs(), "timestamptz_ns"),
        (IcebergString(), "string"),
        (IcebergUuid(), "uuid"),
        (IcebergBinary(), "binary"),
        (IcebergDecimal(38, 18), "decimal(38,18)"),
    ]

    for (type_val, str_val) in test_cases
        field = Field(Int32(1), "test", type_val)
        json_str = schema_to_json(Schema([field]))
        parsed = JSON.parse(json_str)
        @test parsed["fields"][1]["type"] == str_val
    end
end

@testset "Arrow Type Mappings" begin
    @testset "iceberg_type_to_arrow_type with type structs" begin
        # Basic types
        @test iceberg_type_to_arrow_type(IcebergBoolean()) == Bool
        @test iceberg_type_to_arrow_type(IcebergInt()) == Int32
        @test iceberg_type_to_arrow_type(IcebergLong()) == Int64
        @test iceberg_type_to_arrow_type(IcebergFloat()) == Float32
        @test iceberg_type_to_arrow_type(IcebergDouble()) == Float64
        @test iceberg_type_to_arrow_type(IcebergString()) == String

        # Temporal types
        @test iceberg_type_to_arrow_type(IcebergDate()) == Dates.Date
        @test iceberg_type_to_arrow_type(IcebergTime()) == Int64
        @test iceberg_type_to_arrow_type(IcebergTimestamp()) == Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, nothing}
        @test iceberg_type_to_arrow_type(IcebergTimestamptz()) == Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, :UTC}
        @test iceberg_type_to_arrow_type(IcebergTimestampNs()) == Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.NANOSECOND, nothing}
        @test iceberg_type_to_arrow_type(IcebergTimestamptzNs()) == Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.NANOSECOND, :UTC}

        # Complex types
        @test iceberg_type_to_arrow_type(IcebergUuid()) == NTuple{16, UInt8}
        @test iceberg_type_to_arrow_type(IcebergBinary()) == Vector{UInt8}

        # Decimal types — all map to Arrow.Decimal{P, S, Int128} regardless of precision
        @test iceberg_type_to_arrow_type(IcebergDecimal(5, 2)) == Arrow.Decimal{5, 2, Int128}
        @test iceberg_type_to_arrow_type(IcebergDecimal(9, 0)) == Arrow.Decimal{9, 0, Int128}
        @test iceberg_type_to_arrow_type(IcebergDecimal(15, 4)) == Arrow.Decimal{15, 4, Int128}
        @test iceberg_type_to_arrow_type(IcebergDecimal(18, 6)) == Arrow.Decimal{18, 6, Int128}
        @test iceberg_type_to_arrow_type(IcebergDecimal(25, 10)) == Arrow.Decimal{25, 10, Int128}
        @test iceberg_type_to_arrow_type(IcebergDecimal(38, 18)) == Arrow.Decimal{38, 18, Int128}
    end

    @testset "iceberg_type_to_arrow_type with strings (backwards compatibility)" begin
        @test iceberg_type_to_arrow_type("boolean") == Bool
        @test iceberg_type_to_arrow_type("int") == Int32
        @test iceberg_type_to_arrow_type("long") == Int64
        @test iceberg_type_to_arrow_type("float") == Float32
        @test iceberg_type_to_arrow_type("double") == Float64
        @test iceberg_type_to_arrow_type("string") == String
        @test iceberg_type_to_arrow_type("date") == Dates.Date
        @test iceberg_type_to_arrow_type("timestamp") == Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, nothing}
        @test iceberg_type_to_arrow_type("decimal(38,18)") == Arrow.Decimal{38, 18, Int128}
    end

    @testset "arrow_type with nullable fields" begin
        # Required field should return base type
        field_required = Field(Int32(1), "id", IcebergLong(); required=true)
        @test arrow_type(field_required) == Int64

        # Nullable field should return Union{Missing, T}
        field_nullable = Field(Int32(2), "name", IcebergString(); required=false)
        @test arrow_type(field_nullable) == Union{Missing, String}

        # Test with date field
        field_date_required = Field(Int32(3), "birth_date", IcebergDate(); required=true)
        @test arrow_type(field_date_required) == Dates.Date

        field_date_nullable = Field(Int32(4), "death_date", IcebergDate(); required=false)
        @test arrow_type(field_date_nullable) == Union{Missing, Dates.Date}

        # Test with timestamp field
        field_ts_nullable = Field(Int32(5), "created_at", IcebergTimestamp(); required=false)
        @test arrow_type(field_ts_nullable) == Union{Missing, Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, nothing}}
    end

    @testset "arrow_types for schema" begin
        schema = Schema([
            Field(Int32(1), "id", IcebergLong(); required=true),
            Field(Int32(2), "name", IcebergString(); required=false),
            Field(Int32(3), "event_date", IcebergDate(); required=false),
            Field(Int32(4), "created_at", IcebergTimestamp(); required=true),
        ])

        types = arrow_types(schema)

        # Required fields should have base types
        @test types["id"] == Int64
        @test types["created_at"] == Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, nothing}

        # Nullable fields should have Union{Missing, T}
        @test types["name"] == Union{Missing, String}
        @test types["event_date"] == Union{Missing, Dates.Date}
    end
end

println("All schema tests passed!")
