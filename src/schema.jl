"""
Schema and table creation types for Iceberg table writes.

This module provides Julia representations of Iceberg schema, partition specs,
and related metadata needed for creating and writing to Iceberg tables.
"""

"""
    IcebergType

Enum for valid Iceberg primitive types.

Values:
- `BOOLEAN`: Boolean type
- `INT`: 32-bit integer
- `LONG`: 64-bit integer
- `FLOAT`: 32-bit floating point
- `DOUBLE`: 64-bit floating point
- `DECIMAL`: Decimal (not directly supported in enum, use string)
- `DATE`: Date type
- `TIME`: Time type
- `TIMESTAMP`: Timestamp with timezone
- `TIMESTAMPTZ`: Timestamp without timezone
- `STRING`: String/text type
- `UUID`: UUID type
- `BINARY`: Binary type

Note: For complex types or decimal with specific precision/scale, pass type strings directly.
"""
@enum IcebergType begin
    BOOLEAN = 0
    INT = 1
    LONG = 2
    FLOAT = 3
    DOUBLE = 4
    DATE = 5
    TIME = 6
    TIMESTAMP = 7
    TIMESTAMPTZ = 8
    STRING = 9
    UUID = 10
    BINARY = 11
end

"""
    SortDirection

Enum for sort direction in sort orders.

Values:
- `ASC`: Ascending order
- `DESC`: Descending order
"""
@enum SortDirection ASC=0 DESC=1

"""
    NullOrder

Enum for null ordering in sort fields.

Values:
- `NULLS_FIRST`: Nulls come first
- `NULLS_LAST`: Nulls come last
"""
@enum NullOrder NULLS_FIRST=0 NULLS_LAST=1

# Convert IcebergType enum to string for JSON
function _iceberg_type_to_string(t::IcebergType)::String
    mapping = Dict(
        BOOLEAN => "boolean",
        INT => "int",
        LONG => "long",
        FLOAT => "float",
        DOUBLE => "double",
        DATE => "date",
        TIME => "time",
        TIMESTAMP => "timestamp",
        TIMESTAMPTZ => "timestamptz",
        STRING => "string",
        UUID => "uuid",
        BINARY => "binary",
    )
    return mapping[t]
end

# Convert SortDirection enum to string for JSON
function _sort_direction_to_string(direction::SortDirection)::String
    direction == ASC ? "asc" : "desc"
end

# Convert NullOrder enum to string for JSON
function _null_order_to_string(null_order::NullOrder)::String
    null_order == NULLS_FIRST ? "nulls-first" : "nulls-last"
end

"""
    Field

Represents a single field in an Iceberg schema.

# Fields
- `id::Int32`: Field ID (must be unique within schema and positive)
- `name::String`: Field name
- `type::String`: Iceberg type (e.g., "int", "long", "string", "double", "boolean", "timestamp", etc.)
- `required::Bool`: Whether the field is required (default: false)
- `doc::Union{String, Nothing}`: Optional documentation for the field
"""
struct Field
    id::Int32
    name::String
    type::String
    required::Bool
    doc::Union{String, Nothing}

    # Constructor with doc as optional keyword argument
    Field(id::Int32, name::String, type::String; required::Bool=false, doc::Union{String, Nothing}=nothing) =
        new(id, name, type, required, doc)
end

"""
    Field(id::Int32, name::String, type::IcebergType; required::Bool=false, doc::Union{String, Nothing}=nothing)

Constructor that takes an IcebergType enum and converts it to string.
"""
Field(id::Int32, name::String, type::IcebergType; required::Bool=false, doc::Union{String, Nothing}=nothing) =
    Field(id, name, _iceberg_type_to_string(type); required=required, doc=doc)

"""
    Schema

Represents an Iceberg schema (top-level struct).

# Fields
- `fields::Vector{Field}`: List of fields in the schema
- `identifier_field_ids::Vector{Int32}`: Field IDs that form the table identifier (default: empty)

Schema must have at least one field.
"""
struct Schema
    fields::Vector{Field}
    identifier_field_ids::Vector{Int32}

    function Schema(fields::Vector{Field}; identifier_field_ids::Vector{Int32}=Int32[])
        if isempty(fields)
            throw(ArgumentError("Schema must have at least one field"))
        end
        new(fields, identifier_field_ids)
    end
end

"""
    PartitionField

Represents a single partition field in a partition spec.

# Fields
- `source_id::Int32`: ID of the source field in the schema
- `field_id::Int32`: Partition field ID (usually source_id or incremented value)
- `name::String`: Name of the partition field
- `transform::String`: Partition transform (e.g., "identity", "year", "month", "day", "hour", "bucket[16]", "truncate[4]")
"""
struct PartitionField
    source_id::Int32
    field_id::Int32
    name::String
    transform::String

    # Constructor with field_id defaulting to source_id
    PartitionField(source_id::Int32, name::String, transform::String; field_id::Union{Int32, Nothing}=nothing) =
        new(source_id, field_id === nothing ? source_id : field_id, name, transform)
end

"""
    PartitionSpec

Represents an Iceberg partition specification.

# Fields
- `spec_id::Int32`: Partition spec ID (0 for default, default: 0)
- `fields::Vector{PartitionField}`: List of partition fields
"""
struct PartitionSpec
    spec_id::Int32
    fields::Vector{PartitionField}

    function PartitionSpec(fields::Vector{PartitionField}; spec_id::Int32=Int32(0))
        new(spec_id, fields)
    end
end

"""
    SortField

Represents a single field in a sort order.

# Fields
- `source_id::Int32`: ID of the source field in the schema
- `transform::String`: Sort transform (usually "identity", default: "identity")
- `direction::SortDirection`: Sort direction (default: ASC)
- `null_order::NullOrder`: Null ordering (default: NULLS_LAST)
"""
struct SortField
    source_id::Int32
    transform::String
    direction::SortDirection
    null_order::NullOrder

    function SortField(
        source_id::Int32;
        transform::String="identity",
        direction::SortDirection=ASC,
        null_order::NullOrder=NULLS_LAST
    )
        new(source_id, transform, direction, null_order)
    end
end

"""
    SortOrder

Represents an Iceberg sort order specification.

# Fields
- `order_id::Int64`: Sort order ID (usually 0 for default, default: 0)
- `fields::Vector{SortField}`: List of sort fields
"""
struct SortOrder
    order_id::Int64
    fields::Vector{SortField}

    function SortOrder(fields::Vector{SortField}; order_id::Int64=Int64(0))
        if isempty(fields)
            throw(ArgumentError("SortOrder must have at least one field"))
        end
        new(order_id, fields)
    end
end

# ============================================================================
# JSON Serialization for FFI
# ============================================================================

"""
    field_to_dict(field::Field)::Dict

Convert a Field to a dictionary suitable for JSON serialization to send to Rust.
"""
function field_to_dict(field::Field)::Dict
    d = Dict(
        "id" => field.id,
        "name" => field.name,
        "required" => field.required,
        "type" => field.type,
    )
    if field.doc !== nothing
        d["doc"] = field.doc
    end
    return d
end

"""
    schema_to_json(schema::Schema)::String

Serialize a Schema to JSON for transmission to Rust via FFI.

The JSON format matches Iceberg's schema specification.
"""
function schema_to_json(schema::Schema)::String
    dict = Dict(
        "type" => "struct",
        "fields" => [field_to_dict(f) for f in schema.fields],
    )
    if !isempty(schema.identifier_field_ids)
        dict["identifier-field-ids"] = schema.identifier_field_ids
    end
    return JSON.json(dict)
end

"""
    partition_field_to_dict(field::PartitionField)::Dict

Convert a PartitionField to a dictionary suitable for JSON serialization.
"""
function partition_field_to_dict(field::PartitionField)::Dict
    return Dict(
        "source-id" => field.source_id,
        "field-id" => field.field_id,
        "name" => field.name,
        "transform" => field.transform,
    )
end

"""
    partition_spec_to_json(spec::PartitionSpec)::String

Serialize a PartitionSpec to JSON for transmission to Rust via FFI.

Returns empty JSON object "{}" if the spec has no fields.
"""
function partition_spec_to_json(spec::PartitionSpec)::String
    if isempty(spec.fields)
        return "{}"
    end
    dict = Dict(
        "spec-id" => spec.spec_id,
        "fields" => [partition_field_to_dict(f) for f in spec.fields],
    )
    return JSON.json(dict)
end

"""
    sort_field_to_dict(field::SortField)::Dict

Convert a SortField to a dictionary suitable for JSON serialization.
"""
function sort_field_to_dict(field::SortField)::Dict
    return Dict(
        "source-id" => field.source_id,
        "transform" => field.transform,
        "direction" => _sort_direction_to_string(field.direction),
        "null-order" => _null_order_to_string(field.null_order),
    )
end

"""
    sort_order_to_json(order::SortOrder)::String

Serialize a SortOrder to JSON for transmission to Rust via FFI.

Returns empty JSON object "{}" if the order has no fields.
"""
function sort_order_to_json(order::SortOrder)::String
    if isempty(order.fields)
        return "{}"
    end
    dict = Dict(
        "order-id" => order.order_id,
        "fields" => [sort_field_to_dict(f) for f in order.fields],
    )
    return JSON.json(dict)
end

# ============================================================================
# Builder patterns for convenience
# ============================================================================

"""
    SchemaBuilder

Builder for constructing Iceberg schemas fluently.

# Example
```julia
schema = SchemaBuilder()
    |> s -> add_field(s, 1, "id", LONG; required=true)
    |> s -> add_field(s, 2, "name", STRING)
    |> s -> add_field(s, 3, "timestamp", TIMESTAMP)
    |> build
```
"""
mutable struct SchemaBuilder
    fields::Vector{Field}
    identifier_field_ids::Vector{Int32}
    next_field_id::Int32
end

function SchemaBuilder()
    SchemaBuilder(Field[], Int32[], Int32(1))
end

"""
    add_field(
        builder::SchemaBuilder,
        name::String,
        type::Union{String,IcebergType};
        required::Bool=false,
        doc::Union{String, Nothing}=nothing
    )::SchemaBuilder

Add a field to the schema builder with auto-incrementing field ID.
"""
function add_field(
    builder::SchemaBuilder,
    name::String,
    type::Union{String,IcebergType};
    required::Bool=false,
    doc::Union{String, Nothing}=nothing
)::SchemaBuilder
    type_str = isa(type, IcebergType) ? _iceberg_type_to_string(type) : type
    field = Field(builder.next_field_id, name, type_str; required=required, doc=doc)
    push!(builder.fields, field)
    builder.next_field_id += 1
    return builder
end

"""
    add_field(
        builder::SchemaBuilder,
        id::Int32,
        name::String,
        type::Union{String,IcebergType};
        required::Bool=false,
        doc::Union{String, Nothing}=nothing
    )::SchemaBuilder

Add a field to the schema builder with explicit field ID.
"""
function add_field(
    builder::SchemaBuilder,
    id::Int32,
    name::String,
    type::Union{String,IcebergType};
    required::Bool=false,
    doc::Union{String, Nothing}=nothing
)::SchemaBuilder
    type_str = isa(type, IcebergType) ? _iceberg_type_to_string(type) : type
    field = Field(id, name, type_str; required=required, doc=doc)
    push!(builder.fields, field)
    builder.next_field_id = max(builder.next_field_id, id + 1)
    return builder
end

"""
    with_identifier(builder::SchemaBuilder, field_ids::Vector{Int32})::SchemaBuilder

Set the identifier field IDs for the schema.
"""
function with_identifier(builder::SchemaBuilder, field_ids::Vector{Int32})::SchemaBuilder
    builder.identifier_field_ids = field_ids
    return builder
end

"""
    build(builder::SchemaBuilder)::Schema

Build the final Schema from the builder.
"""
function build(builder::SchemaBuilder)::Schema
    Schema(builder.fields; identifier_field_ids=builder.identifier_field_ids)
end
