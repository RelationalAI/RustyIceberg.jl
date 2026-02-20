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

"""
    iceberg_type_to_arrow_type(iceberg_type::String) -> Type

Map an Iceberg field type string to the corresponding Arrow/Julia output type.

This function determines what Julia/Arrow type users should use when providing data
for a given Iceberg field type. This is essential for correct Parquet serialization.

# Mappings

According to the Iceberg specification:
- `"boolean"` → `Bool`
- `"int"` → `Int32`
- `"long"` → `Int64`
- `"float"` → `Float32`
- `"double"` → `Float64`
- `"date"` → `Date` (from Dates module) - Arrow converts to Date32, physically written as Int32 days since epoch
- `"time"` → `Int64` - microseconds since midnight (Arrow doesn't have a native Time type)
- `"timestamp"` → `Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, nothing}` - microseconds since epoch (no timezone)
- `"timestamptz"` → `Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, :UTC}` - microseconds since epoch UTC (with timezone)
- `"timestamp_ns"` → `Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.NANOSECOND, nothing}` - nanoseconds since epoch (no timezone)
- `"timestamptz_ns"` → `Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.NANOSECOND, :UTC}` - nanoseconds since epoch UTC (with timezone)
- `"string"` → `String`
- `"uuid"` → `NTuple{16, UInt8}` - 16-byte UUID representation
- `"binary"` → `Vector{UInt8}` - variable-length byte array
- `"decimal(p,s)"` → `Int32` (if p ≤ 9), `Int64` (if p ≤ 18), or `NTuple{16, UInt8}` (if p > 18)

# Example

```julia
using RustyIceberg
using Dates

# For an Iceberg field with type "date"
arrow_type = iceberg_type_to_arrow_type("date")
# Returns Date type - users should provide Date objects

# For an Iceberg field with type "timestamp"
arrow_type = iceberg_type_to_arrow_type("timestamp")
# Returns Arrow.Timestamp type - users should provide Arrow.Timestamp arrays

# For an Iceberg field with type "decimal(5,2)"
arrow_type = iceberg_type_to_arrow_type("decimal(5,2)")
# Returns Int32 - users should provide Int32 values

# For an Iceberg field with type "decimal(15,4)"
arrow_type = iceberg_type_to_arrow_type("decimal(15,4)")
# Returns Int64 - users should provide Int64 values

# For an Iceberg field with type "decimal(38,18)"
arrow_type = iceberg_type_to_arrow_type("decimal(38,18)")
# Returns NTuple{16, UInt8} - users should provide 16-byte arrays
```

# Note on Temporal Types

Iceberg stores temporal types as physical integers in Parquet:
- **date**: Int32 (days since 1970-01-01)
- **timestamp**: Int64 (microseconds since 1970-01-01 00:00:00)
- **timestamptz**: Int64 (microseconds since 1970-01-01 00:00:00 UTC)
- **timestamp_ns**: Int64 (nanoseconds since 1970-01-01 00:00:00)
- **timestamptz_ns**: Int64 (nanoseconds since 1970-01-01 00:00:00 UTC)

When providing data, ensure it matches the physical representation. Arrow will
automatically convert Date objects to Date32 (which physically represents Int32),
and timestamps should be provided as raw Int64 values.
"""
function iceberg_type_to_arrow_type(iceberg_type::String)
    # Remove whitespace
    type_str = strip(iceberg_type)

    # Primitive types
    if type_str == "boolean"
        return Bool
    elseif type_str == "int"
        return Int32
    elseif type_str == "long"
        return Int64
    elseif type_str == "float"
        return Float32
    elseif type_str == "double"
        return Float64
    elseif type_str == "date"
        # Date objects in Julia - Arrow converts to Date32 (physically Int32)
        return Dates.Date
    elseif type_str == "time"
        # Time in microseconds since midnight - Arrow doesn't have a native Time type
        # Users should provide Int64 values representing microseconds since midnight
        return Int64
    elseif type_str == "timestamp"
        # Timestamp in microseconds since epoch as Arrow Timestamp with microsecond precision
        return Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, nothing}
    elseif type_str == "timestamptz"
        # Timestamptz in microseconds since epoch UTC as Arrow Timestamp with microsecond precision and UTC timezone
        return Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, :UTC}
    elseif type_str == "timestamp_ns"
        # Timestamp in nanoseconds since epoch as Arrow Timestamp with nanosecond precision
        return Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.NANOSECOND, nothing}
    elseif type_str == "timestamptz_ns"
        # Timestamptz in nanoseconds since epoch UTC as Arrow Timestamp with nanosecond precision and UTC timezone
        return Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.NANOSECOND, :UTC}
    elseif type_str == "string"
        return String
    elseif type_str == "uuid"
        # UUID as 16 bytes
        return NTuple{16, UInt8}
    elseif type_str == "binary"
        # Binary as byte vector
        return Vector{UInt8}
    elseif startswith(type_str, "decimal(")
        # Decimal types like "decimal(38, 18)" use different physical types based on precision:
        # - P <= 9: int32
        # - P <= 18: int64
        # - P > 18: fixed-length byte array
        # Extract precision from "decimal(P,S)" format
        m = Base.match(r"decimal\((\d+)", type_str)
        if m !== nothing
            precision = parse(Int, m.captures[1])
            if precision <= 9
                return Int32
            elseif precision <= 18
                return Int64
            else
                # For precision > 18, use fixed-length byte array
                # Byte length needed = ceil((precision * log2(10)) / 8) ≈ ceil(precision * 0.415)
                # For typical cases (P <= 38), 16 bytes is sufficient
                return NTuple{16, UInt8}
            end
        else
            # Fallback if parsing fails
            return NTuple{16, UInt8}
        end
    else
        throw(ArgumentError("Unknown Iceberg type: $type_str"))
    end
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
    arrow_type(field::Field) -> Type

Get the Arrow/Julia type that should be used for data corresponding to this field.

This is a convenience method that calls `iceberg_type_to_arrow_type()` with the field's type.

# Example

```julia
field = Field(Int32(1), "event_date", "date"; required=true)
arrow_t = arrow_type(field)
# Returns Date type - users should provide Date objects
```
"""
function arrow_type(field::Field)
    iceberg_type_to_arrow_type(field.type)
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
    arrow_types(schema::Schema) -> Dict{String, Type}

Get a dictionary mapping field names to their Arrow/Julia types for the schema.

This helps users understand what data types they need to provide when writing to
an Iceberg table with this schema.

# Example

```julia
schema = Schema([
    Field(Int32(1), "id", "long"; required=true),
    Field(Int32(2), "event_date", "date"),
    Field(Int32(3), "event_time", "timestamp"),
])

types = arrow_types(schema)
# Returns Dict{String, Type}:
#   "id" => Int64
#   "event_date" => Date
#   "event_time" => Int64
```
"""
function arrow_types(schema::Schema)::Dict{String, Type}
    result = Dict{String, Type}()
    for field in schema.fields
        result[field.name] = arrow_type(field)
    end
    return result
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
