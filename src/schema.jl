"""
Schema and table creation types for Iceberg table writes.

This module provides Julia representations of Iceberg schema, partition specs,
and related metadata needed for creating and writing to Iceberg tables.
"""

# ============================================================================
# Iceberg Type System
# ============================================================================

"""
    AbstractIcebergType

Abstract base type for all Iceberg field types.

Subtypes include primitive types (IcebergBoolean, IcebergInt, etc.) and
parameterized types (IcebergDecimal).

# Example
```julia
field = Field(Int32(1), "name", IcebergString(); required=true)
field = Field(Int32(2), "count", IcebergLong())
field = Field(Int32(3), "amount", IcebergDecimal(38, 18))
```
"""
abstract type AbstractIcebergType end

# Primitive types - singleton structs
struct IcebergBoolean <: AbstractIcebergType end
struct IcebergInt <: AbstractIcebergType end
struct IcebergLong <: AbstractIcebergType end
struct IcebergFloat <: AbstractIcebergType end
struct IcebergDouble <: AbstractIcebergType end
struct IcebergDate <: AbstractIcebergType end
struct IcebergTime <: AbstractIcebergType end
struct IcebergTimestamp <: AbstractIcebergType end
struct IcebergTimestamptz <: AbstractIcebergType end
struct IcebergTimestampNs <: AbstractIcebergType end
struct IcebergTimestamptzNs <: AbstractIcebergType end
struct IcebergString <: AbstractIcebergType end
struct IcebergUuid <: AbstractIcebergType end
struct IcebergBinary <: AbstractIcebergType end

"""
    IcebergDecimal(precision::Int, scale::Int)

Decimal type with specified precision and scale.

# Example
```julia
field = Field(Int32(1), "amount", IcebergDecimal(38, 18))
```
"""
struct IcebergDecimal <: AbstractIcebergType
    precision::Int
    scale::Int
end

# Convert Iceberg type to string for JSON serialization
_iceberg_type_to_string(::IcebergBoolean) = "boolean"
_iceberg_type_to_string(::IcebergInt) = "int"
_iceberg_type_to_string(::IcebergLong) = "long"
_iceberg_type_to_string(::IcebergFloat) = "float"
_iceberg_type_to_string(::IcebergDouble) = "double"
_iceberg_type_to_string(::IcebergDate) = "date"
_iceberg_type_to_string(::IcebergTime) = "time"
_iceberg_type_to_string(::IcebergTimestamp) = "timestamp"
_iceberg_type_to_string(::IcebergTimestamptz) = "timestamptz"
_iceberg_type_to_string(::IcebergTimestampNs) = "timestamp_ns"
_iceberg_type_to_string(::IcebergTimestamptzNs) = "timestamptz_ns"
_iceberg_type_to_string(::IcebergString) = "string"
_iceberg_type_to_string(::IcebergUuid) = "uuid"
_iceberg_type_to_string(::IcebergBinary) = "binary"
_iceberg_type_to_string(d::IcebergDecimal) = "decimal($(d.precision),$(d.scale))"

# Parse string to Iceberg type (for reading schemas from JSON)
function _string_to_iceberg_type(s::AbstractString)::AbstractIcebergType
    type_str = strip(s)
    if type_str == "boolean"
        return IcebergBoolean()
    elseif type_str == "int"
        return IcebergInt()
    elseif type_str == "long"
        return IcebergLong()
    elseif type_str == "float"
        return IcebergFloat()
    elseif type_str == "double"
        return IcebergDouble()
    elseif type_str == "date"
        return IcebergDate()
    elseif type_str == "time"
        return IcebergTime()
    elseif type_str == "timestamp"
        return IcebergTimestamp()
    elseif type_str == "timestamptz"
        return IcebergTimestamptz()
    elseif type_str == "timestamp_ns"
        return IcebergTimestampNs()
    elseif type_str == "timestamptz_ns"
        return IcebergTimestamptzNs()
    elseif type_str == "string"
        return IcebergString()
    elseif type_str == "uuid"
        return IcebergUuid()
    elseif type_str == "binary"
        return IcebergBinary()
    elseif startswith(type_str, "decimal(")
        m = Base.match(r"decimal\((\d+)\s*,\s*(\d+)\)", type_str)
        if m !== nothing
            precision = parse(Int, m.captures[1])
            scale = parse(Int, m.captures[2])
            return IcebergDecimal(precision, scale)
        else
            throw(ArgumentError("Invalid decimal type format: $type_str"))
        end
    else
        throw(ArgumentError("Unknown Iceberg type: $type_str"))
    end
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

# Convert SortDirection enum to string for JSON
function _sort_direction_to_string(direction::SortDirection)::String
    direction == ASC ? "asc" : "desc"
end

"""
    iceberg_type_to_arrow_type(iceberg_type::AbstractIcebergType) -> Type

Map an Iceberg field type to the corresponding Arrow/Julia output type.

This function determines what Julia/Arrow type users should use when providing data
for a given Iceberg field type. This is essential for correct Parquet serialization.

# Mappings

According to the Iceberg specification:
- `IcebergBoolean()` → `Bool`
- `IcebergInt()` → `Int32`
- `IcebergLong()` → `Int64`
- `IcebergFloat()` → `Float32`
- `IcebergDouble()` → `Float64`
- `IcebergDate()` → `Date` (from Dates module)
- `IcebergTime()` → `Int64` - microseconds since midnight
- `IcebergTimestamp()` → `Arrow.Timestamp{..., nothing}` - microseconds since epoch (no timezone)
- `IcebergTimestamptz()` → `Arrow.Timestamp{..., Symbol("+00:00")}` - microseconds since epoch UTC
- `IcebergString()` → `String`
- `IcebergUuid()` → `NTuple{16, UInt8}` - 16-byte UUID representation
- `IcebergBinary()` → `Vector{UInt8}` - variable-length byte array
- `IcebergDecimal(p,s)` → `Int32` (if p ≤ 9), `Int64` (if p ≤ 18), or `Int128` (if p > 18)

  Note: these are the physical storage types used by the `write_columns` API. The Arrow IPC
  `write` path does not support decimal columns via this mapping.

# Example

```julia
using RustyIceberg
using Dates

arrow_type = iceberg_type_to_arrow_type(IcebergDate())
# Returns Date type

arrow_type = iceberg_type_to_arrow_type(IcebergDecimal(38, 18))
# Returns Int128
```
"""
iceberg_type_to_arrow_type(::IcebergBoolean) = Bool
iceberg_type_to_arrow_type(::IcebergInt) = Int32
iceberg_type_to_arrow_type(::IcebergLong) = Int64
iceberg_type_to_arrow_type(::IcebergFloat) = Float32
iceberg_type_to_arrow_type(::IcebergDouble) = Float64
iceberg_type_to_arrow_type(::IcebergDate) = Dates.Date
iceberg_type_to_arrow_type(::IcebergTime) = Int64
# The Iceberg spec does not define a canonical Arrow format mapping (Arrow is
# an in-memory format, not a storage format).  Each implementation chooses its
# own timezone string for timestamptz.  iceberg-rs (our Rust layer) defines:
# https://github.com/RelationalAI/iceberg-rust/blob/418213731e91544f5eb31a3efa459e88f599030e/crates/iceberg/src/arrow/schema.rs#L45
#
#     const UTC_TIME_ZONE: &str = "+00:00";
#
# and emits that string into every Arrow IPC schema it writes for Timestamptz
# fields.  When reading it accepts both "+00:00" and "UTC" for compatibility,
# but it always *writes* "+00:00".
#
# The likely reason for this choice: Parquet encodes UTC-adjusted timestamps
# via an isAdjustedToUTC=true flag rather than a named timezone, so
# implementations tend to use the offset form "+00:00" rather than an IANA
# name like "UTC" when bridging to Arrow.
#
# Arrow.jl converts the timezone string from the IPC wire format to a Symbol,
# so a field written by iceberg-rs arrives as
#
#     Arrow.Timestamp{MICROSECOND, Symbol("+00:00")}
#
# Using :UTC here would cause a type-mismatch against data returned by the
# Rust layer, even though the two strings represent the same instant in time.
iceberg_type_to_arrow_type(::IcebergTimestamp) = Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, nothing}
iceberg_type_to_arrow_type(::IcebergTimestamptz) = Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, Symbol("+00:00")}
iceberg_type_to_arrow_type(::IcebergTimestampNs) = Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.NANOSECOND, nothing}
iceberg_type_to_arrow_type(::IcebergTimestamptzNs) = Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.NANOSECOND, Symbol("+00:00")}
iceberg_type_to_arrow_type(::IcebergString) = String
iceberg_type_to_arrow_type(::IcebergUuid) = NTuple{16, UInt8}
iceberg_type_to_arrow_type(::IcebergBinary) = Vector{UInt8}
function iceberg_type_to_arrow_type(d::IcebergDecimal)
    if d.precision <= 9
        return Int32
    elseif d.precision <= 18
        return Int64
    else
        return Int128
    end
end

# String-based version for backwards compatibility and parsing JSON schemas
iceberg_type_to_arrow_type(s::AbstractString) = iceberg_type_to_arrow_type(_string_to_iceberg_type(s))

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
- `type::AbstractIcebergType`: Iceberg type (e.g., IcebergLong(), IcebergString(), IcebergDecimal(38, 18))
- `required::Bool`: Whether the field is required (default: false)
- `doc::Union{String, Nothing}`: Optional documentation for the field

# Example
```julia
Field(Int32(1), "id", IcebergLong(); required=true)
Field(Int32(2), "name", IcebergString())
Field(Int32(3), "amount", IcebergDecimal(38, 18))
```
"""
struct Field
    id::Int32
    name::String
    type::AbstractIcebergType
    required::Bool
    doc::Union{String, Nothing}

    # Constructor with doc as optional keyword argument
    Field(id::Int32, name::String, type::AbstractIcebergType; required::Bool=false, doc::Union{String, Nothing}=nothing) =
        new(id, name, type, required, doc)
end

# Constructor from string type (for backwards compatibility / parsing JSON)
Field(id::Int32, name::String, type::AbstractString; required::Bool=false, doc::Union{String, Nothing}=nothing) =
    Field(id, name, _string_to_iceberg_type(type); required=required, doc=doc)

"""
    arrow_type(field::Field) -> Type

Get the Arrow/Julia type that should be used for data corresponding to this field.

This function returns the Arrow/Julia type for the field's type, and accounts for nullability:
- If the field is required, returns the type as-is
- If the field is not required (nullable), returns `Union{Missing, T}` where T is the base type

# Example

```julia
field_required = Field(Int32(1), "event_date", IcebergDate(); required=true)
arrow_t = arrow_type(field_required)
# Returns Date type - users should provide Date objects

field_nullable = Field(Int32(2), "description", IcebergString(); required=false)
arrow_t = arrow_type(field_nullable)
# Returns Union{Missing, String} - users can provide String or missing values
```
"""
function arrow_type(field::Field)
    base_type = iceberg_type_to_arrow_type(field.type)
    if field.required
        return base_type
    else
        return Union{Missing, base_type}
    end
end

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
an Iceberg table with this schema. For nullable fields, the type will be `Union{Missing, T}`.

# Example

```julia
schema = Schema([
    Field(Int32(1), "id", "long"; required=true),
    Field(Int32(2), "event_date", "date"; required=false),
    Field(Int32(3), "event_time", "timestamp"; required=true),
])

types = arrow_types(schema)
# Returns Dict{String, Type}:
#   "id" => Int64
#   "event_date" => Union{Missing, Date}
#   "event_time" => Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MICROSECOND, nothing}
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
        "type" => _iceberg_type_to_string(field.type),
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
    |> s -> add_field(s, "id", IcebergLong(); required=true)
    |> s -> add_field(s, "name", IcebergString())
    |> s -> add_field(s, "timestamp", IcebergTimestamp())
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
        type::Union{AbstractString, AbstractIcebergType};
        required::Bool=false,
        doc::Union{String, Nothing}=nothing
    )::SchemaBuilder

Add a field to the schema builder with auto-incrementing field ID.
"""
function add_field(
    builder::SchemaBuilder,
    name::String,
    type::Union{AbstractString, AbstractIcebergType};
    required::Bool=false,
    doc::Union{String, Nothing}=nothing
)::SchemaBuilder
    iceberg_type = isa(type, AbstractIcebergType) ? type : _string_to_iceberg_type(type)
    field = Field(builder.next_field_id, name, iceberg_type; required=required, doc=doc)
    push!(builder.fields, field)
    builder.next_field_id += 1
    return builder
end

"""
    add_field(
        builder::SchemaBuilder,
        id::Int32,
        name::String,
        type::Union{AbstractString, AbstractIcebergType};
        required::Bool=false,
        doc::Union{String, Nothing}=nothing
    )::SchemaBuilder

Add a field to the schema builder with explicit field ID.
"""
function add_field(
    builder::SchemaBuilder,
    id::Int32,
    name::String,
    type::Union{AbstractString, AbstractIcebergType};
    required::Bool=false,
    doc::Union{String, Nothing}=nothing
)::SchemaBuilder
    iceberg_type = isa(type, AbstractIcebergType) ? type : _string_to_iceberg_type(type)
    field = Field(id, name, iceberg_type; required=required, doc=doc)
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

function _schema_from_json(json::String)::Schema
    parsed = JSON.parse(json)
    fields = Field[
        Field(
            Int32(f["id"]),
            String(f["name"]),
            String(f["type"]);
            required = get(f, "required", false),
            doc = get(f, "doc", nothing),
        )
        for f in parsed["fields"]
    ]
    identifier_field_ids = Int32[Int32(id) for id in get(parsed, "identifier-field-ids", [])]
    return Schema(fields; identifier_field_ids=identifier_field_ids)
end

"""
    schema_from_table(table::Table)::Schema

Get the current schema of an Iceberg table as a `Schema` object.

Parses the table's JSON schema and constructs a `Schema` with typed `Field` objects.

# Example
```julia
table = table_open("s3://bucket/path/metadata/metadata.json")
schema = schema_from_table(table)
for field in schema.fields
    println(field.name, ": ", field.type)
end
```
"""
function schema_from_table(table::Table)::Schema
    return _schema_from_json(table_schema(table))
end
