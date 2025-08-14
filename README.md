# Iceberg.jl

A Julia package that provides bindings to the Iceberg C API, allowing you to read Apache Iceberg tables directly from Julia.

## Overview

This package wraps the `iceberg_c_api.h` interface with Julia bindings, providing both low-level C API access and high-level Julia interfaces for working with Iceberg tables. It supports reading data from Iceberg tables and provides an iterator interface over Arrow format data.

## Features

- **Low-level C API bindings**: Direct access to all Iceberg C API functions
- **High-level Julia interface**: Easy-to-use `read_iceberg_table()` function that returns an iterator
- **Arrow integration**: Seamless iteration over Arrow.Table objects
- **Dynamic library loading**: Automatic loading and unloading of the C library
- **Memory management**: Proper cleanup of C resources
- **Error handling**: Comprehensive error reporting and handling
- **Iterator-based API**: Memory-efficient streaming of data

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd Iceberg.jl/Iceberg
```

2. Install the package in Julia:
```julia
using Pkg
Pkg.add(path=".")
```

3. Install dependencies:
```julia
Pkg.instantiate()
```

## Prerequisites

Before using this package, you need to:

1. **Build the Iceberg C API library**: Make sure you have the `libiceberg_rust_ffi.dylib` (macOS) or `libiceberg_rust_ffi.so` (Linux) library built from the `iceberg_rust_ffi` project.

2. **Set up S3 credentials**: If reading from S3, ensure your AWS credentials are properly configured.

## Usage

### High-level API (Recommended)

```julia
using Iceberg
using DataFrames  # Optional: for converting Arrow.Table to DataFrame

# Read an entire Iceberg table - returns an iterator over Arrow.Table objects
table_iterator = read_iceberg_table(
    "s3://bucket/path/to/table",
    "metadata/metadata-file.json"
)

# Iterate over Arrow.Table objects
for arrow_table in table_iterator
    # Convert to DataFrame if needed
    df = DataFrame(arrow_table)
    
    # Process your data...
    println("Batch size: ", size(df))
    println("Columns: ", names(df))
end

# Read specific columns only
table_iterator = read_iceberg_table(
    "s3://bucket/path/to/table",
    "metadata/metadata-file.json",
    columns=["id", "name", "value"]
)

# Collect all batches into a single DataFrame (if needed)
all_dataframes = DataFrame[]
for arrow_table in table_iterator
    push!(all_dataframes, DataFrame(arrow_table))
end
combined_df = reduce(vcat, all_dataframes)
```

### Low-level API

```julia
using Iceberg

# Load the library
load_iceberg_library()

# Open a table
result, table = iceberg_table_open(
    "s3://bucket/path/to/table",
    "metadata/metadata-file.json"
)

if result == ICEBERG_OK
    # Create a scan
    result, scan = iceberg_table_scan(table)
    
    if result == ICEBERG_OK
        # Read batches
        while true
            result, batch_ptr = iceberg_scan_next_batch(scan)
            
            if result == ICEBERG_END_OF_STREAM
                break
            elseif result == ICEBERG_OK
                # Process the batch
                batch = unsafe_load(batch_ptr)
                
                # Convert to Arrow.Table
                io = IOBuffer(unsafe_wrap(Array, batch.data, batch.length))
                arrow_table = Arrow.Table(io)
                
                # Convert to DataFrame if needed
                df = DataFrame(arrow_table)
                
                # Do something with the data...
                println("Batch: ", size(df))
                
                # Free the batch
                iceberg_arrow_batch_free(batch_ptr)
            end
        end
        
        # Cleanup
        iceberg_scan_free(scan)
    end
    
    iceberg_table_free(table)
end

# Unload the library
unload_iceberg_library()
```

## API Reference

### High-level Functions

- `read_iceberg_table(table_path, metadata_path; columns=String[])` - Read an Iceberg table and return an iterator over Arrow.Table objects

### Iterator Interface

The `read_iceberg_table()` function returns an `IcebergTableIterator` that implements the standard Julia iterator interface:

```julia
# Iterator properties
Base.eltype(::Type{IcebergTableIterator}) == Arrow.Table
Base.IteratorSize(::Type{IcebergTableIterator}) == Base.SizeUnknown()

# Usage
for arrow_table in table_iterator
    # Process each Arrow.Table
end
```

### Low-level Functions

- `load_iceberg_library(lib_path)` - Load the C library
- `unload_iceberg_library()` - Unload the C library
- `iceberg_table_open(table_path, metadata_path)` - Open an Iceberg table
- `iceberg_table_free(table)` - Free table resources
- `iceberg_table_scan(table)` - Create a scan for the table
- `iceberg_scan_select_columns(scan, column_names)` - Select specific columns
- `iceberg_scan_free(scan)` - Free scan resources
- `iceberg_scan_next_batch(scan)` - Get the next Arrow batch
- `iceberg_arrow_batch_free(batch_ptr)` - Free batch resources
- `iceberg_error_message()` - Get the last error message

### Constants

- `ICEBERG_OK` - Success
- `ICEBERG_ERROR` - General error
- `ICEBERG_NULL_POINTER` - Null pointer error
- `ICEBERG_IO_ERROR` - I/O error
- `ICEBERG_INVALID_TABLE` - Invalid table error
- `ICEBERG_END_OF_STREAM` - End of stream

## Testing

Run the test suite:

```julia
using Pkg
Pkg.test("Iceberg")
```

The tests replicate the functionality of the C integration test (`integration_test.c`) but using Julia bindings. Note that some tests may fail if S3 credentials are not configured or if the test data is not available.

## Error Handling

The package provides comprehensive error handling:

```julia
try
    table_iterator = read_iceberg_table("s3://bucket/table", "metadata.json")
    for arrow_table in table_iterator
        # Process data...
    end
catch e
    println("Error: ", e)
    # Check for specific error types
    if occursin("S3", string(e))
        println("S3 access error - check credentials")
    end
end
```

## Memory Management

The package automatically manages memory for C resources:

- Tables and scans are automatically freed when the iterator completes
- Arrow batches are freed after processing each iteration
- The library is automatically unloaded when the Julia process exits
- The iterator interface provides memory-efficient streaming

## Performance

- The package uses zero-copy operations where possible
- Arrow data is efficiently streamed via the iterator interface
- Memory is managed efficiently with proper cleanup
- No need to load entire tables into memory at once

## Troubleshooting

### Common Issues

1. **Library not found**: Ensure `libiceberg_rust_ffi.dylib` is in your library path
2. **S3 access denied**: Check your AWS credentials and permissions
3. **Memory errors**: Ensure you're not holding references to freed C objects

### Debug Mode

Enable debug output by setting the environment variable:
```bash
export JULIA_DEBUG=Iceberg
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the same license as the parent Iceberg project.

## Dependencies

- **Libdl**: For dynamic library loading
- **Arrow**: For Arrow format support
- **Test**: For testing (development dependency) 