# RustyIceberg.jl

[![CI](https://github.com/RelationalAI/RustyIceberg.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/RelationalAI/RustyIceberg.jl/actions/workflows/CI.yml)

A Julia package that provides bindings to the Iceberg C API, allowing you to read Apache Iceberg tables directly from Julia.

## Overview

This package wraps the iceberg_rust_ffi interface with Julia bindings, providing both low-level C API access and high-level Julia interfaces for working with Iceberg tables. It supports reading data from Iceberg tables and provides an iterator interface over Arrow format data.

## Features

- **Low-level Rust FFI bindings**: Direct access to all Iceberg Rust FFI API functions
- **High-level Julia interface**: Easy-to-use `read_iceberg_table()` function that returns an iterator
- **Arrow integration**: Seamless iteration over Arrow.Table objects
- **Dynamic library loading**: Automatic loading and unloading of the C library
- **Memory management**: Proper cleanup of C resources
- **Error handling**: Comprehensive error reporting and handling
- **Iterator-based API**: Streaming of data

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
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

## Development

When working on RustyIceberg.jl, you can either use
[iceberg_rust_ffi_jll.jl](https://github.com/RelationalAI/iceberg_rust_ffi_jll.jl) or use
a local build of [iceberg_rust_ffi](https://github.com/RelationalAI/iceberg_rust_ffi).
When using a local build, set the environment variable `ICEBERG_RUST_LIB` to the directory
containing the build. For example, if you have the `iceberg_rust_ffi` repository at
`~/repos/iceberg_rust_ffi` and build the library by running `cargo build --release` from
the base of that repository, then you could use that local build by setting
ICEBERG_RUST_LIB="~/repos/iceberg_rust_ffi/target/release".

## Prerequisites

Before using this package, you need to:

1. **Build the Iceberg Rust FFI library**: Make sure you have the `libiceberg_rust_ffi.dylib` (macOS) or `libiceberg_rust_ffi.so` (Linux) library built from the `iceberg_rust_ffi` folder (this will happen automatically also if you just use `make`).

2. **Set up S3 credentials**: If reading from S3, ensure your AWS credentials are properly configured.

## Testing

Run the test suite:

```julia
using Pkg
Pkg.test("RustyIceberg")
```

The tests replicate the functionality of the C integration test (`integration_test.c`) but using Julia bindings. Note that some tests may fail if S3 credentials are not configured or if the test data is not available.

## Troubleshooting

### Common Issues

1. **Library not found**: Ensure `libiceberg_rust_ffi.dylib` is in your library path
2. **S3 access denied**: Check your AWS credentials and permissions
3. **Memory errors**: Ensure you're not holding references to freed C objects

## Dependencies

- **Libdl**: For dynamic library loading
- **Arrow**: For Arrow format support
- **Test**: For testing (development dependency)