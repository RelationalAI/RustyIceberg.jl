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

When working on RustyIceberg.jl, you can either use the precompiled
[iceberg_rust_ffi_jll.jl](https://github.com/RelationalAI/iceberg_rust_ffi_jll.jl) package
or use a local build of [iceberg_rust_ffi](https://github.com/RelationalAI/iceberg_rust_ffi).

### Using Local Builds

To use a local Rust library build, set a preference using `Preferences.jl`:

```julia
using Preferences
set_preferences!("iceberg_rust_ffi_jll", "libiceberg_rust_ffi_path" => "/path/to/target/release/"; force=true)
```

For example, if you have the `iceberg_rust_ffi` repository at `~/repos/iceberg_rust_ffi`
and build the library by running `cargo build --release`, you would set:

```julia
set_preferences!("iceberg_rust_ffi_jll", "libiceberg_rust_ffi_path" => expanduser("~/repos/iceberg_rust_ffi/target/release/"); force=true)
```

**Note**: After setting preferences, you need to restart Julia or trigger package recompilation for changes to take effect.

### Makefile Targets

The project includes convenient Makefile targets for development:

**Development mode (uses local Rust build):**
- `make test-dev` - Build Rust library and run tests with local build
- `make repl-dev` - Build Rust library and start REPL with local build
- `make set-local-lib` - Set preference to use local Rust library

**Production mode (uses JLL package):**
- `make test` - Run tests with JLL package
- `make repl` - Start REPL with JLL package
- `make clear-local-lib` - Clear local library preference

**Examples:**
```bash
# Development workflow
make test-dev                        # Test with local debug build
make BUILD_TYPE=release test-dev     # Test with local release build

# Switch back to JLL package
make test                            # Clears local preference and tests with JLL
```

To switch back to the JLL package, either run `make clear-local-lib` or manually delete the preference:

```julia
using Preferences
delete_preferences!("iceberg_rust_ffi_jll", "libiceberg_rust_ffi_path")
```

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

- **Arrow**: For Arrow format support
- **Preferences**: For local library path configuration
- **iceberg_rust_ffi_jll**: Precompiled Rust FFI library (JLL package)
- **Test**: For testing (development dependency)