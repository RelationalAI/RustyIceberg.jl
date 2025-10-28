# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository. Most of the commands are for stuff inside `iceberg_rust_ffi` folder, with the exception of the `RustyIceberg.jl` section below.

## Common Development Commands

### Building
- `cargo build` - Build the Rust FFI library (debug)
- `cargo build --release --no-default-features` - Build the Rust FFI library (production)
- `make build-lib` - Build the Rust library and generate C header using cbindgen

### Testing
- `./run_integration_test.sh` - Recommended way to run the full integration test (builds everything and runs test with colored output) locally (requires containers).
- `make all` - Build everything and run integration test (requires containers)
- `make run-containers` - Start Docker containers for S3 testing
- `make test` - Run the integration test (requires build and containers)
- `make stop-containers` - Stop Docker containers
- `cargo test` - Run Rust unit tests

### Code Quality
- `cargo fmt` - Format Rust code
- `cargo clippy` - Run Rust linter
- `cargo check` - Quick check for Rust compilation errors

### Cleanup
- `make clean` - Clean build artifacts (but keep target directory)
- `make clean-all` - Clean everything including target directory

## Architecture Overview

This project provides a **Foreign Function Interface (FFI)** for Apache Iceberg, allowing C programs (and other languages through C bindings) to access Iceberg tables stored in object storage systems like S3. The majority of the infrastucture relies on object_store_ffi crate. If you don't have access to that crate's code locally, access it at this [URL](https://github.com/RelationalAI/object_store_ffi).

### Key Components

#### Rust Library (`src/lib.rs`)
- **Core FFI Implementation**: Exposes Iceberg functionality through C-compatible functions
- **Async Runtime Integration**: Uses Tokio for async operations with object_store_ffi for callback handling. Async operations rely on `export_runtime_op!` macro, which has a sync block, which is a builder function, where all deserialization and conversion is done. Then the result of that is passed to an async block. Each parameter has to implement Send trait, in order to be passed to the async block
- **Julia Integration**: Conditional compilation features for Julia interop (`julia` feature flag)
- **Memory Management**: Safe FFI patterns with proper cleanup functions

#### C header (`include/iceberg_rust_ffi.h`)
- **Manual Generation**: C header is not generated right now. Whenever you make a change in the Rust library, examine whether the header should be updated.
- **C99 Compatible**: Ensures compatibility with standard C compilers
- **Response Structures**: Async operations return response structures with context for cancellation

#### Integration Test (`tests/integration_test.c`)
- **Dynamic Loading**: Uses `dlopen`/`dlsym` to load the Rust library at runtime
- **Async API Testing**: Tests the new async API with response structures and callbacks
- **S3 Integration**: Connects to S3 (or MinIO) to test real object storage operations

### FFI Design Patterns

#### Async Operations with Callbacks
The FFI uses an async callback pattern where:
1. C calls an async function with a response structure
2. Rust spawns the operation and returns immediately
3. When complete, Rust invokes a callback to signal completion
4. C polls or waits for completion, then checks the response structure

#### Memory Management
- **Owned Pointers**: Rust allocates, C receives opaque pointers
- **Cleanup Functions**: Every allocated resource has a corresponding `_free` function
- **Error Handling**: Errors are returned via response structures with allocated error strings

#### Context and Cancellation
- Operations return a context pointer that can be used for cancellation
- `iceberg_cancel_context` and `iceberg_destroy_context` functions manage operation lifecycle

### S3 Configuration

The integration test expects AWS S3 credentials through environment variables:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` or `AWS_DEFAULT_REGION`
- `AWS_ENDPOINT_URL` (for MinIO or custom S3-compatible storage)

Use the `.env` file or export variables directly. The test is designed to fail with permission errors when S3 paths are inaccessible, which confirms the API is working correctly.

### Build System

#### Cargo Features
- Default features: `["julia"]`
- `julia` feature: Enables Julia thread adoption and GC integration
- Integration tests use `--no-default-features` to avoid Julia dependencies

### RustyIceberg.jl

Whenever making API changes in the iceberg_rust_ffi, the corresponding changes should be made in its parent folder. The parent folder is home for Julia package, which provides Julia bindings on top of the FFI.
Once changes are made there, they should be tested by:
1. Doing `cargo build` (with default features) for the iceberg_rust_ffi.
2. Invoking `ICEBERG_RUST_LIB=<path_to_iceberg_rust_ffi_folder>/target/debug julia --project=. examples/basic_usage.jl` in the RustyIceberg.jl directory.

## Development Notes

### Working with FFI
- Always check for null pointers in C code before dereferencing
- Use the provided `_free` functions to avoid memory leaks
- Error messages are allocated strings that must be freed with `iceberg_destroy_cstring`

### Testing Changes
Run the integration test after making changes to verify the FFI still works:
```bash
./run_integration_test.sh
```

### Object Store Integration
This crate depends on `object_store_ffi` for async runtime management and callback handling. The integration provides:
- Cross-platform async runtime setup
- Callback infrastructure for async operations
- Context management for cancellation support