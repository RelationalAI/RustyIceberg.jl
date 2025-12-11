# RustyIceberg.jl

[![CI](https://github.com/RelationalAI/RustyIceberg.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/RelationalAI/RustyIceberg.jl/actions/workflows/CI.yml)

A Julia package that provides bindings on top of Iceberg Rust crate, allowing you to read Apache Iceberg tables directly from Julia.

## Overview

This package wraps the iceberg_rust_ffi interface with Julia bindings, providing Julia interfaces for working with Iceberg tables. It supports reading data from Iceberg tables in full-scan and incremental-scan modes.

## Installation
1. Install the package in Julia:
```julia
using Pkg
Pkg.add("RustyIceberg")
```

2. Install dependencies:
```julia
Pkg.instantiate()
```

## Development

When working on RustyIceberg.jl, you can either use the precompiled
[iceberg_rust_ffi_jll.jl](https://github.com/JuliaBinaryWrappers/iceberg_rust_ffi_jll.jl) package
or use a local build of [iceberg_rust_ffi](./iceberg_rust_ffi/).

### Custom JLL
If you want to test a custom code, you can refer directly to this repo's branch (or a fork). However, if the change involves the FFI change, you might want to build a custom JLL so that the downstream projects don't have to have Rust toolchain in their repository. The way we do it here is by mimicking the JuliaBinaryWrappers/iceberg_rust_ffi_jll.jl structure in https://github.com/RelationalAI/iceberg_rust_ffi_jll.jl/. All one has to do is invoke this [GitHub Action there](https://github.com/RelationalAI/iceberg_rust_ffi_jll.jl/actions/workflows/build-and-deploy.yml), follow the README in that repo for details.
This workflow will produce a new branch in that repo. Then in your Julia simply refer to that repo with a repo-rev equal to the commit of the newly produced branch in that repo.

### Using Local Builds

To use a local Rust library build, set a preference using `Preferences.jl` with the **full path to the library file**:

```julia
using Libdl, Preferences
lib_path = joinpath(expanduser("~/repos/iceberg_rust_ffi/target/release"), "libiceberg_rust_ffi." * Libdl.dlext)
set_preferences!("iceberg_rust_ffi_jll", "libiceberg_rust_ffi_path" => lib_path; force=true)
```

The `Libdl.dlext` ensures the correct extension for your platform:
- macOS: `.dylib`
- Linux: `.so`
- Windows: `.dll`

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

### Testing with Docker

For end-to-end testing with Apache Polaris and MinIO, use Docker Compose:

```bash
cd docker
docker compose up -d
```

This starts:
- **MinIO** (port 9000): S3-compatible storage with credentials `root` / `password`
- **Polaris** (port 8181): Iceberg REST catalog with credentials `root` / `s3cr3t`
- **Test datasets**: TPC-H scale factor 0.01 and incremental test data

See [docker/README.md](./docker/README.md) for detailed configuration and troubleshooting.

## CI
CI runs with the custom iceberg_rust_ffi, built from the source. Releases run with official JLL, which is the default. CI overrides default using Preferences.jl.

## Release

### JLL Release
To make a JLL release, create a new PR in JuliaPackaging/Yggdrasil repo, e.g. like [this one](https://github.com/JuliaPackaging/Yggdrasil/pull/12532/files).
It's not necessary, but it's a good practice to upgrade version in Cargo.toml in iceberg_rust_ffi, so that we can correlate JLL version with iceberg_rust_ffi Rust version.

### RustyIceberg release
To create a new RustyIceberg release, simply bump the version in Project.toml, merge that in `main`, and then open that commit and comment like [here](https://github.com/RelationalAI/RustyIceberg.jl/commit/cbebb0e9611f70867e6ad2fbca0060a44345ae31#commitcomment-170551595). This will trigger an update in JuliaRegistries (should take ~20m), which will then invoke a TagBot in this repository, which will also run CI tests with the official JLL.
