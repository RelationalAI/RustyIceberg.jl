# Binary-compatibility tests for IcebergPerfConfig <-> IcebergPerfConfigFFI.
#
# IcebergPerfConfig (Julia) is passed BY VALUE across the FFI into new_scan /
# new_incremental_scan, where Rust reinterprets the bytes as its repr(C)
# IcebergPerfConfigFFI struct. A silent layout drift would corrupt scans, so we
# guard the layout here. These tests are server-free (no catalog / object store):
# `iceberg_perf_config_roundtrip` is a pure synchronous FFI echo.

@testset "IcebergPerfConfig FFI binary compatibility" begin
    # The struct must be isbits (so it passes by value with no conversion) and
    # exactly 5 × UInt64 wide, matching the Rust `#[repr(C)]` struct of 5 × u64.
    @test isbitstype(RustyIceberg.IcebergPerfConfig)
    @test sizeof(RustyIceberg.IcebergPerfConfig) == 5 * sizeof(UInt64)

    # Round-trip through the real FFI with five DISTINCT sentinel values so a
    # transposed pair of fields is detected (not just a size mismatch).
    cfg = RustyIceberg.IcebergPerfConfig(;
        batch_size = 11,
        manifest_file_concurrency_limit = 22,
        manifest_entry_concurrency_limit = 33,
        file_prefetch_depth = 44,
        serialization_concurrency_limit = 55,
    )
    out = zeros(UInt64, 5)
    GC.@preserve out @ccall RustyIceberg.rust_lib.iceberg_perf_config_roundtrip(
        cfg::RustyIceberg.IcebergPerfConfig,
        out::Ptr{UInt64},
    )::Cvoid
    @test out == UInt64[11, 22, 33, 44, 55]

    println("✅ IcebergPerfConfig FFI binary compatibility verified")
end
