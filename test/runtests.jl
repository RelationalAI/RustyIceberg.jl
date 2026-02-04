using Test
using RustyIceberg
using DataFrames
using Arrow
using FunctionWrappers: FunctionWrapper
using HTTP
using JSON
using Base64

@testset "RustyIceberg.jl" begin

# Include schema tests
include("schema_tests.jl")

@testset "Runtime Initialization" begin
    # Test runtime initialization - this should work
    @test_nowarn init_runtime()

    # Test that we can initialize multiple times safely. But a new static config
    # wouldn't take effect, would silently ignore the config.
    @test_nowarn init_runtime(StaticConfig(1))

    println("✅ Runtime initialization successful")
end

# Include catalog tests after runtime initialization
include("catalog_tests.jl")

# Include scan tests after runtime initialization
include("scan_tests.jl")

# Include transaction tests
include("transaction_tests.jl")

end # End of testset

println("\n🎉 All tests completed!")
