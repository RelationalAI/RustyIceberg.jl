#!/usr/bin/env julia
using BenchmarkTools

# Basic usage example for RustyIceberg.jl
println("=== RustyIceberg.jl Basic Usage Example ===")

# Load the package
println("Loading RustyIceberg package...")
include("../src/RustyIceberg.jl")
using .RustyIceberg
using DataFrames

println("✅ Package loaded successfully!")

# Function to load environment variables from .env file
function load_env_file(env_path::String)
    if !isfile(env_path)
        println("⚠️  .env file not found at $env_path")
        println("   Please copy env.example to .env and configure your environment variables")
        return false
    end

    println("Loading environment variables from $env_path...")
    for line in eachline(env_path)
        line = strip(line)
        # Skip comments and empty lines
        if !isempty(line) && !startswith(line, "#")
            if contains(line, "=")
                key, value = split(line, "=", limit=2)
                key = strip(key)
                value = strip(value)
                ENV[key] = value
                println("   Set $key")
            end
        end
    end
    println("✅ Environment variables loaded!")
    return true
end

# Load environment variables
env_loaded = load_env_file(joinpath(@__DIR__, ".env"))

# Initialize Iceberg runtime
println("Initializing Iceberg runtime...")
init_iceberg_runtime()
println("✅ Runtime initialized!")

# Test actual table reading using the same paths as integration test
println("Testing table reading with actual data...")

# Use the same table and metadata paths as in integration_test.c
# table_path = "s3://vustef-dev/tpch-sf0.1-no-part/nation"
# metadata_path = "metadata/00001-1744d9f4-1472-4f8c-ac86-b0b7c291248e.metadata.json"
table_path = "s3://vustef-dev/tpch-sf0.1-no-part/customer"
metadata_path = "metadata/00001-0789fc06-57dd-45b5-b5cc-42ef1386b497.metadata.json"

println("Table path: $table_path")
println("Metadata path: $metadata_path")

function read_table(table_path, metadata_path, benchmark::Bool=false)
    try
        # Read the table using the high-level function - now returns an iterator
        !benchmark && println("Reading Iceberg table...")
        table_iterator = read_iceberg_table(table_path, metadata_path)

        !benchmark && println("✅ Table iterator created successfully!")

        # Iterate over Arrow.Table objects and convert to DataFrames
        all_dataframes = DataFrame[]
        batch_count = 0

        for arrow_table in table_iterator
            batch_count += 1
            df = DataFrame(arrow_table)
            push!(all_dataframes, df)

            if !benchmark
                println("📦 Batch $batch_count:")
                println("   - Rows: $(nrow(df))")
                println("   - Columns: $(ncol(df))")
                println("   - Column names: $(names(df))")
            end

            # Show first few rows of first batch
            if !benchmark
                if batch_count == 1 && nrow(df) > 0
                        println("📋 First few rows:")
                        println(first(df, 30))
                end
            end
        end

        # Combine all DataFrames
        if !benchmark
            if !isempty(all_dataframes)
                combined_df = reduce(vcat, all_dataframes)
                println("\n📊 Combined DataFrame info:")
                println("   - Total rows: $(nrow(combined_df))")
                println("   - Total columns: $(ncol(combined_df))")
                println("   - Total batches: $batch_count")
            else
                println("\n📊 No data found in table")
            end

            # Test with specific columns
            println("\nTesting column selection...")
            if !isempty(all_dataframes) && !isempty(names(all_dataframes[1]))
                selected_columns = names(all_dataframes[1])[1:min(2, length(names(all_dataframes[1])))]
                println("Selecting columns: $selected_columns")

                selected_iterator = read_iceberg_table(table_path, metadata_path, columns=selected_columns)
                selected_dataframes = DataFrame[]

                for arrow_table in selected_iterator
                    df = DataFrame(arrow_table)
                    push!(selected_dataframes, df)
                end

                if !isempty(selected_dataframes)
                    combined_selected = reduce(vcat, selected_dataframes)
                    println("✅ Column selection successful!")
                    println("   - Selected columns: $(names(combined_selected))")
                    println("   - Rows: $(nrow(combined_selected))")
                end
            end
        end

    catch e
        println("❌ Error reading table: $e")
        if env_loaded
            println("\n💡 Troubleshooting tips:")
            println("   1. Make sure your .env file has correct AWS credentials")
            println("   2. Verify the S3 endpoint is accessible")
            println("   3. Check that the table path and metadata path are correct")
            println("   4. Ensure the Rust library was built correctly")
        else
            println("\n💡 Please configure your .env file first:")
            println("   cp env.example .env")
            println("   # Then edit .env with your actual credentials and paths")
        end
    end
end

read_table(table_path, metadata_path)

# 139.667 ms (11774 allocations: 3.61 MiB)
@btime read_table(table_path, metadata_path, true)

println("\n✅ Basic usage example completed!")
println("\nTo use with your own data:")
println("  # Get iterator over Arrow.Table objects:")
println("  iterator = read_iceberg_table(\"your-table-path\", \"your-metadata-path\")")
println("  for arrow_table in iterator")
println("      df = DataFrame(arrow_table)  # Convert to DataFrame if needed")
println("      # Process your data...")
println("  end")
println("\n  # Or with column selection:")
println("  iterator = read_iceberg_table(\"your-table-path\", \"your-metadata-path\", columns=[\"col1\", \"col2\"])")
