# RustyIceberg.jl Basic Usage Example

This example demonstrates how to use RustyIceberg.jl to read Iceberg tables from S3.

## Prerequisites

1. **Julia** with the following packages installed:
   - `Arrow`
   - `DataFrames`

2. **Iceberg C API library** built and available

3. **S3 access** with appropriate credentials

## Setup

1. **Copy the environment template:**
   ```bash
   cp env.example .env
   ```

2. **Edit the `.env` file** with your actual configuration:
   ```bash
   # S3 Configuration
   AWS_ACCESS_KEY_ID=your_actual_access_key
   AWS_SECRET_ACCESS_KEY=your_actual_secret_key
   AWS_DEFAULT_REGION=us-east-1

   # Iceberg Configuration
   ICEBERG_S3_ENDPOINT=http://localhost:9000
   ICEBERG_S3_FORCE_PATH_STYLE=true
   ```

## Running the Example

```bash
julia basic_usage.jl
```

## What the Example Does

1. **Loads environment variables** from the `.env` file
2. **Loads the Iceberg C API library** using the path from environment variables
3. **Reads an actual Iceberg table** from S3 using the same paths as the integration test:
   - Table: `s3://vustef-dev/tpch-sf0.1-no-part/nation`
   - Metadata: `metadata/00001-894cf98a-a055-47ba-a701-327455060d32.metadata.json`
4. **Displays table information** including row count, column names, and sample data
5. **Tests column selection** by reading only specific columns

## Expected Output

The example should output something like:
```
=== RustyIceberg.jl Basic Usage Example ===
Loading RustyIceberg package...
✅ Package loaded successfully!
Loading environment variables from /path/to/.env...
   Set AWS_ACCESS_KEY_ID
   Set AWS_SECRET_ACCESS_KEY
   ...
✅ Environment variables loaded!
Testing library loading...
Using library path from environment: ../../iceberg_rust_ffi/libiceberg_rust_ffi.dylib
✅ Library loaded!
Testing table reading with actual data...
Table path: s3://vustef-dev/tpch-sf0.1-no-part/nation
Metadata path: metadata/00001-894cf98a-a055-47ba-a701-327455060d32.metadata.json
Reading Iceberg table...
✅ Table read successfully!
📊 DataFrame info:
   - Rows: 25
   - Columns: 4
   - Column names: ["n_nationkey", "n_name", "n_regionkey", "n_comment"]
📋 First few rows:
...
```

## Troubleshooting

If you encounter errors:

1. **Check your `.env` file** - make sure all paths and credentials are correct
2. **Verify S3 access** - ensure your credentials have access to the specified bucket
4. **Verify table exists** - ensure the table and metadata files exist at the specified paths

## Customization

To use with your own data:

1. Update the `table_path` and `metadata_path` variables in `basic_usage.jl`
2. Ensure your S3 credentials have access to the new table
3. Run the example again

The `read_iceberg_table` function supports:
- Reading all columns: `read_iceberg_table(table_path, metadata_path)`
- Reading specific columns: `read_iceberg_table(table_path, metadata_path, columns=["col1", "col2"])` 