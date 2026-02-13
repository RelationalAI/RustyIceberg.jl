# Test configuration helper
#
# This module provides functions to get test configuration from environment variables
# with sensible defaults for local development with docker-compose.

"""
    get_catalog_uri() -> String

Get the Polaris catalog URI from environment variable POLARIS_CATALOG_URI.
Defaults to "http://localhost:8181/api/catalog".
"""
function get_catalog_uri()
    return get(ENV, "POLARIS_CATALOG_URI", "http://localhost:8181/api/catalog")
end

"""
    get_token_endpoint() -> String

Get the Polaris token endpoint from environment variable POLARIS_TOKEN_ENDPOINT.
Defaults to "http://localhost:8181/api/catalog/v1/oauth/tokens".
"""
function get_token_endpoint()
    return get(ENV, "POLARIS_TOKEN_ENDPOINT", "http://localhost:8181/api/catalog/v1/oauth/tokens")
end

"""
    get_polaris_credentials() -> Tuple{String, String}

Get the Polaris client credentials from environment variables.
Returns (client_id, client_secret).
"""
function get_polaris_credentials()
    client_id = get(ENV, "POLARIS_CLIENT_ID", "root")
    client_secret = get(ENV, "POLARIS_CLIENT_SECRET", "s3cr3t")
    return (client_id, client_secret)
end

"""
    get_polaris_realm() -> String

Get the Polaris realm from environment variable POLARIS_REALM.
Defaults to "POLARIS".
"""
function get_polaris_realm()
    return get(ENV, "POLARIS_REALM", "POLARIS")
end

"""
    get_s3_config() -> Dict{String, String}

Get the S3/MinIO configuration from environment variables.
Returns a Dict with keys: endpoint, access_key_id, secret_access_key, region.
"""
function get_s3_config()
    return Dict(
        "endpoint" => get(ENV, "S3_ENDPOINT", "http://localhost:9000"),
        "access_key_id" => get(ENV, "S3_ACCESS_KEY_ID", "root"),
        "secret_access_key" => get(ENV, "S3_SECRET_ACCESS_KEY", "password"),
        "region" => get(ENV, "S3_REGION", "us-east-1")
    )
end

"""
    get_warehouse_name() -> String

Get the warehouse name from environment variable ICEBERG_WAREHOUSE.
Defaults to "warehouse".
"""
function get_warehouse_name()
    return get(ENV, "ICEBERG_WAREHOUSE", "warehouse")
end

"""
    get_catalog_properties() -> Dict{String, String}

Get the full catalog properties dict for RustyIceberg.catalog_create_rest.
Combines Polaris credentials with S3 configuration.
"""
function get_catalog_properties()
    client_id, client_secret = get_polaris_credentials()
    s3 = get_s3_config()

    return Dict(
        "credential" => "$(client_id):$(client_secret)",
        "scope" => "PRINCIPAL_ROLE:ALL",
        "warehouse" => get_warehouse_name(),
        "s3.endpoint" => s3["endpoint"],
        "s3.access-key-id" => s3["access_key_id"],
        "s3.secret-access-key" => s3["secret_access_key"],
        "s3.region" => s3["region"]
    )
end

"""
    get_catalog_properties_minimal() -> Dict{String, String}

Get minimal catalog properties (without S3 config) for tests that don't need it.
"""
function get_catalog_properties_minimal()
    client_id, client_secret = get_polaris_credentials()

    return Dict(
        "credential" => "$(client_id):$(client_secret)",
        "scope" => "PRINCIPAL_ROLE:ALL",
        "warehouse" => get_warehouse_name()
    )
end

const AWS_ENV_VARS = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
                      "AWS_REGION", "AWS_ENDPOINT_URL", "AWS_SESSION_TOKEN"]

"""
    without_aws_env(f)

Run `f()` with all AWS environment variables unset, then restore them.
Useful for tests that need to verify credentials are obtained through
the catalog rather than from ambient environment.
"""
function without_aws_env(f)
    saved = Dict{String,String}()
    for var in AWS_ENV_VARS
        if haskey(ENV, var)
            saved[var] = ENV[var]
            delete!(ENV, var)
        end
    end
    try
        f()
    finally
        for (var, val) in saved
            ENV[var] = val
        end
    end
end
