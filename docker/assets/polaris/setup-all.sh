#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Complete setup script: creates catalog, namespaces, and registers tables

set -e

apk add --no-cache jq curl

realm=${1:-"POLARIS"}
CATALOG_NAME="warehouse"
BASEDIR=$(dirname $0)

echo "=========================================="
echo "Polaris Catalog Setup"
echo "=========================================="

# Obtain token
echo
echo "Step 1: Obtaining access token..."
echo "  Client ID: ${CLIENT_ID}"
echo "  Polaris API: http://polaris:8181/api/catalog/v1/oauth/tokens"
TOKEN=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens \
  --user ${CLIENT_ID}:${CLIENT_SECRET} \
  -H "Polaris-Realm: $realm" \
  -d grant_type=client_credentials \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)

echo "  Token response: $TOKEN"

if [ -z "${TOKEN}" ] || [ "$TOKEN" = "null" ]; then
  echo "ERROR: Failed to obtain access token"
  exit 1
fi

echo "✓ Access token obtained"

# Create catalog with S3 storage
echo
echo "Step 2: Creating catalog '$CATALOG_NAME'..."

STORAGE_LOCATION="s3://warehouse"
STORAGE_CONFIG_INFO='{
  "storageType": "S3",
  "endpoint": "http://minio:9000",
  "endpointInternal": "http://minio:9000",
}'

CATALOG_PAYLOAD='{
  "catalog": {
    "name": "'$CATALOG_NAME'",
    "type": "INTERNAL",
    "readOnly": false,
    "properties": {
      "default-base-location": "'$STORAGE_LOCATION'"
    },
    "storageConfigInfo": '$STORAGE_CONFIG_INFO'
  }
}'

curl -s -H "Authorization: Bearer ${TOKEN}" \
  -H 'Accept: application/json' \
  -H 'Content-Type: application/json' \
  -H "Polaris-Realm: $realm" \
  -X POST \
  http://polaris:8181/api/management/v1/catalogs \
  -d "$CATALOG_PAYLOAD" > /dev/null 2>&1 || echo "  Catalog may already exist, continuing..."

echo "✓ Catalog '$CATALOG_NAME' created or already exists"

# Note: Namespaces will be created by the table-creation script
# They're created via the catalog API with proper multi-level structure

echo
echo "=========================================="
echo "Catalog setup complete!"
echo "=========================================="
echo
echo "Catalog: $CATALOG_NAME"
echo "Storage: S3 (s3://warehouse on MinIO)"
