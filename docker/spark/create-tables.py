#!/usr/bin/env python3
"""
Register Iceberg tables with Polaris catalog.
This script creates table metadata entries in Polaris for the pre-loaded datasets.
Run this after datasets have been uploaded to S3.
"""

import os
import sys
import time
import traceback
import requests

# Configuration
POLARIS_API = os.getenv('POLARIS_API', 'http://polaris:8181/api/management/v1')
POLARIS_CATALOG_API = os.getenv('POLARIS_CATALOG_API', 'http://polaris:8181/api/catalog/v1')
CLIENT_ID = os.getenv('CLIENT_ID', 'root')
CLIENT_SECRET = os.getenv('CLIENT_SECRET', 's3cr3t')
CATALOG_NAME = 'warehouse'
S3_WAREHOUSE = 's3://warehouse'

# Table definitions: (namespace, table_name, s3_location)
# Namespaces can contain dots, e.g., 'tpch.sf01' is a valid single-level namespace name
TABLES_TO_CREATE = [
    # TPC-H tables in 'tpch.sf01' namespace (contains a dot in the name)
    # Data is stored at tpch.sf01/ and Polaris assigns location s3://warehouse/tpch.sf01/
    ('tpch.sf01', 'customer', f'{S3_WAREHOUSE}/tpch.sf01/customer'),
    ('tpch.sf01', 'lineitem', f'{S3_WAREHOUSE}/tpch.sf01/lineitem'),
    ('tpch.sf01', 'nation', f'{S3_WAREHOUSE}/tpch.sf01/nation'),
    ('tpch.sf01', 'orders', f'{S3_WAREHOUSE}/tpch.sf01/orders'),
    ('tpch.sf01', 'part', f'{S3_WAREHOUSE}/tpch.sf01/part'),
    ('tpch.sf01', 'partsupp', f'{S3_WAREHOUSE}/tpch.sf01/partsupp'),
    ('tpch.sf01', 'region', f'{S3_WAREHOUSE}/tpch.sf01/region'),
    ('tpch.sf01', 'supplier', f'{S3_WAREHOUSE}/tpch.sf01/supplier'),
    # Incremental test tables
    ('incremental', 'test1', f'{S3_WAREHOUSE}/incremental/test1'),
]


def wait_for_polaris(max_retries=30):
    """Wait for Polaris to be ready by checking if we can get a token."""
    print("Waiting for Polaris to be ready...")
    for attempt in range(max_retries):
        try:
            # Check if Polaris responds to token request
            response = requests.post(
                f'{POLARIS_CATALOG_API}/oauth/tokens',
                auth=(CLIENT_ID, CLIENT_SECRET),
                data={
                    'grant_type': 'client_credentials',
                    'scope': 'PRINCIPAL_ROLE:ALL'
                },
                headers={'Polaris-Realm': 'POLARIS'},
                timeout=5
            )
            # If we get any response (200, 400+), service is up
            if response.status_code >= 100:
                print("✓ Polaris is responding")
                return True
        except Exception as e:
            if attempt == 0:
                print(f"  Attempt {attempt + 1}/{max_retries}: {type(e).__name__}")
            elif attempt % 5 == 0:
                print(f"  Attempt {attempt + 1}/{max_retries}")

        if attempt < max_retries - 1:
            time.sleep(1)

    print("ERROR: Polaris did not become ready in time")
    return False


def get_access_token():
    """Obtain OAuth access token from Polaris."""
    print("\nObtaining access token from Polaris...")
    try:
        response = requests.post(
            f'{POLARIS_CATALOG_API}/oauth/tokens',
            auth=(CLIENT_ID, CLIENT_SECRET),
            data={
                'grant_type': 'client_credentials',
                'scope': 'PRINCIPAL_ROLE:ALL'
            },
            headers={'Polaris-Realm': 'POLARIS'},
            timeout=10
        )

        print(f"Token endpoint response: {response.status_code}")

        if response.status_code != 200:
            print(f"ERROR: Failed to get token: {response.status_code}")
            print(f"Response body: {response.text}")
            return None

        try:
            token = response.json().get('access_token')
        except:
            print(f"ERROR: Could not parse response as JSON")
            print(f"Response body: {response.text}")
            return None

        if not token:
            print("ERROR: No access token in response")
            print(f"Response: {response.json()}")
            return None

        print(f"✓ Got access token: {token[:20]}...")
        return token
    except Exception as e:
        print(f"ERROR: Failed to obtain token: {e}")
        traceback.print_exc()
        return None


def create_namespace(token, namespace):
    """Create a namespace if it doesn't exist.
    namespace should be a string, which can contain dots (e.g., 'tpch.sf01').
    """
    print(f"  Creating namespace: {namespace}")

    # Check if namespace already exists
    try:
        ns_path = f'{POLARIS_CATALOG_API}/{CATALOG_NAME}/namespaces/{namespace}'
        response = requests.get(
            ns_path,
            headers={'Authorization': f'Bearer {token}'},
            timeout=10
        )

        if response.status_code == 200:
            print(f"    ℹ Namespace already exists")
            return True
    except Exception as e:
        pass

    # Create the namespace
    try:
        payload = {'namespace': [namespace]}  # Single-level namespace as a list with one element
        response = requests.post(
            f'{POLARIS_CATALOG_API}/{CATALOG_NAME}/namespaces',
            headers={
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            },
            json=payload,
            timeout=10
        )

        if response.status_code in [200, 201]:
            print(f"    ✓ Namespace created")
            return True
        elif response.status_code == 409:
            print(f"    ℹ Namespace already exists")
            return True
        else:
            print(f"    ERROR: Failed to create namespace (status {response.status_code})")
            print(f"    Response: {response.text}")
            return False
    except Exception as e:
        print(f"    ERROR: Failed to create namespace: {e}")
        return False


def create_table(token, namespace, table_name, location):
    """Register an existing Iceberg table in Polaris using its metadata file.
    namespace should be a string, which can contain dots (e.g., 'tpch.sf01').
    """
    print(f"\nRegistering table: {CATALOG_NAME}.{namespace}.{table_name}")
    print(f"  Location: {location}")

    # Check if table already exists
    try:
        ns_path = f'{POLARIS_CATALOG_API}/{CATALOG_NAME}/namespaces/{namespace}'
        response = requests.get(
            f'{ns_path}/tables/{table_name}',
            headers={'Authorization': f'Bearer {token}'},
            timeout=10
        )

        if response.status_code == 200:
            print(f"  ℹ Table already exists, skipping")
            return True

    except Exception as e:
        pass

    # Find the latest metadata file in the table location
    try:
        import boto3
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='root',
            aws_secret_access_key='password',
            region_name='us-east-1'
        )

        # Extract bucket and prefix from location
        bucket = 'warehouse'
        table_prefix = location.replace('s3://warehouse/', '')
        metadata_prefix = f'{table_prefix}/metadata/'

        # List metadata files
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=metadata_prefix)

        if 'Contents' not in response:
            print(f"  ERROR: No metadata files found at {metadata_prefix}")
            return False

        # Find the latest .metadata.json file
        metadata_files = sorted([
            obj['Key'] for obj in response['Contents']
            if obj['Key'].endswith('.metadata.json')
        ])

        if not metadata_files:
            print(f"  ERROR: No .metadata.json files found")
            return False

        latest_metadata = metadata_files[-1]
        metadata_location = f's3://warehouse/{latest_metadata}'
        print(f"  Found metadata: {latest_metadata}")

        # Register the table using the registerTable endpoint
        payload = {
            'name': table_name,
            'metadata-location': metadata_location,
            'overwrite': False
        }

        register_path = f'{POLARIS_CATALOG_API}/{CATALOG_NAME}/namespaces/{namespace}/register'
        response = requests.post(
            register_path,
            headers={
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            },
            json=payload,
            timeout=10
        )

        print(f"  Response status: {response.status_code}")

        if response.status_code in [200, 201]:
            print(f"  ✓ Table registered successfully")
            return True
        elif response.status_code == 409:
            print(f"  ℹ Table already exists")
            return True
        else:
            print(f"  ERROR: Failed to register table (status {response.status_code})")
            print(f"  Response: {response.text}")
            return False

    except Exception as e:
        print(f"  ERROR: Failed to register table: {e}")
        traceback.print_exc()
        return False


def main():
    """Main routine."""
    print("=" * 60)
    print("Iceberg Table Registration")
    print("=" * 60)
    print(f"Catalog: {CATALOG_NAME}")
    print(f"Warehouse: {S3_WAREHOUSE}")
    print(f"Polaris API: {POLARIS_API}")
    print()

    # Wait for Polaris
    if not wait_for_polaris():
        sys.exit(1)

    # Get access token
    token = get_access_token()
    if not token:
        sys.exit(1)

    # Create namespaces
    print("\n" + "=" * 60)
    print("Creating namespaces...")
    print("=" * 60)

    namespaces_needed = set(ns for ns, _, _ in TABLES_TO_CREATE)

    # Sort namespace names for consistent ordering
    sorted_namespaces = sorted(namespaces_needed)
    for namespace in sorted_namespaces:
        if not create_namespace(token, namespace):
            print(f"ERROR: Failed to create namespace {namespace}")
            sys.exit(1)

    # Create tables
    print("\n" + "=" * 60)
    print("Creating tables...")
    print("=" * 60)

    all_success = True
    for namespace, table_name, location in TABLES_TO_CREATE:
        success = create_table(token, namespace, table_name, location)
        if not success:
            all_success = False

    print("\n" + "=" * 60)
    if all_success:
        print("All tables created successfully!")
        print("=" * 60)
        return 0
    else:
        print("Some tables failed to create")
        print("=" * 60)
        return 1


if __name__ == '__main__':
    sys.exit(main())
