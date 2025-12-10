#!/usr/bin/env python3
"""
Initialize Iceberg test datasets (tpch.sf01 and incremental) in S3.
This script is run during Spark container startup to set up the test data.
"""

import os
import json
import shutil
import subprocess
import sys
from pathlib import Path

# Configuration from environment
MINIO_ENDPOINT = os.getenv('AWS_ENDPOINT_URL', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'root')
MINIO_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')
MINIO_BUCKET = 'warehouse'
S3_PATH_PREFIX = 's3://warehouse'

# Source data directories (mounted as volumes)
TPCH_SOURCE = '/input/tpch.sf01'
INCREMENTAL_SOURCE = '/input_incremental'

# Destination paths in S3
TPCH_S3_PATH = f'{S3_PATH_PREFIX}/tpch.sf01'
INCREMENTAL_S3_PATH = f'{S3_PATH_PREFIX}/incremental'


def wait_for_minio(max_retries=30):
    """Wait for MinIO to be available."""
    print(f"Waiting for MinIO at {MINIO_ENDPOINT}...")
    import boto3
    from botocore.exceptions import ConnectionError, ClientError

    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1',
    )

    for attempt in range(max_retries):
        try:
            s3_client.head_bucket(Bucket=MINIO_BUCKET)
            print("MinIO is ready!")
            return True
        except (ConnectionError, ClientError) as e:
            print(f"Attempt {attempt + 1}/{max_retries}: MinIO not ready yet... {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(1)

    return False


def wait_for_polaris(max_retries=30):
    """Wait for Polaris catalog to be available and authenticated."""
    print("Waiting for Polaris catalog...")
    import requests

    for attempt in range(max_retries):
        try:
            # Test authentication endpoint to ensure Polaris is fully ready
            response = requests.post(
                'http://polaris:8181/api/catalog/v1/oauth/tokens',
                auth=('root', 's3cr3t'),
                data={'grant_type': 'client_credentials', 'scope': 'PRINCIPAL_ROLE:ALL'},
                headers={'Polaris-Realm': 'POLARIS'},
                timeout=5
            )
            # If we get any response (200 or auth error), service is up
            if response.status_code >= 100:
                print("Polaris is ready!")
                return True
        except Exception as e:
            if attempt % 5 == 0:
                print(f"Attempt {attempt + 1}/{max_retries}: Polaris not ready yet... {type(e).__name__}")

        if attempt < max_retries - 1:
            import time
            time.sleep(1)

    return False


def upload_datasets_with_mc():
    """Upload datasets using MinIO client (mc) command."""
    print("\nSetting up MinIO client alias...")

    # Set MinIO alias
    alias_cmd = [
        'mc', 'alias', 'set', 'minio',
        MINIO_ENDPOINT,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY
    ]

    try:
        subprocess.run(alias_cmd, check=True, capture_output=True)
        print("MinIO alias configured")
    except subprocess.CalledProcessError as e:
        print(f"Failed to configure MinIO alias: {e.stderr.decode()}")
        return False

    # Clean up existing bucket contents
    print(f"Cleaning up bucket {MINIO_BUCKET}...")
    cleanup_cmd = ['mc', 'rm', '-r', '--force', f'minio/{MINIO_BUCKET}']
    try:
        subprocess.run(cleanup_cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError:
        pass  # Ignore if bucket is empty or doesn't exist

    # Recreate bucket
    print(f"Creating bucket {MINIO_BUCKET}...")
    create_bucket_cmd = ['mc', 'mb', '--ignore-existing', f'minio/{MINIO_BUCKET}']
    try:
        subprocess.run(create_bucket_cmd, check=True, capture_output=True)
        print(f"Bucket {MINIO_BUCKET} ready")
    except subprocess.CalledProcessError as e:
        print(f"Failed to create bucket: {e.stderr.decode()}")
        return False

    # Upload tpch dataset if exists
    if Path(TPCH_SOURCE).exists():
        print(f"\nUploading tpch.sf01 dataset from {TPCH_SOURCE}...")
        upload_cmd = [
            'mc', 'cp', '-r', '--attr=x-amz-meta-test=true',
            TPCH_SOURCE,
            f'minio/{MINIO_BUCKET}/tpch.sf01/'
        ]
        try:
            result = subprocess.run(upload_cmd, check=True, capture_output=True, text=True)
            print("tpch.sf01 dataset uploaded successfully")
        except subprocess.CalledProcessError as e:
            print(f"Failed to upload tpch.sf01: {e.stderr}")
            return False
    else:
        print(f"Warning: tpch.sf01 source not found at {TPCH_SOURCE}")

    # Upload incremental dataset if exists
    if Path(INCREMENTAL_SOURCE).exists():
        print(f"\nUploading incremental dataset from {INCREMENTAL_SOURCE}...")
        upload_cmd = [
            'mc', 'cp', '-r',
            INCREMENTAL_SOURCE,
            f'minio/{MINIO_BUCKET}/'
        ]
        try:
            result = subprocess.run(upload_cmd, check=True, capture_output=True, text=True)
            print("Incremental dataset uploaded successfully")
        except subprocess.CalledProcessError as e:
            print(f"Failed to upload incremental: {e.stderr}")
            return False
    else:
        print(f"Warning: incremental source not found at {INCREMENTAL_SOURCE}")

    return True


def upload_datasets_with_boto3():
    """Upload datasets using boto3 as fallback."""
    print("\nUploading datasets using boto3...")
    import boto3
    from botocore.exceptions import ClientError

    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1',
    )

    # Upload tpch dataset
    if Path(TPCH_SOURCE).exists():
        print(f"Uploading tpch.sf01 from {TPCH_SOURCE}...")
        for root, dirs, files in os.walk(TPCH_SOURCE):
            for file in files:
                local_path = Path(root) / file
                relative_path = local_path.relative_to(TPCH_SOURCE)
                s3_key = f'tpch.sf01/{relative_path}'

                try:
                    s3_client.upload_file(
                        str(local_path),
                        MINIO_BUCKET,
                        s3_key,
                        ExtraArgs={'Metadata': {'test': 'true'}}
                    )
                    print(f"  Uploaded {s3_key}")
                except ClientError as e:
                    print(f"  Failed to upload {s3_key}: {e}")
                    return False

    # Upload incremental dataset
    if Path(INCREMENTAL_SOURCE).exists():
        print(f"Uploading incremental from {INCREMENTAL_SOURCE}...")
        for root, dirs, files in os.walk(INCREMENTAL_SOURCE):
            for file in files:
                local_path = Path(root) / file
                relative_path = local_path.relative_to(INCREMENTAL_SOURCE)
                s3_key = f'incremental/{relative_path}'

                try:
                    s3_client.upload_file(
                        str(local_path),
                        MINIO_BUCKET,
                        s3_key
                    )
                    print(f"  Uploaded {s3_key}")
                except ClientError as e:
                    print(f"  Failed to upload {s3_key}: {e}")
                    return False

    return True


def main():
    """Main initialization routine."""
    print("=" * 60)
    print("Initializing Iceberg test datasets")
    print("=" * 60)

    # Wait for MinIO to be available
    if not wait_for_minio():
        print("ERROR: MinIO did not become available in time")
        sys.exit(1)

    # Wait for Polaris to be available
    if not wait_for_polaris():
        print("WARNING: Polaris did not become available in time, continuing anyway...")

    # Try to upload with mc first (more efficient), fall back to boto3
    print("\nAttempting to upload datasets with mc...")
    success = False

    try:
        # Check if mc is available
        subprocess.run(['which', 'mc'], check=True, capture_output=True)
        success = upload_datasets_with_mc()
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("mc not found, falling back to boto3...")
        success = upload_datasets_with_boto3()

    if not success:
        print("ERROR: Failed to upload datasets")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Dataset initialization completed successfully!")
    print("=" * 60)


if __name__ == '__main__':
    main()
