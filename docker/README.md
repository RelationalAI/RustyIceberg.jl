# Iceberg Docker Setup

This directory contains a Docker Compose setup for testing RustyIceberg.jl with Apache Polaris as the Iceberg REST catalog.

## Overview

The setup provides:
- **MinIO**: S3-compatible object storage for datasets
- **Apache Polaris**: Production-ready Iceberg REST catalog
- **Spark**: Container for dataset initialization and table registration

## Quick Start

Start all services:

```bash
docker compose up -d
```

Wait for services to initialize (2-3 minutes). Monitor progress:

```bash
docker compose logs -f
```

Stop all services:

```bash
docker compose down
```

## Services

### minio
- **Port**: 9000 (API), 9001 (Console)
- **Credentials**: root / password
- **Purpose**: S3-compatible storage for datasets and Iceberg metadata

### polaris
- **Port**: 8181
- **Credentials**: root / s3cr3t
- **Purpose**: Iceberg REST catalog server

### spark-init
- Uploads test datasets (TPC-H scale factor 0.01, incremental test data) to MinIO
- Waits for MinIO and Polaris to be ready before uploading

### polaris-setup
- Creates the "warehouse" catalog in Polaris
- Configures S3 storage backend

### table-creation
- Registers pre-loaded Iceberg tables in Polaris
- Discovers existing table metadata from S3

### spark-iceberg
- Long-running Spark container for interactive use
- Configured to connect to Polaris catalog
- Available at: http://localhost:8080 (Spark Web UI)

## Accessing Services

**MinIO Console**: http://localhost:9001
- Username: `root`
- Password: `password`

**Polaris API**: http://localhost:8181

## Datasets

The setup automatically uploads two test datasets:

- **TPC-H Scale Factor 0.01**: `s3://warehouse/tpch.sf01/`
  - Tables: customer, lineitem, nation, orders, part, partsupp, region, supplier
  - Namespace: `warehouse.tpch.sf01`

- **Incremental Test Data**: `s3://warehouse/incremental/`
  - Tables: test1
  - Namespace: `warehouse.incremental`

## Using with Julia Tests

The GitHub Actions workflow automatically:
1. Builds Docker images with layer caching
2. Starts the Polaris and MinIO services
3. Waits for services to be ready
4. Runs Julia tests against the live catalog
5. Cleans up services after tests complete

See `.github/workflows/test.yml` for details.

## Troubleshooting

### Services fail to start
- Check Docker and Docker Compose are installed: `docker --version` and `docker compose version`
- Ensure ports 8181, 9000, 9001 are available

### Polaris not responding
- Wait longer for startup (especially on first run)
- Check logs: `docker compose logs polaris`

### MinIO bucket not created
- Check `docker compose logs setup_bucket`
- Manually create bucket: `docker compose exec minio mc mb minio/warehouse`

### Tables not registering
- Verify datasets uploaded: `docker compose exec minio mc ls minio/warehouse`
- Check table creation logs: `docker compose logs table-creation`
