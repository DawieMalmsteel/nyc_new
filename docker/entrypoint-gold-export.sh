#!/usr/bin/env bash
# Export gold datasets from Trino to MinIO S3 as Parquet.
set -euo pipefail

python3 /opt/project/scripts/export_gold_to_minio.py
