#!/usr/bin/env bash
set -euo pipefail

INPUT_ARGS="data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet data/raw/yellow_taxi/year=2024/month=02/yellow_tripdata_2024-02.parquet data/raw/yellow_taxi/year=2024/month=03/yellow_tripdata_2024-03.parquet" \
MAX_EVENTS=-1 \
INVALID_RATE="${INVALID_RATE:-0.01}" \
EVENTS_PER_SECOND="${EVENTS_PER_SECOND:-0}" \
USE_DOCKER_SPARK=1 \
bash scripts/local_e2e_test.sh
