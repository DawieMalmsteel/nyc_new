#!/usr/bin/env bash
set -euo pipefail

python3 generator/taxi_event_generator.py \
  --input \
    data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet \
    data/raw/yellow_taxi/year=2024/month=02/yellow_tripdata_2024-02.parquet \
    data/raw/yellow_taxi/year=2024/month=03/yellow_tripdata_2024-03.parquet \
  --bootstrap-server "${BOOTSTRAP_SERVER:-localhost:29092}" \
  --topic "${TOPIC:-taxi.trip.events.full}" \
  --events-per-second "${EVENTS_PER_SECOND:-0}" \
  --max-events "${MAX_EVENTS:--1}" \
  --invalid-rate "${INVALID_RATE:-0.01}" \
  --batch-size "${BATCH_SIZE:-20000}" \
  --flush-every "${FLUSH_EVERY:-10000}"
