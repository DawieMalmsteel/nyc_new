#!/usr/bin/env bash
set -euo pipefail

INPUT_ARGS=${INPUT_ARGS:-"data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet"}

python3 generator/taxi_event_generator.py \
  --input ${INPUT_ARGS} \
  --bootstrap-server "${BOOTSTRAP_SERVER:-localhost:29092}" \
  --topic "${TOPIC:-taxi.trip.events}" \
  --events-per-second "${EVENTS_PER_SECOND:-0}" \
  --max-events "${MAX_EVENTS:--1}" \
  --invalid-rate "${INVALID_RATE:-0.02}" \
  --batch-size "${BATCH_SIZE:-10000}" \
  --flush-every "${FLUSH_EVERY:-5000}"
