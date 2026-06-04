#!/usr/bin/env bash
# Wait for Kafka, then run the event generator with the provided env-driven args.
set -euo pipefail
wait-kafka kafka:9092

INPUT_ARGS="${INPUT_ARGS:?INPUT_ARGS required}"
exec python3 /opt/project/generator/taxi_event_generator.py \
  --bootstrap-server kafka:9092 \
  --topic "${TOPIC:-taxi.trip.events}" \
  --input ${INPUT_ARGS} \
  --max-events "${MAX_EVENTS:--1}" \
  --invalid-rate "${INVALID_RATE:-0.02}" \
  --events-per-second "${EVENTS_PER_SECOND:-0}" \
  --batch-size "${BATCH_SIZE:-10000}" \
  --flush-every "${FLUSH_EVERY:-5000}"
