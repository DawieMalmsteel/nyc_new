#!/usr/bin/env bash
set -euo pipefail

SPARK_VERSION="3.5.1"
SCALA_BIN="2.12"
PKG="org.apache.spark:spark-sql-kafka-0-10_${SCALA_BIN}:${SPARK_VERSION}"

TOPIC="${TOPIC:-taxi.trip.events}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/opt/project/data/checkpoints/spark_stream_taxi_events_docker}"
CHECKPOINT_PATH="${CHECKPOINT_PATH:-${CHECKPOINT_ROOT}/${TOPIC}}"
SILVER_PATH="${SILVER_PATH:-/opt/project/data/silver/trips}"
QUARANTINE_PATH="${QUARANTINE_PATH:-/opt/project/data/quarantine/invalid_trips}"
LOOKUP_PATH="${LOOKUP_PATH:-/opt/project/data/lookup/taxi_zone_lookup.csv}"

# Ensure output dirs exist from host-mount perspective
mkdir -p data/checkpoints/spark_stream_taxi_events_docker data/silver/trips data/quarantine/invalid_trips

echo "[info] submit Spark job in docker (master= spark://spark-master:7077)"
echo "[info] topic=${TOPIC}"

docker compose exec -T spark-master bash -lc 'mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars'

docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages "${PKG}" \
  /opt/project/jobs/spark_stream_taxi_events.py \
  --bootstrap-server kafka:9092 \
  --topic "${TOPIC}" \
  --lookup-path "${LOOKUP_PATH}" \
  --silver-path "${SILVER_PATH}" \
  --quarantine-path "${QUARANTINE_PATH}" \
  --checkpoint-path "${CHECKPOINT_PATH}" \
  --trigger-available-now

docker compose exec -T spark-master bash -lc "chown -R $(id -u):$(id -g) /opt/project/data/silver /opt/project/data/quarantine /opt/project/data/checkpoints || true"
