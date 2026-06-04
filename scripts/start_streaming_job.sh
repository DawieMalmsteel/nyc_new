#!/usr/bin/env bash
set -euo pipefail

SPARK_VERSION=$(spark-submit --version 2>&1 | awk '/version/{print $NF; exit}')
SCALA_BIN="2.13"
PKG="org.apache.spark:spark-sql-kafka-0-10_${SCALA_BIN}:${SPARK_VERSION}"

echo "[info] Spark version: ${SPARK_VERSION}"
echo "[info] Using package: ${PKG}"

spark-submit \
  --packages "${PKG}" \
  jobs/spark_stream_taxi_events.py \
  --bootstrap-server "${BOOTSTRAP_SERVER:-localhost:29092}" \
  --topic "${TOPIC:-taxi.trip.events}" \
  --lookup-path "${LOOKUP_PATH:-data/lookup/taxi_zone_lookup.csv}" \
  --silver-path "${SILVER_PATH:-data/silver/trips}" \
  --quarantine-path "${QUARANTINE_PATH:-data/quarantine/invalid_trips}" \
  --checkpoint-path "${CHECKPOINT_PATH:-data/checkpoints/spark_stream_taxi_events}" \
  --trigger-available-now
