#!/usr/bin/env bash
set -euo pipefail

SPARK_VERSION="3.5.1"
SCALA_BIN="2.12"
PKG_KAFKA="org.apache.spark:spark-sql-kafka-0-10_${SCALA_BIN}:${SPARK_VERSION}"
PKG_S3="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
PKG="${PKG_KAFKA},${PKG_S3}"

echo "[info] S3 mode enabled (MinIO default)"

TOPIC="${TOPIC:-taxi.trip.events}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/opt/project/data/checkpoints/spark_stream_taxi_events_docker}"
CHECKPOINT_PATH="${CHECKPOINT_PATH:-${CHECKPOINT_ROOT}/${TOPIC}}"
SILVER_PATH="${SILVER_PATH:-s3a://nyc-silver/trips}"
QUARANTINE_PATH="${QUARANTINE_PATH:-s3a://nyc-quarantine/invalid_trips}"
LOOKUP_PATH="${LOOKUP_PATH:-s3a://nyc-lookup/taxi_zone_lookup.csv}"

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