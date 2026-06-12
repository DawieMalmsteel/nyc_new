#!/usr/bin/env bash
# Wait for Kafka, then create the base topics.
set -euo pipefail
wait-kafka svc-kafka:9092
python3 /opt/project/scripts/create_kafka_topics.py \
  --bootstrap-server svc-kafka:9092 \
  --partitions "${TOPIC_PARTITIONS:-3}" \
  --replication-factor "${TOPIC_REPLICATION:-1}"
