#!/bin/bash
# entrypoint-cdc-bridge.sh — waits for Kafka + Debezium, then runs CDC bridge.
set -e
echo "[cdc-bridge] waiting for Kafka ..."
wait-kafka kafka:9092 60
echo "[cdc-bridge] Kafka ready, running bridge ..."
exec python3 /opt/project/scripts/cdc_bridge.py "$@"
