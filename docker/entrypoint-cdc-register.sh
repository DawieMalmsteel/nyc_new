#!/bin/bash
# entrypoint-cdc-register.sh — waits for Debezium, then registers connector.
set -e
echo "[cdc-register] registering Debezium connector ..."
exec python3 /opt/project/scripts/cdc_register_connector.py "$@"
