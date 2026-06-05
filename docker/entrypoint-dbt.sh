#!/usr/bin/env bash
# Run dbt build (deps, seed, run, test).
set -euo pipefail
TRINO_HOST="${TRINO_HOST:-trino-coordinator}"
TRINO_PORT="${TRINO_PORT:-8080}"
# Wait for Trino.
for i in {1..60}; do
  if curl -sf "http://${TRINO_HOST}:${TRINO_PORT}/v1/info" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# Sync hive partition metadata so the source views see new data.
cd /opt/project
python3 /opt/project/scripts/trino_sync_partitions.py

cd /opt/project/dbt
exec dbt build "$@"