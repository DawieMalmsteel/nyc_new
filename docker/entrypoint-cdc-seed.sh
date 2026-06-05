#!/bin/bash
# entrypoint-cdc-seed.sh — waits for Postgres, then seeds data.
set -e
echo "[cdc-seed] waiting for Postgres ..."
for i in $(seq 30); do
    pg_isready -h nyc_postgres -U postgres -d nyc_taxi && break
    echo "  waiting ... $i"
    sleep 2
done
echo "[cdc-seed] Postgres ready, seeding ..."
exec python3 /opt/project/scripts/cdc_seed.py "$@"
