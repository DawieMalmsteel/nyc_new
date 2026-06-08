#!/bin/bash
# entrypoint-cdc-seed.sh — waits for Postgres, then seeds data.
set -e
echo "[cdc-seed] waiting for Postgres ..."
for i in $(seq 30); do
    python3 -c "import psycopg2; psycopg2.connect(host='svc-postgres-cdc', user='postgres', password='postgres', dbname='nyc_taxi').close()" && break
    echo "  waiting ... $i"
    sleep 2
done
echo "[cdc-seed] Postgres ready, seeding ..."
exec python3 /opt/project/scripts/cdc_seed.py "$@"
