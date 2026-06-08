#!/bin/bash
# entrypoint-init-postgres.sh
# Runs once when Postgres volume is first created.
# Creates the trips table with REPLICA IDENTITY FULL for Debezium.

set -e

echo "[postgres-init] waiting for Postgres ..."
for i in $(seq 30); do
    python3 -c "import psycopg2; psycopg2.connect(host='svc-postgres-cdc', user='postgres', password='postgres', dbname='nyc_taxi').close()" && break
    echo "  waiting ... $i"
    sleep 2
done

echo "[postgres-init] Postgres ready, creating trips table ..."

PGPASSWORD=postgres psql -h svc-postgres-cdc -U postgres -d nyc_taxi <<-EOSQL
    CREATE TABLE IF NOT EXISTS trips (
        trip_id            SERIAL PRIMARY KEY,
        vendor_id          INTEGER,
        pickup_datetime    TIMESTAMP,
        dropoff_datetime   TIMESTAMP,
        passenger_count    INTEGER,
        trip_distance      DOUBLE PRECISION,
        rate_code_id       INTEGER,
        pickup_location_id INTEGER,
        dropoff_location_id INTEGER,
        payment_type       INTEGER,
        fare_amount        DOUBLE PRECISION,
        extra              DOUBLE PRECISION,
        mta_tax            DOUBLE PRECISION,
        tip_amount         DOUBLE PRECISION,
        tolls_amount       DOUBLE PRECISION,
        improvement_surcharge DOUBLE PRECISION,
        total_amount       DOUBLE PRECISION,
        created_at         TIMESTAMP DEFAULT NOW(),
        updated_at         TIMESTAMP DEFAULT NOW()
    );

    ALTER TABLE trips REPLICA IDENTITY FULL;
EOSQL

echo "[postgres-init] done"
