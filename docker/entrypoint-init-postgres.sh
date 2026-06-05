#!/bin/bash
# entrypoint-init-postgres.sh
# Runs once when Postgres volume is first created.
# WAL logical replication is set via docker command args.
# This script only creates the trips table.

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
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
