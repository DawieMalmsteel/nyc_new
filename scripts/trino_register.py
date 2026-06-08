#!/usr/bin/env python3
"""Register silver + quarantine tables in the Trino `hive.nyc` catalog.

Idempotent: drops the tables first, then re-creates them pointing at the
local-FS parquet outputs of the streaming job, syncs partition metadata,
and prints row counts.
"""
import os
import sys
import time

from trino.dbapi import connect
from trino.exceptions import TrinoUserError


TRINO_HOST = os.environ.get("TRINO_HOST", "trino-coordinator")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
SILVER_PATH = os.environ.get("SILVER_PATH", "/opt/project/data/silver/trips")
QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "/opt/project/data/quarantine/invalid_trips")
ZONES_PATH = os.environ.get("ZONES_PATH", "/opt/project/data/lookup/")
SCHEMA = "hive.nyc"

SILVER_PATH = os.environ.get("SILVER_PATH", "s3://nyc-silver/trips")
QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "s3://nyc-quarantine/invalid_trips")
ZONES_PATH = os.environ.get("ZONES_PATH", "s3://nyc-lookup/")

TRIPS_COLS = [
    "trip_id BIGINT",
    "source_file VARCHAR",
    "vendor_id INTEGER",
    "pickup_ts TIMESTAMP",
    "dropoff_ts TIMESTAMP",
    "passenger_count INTEGER",
    "trip_distance DOUBLE",
    "rate_code_id INTEGER",
    "pickup_location_id INTEGER",
    "dropoff_location_id INTEGER",
    "payment_type INTEGER",
    "fare_amount DOUBLE",
    "extra DOUBLE",
    "mta_tax DOUBLE",
    "tip_amount DOUBLE",
    "tolls_amount DOUBLE",
    "improvement_surcharge DOUBLE",
    "total_amount DOUBLE",
    "pickup_borough VARCHAR",
    "pickup_zone VARCHAR",
    "pickup_service_zone VARCHAR",
    "dropoff_borough VARCHAR",
    "dropoff_zone VARCHAR",
    "dropoff_service_zone VARCHAR",
    "pickup_date DATE",
    "pickup_hour INTEGER",
    "event_ts TIMESTAMP",
    "ingestion_ts TIMESTAMP",
    "pickup_year INTEGER",
    "pickup_month INTEGER",
]

INVALID_COLS = [
    "validation_errors ARRAY(VARCHAR)",
    "quarantine_ts TIMESTAMP",
] + TRIPS_COLS


def wait_for_trino(host: str, port: int, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            conn = connect(host=host, port=port, user="bootstrap")
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            conn.close()
            return
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2)
    raise SystemExit(f"trino not ready: {last_err}")


def exec_(cur, sql: str) -> None:
    try:
        cur.execute(sql)
        cur.fetchall()
    except TrinoUserError as e:
        print(f"[trino] user error: {sql[:80]!r} -> {e}", file=sys.stderr)
        raise


def make_create(table: str, cols: list[str], location: str, partitioned: bool = True) -> str:
    partitioned_clause = ""
    if partitioned:
        partitioned_clause = ", partitioned_by = ARRAY['pickup_year','pickup_month']"
    return (
        f"CREATE TABLE {SCHEMA}.{table} (\n  "
        + ",\n  ".join(cols)
        + f"\n) WITH (external_location = '{location}', format = 'PARQUET'"
        + partitioned_clause
        + ")"
    )


def main() -> int:
    wait_for_trino(TRINO_HOST, TRINO_PORT)

    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="bootstrap")
    cur = conn.cursor()

    print(f"[trino] create schema {SCHEMA}")
    exec_(cur, f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

    for table, cols, location, partitioned in [
        ("trips", TRIPS_COLS, SILVER_PATH, True),
        ("invalid_trips", INVALID_COLS, QUARANTINE_PATH, False),
    ]:
        print(f"[trino] create {table}")
        exec_(cur, f"DROP TABLE IF EXISTS {SCHEMA}.{table}")
        exec_(cur, make_create(table, cols, location, partitioned=partitioned))

    # Create taxi_zone_lookup as external table from CSV
    print("[trino] create taxi_zone_lookup")
    exec_(cur, f"DROP TABLE IF EXISTS {SCHEMA}.taxi_zone_lookup")
    exec_(cur, f"""
        CREATE TABLE {SCHEMA}.taxi_zone_lookup (
          location_id VARCHAR,
          borough VARCHAR,
          zone VARCHAR,
          service_zone VARCHAR
        )
        WITH (
          external_location = '{ZONES_PATH}',
          format = 'CSV',
          csv_separator = ',',
          csv_escape = '\\',
          csv_quote = '\"',
          skip_header_line_count = 1
        )
    """)

    print("[trino] sync partitions + smoke test")
    for table in ("trips",):
        cur.execute(
            f"CALL hive.system.sync_partition_metadata("
            f"schema_name => 'nyc', table_name => '{table}', mode => 'FULL')"
        )
        cur.fetchall()
    for table in ("trips", "invalid_trips"):
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
        n = cur.fetchone()[0]
        print(f"[trino]   {table:<15} = {n}")

    # Check taxi_zone_lookup separately (not partitioned)
    cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.taxi_zone_lookup")
    n = cur.fetchone()[0]
    print(f"[trino]   taxi_zone_lookup = {n}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
