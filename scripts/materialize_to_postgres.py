#!/usr/bin/env python3
"""Copy gold datasets from Trino to Postgres analytics database.

Reads each gold table from hive.nyc_gold via Trino, then DROP/CREATE/INSERT
into postgres-analytics. Idempotent — safe to re-run.

Usage:
    python3 scripts/materialize_to_postgres.py
"""

import os
import sys
import time

PG_HOST = os.environ.get("PG_ANALYTICS_HOST", "svc-postgres-analytics")
PG_PORT = int(os.environ.get("PG_ANALYTICS_PORT", "5432"))
PG_USER = os.environ.get("PG_ANALYTICS_USER", "analytics")
PG_PASSWORD = os.environ.get("PG_ANALYTICS_PASSWORD", "analytics")
PG_DB = os.environ.get("PG_ANALYTICS_DB", "nyc_analytics")

TRINO_HOST = os.environ.get("TRINO_HOST", "svc-trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))


def wait_for_postgres() -> None:
    import psycopg2
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, user=PG_USER,
                password=PG_PASSWORD, dbname="postgres",
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            return
        except Exception:
            time.sleep(2)
    raise SystemExit("Postgres not ready")


def wait_for_trino() -> None:
    from trino.dbapi import connect
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="materialize")
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            conn.close()
            return
        except Exception:
            time.sleep(2)
    raise SystemExit("Trino not ready")


# Key gold tables to materialize into Postgres.
# These are all aggregated (< 300K rows each) — safe for Postgres.
TABLES = [
    "fact_trips_daily",
    "fact_trips_hourly",
    "fact_trips_hourly_zone",
    "fact_trips_borough",
    "dim_zone",
    "dim_zone_grouped",
    "dim_date",
    "dim_vendor",
    "dim_payment_type",
    "dim_rate_code",
    "kpi_daily_overview",
    "kpi_weekly_trends",
    "kpi_monthly_summary",
    "kpi_borough_comparison",
    "kpi_zone_performance",
    "kpi_zone_net_flow",
    "kpi_payment_trends",
    "kpi_vendor_performance",
    "route_top_pickup_zones",
    "route_top_dropoff_zones",
    "route_popular_routes",
    "route_airport_analysis",
    "route_airport_zone_matrix",
    "route_cross_borough",
    "od_borough_matrix",
    "ops_peak_hours_heatmap",
    "ops_trip_distance_distribution",
    "ops_passenger_count_pattern",
    "ops_utilization_rate",
    "dq_validation_summary",
    "dq_invalid_by_reason",
    "dq_row_count_trend",
    "dq_batch_metadata",
]


def main() -> int:
    wait_for_postgres()
    wait_for_trino()

    import psycopg2
    from trino.dbapi import connect as trino_connect

    pg_conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASSWORD, dbname=PG_DB,
    )
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    trino_conn = trino_connect(
        host=TRINO_HOST, port=TRINO_PORT, user="materialize",
    )
    trino_cur = trino_conn.cursor()

    total_ok = 0
    total_fail = 0

    for table in TABLES:
        print(f"[materialize] {table}: fetching schema...")

        # Get column info from Trino
        trino_cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'nyc_gold' AND table_name = %s "
            "ORDER BY ordinal_position",
            (table,),
        )
        columns = trino_cur.fetchall()
        if not columns:
            print(f"[materialize] {table}: SKIP — not found in Trino")
            continue

        # Map Trino types to Postgres types
        type_map = {
            "varchar": "TEXT",
            "bigint": "BIGINT",
            "integer": "INTEGER",
            "double": "DOUBLE PRECISION",
            "date": "DATE",
            "timestamp(3)": "TIMESTAMP",
            "boolean": "BOOLEAN",
        }
        col_defs = [
            f'"{col}" {type_map.get(dtype.lower(), "TEXT")}'
            for col, dtype in columns
        ]
        col_names = [f'"{col}"' for col, _ in columns]

        # Drop and recreate
        pg_cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        pg_cur.execute(
            f'CREATE TABLE "{table}" ({", ".join(col_defs)})'
        )

        # Fetch data from Trino in chunks and INSERT
        trino_cur.execute(f"SELECT * FROM hive.nyc_gold.{table}")
        batch_size = 5000
        total_rows = 0
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = (
            f'INSERT INTO "{table}" ({", ".join(col_names)}) '
            f"VALUES ({placeholders})"
        )

        start = time.time()
        while True:
            rows = trino_cur.fetchmany(batch_size)
            if not rows:
                break
            # Convert None/NaN to postgres-safe values
            safe_rows = []
            for row in rows:
                safe_rows.append(tuple(
                    None if v is None else
                    None if isinstance(v, float) and v != v  # NaN
                    else v
                    for v in row
                ))
            from psycopg2.extras import execute_values
            execute_values(pg_cur, insert_sql, safe_rows, page_size=batch_size)
            total_rows += len(rows)

        elapsed = time.time() - start
        total_ok += 1
        print(
            f"[materialize] {table}: done "
            f"({total_rows} rows, {elapsed:.1f}s)"
        )

    trino_conn.close()
    pg_conn.close()

    print(f"\n{'='*50}")
    print(f"Postgres materialize complete: {total_ok} OK, {total_fail} FAILED")
    print(f"{'='*50}")
    return 1 if total_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
