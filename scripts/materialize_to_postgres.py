#!/usr/bin/env python3
"""Copy gold datasets from Trino dbt views to Postgres analytics database.

Reads each gold dataset's SQL directly from export_gold_to_minio.py's
GOLD_DATASETS definition, executes against Trino, and INSERTs into Postgres.
Does NOT depend on gold_export — both can run in parallel.

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


def main() -> int:
    wait_for_postgres()
    wait_for_trino()

    # Import GOLD_DATASETS from the export script — single source of truth.
    # We run the exact same SQL queries, just INSERT into Postgres
    # instead of CTAS to MinIO.
    sys.path.insert(0, os.path.dirname(__file__) or ".")
    from export_gold_to_minio import GOLD_DATASETS

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
        catalog="hive",
    )
    trino_cur = trino_conn.cursor()

    total_ok = 0
    total_fail = 0

    for ds in GOLD_DATASETS:
        name = ds["name"]
        sql = ds["sql"]
        print(f"[materialize] {name}: running source query...")

        try:
            # Run the gold dataset SQL directly against Trino
            trino_cur.execute(sql)
            rows = trino_cur.fetchall()
            if not rows:
                print(f"[materialize] {name}: done (0 rows)")
                total_ok += 1
                continue

            # Get column info from the result description
            col_names = [d[0] for d in trino_cur.description]
            col_defs = [f'"{c}" TEXT' for c in col_names]

            # Drop + recreate in Postgres
            pg_cur.execute(f'DROP TABLE IF EXISTS "{name}"')
            pg_cur.execute(f'CREATE TABLE "{name}" ({", ".join(col_defs)})')

            # Batch INSERT
            from psycopg2.extras import execute_values
            placeholders = ", ".join(["%s"] * len(col_names))
            insert_sql = (
                f'INSERT INTO "{name}" ({", ".join(f"{c}" for c in col_names)}) '
                f"VALUES %s"
            )

            # Clean NaN → None for Postgres
            safe_rows = []
            for row in rows:
                safe_rows.append(tuple(
                    None if v is None else
                    None if isinstance(v, float) and v != v  # NaN check
                    else v
                    for v in row
                ))

            start = time.time()
            execute_values(pg_cur, insert_sql, safe_rows, page_size=5000)
            elapsed = time.time() - start

            total_ok += 1
            print(f"[materialize] {name}: done ({len(rows)} rows, {elapsed:.1f}s)")

        except Exception as e:
            total_fail += 1
            print(f"[materialize] {name}: FAILED — {e}", file=sys.stderr)

    trino_conn.close()
    pg_conn.close()

    print(f"\n{'='*50}")
    print(f"Postgres materialize complete: {total_ok} OK, {total_fail} FAILED")
    print(f"{'='*50}")
    return 1 if total_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
