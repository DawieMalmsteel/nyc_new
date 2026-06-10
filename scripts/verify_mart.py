#!/usr/bin/env python3
"""Verify row counts of all mart tables in Trino."""
import os
from trino.dbapi import connect

host = os.environ.get("TRINO_HOST", "localhost")
port = int(os.environ.get("TRINO_PORT", "8083"))
c = connect(host=host, port=port, user='analytics')
cur = c.cursor()
try:
    cur.execute("SET SESSION query_max_run_time='30s'")
except Exception:
    pass  # optional session param
for tbl in ['dim_zone', 'fact_trips', 'mart_hourly_summary', 'mart_revenue_by_day']:
    try:
        cur.execute(f"SELECT COUNT(*) FROM hive.mart.{tbl}")
        row = cur.fetchone()
        print(f'{tbl}: {row[0]:>10,} rows' if row else f'{tbl}: no result')
    except Exception as e:
        print(f'{tbl}: ERROR - {e}')
