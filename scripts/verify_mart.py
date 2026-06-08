#!/usr/bin/env python3
"""Verify row counts of all mart tables in Trino."""
import os
from trino.dbapi import connect

host = os.environ.get("TRINO_HOST", "localhost")
port = int(os.environ.get("TRINO_PORT", "8083"))
c = connect(host=host, port=port, user='analytics')
cur = c.cursor()
for tbl in ['dim_zone', 'fact_trips', 'fact_invalid_trips', 'mart_hourly_summary']:
    cur.execute(f"SELECT COUNT(*) FROM hive.mart.{tbl}")
    print(f'{tbl}: {cur.fetchone()[0]:>10,} rows')
