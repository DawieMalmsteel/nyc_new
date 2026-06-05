#!/usr/bin/env python3
"""Verify row counts of all mart tables in Trino."""
from trino.dbapi import connect

c = connect(host='localhost', port=8083, user='analytics')
cur = c.cursor()
for tbl in ['dim_zone', 'fact_trips', 'fact_invalid_trips', 'mart_hourly_summary']:
    cur.execute(f"SELECT COUNT(*) FROM hive.mart.{tbl}")
    print(f'{tbl}: {cur.fetchone()[0]:>10,} rows')
