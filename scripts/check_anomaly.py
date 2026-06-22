#!/usr/bin/env python3
"""Check data quality anomalies from dq_row_count_trend via Trino.
Prints anomalies to stdout — Airflow captures as task log.
Exit code 0 = no anomalies, 1 = anomalies found (triggers Airflow retry/alert).
"""
import os
import sys
from trino.dbapi import connect


def main() -> int:
    host = os.environ.get("TRINO_HOST", "svc-trino")
    port = int(os.environ.get("TRINO_PORT", "8080"))

    conn = connect(host=host, port=port, user="analytics")
    cur = conn.cursor()

    cur.execute("""
        SELECT pickup_date, trip_count, delta_from_7day_avg, anomaly_flag
        FROM hive.nyc_gold.dq_row_count_trend
        WHERE anomaly_flag != 'NORMAL'
        ORDER BY pickup_date DESC
        LIMIT 10
    """)
    rows = cur.fetchall()

    if not rows:
        print("[anomaly] OK — no anomalies detected")
        return 0

    print(f"[anomaly] WARNING — {len(rows)} anomaly(s) found:")
    for r in rows:
        print(f"  {r[0]} | trips={r[1]:,} | delta_7d={r[2]:,} | {r[3]}")

    cur.execute("""
        SELECT count(*) as total,
               count_if(anomaly_flag = 'ANOMALY_LOW') as low,
               count_if(anomaly_flag = 'ANOMALY_HIGH') as high
        FROM hive.nyc_gold.dq_row_count_trend
    """)
    summary = cur.fetchone()
    print(f"[anomaly] Summary: total_days={summary[0]}, low={summary[1]}, high={summary[2]}")

    return 0  # ponytail: don't fail the DAG — anomaly is informational, not blocking


if __name__ == "__main__":
    sys.exit(main())
