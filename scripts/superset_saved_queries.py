#!/usr/bin/env python3
"""superset_saved_queries.py — Register 24 saved queries in Superset SQL Lab.

Idempotent: skips if a saved query with the same label already exists.
All queries target Trino (hive.mart / hive.nyc_gold) for rich drill-down.

Usage:
    python3 scripts/superset_saved_queries.py
"""
import json
import os
import sys
import urllib.error
import urllib.request

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088") + "/api/v1"
TRINO_URI = os.environ.get(
    "TRINO_URI", "trino://analytics@svc-trino:8080/hive/mart"
)
TRINO_DB_NAME = "NYC Trino"

# ── Saved Query Definitions ────────────────────────────────────────────
# Each: (label, description, sql)
SAVED_QUERIES = [
    # ═══════════ Group 1: Revenue & Performance ═══════════
    (
        "SQ-01: Daily Revenue Summary",
        "Tổng doanh thu + trip count 30 ngày gần nhất",
        """SELECT
  pickup_date,
  COUNT(*)        AS trips,
  SUM(total_amount) AS revenue,
  AVG(tip_amount / NULLIF(total_amount, 0)) * 100 AS tip_rate_pct
FROM hive.mart.fact_trips
WHERE pickup_date >= current_date - INTERVAL '30' DAY
GROUP BY pickup_date
ORDER BY pickup_date DESC""",
    ),
    (
        "SQ-02: Top 10 Revenue Zones",
        "Top 10 pickup zone theo tổng doanh thu + tip rate",
        """SELECT
  pickup_zone,
  pickup_borough,
  COUNT(*)          AS trips,
  SUM(total_amount) AS revenue,
  AVG(tip_amount / NULLIF(total_amount, 0)) * 100 AS tip_rate_pct
FROM hive.mart.fact_trips
WHERE pickup_zone IS NOT NULL
GROUP BY pickup_zone, pickup_borough
ORDER BY revenue DESC
LIMIT 10""",
    ),
    (
        "SQ-03: Revenue by Hour × Day of Week",
        "Heatmap doanh thu: giờ × thứ trong tuần",
        """SELECT
  pickup_dow,
  pickup_hour,
  COUNT(*)          AS trips,
  SUM(total_amount) AS revenue,
  AVG(total_amount) AS avg_per_trip
FROM hive.mart.fact_trips
GROUP BY pickup_dow, pickup_hour
ORDER BY pickup_dow, pickup_hour""",
    ),
    (
        "SQ-04: Average Fare by Distance Bucket",
        "Phân tích fare theo khoảng cách",
        """SELECT
  CASE
    WHEN trip_distance < 1   THEN '<1 mi'
    WHEN trip_distance < 3   THEN '1-3 mi'
    WHEN trip_distance < 10  THEN '3-10 mi'
    WHEN trip_distance < 20  THEN '10-20 mi'
    ELSE '20+ mi'
  END                AS dist_bucket,
  COUNT(*)           AS trips,
  AVG(fare_amount)   AS avg_fare,
  AVG(total_amount)  AS avg_total
FROM hive.mart.fact_trips
GROUP BY 1
ORDER BY MIN(trip_distance)""",
    ),
    (
        "SQ-05: Monthly Revenue Comparison",
        "So sánh doanh thu giữa các tháng (MoM growth)",
        """SELECT
  pickup_year,
  pickup_month,
  COUNT(*)          AS trips,
  SUM(total_amount) AS revenue,
  SUM(tip_amount)   AS tips,
  AVG(total_amount) AS avg_per_trip
FROM hive.mart.fact_trips
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month""",
    ),
    # ═══════════ Group 2: Route Analysis ═══════════
    (
        "SQ-06: Top 20 Popular Routes",
        "Cặp pickup-dropoff phổ biến nhất",
        """SELECT
  pickup_zone,
  dropoff_zone,
  COUNT(*)           AS trips,
  AVG(total_amount)  AS avg_fare,
  AVG(trip_distance) AS avg_distance
FROM hive.mart.fact_trips
WHERE pickup_zone IS NOT NULL AND dropoff_zone IS NOT NULL
GROUP BY pickup_zone, dropoff_zone
ORDER BY trips DESC
LIMIT 20""",
    ),
    (
        "SQ-07: Borough-to-Borough Flow Matrix",
        "Ma trận luồng đi lại giữa các borough",
        """SELECT
  pickup_borough,
  dropoff_borough,
  COUNT(*)           AS trips,
  SUM(total_amount)  AS revenue,
  AVG(trip_distance) AS avg_distance
FROM hive.mart.fact_trips
WHERE pickup_borough IS NOT NULL AND dropoff_borough IS NOT NULL
GROUP BY pickup_borough, dropoff_borough
ORDER BY trips DESC""",
    ),
    (
        "SQ-08: Airport Trip Analysis",
        "Trip đến/từ JFK, LaGuardia, Newark",
        """SELECT
  CASE
    WHEN pickup_zone LIKE '%JFK%' THEN 'JFK'
    WHEN pickup_zone LIKE '%LaGuardia%' THEN 'LGA'
    WHEN pickup_zone LIKE '%Newark%' THEN 'EWR'
  END AS airport,
  CASE
    WHEN pickup_zone LIKE '%JFK%' OR pickup_zone LIKE '%LaGuardia%'
      OR pickup_zone LIKE '%Newark%'
    THEN 'From Airport'
    ELSE 'To Airport'
  END AS direction,
  COUNT(*)           AS trips,
  SUM(total_amount)  AS revenue,
  AVG(total_amount)  AS avg_fare
FROM hive.mart.fact_trips
WHERE pickup_zone LIKE '%JFK%'
   OR pickup_zone LIKE '%LaGuardia%'
   OR pickup_zone LIKE '%Newark%'
   OR dropoff_zone LIKE '%JFK%'
   OR dropoff_zone LIKE '%LaGuardia%'
   OR dropoff_zone LIKE '%Newark%'
GROUP BY 1, 2
ORDER BY trips DESC""",
    ),
    (
        "SQ-09: Longest Trips (Top 50)",
        "50 trip dài nhất theo distance",
        """SELECT
  pickup_ts,
  pickup_zone,
  dropoff_zone,
  trip_distance,
  total_amount,
  passenger_count,
  trip_duration_sec / 60 AS duration_min
FROM hive.mart.fact_trips
ORDER BY trip_distance DESC
LIMIT 50""",
    ),
    (
        "SQ-10: Intra-Borough vs Inter-Borough",
        "Tỉ lệ trip nội borough / liên borough",
        """SELECT
  CASE WHEN pickup_borough = dropoff_borough
    THEN 'Intra-Borough' ELSE 'Inter-Borough'
  END               AS trip_type,
  COUNT(*)          AS trips,
  AVG(trip_distance) AS avg_distance,
  AVG(total_amount)  AS avg_total
FROM hive.mart.fact_trips
WHERE pickup_borough IS NOT NULL AND dropoff_borough IS NOT NULL
GROUP BY 1""",
    ),
    # ═══════════ Group 3: Payment & Fare ═══════════
    (
        "SQ-11: Payment Type Breakdown",
        "Phân bố phương thức thanh toán + tip rate",
        """SELECT
  payment_type,
  COUNT(*)           AS trips,
  SUM(total_amount)  AS revenue,
  AVG(tip_amount)    AS avg_tip,
  AVG(tip_amount / NULLIF(total_amount, 0)) * 100 AS tip_rate_pct
FROM hive.mart.fact_trips
GROUP BY payment_type
ORDER BY trips DESC""",
    ),
    (
        "SQ-12: Credit Card vs Cash — Tip Comparison",
        "So sánh tipping giữa credit card và cash",
        """SELECT
  CASE payment_type
    WHEN 1 THEN 'Credit Card'
    WHEN 2 THEN 'Cash'
    ELSE 'Other'
  END               AS payment,
  COUNT(*)          AS trips,
  AVG(tip_amount)   AS avg_tip,
  AVG(tip_amount / NULLIF(fare_amount, 0)) * 100 AS tip_pct_of_fare
FROM hive.mart.fact_trips
WHERE payment_type IN (1, 2)
GROUP BY 1""",
    ),
    (
        "SQ-13: High-Value Trips ($100+)",
        "Trip có total >= $100 — breakdown",
        """SELECT
  pickup_date,
  pickup_zone,
  dropoff_zone,
  total_amount,
  fare_amount,
  tip_amount,
  trip_distance
FROM hive.mart.fact_trips
WHERE total_amount >= 100
ORDER BY total_amount DESC""",
    ),
    (
        "SQ-14: Fare Components Breakdown",
        "Cấu phần giá vé: fare + tolls + surcharge + tips",
        """SELECT
  pickup_date,
  COUNT(*)            AS trips,
  SUM(fare_amount)    AS fare,
  SUM(extra)          AS extra,
  SUM(mta_tax)        AS mta_tax,
  SUM(tip_amount)     AS tips,
  SUM(tolls_amount)   AS tolls,
  SUM(improvement_surcharge) AS impr_surcharge,
  SUM(total_amount)   AS total
FROM hive.mart.fact_trips
GROUP BY pickup_date
ORDER BY pickup_date""",
    ),
    # ═══════════ Group 4: Passenger & Trip Profile ═══════════
    (
        "SQ-15: Passenger Count Distribution",
        "Phân bố số hành khách mỗi trip",
        """SELECT
  passenger_count,
  COUNT(*)           AS trips,
  AVG(total_amount)  AS avg_total,
  AVG(trip_distance) AS avg_distance
FROM hive.mart.fact_trips
GROUP BY passenger_count
ORDER BY passenger_count""",
    ),
    (
        "SQ-16: Trip Duration Analysis",
        "Phân tích thời gian trip theo bucket",
        """SELECT
  CASE
    WHEN trip_duration_sec < 300  THEN '<5 min'
    WHEN trip_duration_sec < 600  THEN '5-10 min'
    WHEN trip_duration_sec < 1800 THEN '10-30 min'
    WHEN trip_duration_sec < 3600 THEN '30-60 min'
    ELSE '60+ min'
  END                AS duration_bucket,
  COUNT(*)           AS trips,
  AVG(trip_distance) AS avg_distance,
  AVG(total_amount)  AS avg_total
FROM hive.mart.fact_trips
GROUP BY 1
ORDER BY MIN(trip_duration_sec)""",
    ),
    (
        "SQ-17: Rush Hour vs Off-Peak",
        "So sánh trip trong/ngoài giờ cao điểm",
        """SELECT
  CASE
    WHEN pickup_hour BETWEEN 7 AND 9   THEN 'Morning Rush'
    WHEN pickup_hour BETWEEN 16 AND 19 THEN 'Evening Rush'
    WHEN pickup_hour BETWEEN 10 AND 15 THEN 'Midday'
    WHEN pickup_hour BETWEEN 20 AND 23 THEN 'Evening'
    ELSE 'Night'
  END                AS period,
  COUNT(*)           AS trips,
  AVG(trip_duration_sec) / 60 AS avg_duration_min,
  AVG(total_amount)  AS avg_total
FROM hive.mart.fact_trips
GROUP BY 1
ORDER BY MIN(pickup_hour)""",
    ),
    (
        "SQ-18: Vendor Performance Comparison",
        "So sánh Creative Mobile vs VeriFone",
        """SELECT
  vendor_id,
  COUNT(*)           AS trips,
  SUM(total_amount)  AS revenue,
  AVG(total_amount)  AS avg_revenue,
  AVG(trip_distance) AS avg_distance
FROM hive.mart.fact_trips
GROUP BY vendor_id
ORDER BY trips DESC""",
    ),
    # ═══════════ Group 5: Data Quality & Operations ═══════════
    (
        "SQ-19: Validation Error Summary",
        "Tổng quan lỗi validation — theo reason",
        """SELECT
  validation_error,
  SUM(error_count) AS total_errors
FROM hive.mart.fact_invalid_trips
GROUP BY validation_error
ORDER BY total_errors DESC""",
    ),
    (
        "SQ-20: Invalid Trip Rate by Day",
        "Tỉ lệ trip lỗi / tổng trip theo ngày",
        """WITH valid AS (
    SELECT pickup_date, COUNT(*) AS valid_trips
    FROM hive.mart.fact_trips
    GROUP BY pickup_date
),
invalid AS (
    SELECT pickup_date, SUM(error_count) AS invalid_trips
    FROM hive.mart.fact_invalid_trips
    GROUP BY pickup_date
)
SELECT
  v.pickup_date,
  v.valid_trips,
  COALESCE(i.invalid_trips, 0) AS invalid_trips,
  ROUND(COALESCE(i.invalid_trips, 0) * 100.0 / v.valid_trips, 2) AS error_rate_pct
FROM valid v
LEFT JOIN invalid i ON v.pickup_date = i.pickup_date
ORDER BY v.pickup_date""",
    ),
    (
        "SQ-21: Row Count Reconciliation",
        "So sánh row count giữa silver, mart, và gold",
        """SELECT 'silver' AS layer, COUNT(*) AS rows FROM hive.nyc.trips
UNION ALL
SELECT 'mart',   COUNT(*) FROM hive.mart.fact_trips
UNION ALL
SELECT 'gold',   COUNT(*) FROM hive.nyc_gold.fact_trips_daily""",
    ),
    (
        "SQ-22: Zone Coverage Check",
        "Zone nào trong dim_zone nhưng chưa từng có trip?",
        """SELECT
  z.zone,
  z.borough
FROM hive.mart.dim_zone z
LEFT JOIN (
    SELECT DISTINCT pickup_zone AS zone FROM hive.mart.fact_trips
    UNION
    SELECT DISTINCT dropoff_zone FROM hive.mart.fact_trips
) t ON z.zone = t.zone
WHERE t.zone IS NULL
ORDER BY z.borough, z.zone""",
    ),
    # ═══════════ Group 6: Trend & Pattern ═══════════
    (
        "SQ-23: Weekday vs Weekend Patterns",
        "So sánh trip pattern ngày thường và cuối tuần",
        """SELECT
  CASE WHEN pickup_dow IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
  pickup_hour,
  COUNT(*)          AS trips,
  SUM(total_amount) AS revenue
FROM hive.mart.fact_trips
GROUP BY 1, pickup_hour
ORDER BY 1, pickup_hour""",
    ),
    (
        "SQ-24: Daily KPIs with WoW Comparison",
        "Bảng KPI hàng ngày có so sánh tuần trước",
        """WITH daily AS (
    SELECT
      pickup_date,
      COUNT(*) AS trips,
      SUM(total_amount) AS revenue,
      AVG(tip_amount / NULLIF(total_amount, 0)) * 100 AS tip_rate_pct
    FROM hive.mart.fact_trips
    GROUP BY pickup_date
)
SELECT
  d.pickup_date,
  d.trips,
  d.revenue,
  ROUND(d.tip_rate_pct, 1) AS tip_rate_pct,
  LAG(d.trips, 7) OVER (ORDER BY d.pickup_date) AS trips_prev_week,
  ROUND(
    (d.trips - LAG(d.trips, 7) OVER (ORDER BY d.pickup_date))
    * 100.0 / NULLIF(LAG(d.trips, 7) OVER (ORDER BY d.pickup_date), 0),
    1
  ) AS trips_wow_change_pct
FROM daily d
ORDER BY d.pickup_date DESC""",
    ),
]


def main() -> int:
    # 1. Login
    token = _req("POST", "/security/login",
                 {"username": "admin", "password": "admin", "provider": "db"})["access_token"]
    H = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    get = lambda p: _api(H, "GET", p)
    post = lambda p, d: _api(H, "POST", p, d)

    # 2. Find or create Trino DB connection
    dbs = get("/database/")["result"]
    db_id = next((r["id"] for r in dbs if r["database_name"] == TRINO_DB_NAME), None)
    if db_id:
        print(f"[db] Trino exists: id={db_id}")
    else:
        db_id = post("/database/", {
            "database_name": TRINO_DB_NAME,
            "engine": "trino",
            "configuration_method": "sqlalchemy_form",
            "sqlalchemy_uri": TRINO_URI,
            "expose_in_sqllab": True,
        })["id"]
        print(f"[db] Trino created: id={db_id}")

    # 3. List existing saved queries
    existing = get("/saved_query/?q=(page_size:500)")["result"]
    by_label = {q["label"]: q["id"] for q in existing}

    # 4. Create saved queries (idempotent)
    created, skipped = 0, 0
    for label, description, sql in SAVED_QUERIES:
        if label in by_label:
            skipped += 1
            print(f"[sq] SKIP (exists): {label}")
            continue
        try:
            sq_id = post("/saved_query/", {
                "db_id": db_id,
                "schema": "hive.mart",
                "label": label,
                "description": description,
                "sql": sql.strip(),
            })["id"]
            created += 1
            print(f"[sq] CREATED id={sq_id}: {label}")
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            print(f"[sq] ERROR {label}: {e.code} — {body[:200]}")
        except Exception as e:
            print(f"[sq] ERROR {label}: {e}")

    print(f"\n{'='*60}")
    print(f"Done: DB={db_id}, Created={created}, Skipped={skipped}, Total={len(SAVED_QUERIES)}")
    print(f"{'='*60}")
    return 0 if created + skipped == len(SAVED_QUERIES) else 1


def _req(method, path, data=None):
    body = json.dumps(data).encode() if data else None
    r = urllib.request.Request(f"{BASE}{path}", data=body,
                               headers={"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read())


def _api(headers, method, path, data=None):
    body = json.dumps(data).encode() if data else None
    r = urllib.request.Request(f"{BASE}{path}", data=body,
                               headers=headers, method=method)
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read())


if __name__ == "__main__":
    sys.exit(main())
