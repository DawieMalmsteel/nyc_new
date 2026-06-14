#!/usr/bin/env python3
"""Export gold datasets from Trino to MinIO S3 as Parquet.

Connects to Trino, creates schema `hive.nyc_gold`, and for each gold
dataset runs CTAS with external_location pointing at s3://nyc-gold/.

Idempotent: DROP + CREATE TABLE AS SELECT for every dataset.

Usage:
    python3 scripts/export_gold_to_minio.py
"""
import os
import sys
import time

from trino.dbapi import connect
from trino.exceptions import TrinoUserError

# Import minio lazily — install at runtime if needed
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "svc-minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = "nyc-gold"

TRINO_HOST = os.environ.get("TRINO_HOST", "trino-coordinator")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
SCHEMA = "hive.nyc_gold"
GOLD_PATH = "s3://nyc-gold"

# ──────────────────────────────────────────────
# Dataset definitions
# Each dataset has: name, source_sql, location_subdir
# source_sql is a query referencing existing tables/views in hive.mart or hive.nyc
# ──────────────────────────────────────────────

GOLD_DATASETS = [
    # ── 1. Fact Tables ──
    # NOTE: fact_trips_enriched first so dbt gold_fact_trips view settles
    {
        "name": "fact_trips_enriched",
        "partitioned": True,
        "sql": """
            SELECT
                trip_id, source_file, vendor_id,
                pickup_ts, dropoff_ts, passenger_count,
                trip_distance, rate_code_id, payment_type,
                fare_amount, extra, mta_tax, tip_amount,
                tolls_amount, improvement_surcharge, total_amount,
                tip_amount / NULLIF(total_amount, 0) AS tip_rate,
                date_diff('second', pickup_ts, dropoff_ts) AS trip_duration_sec,
                pickup_location_id, dropoff_location_id,
                pickup_zone, dropoff_zone,
                pickup_borough, dropoff_borough,
                pickup_service_zone, dropoff_service_zone,
                -- Enrichment columns
                CASE
                    WHEN pickup_zone IN ('JFK Airport','LaGuardia Airport','Newark Airport')
                      OR dropoff_zone IN ('JFK Airport','LaGuardia Airport','Newark Airport')
                    THEN TRUE ELSE FALSE
                END AS is_airport_trip,
                CASE
                    WHEN trip_distance < 1 THEN 'very_short'
                    WHEN trip_distance < 3 THEN 'short'
                    WHEN trip_distance < 10 THEN 'medium'
                    ELSE 'long'
                END AS trip_distance_category,
                CASE
                    WHEN pickup_hour_ts >= TIMESTAMP '2024-01-01 06:00'
                     AND pickup_hour_ts < TIMESTAMP '2024-01-01 10:00'
                    THEN 'morning_rush'
                    WHEN pickup_hour_ts >= TIMESTAMP '2024-01-01 16:00'
                     AND pickup_hour_ts < TIMESTAMP '2024-01-01 20:00'
                    THEN 'evening_rush'
                    WHEN pickup_hour_ts >= TIMESTAMP '2024-01-01 22:00'
                      OR pickup_hour_ts < TIMESTAMP '2024-01-01 05:00'
                    THEN 'late_night'
                    ELSE 'regular'
                END AS trip_time_category,
                -- Partition columns MUST be last
                pickup_year, pickup_month
            FROM hive.mart.gold_fact_trips
        """,
    },
    {
        "name": "fact_trips",
        "partitioned": True,
        "sql": """
            SELECT
                trip_id, source_file, vendor_id,
                pickup_ts, dropoff_ts, passenger_count,
                trip_distance, rate_code_id, payment_type,
                fare_amount, extra, mta_tax, tip_amount,
                tolls_amount, improvement_surcharge, total_amount,
                tip_amount / NULLIF(total_amount, 0) AS tip_rate,
                date_diff('second', pickup_ts, dropoff_ts) AS trip_duration_sec,
                pickup_location_id, dropoff_location_id,
                pickup_zone, dropoff_zone,
                pickup_borough, dropoff_borough,
                pickup_service_zone, dropoff_service_zone,
                pickup_year, pickup_month
            FROM hive.mart.gold_fact_trips
        """,
    },
    {
        "name": "fact_trips_daily",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_date,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS total_revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(tip_amount) AS avg_tip,
                AVG(tip_amount / NULLIF(total_amount, 0)) AS avg_tip_pct,
                AVG(trip_distance) AS avg_distance,
                SUM(passenger_count) AS total_passengers
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_date
            ORDER BY pickup_date
        """,
    },
    {
        "name": "fact_trips_hourly",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_date,
                pickup_hour,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(tip_amount) AS avg_tip,
                AVG(trip_distance) AS avg_distance
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_date, pickup_hour
            ORDER BY pickup_date, pickup_hour
        """,
    },
    {
        "name": "fact_trips_hourly_zone",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_date,
                pickup_hour,
                pickup_zone,
                pickup_borough,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS total_revenue,
                AVG(fare_amount) AS avg_fare,
                SUM(CASE WHEN dropoff_location_id IS NOT NULL THEN 1 ELSE 0 END) AS dropoff_count
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_date, pickup_hour, pickup_zone, pickup_borough
            ORDER BY pickup_date, pickup_hour, trip_count DESC
        """,
    },
    {
        "name": "fact_trips_borough",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_date,
                pickup_borough,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS revenue,
                AVG(trip_distance) AS avg_distance,
                AVG(fare_amount) AS avg_fare
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_date, pickup_borough
            ORDER BY pickup_date, revenue DESC
        """,
    },

    # ── 2. Dimension Tables ──
    {
        "name": "dim_zone",
        "partitioned": False,
        "sql": """
            SELECT
                CAST(location_id AS INTEGER) AS location_id,
                borough,
                zone,
                service_zone
            FROM hive.nyc.taxi_zone_lookup
        """,
    },
    {
        "name": "dim_zone_grouped",
        "partitioned": False,
        "sql": """
            WITH zone_volume AS (
                SELECT
                    pickup_zone AS zone,
                    pickup_borough AS borough,
                    COUNT(*) AS trip_count
                FROM hive.mart.gold_fact_trips
                GROUP BY pickup_zone, pickup_borough
            ),
            zones AS (
                SELECT CAST(location_id AS INTEGER) AS location_id, borough, zone, service_zone
                FROM hive.nyc.taxi_zone_lookup
            )
            SELECT
                z.location_id,
                z.zone,
                z.borough,
                z.service_zone,
                COALESCE(v.trip_count, 0) AS pickup_trip_count,
                CASE
                    WHEN COALESCE(v.trip_count, 0) = 0 THEN 'NoData'
                    WHEN v.trip_count >= 100000 THEN 'High'
                    WHEN v.trip_count >= 10000 THEN 'Medium'
                    WHEN v.trip_count >= 1000 THEN 'Low'
                    ELSE 'VeryLow'
                END AS trip_volume_tier,
                CASE
                    WHEN COALESCE(v.trip_count, 0) >= 100000 THEN z.zone
                    ELSE z.borough || '_Other'
                END AS group_name
            FROM zones z
            LEFT JOIN zone_volume v ON z.zone = v.zone AND z.borough = v.borough
            ORDER BY v.trip_count DESC NULLS LAST
        """,
    },
    {
        "name": "dim_date",
        "partitioned": False,
        "sql": """
            WITH date_range AS (
                SELECT
                    CAST(MIN(pickup_date) AS DATE) AS min_date,
                    CAST(MAX(pickup_date) AS DATE) AS max_date
                FROM hive.mart.gold_fact_trips
            ),
            dates AS (
                SELECT dt
                FROM date_range
                CROSS JOIN UNNEST(
                    SEQUENCE(min_date, max_date, INTERVAL '1' DAY)
                ) AS t(dt)
            )
            SELECT
                dt AS date,
                EXTRACT(YEAR FROM dt) AS year,
                EXTRACT(MONTH FROM dt) AS month,
                EXTRACT(DAY FROM dt) AS day,
                EXTRACT(DAY_OF_WEEK FROM dt) AS day_of_week,
                CASE WHEN EXTRACT(DAY_OF_WEEK FROM dt) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
                FALSE AS is_holiday,
                EXTRACT(QUARTER FROM dt) AS quarter,
                EXTRACT(WEEK FROM dt) AS week_of_year
            FROM dates
            ORDER BY dt
        """,
    },
    {
        "name": "dim_vendor",
        "partitioned": False,
        "sql": """
            SELECT 1 AS vendor_id, 'Creative Mobile' AS vendor_name
            UNION ALL
            SELECT 2, 'VeriFone'
        """,
    },
    {
        "name": "dim_payment_type",
        "partitioned": False,
        "sql": """
            SELECT 1 AS payment_type_code, 'Credit card' AS description
            UNION ALL
            SELECT 2, 'Cash'
            UNION ALL
            SELECT 3, 'No charge'
            UNION ALL
            SELECT 4, 'Dispute'
            UNION ALL
            SELECT 5, 'Unknown'
            UNION ALL
            SELECT 6, 'Voided trip'
        """,
    },
    {
        "name": "dim_rate_code",
        "partitioned": False,
        "sql": """
            SELECT 1 AS rate_code_id, 'Standard' AS description
            UNION ALL
            SELECT 2, 'JFK'
            UNION ALL
            SELECT 3, 'Newark'
            UNION ALL
            SELECT 4, 'Nassau/Westchester'
            UNION ALL
            SELECT 5, 'Negotiated'
            UNION ALL
            SELECT 6, 'Group ride'
        """,
    },

    # ── 3. KPI & Business Metrics ──
    {
        "name": "kpi_daily_overview",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_date,
                COUNT(*) AS trips,
                SUM(total_amount) AS revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(tip_amount) AS avg_tip,
                AVG(tip_amount / NULLIF(total_amount, 0)) AS avg_tip_pct,
                AVG(trip_distance) AS avg_distance,
                COUNT(DISTINCT vendor_id) AS unique_vendors,
                COUNT(*) FILTER (WHERE tip_amount > 0) * 100.0 / NULLIF(COUNT(*), 0) AS utilization_rate
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_date
            ORDER BY pickup_date
        """,
    },
    {
        "name": "kpi_weekly_trends",
        "partitioned": False,
        "sql": """
            WITH daily AS (
                SELECT
                    pickup_date,
                    COUNT(*) AS trip_count,
                    SUM(total_amount) AS revenue,
                    AVG(fare_amount) AS avg_fare
                FROM hive.mart.gold_fact_trips
                GROUP BY pickup_date
            ),
            weekly AS (
                SELECT
                    EXTRACT(YEAR FROM pickup_date) AS year,
                    EXTRACT(WEEK FROM pickup_date) AS week,
                    DATE_TRUNC('week', pickup_date) AS week_start,
                    SUM(trip_count) AS trip_count,
                    SUM(revenue) AS revenue,
                    AVG(avg_fare) AS avg_fare
                FROM daily
                GROUP BY 1, 2, 3
            )
            SELECT
                year, week, week_start,
                trip_count, revenue, avg_fare,
                LAG(trip_count) OVER (ORDER BY year, week) AS prev_week_trips,
                (trip_count - LAG(trip_count) OVER (ORDER BY year, week))
                    * 100.0 / NULLIF(LAG(trip_count) OVER (ORDER BY year, week), 0) AS trip_growth_pct,
                (revenue - LAG(revenue) OVER (ORDER BY year, week))
                    * 100.0 / NULLIF(LAG(revenue) OVER (ORDER BY year, week), 0) AS revenue_growth_pct
            FROM weekly
            ORDER BY year, week
        """,
    },
    {
        "name": "kpi_monthly_summary",
        "partitioned": False,
        "sql": """
            WITH monthly AS (
                SELECT
                    pickup_year,
                    pickup_month,
                    COUNT(*) AS trip_count,
                    SUM(total_amount) AS total_revenue,
                    AVG(fare_amount) AS avg_fare,
                    AVG(trip_distance) AS avg_distance,
                    COUNT(*) / 30.0 AS avg_trip_per_day
                FROM hive.mart.gold_fact_trips
                GROUP BY pickup_year, pickup_month
            )
            SELECT
                pickup_year, pickup_month,
                trip_count, total_revenue, avg_fare, avg_distance,
                ROUND(avg_trip_per_day, 1) AS avg_trip_per_day,
                LAG(total_revenue) OVER (ORDER BY pickup_year, pickup_month) AS prev_month_revenue,
                (total_revenue - LAG(total_revenue) OVER (ORDER BY pickup_year, pickup_month))
                    * 100.0 / NULLIF(LAG(total_revenue) OVER (ORDER BY pickup_year, pickup_month), 0) AS mom_growth_pct
            FROM monthly
            ORDER BY pickup_year, pickup_month
        """,
    },
    {
        "name": "kpi_borough_comparison",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_borough,
                COUNT(*) AS trips,
                SUM(total_amount) AS revenue,
                ROUND(SUM(total_amount) * 100.0 / NULLIF(SUM(SUM(total_amount)) OVER (), 0), 2) AS market_share_pct,
                AVG(fare_amount) AS avg_fare,
                AVG(tip_amount) AS avg_tip,
                AVG(trip_distance) AS avg_distance
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_borough
            ORDER BY revenue DESC
        """,
    },
    {
        "name": "kpi_zone_performance",
        "partitioned": False,
        "sql": """
            WITH pu AS (
                SELECT
                    pickup_location_id AS location_id,
                    pickup_zone AS zone,
                    pickup_borough AS borough,
                    COUNT(*) AS pickups,
                    SUM(total_amount) AS pickup_revenue,
                    AVG(fare_amount) AS avg_fare,
                    AVG(tip_amount) AS avg_tip,
                    AVG(tip_amount / NULLIF(total_amount, 0)) AS avg_tip_pct,
                    COUNT(*) FILTER (WHERE dropoff_location_id IN (1, 129, 132, 138)) AS airport_trip_count
                FROM hive.mart.gold_fact_trips
                GROUP BY pickup_location_id, pickup_zone, pickup_borough
            ),
            do AS (
                SELECT
                    dropoff_location_id AS location_id,
                    COUNT(*) AS dropoffs,
                    SUM(total_amount) AS dropoff_revenue
                FROM hive.mart.gold_fact_trips
                GROUP BY dropoff_location_id
            )
            SELECT
                COALESCE(pu.location_id, do.location_id) AS location_id,
                COALESCE(pu.zone, 'Unknown') AS zone,
                COALESCE(pu.borough, 'Unknown') AS borough,
                COALESCE(pu.pickups, 0) AS pickups,
                COALESCE(do.dropoffs, 0) AS dropoffs,
                COALESCE(do.dropoffs, 0) - COALESCE(pu.pickups, 0) AS net_flow,
                CASE
                    WHEN COALESCE(pu.pickups, 0) > 0
                    THEN ROUND(COALESCE(do.dropoffs, 0) * 1.0 / pu.pickups, 2)
                    ELSE NULL
                END AS net_flow_ratio,
                COALESCE(pu.pickup_revenue, 0) AS pickup_revenue,
                COALESCE(do.dropoff_revenue, 0) AS dropoff_revenue,
                pu.avg_fare,
                pu.avg_tip,
                pu.avg_tip_pct,
                pu.airport_trip_count,
                CASE
                    WHEN COALESCE(pu.pickups, 0) > 0
                    THEN ROUND(pu.airport_trip_count * 100.0 / pu.pickups, 1)
                    ELSE 0
                END AS airport_trip_pct
            FROM pu
            FULL OUTER JOIN do ON pu.location_id = do.location_id
            ORDER BY COALESCE(pu.pickups, 0) DESC
        """,
    },
    {
        "name": "kpi_zone_net_flow",
        "partitioned": False,
        "sql": """
            WITH flow AS (
                SELECT
                    pickup_zone AS zone,
                    pickup_borough AS borough,
                    COUNT(*) AS pickups,
                    SUM(total_amount) AS pickup_revenue
                FROM hive.mart.gold_fact_trips
                GROUP BY pickup_zone, pickup_borough
            ),
            inflow AS (
                SELECT
                    dropoff_zone AS zone,
                    dropoff_borough AS borough,
                    COUNT(*) AS dropoffs,
                    SUM(total_amount) AS dropoff_revenue
                FROM hive.mart.gold_fact_trips
                GROUP BY dropoff_zone, dropoff_borough
            ),
            top_source AS (
                SELECT
                    f.dropoff_zone AS zone,
                    f.dropoff_borough AS borough,
                    f.pickup_zone AS source_zone,
                    f.pickup_borough AS source_borough,
                    COUNT(*) AS trips,
                    ROW_NUMBER() OVER (PARTITION BY f.dropoff_zone ORDER BY COUNT(*) DESC) AS rn
                FROM hive.mart.gold_fact_trips f
                GROUP BY f.dropoff_zone, f.dropoff_borough, f.pickup_zone, f.pickup_borough
            ),
            top_dest AS (
                SELECT
                    f.pickup_zone AS zone,
                    f.pickup_borough AS borough,
                    f.dropoff_zone AS dest_zone,
                    f.dropoff_borough AS dest_borough,
                    COUNT(*) AS trips,
                    ROW_NUMBER() OVER (PARTITION BY f.pickup_zone ORDER BY COUNT(*) DESC) AS rn
                FROM hive.mart.gold_fact_trips f
                GROUP BY f.pickup_zone, f.pickup_borough, f.dropoff_zone, f.dropoff_borough
            )
            SELECT
                COALESCE(f.zone, i.zone) AS zone,
                COALESCE(f.borough, i.borough) AS borough,
                COALESCE(f.pickups, 0) AS pickups,
                COALESCE(i.dropoffs, 0) AS dropoffs,
                COALESCE(i.dropoffs, 0) - COALESCE(f.pickups, 0) AS net_flow,
                CASE
                    WHEN COALESCE(f.pickups, 0) > 0
                    THEN ROUND(COALESCE(i.dropoffs, 0) * 1.0 / f.pickups, 2)
                    ELSE NULL
                END AS net_flow_ratio,
                CASE
                    WHEN COALESCE(f.pickups, 0) > 0 AND COALESCE(i.dropoffs, 0) > 0
                    THEN ROUND(ABS(COALESCE(i.dropoffs, 0) - COALESCE(f.pickups, 0))
                              * 1.0 / GREATEST(f.pickups, i.dropoffs), 2)
                    ELSE NULL
                END AS imbalance_score,
                ts.source_zone AS primary_inflow_source,
                td.dest_zone AS primary_outflow_dest,
                COALESCE(f.pickup_revenue, 0) AS pickup_revenue,
                COALESCE(i.dropoff_revenue, 0) AS dropoff_revenue
            FROM flow f
            FULL OUTER JOIN inflow i ON f.zone = i.zone AND f.borough = i.borough
            LEFT JOIN top_source ts ON ts.zone = COALESCE(f.zone, i.zone)
                                   AND ts.borough = COALESCE(f.borough, i.borough)
                                   AND ts.rn = 1
            LEFT JOIN top_dest td ON td.zone = COALESCE(f.zone, i.zone)
                                 AND td.borough = COALESCE(f.borough, i.borough)
                                 AND td.rn = 1
            ORDER BY net_flow DESC
        """,
    },
    {
        "name": "kpi_payment_trends",
        "partitioned": False,
        "sql": """
            SELECT
                payment_type,
                CASE payment_type
                    WHEN 1 THEN 'Credit card'
                    WHEN 2 THEN 'Cash'
                    WHEN 3 THEN 'No charge'
                    WHEN 4 THEN 'Dispute'
                    WHEN 5 THEN 'Unknown'
                    WHEN 6 THEN 'Voided trip'
                    ELSE 'Other'
                END AS payment_desc,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(tip_amount) AS avg_tip,
                AVG(tip_amount / NULLIF(total_amount, 0)) AS avg_tip_pct
            FROM hive.mart.gold_fact_trips
            GROUP BY payment_type
            ORDER BY trip_count DESC
        """,
    },
    {
        "name": "kpi_vendor_performance",
        "partitioned": False,
        "sql": """
            SELECT
                vendor_id,
                CASE vendor_id
                    WHEN 1 THEN 'Creative Mobile'
                    WHEN 2 THEN 'VeriFone'
                    ELSE 'Unknown'
                END AS vendor_name,
                COUNT(*) AS trips,
                SUM(total_amount) AS revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(tip_amount) AS avg_tip,
                AVG(trip_distance) AS avg_distance,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS market_share_pct
            FROM hive.mart.gold_fact_trips
            GROUP BY vendor_id
            ORDER BY revenue DESC
        """,
    },

    # ── 4. Route & Operational Analysis ──
    {
        "name": "route_top_pickup_zones",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_zone,
                pickup_borough,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(trip_distance) AS avg_distance
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_zone, pickup_borough
            ORDER BY trip_count DESC
            LIMIT 20
        """,
    },
    {
        "name": "route_top_dropoff_zones",
        "partitioned": False,
        "sql": """
            SELECT
                dropoff_zone,
                dropoff_borough,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(trip_distance) AS avg_distance
            FROM hive.mart.gold_fact_trips
            GROUP BY dropoff_zone, dropoff_borough
            ORDER BY trip_count DESC
            LIMIT 20
        """,
    },
    {
        "name": "route_popular_routes",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_zone,
                pickup_borough,
                dropoff_zone,
                dropoff_borough,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS revenue,
                AVG(total_amount) AS avg_revenue,
                AVG(trip_distance) AS avg_distance,
                AVG(tip_amount) AS avg_tip
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_zone, pickup_borough, dropoff_zone, dropoff_borough
            ORDER BY trip_count DESC
            LIMIT 50
        """,
    },
    {
        "name": "route_airport_analysis",
        "partitioned": False,
        "sql": """
            SELECT
                CASE
                    WHEN pickup_location_id IN (1, 129, 132, 138) THEN 'FROM airport'
                    WHEN dropoff_location_id IN (1, 129, 132, 138) THEN 'TO airport'
                END AS direction,
                CASE pickup_location_id
                    WHEN 1 THEN 'EWR'
                    WHEN 129 THEN 'LaGuardia'
                    WHEN 132 THEN 'JFK'
                    WHEN 138 THEN 'JFK'
                    ELSE CASE dropoff_location_id
                        WHEN 1 THEN 'EWR'
                        WHEN 129 THEN 'LaGuardia'
                        WHEN 132 THEN 'JFK'
                        WHEN 138 THEN 'JFK'
                    END
                END AS airport,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(tip_amount) AS avg_tip,
                AVG(trip_distance) AS avg_distance,
                AVG(tip_amount / NULLIF(total_amount, 0)) AS avg_tip_pct
            FROM hive.mart.gold_fact_trips
            WHERE pickup_location_id IN (1, 129, 132, 138)
               OR dropoff_location_id IN (1, 129, 132, 138)
            GROUP BY 1, 2
            ORDER BY trip_count DESC
        """,
    },
    {
        "name": "route_airport_zone_matrix",
        "partitioned": False,
        "sql": """
            SELECT
                CASE
                    WHEN f.pickup_location_id = 1 THEN 'EWR'
                    WHEN f.pickup_location_id = 129 THEN 'LaGuardia'
                    WHEN f.pickup_location_id IN (132, 138) THEN 'JFK'
                END AS airport_zone,
                f.dropoff_zone AS residential_zone,
                f.dropoff_borough AS borough,
                COUNT(*) AS trips,
                SUM(f.total_amount) AS revenue,
                AVG(f.fare_amount) AS avg_fare,
                AVG(f.trip_distance) AS avg_distance,
                CAST(AVG(EXTRACT(HOUR FROM f.pickup_ts)) AS INTEGER) AS peak_hour,
                AVG(f.tip_amount) AS avg_tip
            FROM hive.mart.gold_fact_trips f
            WHERE f.pickup_location_id IN (1, 129, 132, 138)
              AND f.dropoff_location_id NOT IN (1, 129, 132, 138)
            GROUP BY 1, 2, 3
            HAVING COUNT(*) >= 5
            ORDER BY trips DESC
        """,
    },
    {
        "name": "route_cross_borough",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_borough,
                dropoff_borough,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(trip_distance) AS avg_distance,
                AVG(tip_amount) AS avg_tip
            FROM hive.mart.gold_fact_trips
            WHERE pickup_borough != dropoff_borough
              AND pickup_borough NOT IN ('N/A', 'Unknown')
              AND dropoff_borough NOT IN ('N/A', 'Unknown')
            GROUP BY pickup_borough, dropoff_borough
            ORDER BY trip_count DESC
        """,
    },
    {
        "name": "od_borough_matrix",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_borough,
                dropoff_borough,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS total_revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(trip_distance) AS avg_distance,
                AVG(tip_amount) AS avg_tip,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
            FROM hive.mart.gold_fact_trips
            WHERE pickup_borough NOT IN ('N/A', 'Unknown')
              AND dropoff_borough NOT IN ('N/A', 'Unknown')
            GROUP BY pickup_borough, dropoff_borough
            ORDER BY trip_count DESC
        """,
    },
    {
        "name": "ops_peak_hours_heatmap",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_hour,
                EXTRACT(DAY_OF_WEEK FROM pickup_ts) AS day_of_week,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS revenue
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_hour, EXTRACT(DAY_OF_WEEK FROM pickup_ts)
            ORDER BY 2, 1
        """,
    },
    {
        "name": "ops_trip_distance_distribution",
        "partitioned": False,
        "sql": """
            SELECT
                CASE
                    WHEN trip_distance <= 1 THEN '0-1 miles'
                    WHEN trip_distance <= 3 THEN '1-3 miles'
                    WHEN trip_distance <= 5 THEN '3-5 miles'
                    WHEN trip_distance <= 10 THEN '5-10 miles'
                    ELSE '10+ miles'
                END AS distance_bucket,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(tip_amount) AS avg_tip,
                AVG(total_amount) AS avg_total
            FROM hive.mart.gold_fact_trips
            GROUP BY 1
            ORDER BY MIN(trip_distance)
        """,
    },
    {
        "name": "ops_passenger_count_pattern",
        "partitioned": False,
        "sql": """
            SELECT
                passenger_count,
                pickup_hour,
                pickup_borough,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS revenue
            FROM hive.mart.gold_fact_trips
            GROUP BY passenger_count, pickup_hour, pickup_borough
            ORDER BY trip_count DESC
        """,
    },
    {
        "name": "ops_utilization_rate",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_date,
                COUNT(*) AS total_trips,
                COUNT(*) FILTER (WHERE tip_amount > 0) AS tipped_trips,
                ROUND(COUNT(*) FILTER (WHERE tip_amount > 0) * 100.0 / NULLIF(COUNT(*), 0), 1) AS tip_rate_pct,
                COUNT(*) FILTER (WHERE passenger_count > 1) AS multi_passenger_trips,
                ROUND(COUNT(*) FILTER (WHERE passenger_count > 1) * 100.0 / NULLIF(COUNT(*), 0), 1) AS multi_passenger_pct,
                AVG(passenger_count) AS avg_passengers
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_date
            ORDER BY pickup_date
        """,
    },

    # ── 5. Data Quality & Audit ──
    {
        "name": "dq_validation_summary",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_date,
                COUNT(*) AS total_trips,
                SUM(CASE WHEN trip_distance <= 0 THEN 1 ELSE 0 END) AS zero_distance,
                SUM(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END) AS negative_fare,
                SUM(CASE WHEN passenger_count < 1 OR passenger_count > 6 THEN 1 ELSE 0 END) AS invalid_passengers,
                SUM(CASE WHEN tip_amount < 0 THEN 1 ELSE 0 END) AS negative_tip,
                SUM(CASE WHEN total_amount < fare_amount THEN 1 ELSE 0 END) AS total_less_than_fare
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_date
            ORDER BY pickup_date
        """,
    },
    {
        "name": "dq_invalid_by_reason",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_date,
                'zero_distance' AS reason,
                COUNT(*) AS count
            FROM hive.mart.gold_fact_trips
            WHERE trip_distance <= 0
            GROUP BY pickup_date
            UNION ALL
            SELECT
                pickup_date,
                'negative_fare',
                COUNT(*)
            FROM hive.mart.gold_fact_trips
            WHERE fare_amount < 0
            GROUP BY pickup_date
            UNION ALL
            SELECT
                pickup_date,
                'invalid_passengers',
                COUNT(*)
            FROM hive.mart.gold_fact_trips
            WHERE passenger_count < 1 OR passenger_count > 6
            GROUP BY pickup_date
            ORDER BY pickup_date, reason
        """,
    },
    {
        "name": "dq_row_count_trend",
        "partitioned": False,
        "sql": """
            SELECT
                pickup_date,
                COUNT(*) AS trip_count,
                COUNT(*) - AVG(COUNT(*)) OVER (ORDER BY pickup_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS delta_from_7day_avg,
                CASE
                    WHEN COUNT(*) < 0.3 * AVG(COUNT(*)) OVER (ORDER BY pickup_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
                    THEN 'ANOMALY_LOW'
                    WHEN COUNT(*) > 3.0 * AVG(COUNT(*)) OVER (ORDER BY pickup_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
                    THEN 'ANOMALY_HIGH'
                    ELSE 'NORMAL'
                END AS anomaly_flag
            FROM hive.mart.gold_fact_trips
            GROUP BY pickup_date
            ORDER BY pickup_date
        """,
    },
    {
        "name": "dq_batch_metadata",
        "partitioned": False,
        "sql": """
            SELECT
                'export_gold_to_minio.py' AS script_name,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS export_timestamp,
                CURRENT_DATE AS export_date,
                (SELECT COUNT(*) FROM hive.mart.gold_fact_trips) AS fact_trips_row_count,
                CAST(30 AS INTEGER) AS dataset_count
        """,
    },
]


# ──────────────────────────────────────────────
# Trino helpers
# ──────────────────────────────────────────────

def wait_for_trino(host: str, port: int, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            conn = connect(host=host, port=port, user="gold_export")
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            conn.close()
            return
        except Exception as e:
            last_err = e
            time.sleep(2)
    raise SystemExit(f"trino not ready: {last_err}")


def exec_(cur, sql: str, label: str = "") -> None:
    try:
        cur.execute(sql)
        cur.fetchall()
    except TrinoUserError as e:
        print(f"[trino][{label}] ERROR: {e}", file=sys.stderr)
        raise


def clean_s3_path(bucket: str, prefix: str) -> None:
    """Recursively delete objects under prefix from MinIO S3 bucket."""
    # Install minio at runtime if not available
    try:
        from minio import Minio
        from minio.error import S3Error
    except ImportError:
        print("[cleanup] minio library not found, installing...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "minio", "-q",
                               "--disable-pip-version-check"])
        from minio import Minio
        from minio.error import S3Error

    try:
        client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
        if objects:
            removed = 0
            for obj in objects:
                try:
                    client.remove_object(bucket, obj.object_name)
                    removed += 1
                except Exception:
                    pass
            print(f"[cleanup] removed {removed} objects from s3://{bucket}/{prefix}")
    except S3Error as e:
        print(f"[cleanup] S3 error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[cleanup] warning: cannot clean S3 path: {e}", file=sys.stderr)



def _add_parquet_extensions(name: str) -> None:
    """Rename all data files under prefix to have .parquet extension.

    Trino Hive CTAS writes valid Parquet but without the .parquet
    suffix. This makes files discoverable by tools that rely on
    file extensions (DuckDB, pandas, etc.). Handles both flat and
    partitioned datasets.
    """
    try:
        from minio import Minio       # type: ignore[import-untyped]
    except ImportError:
        return  # minio not available in this context
    try:
        mc = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                    secret_key=MINIO_SECRET_KEY, secure=False)
        objs = list(mc.list_objects(MINIO_BUCKET, prefix=f"{name}/",
                                     recursive=True))
        renamed = 0
        for obj in objs:
            src = obj.object_name
            if src.endswith('.parquet') or src.endswith('/'):
                continue
            dst = src + '.parquet'
            mc.copy_object(MINIO_BUCKET, dst, f"{MINIO_BUCKET}/{src}")
            mc.remove_object(MINIO_BUCKET, src)
            renamed += 1
        if renamed:
            print(f"[export] {name}: renamed {renamed} files -> .parquet")
    except Exception:
        pass  # best-effort; Trino reads files fine without extension


def main() -> int:
    wait_for_trino(TRINO_HOST, TRINO_PORT)
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="gold_export")
    cur = conn.cursor()

    # Create schema
    print(f"[trino] create schema {SCHEMA}")
    exec_(cur, f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}", "create_schema")

    total_ok = 0
    total_fail = 0
    total_rows = 0

    for ds in GOLD_DATASETS:
        name = ds["name"]
        location = f"{GOLD_PATH}/{name}/"
        partitioned = ds.get("partitioned", False)
        source_sql = ds["sql"].strip()

        # Validate SQL (basic — just try to get row count estimate)
        count_sql = f"SELECT COUNT(*) FROM ({source_sql}) AS _src"
        try:
            cur.execute(count_sql)
            row_count = cur.fetchone()[0]
        except TrinoUserError as e:
            print(f"[export] {name}: COUNT FAILED — {e}")
            total_fail += 1
            continue

        # Build CTAS
        with_clause = ""
        if partitioned:
            with_clause = ", partitioned_by = ARRAY['pickup_year','pickup_month']"

        ctas_sql = (
            f"CREATE TABLE {SCHEMA}.{name} WITH ("
            f"external_location = '{location}', "
            f"format = 'PARQUET'{with_clause}"
            f") AS {source_sql}"
        )

        start = time.time()
        try:
            print(f"[export] {name}: dropping existing...")
            exec_(cur, f"DROP TABLE IF EXISTS {SCHEMA}.{name}", name)

            # Clean S3 path before CTAS (Trino refuses CTAS if directory exists)
            clean_s3_path(MINIO_BUCKET, f"{name}/")

            print(f"[export] {name}: CTAS ({row_count} rows expected)...")
            exec_(cur, ctas_sql, name)
            elapsed = time.time() - start
            total_ok += 1
            total_rows += row_count
            print(f"[export] {name}: ✅ {row_count} rows, {elapsed:.1f}s")

            # Add .parquet extension to Trino-written files (best-effort)
            _add_parquet_extensions(name)

        except TrinoUserError as e:
            print(f"[export] {name}: ❌ FAILED — {e}")
            total_fail += 1


    conn.close()

    print(f"\n{'='*50}")
    print(f"Gold export complete: {total_ok} OK, {total_fail} FAILED, {total_rows} total rows")
    print(f"{'='*50}")

    return 1 if total_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
