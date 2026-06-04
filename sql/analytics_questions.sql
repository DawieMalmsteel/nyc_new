-- NYC Taxi Analytics — 8+ business questions against the dbt-trino mart.
-- Each query is read-only and must return at least 1 row.

-- 1) Top 10 pickup zones by trip count.
SELECT pickup_zone, COUNT(*) AS n
FROM hive.mart.fact_trips
WHERE pickup_zone IS NOT NULL
GROUP BY pickup_zone
ORDER BY n DESC
LIMIT 10;

-- 2) Hourly trip distribution (24-hour).
SELECT pickup_hour, COUNT(*) AS n, AVG(total_amount) AS avg_total
FROM hive.mart.fact_trips
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- 3) Borough-to-borough trip matrix.
SELECT pickup_borough, dropoff_borough, COUNT(*) AS n
FROM hive.mart.fact_trips
WHERE pickup_borough IS NOT NULL AND dropoff_borough IS NOT NULL
GROUP BY pickup_borough, dropoff_borough
ORDER BY n DESC
LIMIT 20;

-- 4) Average fare by payment type.
SELECT payment_type, COUNT(*) AS n, AVG(fare_amount) AS avg_fare
FROM hive.mart.fact_trips
GROUP BY payment_type
ORDER BY payment_type;

-- 5) Daily gross revenue and trip count.
SELECT pickup_date,
       COUNT(*) AS trip_count,
       SUM(total_amount) AS gross_revenue
FROM hive.mart.fact_trips
GROUP BY pickup_date
ORDER BY pickup_date;

-- 6) Top 10 longest trips by distance.
SELECT pickup_ts, trip_distance, total_amount, pickup_zone, dropoff_zone
FROM hive.mart.fact_trips
ORDER BY trip_distance DESC
LIMIT 10;

-- 7) Top 5 pickup boroughs by gross revenue.
SELECT pickup_borough,
       SUM(total_amount) AS gross_revenue,
       AVG(tip_rate)      AS avg_tip_rate
FROM hive.mart.fact_trips
WHERE pickup_borough IS NOT NULL
GROUP BY pickup_borough
ORDER BY gross_revenue DESC
LIMIT 5;

-- 8) Hourly summary via the dbt mart table.
SELECT pickup_date, pickup_hour, pickup_borough, trip_count, gross_revenue
FROM hive.mart.mart_hourly_summary
ORDER BY pickup_date, pickup_hour, pickup_borough;

-- 9) Mart inventory: list every mart.* relation.
SELECT table_name, table_type
FROM hive.information_schema.tables
WHERE table_schema = 'mart'
ORDER BY table_name;

-- 10) Quarantine mart view is queryable (always 1 row).
SELECT 'invalid_trips_view_resolves' AS check_name,
       COUNT(*) AS rows_in_view
FROM hive.mart.fact_invalid_trips;
