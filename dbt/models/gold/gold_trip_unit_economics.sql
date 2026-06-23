-- Gold: Trip unit economics — revenue per trip, per hour, per km.
{{ config(materialized='view') }}

select
    pickup_date,
    pickup_hour,
    pickup_dow,
    pickup_zone,
    pickup_borough,
    count(*) as trip_count,
    avg(fare_amount) as avg_fare,
    avg(tip_amount) as avg_tip,
    avg(total_amount) as avg_total,
    avg(trip_distance) as avg_distance,
    avg(trip_duration_sec) / 60.0 as avg_duration_min,
    -- unit metrics
    sum(fare_amount) / nullif(sum(trip_distance), 0) as fare_per_km,
    sum(total_amount) / nullif(sum(trip_duration_sec) / 3600.0, 0) as revenue_per_hour,
    count(*) / nullif(count(distinct pickup_hour), 0) as trips_per_active_hour
from {{ ref('gold_fact_trips') }}
-- ponytail: filter extreme outliers (0.0003% of data)
where trip_distance <= 500
  and total_amount <= 500
group by 1, 2, 3, 4, 5
