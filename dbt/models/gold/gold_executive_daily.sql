-- Gold: Executive daily dashboard — 1 row per day, top-level metrics.
{{ config(materialized='view') }}

select
    pickup_date,
    count(*) as trips,
    sum(total_amount) as revenue,
    avg(total_amount) as avg_fare,
    sum(tip_amount) as tips,
    sum(tip_amount) / nullif(sum(total_amount), 0) as tip_rate,
    count(distinct pickup_zone) as active_zones,
    count(distinct vendor_id) as active_vendors,
    avg(trip_distance) as avg_distance,
    avg(trip_duration_sec) / 60.0 as avg_duration_min,
    -- vs previous day
    count(*) - lag(count(*)) over (order by pickup_date) as trip_delta,
    (count(*) * 100.0 / nullif(lag(count(*)) over (order by pickup_date), 0)) - 100 as trip_growth_pct
from {{ ref('gold_fact_trips') }}
group by 1
order by 1
