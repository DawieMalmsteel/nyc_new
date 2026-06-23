-- Gold: Executive weekly dashboard.
{{ config(materialized='view') }}

select
    date_trunc('week', pickup_date) as week_start,
    count(*) as trips,
    sum(total_amount) as revenue,
    avg(total_amount) as avg_fare,
    sum(tip_amount) as tips,
    sum(tip_amount) / nullif(sum(total_amount), 0) as tip_rate,
    count(distinct pickup_zone) as active_zones,
    avg(trip_distance) as avg_distance,
    -- growth
    (count(*) - lag(count(*)) over (order by date_trunc('week', pickup_date))) * 100.0
        / nullif(lag(count(*)) over (order by date_trunc('week', pickup_date)), 0) as trips_wow_pct,
    (sum(total_amount) - lag(sum(total_amount)) over (order by date_trunc('week', pickup_date))) * 100.0
        / nullif(lag(sum(total_amount)) over (order by date_trunc('week', pickup_date)), 0) as revenue_wow_pct
from {{ ref('gold_fact_trips') }}
group by 1
order by 1
