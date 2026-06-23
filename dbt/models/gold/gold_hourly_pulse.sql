-- Gold: 24×7 hourly pulse heatmap.
{{ config(materialized='view') }}

select
    pickup_hour,
    pickup_dow,
    count(*) as trip_count,
    sum(total_amount) as revenue,
    avg(total_amount) as avg_fare,
    avg(trip_distance) as avg_distance,
    count(distinct pickup_zone) as active_zones
from {{ ref('gold_fact_trips') }}
group by 1, 2
