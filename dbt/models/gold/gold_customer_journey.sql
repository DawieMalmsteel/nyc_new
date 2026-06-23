-- Gold: Customer journey — top OD pairs by hour.
{{ config(materialized='view') }}

select
    pickup_hour,
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,
    count(*) as trip_count,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_fare,
    avg(trip_distance) as avg_distance,
    avg(trip_duration_sec) / 60.0 as avg_duration_min
from {{ ref('gold_fact_trips') }}
group by 1, 2, 3, 4, 5
