-- Gold: Tipping culture — tip rate by zone, hour, trip type.
{{ config(materialized='view') }}

select
    pickup_hour,
    pickup_dow,
    pickup_zone,
    pickup_borough,
    payment_type,
    passenger_count,
    count(*) as trip_count,
    avg(tip_amount) as avg_tip,
    avg(tip_rate) as avg_tip_rate,
    sum(tip_amount) as total_tips,
    sum(total_amount) as total_revenue
from {{ ref('gold_fact_trips') }}
where tip_amount > 0
group by 1, 2, 3, 4, 5, 6
