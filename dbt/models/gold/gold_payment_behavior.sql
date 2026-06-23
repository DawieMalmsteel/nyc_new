-- Gold: Payment behavior trend — daily payment type mix.
{{ config(materialized='view') }}

select
    pickup_date,
    payment_type,
    count(*) as trip_count,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_fare,
    avg(tip_amount) as avg_tip,
    sum(tip_amount) / nullif(sum(total_amount), 0) as tip_rate
from {{ ref('gold_fact_trips') }}
group by 1, 2
