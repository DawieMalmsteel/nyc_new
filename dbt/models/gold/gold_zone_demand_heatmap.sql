-- Gold: Hourly demand heatmap by zone.
{{ config(materialized='view') }}

select
    pickup_hour,
    pickup_dow,
    pickup_zone,
    pickup_borough,
    count(*) as pickup_count,
    sum(total_amount) as pickup_revenue,
    avg(total_amount) as avg_fare
from {{ ref('gold_fact_trips') }}
group by 1, 2, 3, 4
