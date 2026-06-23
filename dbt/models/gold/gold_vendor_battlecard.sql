-- Gold: Vendor battlecard — Creative Mobile (1) vs VeriFone (2).
{{ config(materialized='view') }}

select
    pickup_date,
    vendor_id,
    count(*) as trip_count,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_fare,
    avg(tip_amount) as avg_tip,
    sum(tip_amount) / nullif(sum(total_amount), 0) as tip_rate,
    avg(trip_distance) as avg_distance,
    count(distinct pickup_zone) as active_zones,
    -- market share within day
    count(*) * 100.0 / sum(count(*)) over (partition by pickup_date) as market_share_pct,
    sum(total_amount) * 100.0 / nullif(sum(sum(total_amount)) over (partition by pickup_date), 0) as revenue_share_pct
from {{ ref('gold_fact_trips') }}
group by 1, 2
