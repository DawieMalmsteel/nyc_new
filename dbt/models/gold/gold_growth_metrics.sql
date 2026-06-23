-- Gold: Weekly growth metrics — WoW comparison.
{{ config(materialized='view') }}

with daily as (
    select
        pickup_date,
        date_trunc('week', pickup_date) as week_start,
        count(*) as trips,
        sum(total_amount) as revenue,
        avg(total_amount) as avg_fare,
        count(distinct pickup_zone) as active_zones,
        sum(tip_amount) as tips
    from {{ ref('gold_fact_trips') }}
    group by 1
),
weekly as (
    select
        week_start,
        sum(trips) as trips,
        sum(revenue) as revenue,
        avg(avg_fare) as avg_fare,
        sum(active_zones) as active_zone_visits,
        sum(tips) as tips
    from daily
    group by 1
)
select
    week_start,
    trips,
    revenue,
    avg_fare,
    tips,
    active_zone_visits,
    -- WoW growth
    (trips - lag(trips) over (order by week_start)) * 100.0
        / nullif(lag(trips) over (order by week_start), 0) as trips_wow_pct,
    (revenue - lag(revenue) over (order by week_start)) * 100.0
        / nullif(lag(revenue) over (order by week_start), 0) as revenue_wow_pct
from weekly
order by week_start
