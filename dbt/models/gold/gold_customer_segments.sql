-- Gold: Customer segments by spend, distance, frequency (zone-level proxy).
-- ponytail: no rider_id → segments by pickup_zone × day. Tier: VIP > $150 + >10 trips/day.
{{ config(materialized='view') }}

with daily_zone as (
    select
        pickup_date,
        pickup_zone,
        pickup_borough,
        count(*) as trips,
        sum(total_amount) as total_spend,
        avg(total_amount) as avg_spend,
        avg(trip_distance) as avg_distance,
        avg(passenger_count) as avg_passengers,
        sum(tip_amount) / nullif(sum(total_amount), 0) as tip_rate,
        count(distinct pickup_hour) as active_hours
    from {{ ref('gold_fact_trips') }}
    group by 1, 2, 3
)
select
    *,
    case
        when total_spend > 500 and trips > 20 then 'VIP'
        when total_spend > 200 or trips > 10 then 'Regular'
        when trips <= 2 then 'One-time'
        else 'Casual'
    end as segment
from daily_zone
