-- Gold: revenue by zone materialized as table.
{{ config(materialized='view') }}

select
  pickup_borough,
  pickup_zone,
  dropoff_borough,
  dropoff_zone,
  count(*)                                                    as trip_count,
  sum(total_amount)                                           as gross_revenue,
  avg(total_amount)                                           as avg_revenue_per_trip,
  sum(fare_amount)                                            as total_fare,
  sum(tip_amount)                                             as total_tip,
  avg(trip_distance)                                          as avg_distance
from {{ ref('gold_fact_trips') }}
group by 1, 2, 3, 4
order by gross_revenue desc
