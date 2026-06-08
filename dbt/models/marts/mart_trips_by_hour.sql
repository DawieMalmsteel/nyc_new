-- Mart: trip count by hour of day.
{{ config(materialized='view') }}

select
  pickup_hour,
  pickup_dow,
  count(*)                                                    as trip_count,
  sum(total_amount)                                           as gross_revenue,
  avg(total_amount)                                           as avg_revenue_per_trip,
  avg(trip_distance)                                          as avg_distance,
  avg(trip_duration_sec)                                      as avg_duration_sec
from {{ ref('fact_trips') }}
group by 1, 2
order by 1, 2
