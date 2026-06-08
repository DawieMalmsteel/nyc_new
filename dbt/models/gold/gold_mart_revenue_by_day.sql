-- Gold: daily revenue summary materialized as table.
{{ config(materialized='view') }}

select
  pickup_date,
  count(*)                                                    as trip_count,
  sum(fare_amount)                                            as total_fare,
  sum(extra)                                                  as total_extra,
  sum(mta_tax)                                                as total_mta_tax,
  sum(tip_amount)                                             as total_tip,
  sum(tolls_amount)                                           as total_tolls,
  sum(improvement_surcharge)                                  as total_improvement_surcharge,
  sum(total_amount)                                           as gross_revenue,
  avg(fare_amount)                                            as avg_fare,
  avg(total_amount)                                           as avg_total,
  avg(tip_amount)                                             as avg_tip,
  avg(trip_distance)                                          as avg_distance
from {{ ref('gold_fact_trips') }}
group by 1
order by 1
