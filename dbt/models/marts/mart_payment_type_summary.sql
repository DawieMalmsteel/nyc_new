-- Mart: revenue and trip summary by payment type.
{{ config(materialized='view') }}

select
  payment_type,
  case payment_type
    when 1 then 'Credit card'
    when 2 then 'Cash'
    when 3 then 'No charge'
    when 4 then 'Dispute'
    when 5 then 'Unknown'
    when 6 then 'Voided'
    else 'Other'
  end                                                         as payment_type_name,
  count(*)                                                    as trip_count,
  sum(total_amount)                                           as gross_revenue,
  avg(total_amount)                                           as avg_revenue_per_trip,
  sum(tip_amount)                                             as total_tip,
  avg(tip_amount)                                             as avg_tip,
  sum(fare_amount)                                            as total_fare,
  avg(trip_distance)                                          as avg_distance
from {{ ref('fact_trips') }}
group by 1
order by gross_revenue desc
