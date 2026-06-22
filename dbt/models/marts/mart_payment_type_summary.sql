-- Mart: revenue and trip summary by payment type.
-- ponytail: full outer with static payment_type list ensures all 6 types appear even with 0 rows.
{{ config(materialized='view') }}

with payment_types as (
    select * from (values (1, 'Credit card'), (2, 'Cash'), (3, 'No charge'),
                              (4, 'Dispute'), (5, 'Unknown'), (6, 'Voided')) as t(payment_type, payment_type_name)
)
select
    pt.payment_type,
    pt.payment_type_name,
    coalesce(count(*), 0)                                      as trip_count,
    coalesce(sum(t.total_amount), 0)                           as gross_revenue,
    coalesce(avg(t.total_amount), 0)                           as avg_revenue_per_trip,
    coalesce(sum(t.tip_amount), 0)                             as total_tip,
    coalesce(avg(t.tip_amount), 0)                             as avg_tip,
    coalesce(sum(t.fare_amount), 0)                            as total_fare,
    coalesce(avg(t.trip_distance), 0)                          as avg_distance
from payment_types pt
left join {{ ref('fact_trips') }} t on pt.payment_type = t.payment_type
group by pt.payment_type, pt.payment_type_name
order by gross_revenue desc
