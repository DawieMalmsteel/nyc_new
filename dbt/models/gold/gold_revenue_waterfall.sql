-- Gold: Revenue waterfall — breakdown by fare, tip, tolls, surcharge.
{{ config(materialized='view') }}

select
    pickup_date,
    sum(fare_amount) as fare_revenue,
    sum(tip_amount) as tip_revenue,
    sum(tolls_amount) as tolls_revenue,
    sum(mta_tax) as mta_tax_total,
    sum(improvement_surcharge) as improvement_surcharge_total,
    sum(extra) as extra_charges,
    sum(total_amount) as gross_revenue,
    sum(fare_amount) / nullif(sum(total_amount), 0) as fare_pct,
    sum(tip_amount) / nullif(sum(total_amount), 0) as tip_pct
from {{ ref('gold_fact_trips') }}
group by 1
order by 1
