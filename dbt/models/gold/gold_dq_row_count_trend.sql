-- Gold: daily row count trend with anomaly detection.
-- ponytail: dbt model instead of CTAS — runs right after dbt_build, no need to wait for gold_export.
{{ config(materialized='view') }}

select
    pickup_date,
    count(*) as trip_count,
    count(*) - avg(count(*)) over (order by pickup_date rows between 6 preceding and 1 preceding) as delta_from_7day_avg,
    case
        when count(*) < 0.3 * avg(count(*)) over (order by pickup_date rows between 6 preceding and 1 preceding)
        then 'ANOMALY_LOW'
        when count(*) > 3.0 * avg(count(*)) over (order by pickup_date rows between 6 preceding and 1 preceding)
        then 'ANOMALY_HIGH'
        else 'NORMAL'
    end as anomaly_flag
from {{ ref('gold_fact_trips') }}
group by pickup_date
order by pickup_date
