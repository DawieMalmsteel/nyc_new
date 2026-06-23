-- Gold: Risk dashboard — anomaly flag, freshness, data quality summary.
{{ config(materialized='view') }}

select
    a.pickup_date,
    a.trip_count,
    a.delta_from_7day_avg,
    a.anomaly_flag,
    -- freshness: days since latest data
    date_diff('day', a.pickup_date, current_date) as days_since_update,
    -- validation summary join
    coalesce(v.total_trips, 0) as dq_total_trips,
    coalesce(v.zero_distance, 0) as dq_zero_distance,
    coalesce(v.negative_fare, 0) as dq_negative_fare,
    coalesce(v.invalid_passengers, 0) as dq_invalid_passengers,
    case
        when a.anomaly_flag != 'NORMAL' then 'ANOMALY'
        when date_diff('day', a.pickup_date, current_date) > 7 then 'STALE'
        when coalesce(v.invalid_passengers, 0) > 100 then 'QUALITY_ISSUE'
        else 'HEALTHY'
    end as risk_status
from {{ ref('gold_dq_row_count_trend') }} a
left join {{ ref('gold_validation_summary') }} v on a.pickup_date = v.pickup_date
order by a.pickup_date desc
