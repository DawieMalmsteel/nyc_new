-- Gold: Validation summary — daily quality metrics from gold_fact_trips.
{{ config(materialized='view') }}

select
    pickup_date,
    count(*) as total_trips,
    sum(case when trip_distance <= 0 then 1 else 0 end) as zero_distance,
    sum(case when fare_amount < 0 then 1 else 0 end) as negative_fare,
    sum(case when passenger_count < 1 or passenger_count > 6 then 1 else 0 end) as invalid_passengers,
    sum(case when tip_amount < 0 then 1 else 0 end) as negative_tip,
    sum(case when total_amount < fare_amount then 1 else 0 end) as total_less_than_fare
from {{ ref('gold_fact_trips') }}
group by 1
order by 1
