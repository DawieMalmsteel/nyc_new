-- Singular test: total_amount must be >= fare_amount
select pickup_ts, fare_amount, total_amount
from {{ ref('fact_trips') }}
where total_amount < fare_amount
