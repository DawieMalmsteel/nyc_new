-- Singular test: passenger_count must be in 1..6
select passenger_count
from {{ ref('fact_trips') }}
where passenger_count is not null
  and (passenger_count < 1 or passenger_count > 6)
