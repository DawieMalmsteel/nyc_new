-- Singular test: trip_distance must be > 0
select pickup_ts, trip_distance
from {{ ref('fact_trips') }}
where trip_distance <= 0
