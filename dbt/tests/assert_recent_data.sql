-- Data quality: max(pickup_date) must be >= 2024-01-01 (sanity check for historical NYC data).
-- ponytail: 2024-01-01 floor covers all expected batch data. 
select max(pickup_date) as latest_date
from {{ ref('fact_trips') }}
having max(pickup_date) < date '2024-01-01'
