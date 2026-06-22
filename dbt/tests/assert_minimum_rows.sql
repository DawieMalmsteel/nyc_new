-- Data quality: fact_trips must have at least 10 rows (sanity floor).
-- ponytail: bare-minimum check. Tighten threshold if this ever fires without real data loss.
select count(*) as row_count
from {{ ref('fact_trips') }}
having count(*) < 10
