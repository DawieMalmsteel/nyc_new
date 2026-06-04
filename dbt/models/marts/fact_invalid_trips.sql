-- Mart: invalid trips fact, exploded for each error reason.
{{ config(materialized='view') }}
select
  quarantine_ts,
  cast(pickup_year as integer)    as pickup_year,
  cast(pickup_month as integer)   as pickup_month,
  err as validation_error,
  count(*) as error_count
from {{ ref('stg_invalid_trips') }}
cross join unnest(validation_errors) as t(err)
group by 1, 2, 3, 4
