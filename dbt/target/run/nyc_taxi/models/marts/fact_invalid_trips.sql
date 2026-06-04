
  create or replace view
    "hive"."mart"."fact_invalid_trips"
  security definer
  as
    -- Mart: invalid trips fact, exploded for each error reason.

select
  quarantine_ts,
  cast(pickup_year as integer)    as pickup_year,
  cast(pickup_month as integer)   as pickup_month,
  err as validation_error,
  count(*) as error_count
from "hive"."mart"."stg_invalid_trips"
cross join unnest(validation_errors) as t(err)
group by 1, 2, 3, 4
  ;
