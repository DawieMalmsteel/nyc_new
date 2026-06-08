-- Staging: clean zone lookup.
{{ config(materialized='view') }}

select
  cast(location_id as integer) as location_id,
  borough,
  zone,
  service_zone
from hive.nyc.taxi_zone_lookup
