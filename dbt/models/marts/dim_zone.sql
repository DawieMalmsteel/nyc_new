-- Mart: zone dimension (pickup + dropoff union of distinct zones seen in trips).
{{ config(materialized='table') }}

with zones as (
  select pickup_zone as zone, pickup_borough as borough, pickup_service_zone as service_zone from {{ ref('stg_trips') }}
  union
  select dropoff_zone as zone, dropoff_borough as borough, dropoff_service_zone as service_zone from {{ ref('stg_trips') }}
)
select
  row_number() over (order by zone) as zone_sk,
  zone,
  any_value(borough)    as borough,
  any_value(service_zone) as service_zone
from zones
where zone is not null
group by zone
order by zone_sk
