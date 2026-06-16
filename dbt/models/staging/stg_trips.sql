-- Staging: clean column types and names from the raw silver parquet.
{{ config(materialized='view') }}

with src as (
  select
    cast(trip_id as bigint)                             as trip_id,
    cast(source_file as varchar)                            as source_file,
    cast(vendor_id as integer)                          as vendor_id,
    cast(pickup_ts as timestamp)                        as pickup_ts,
    cast(dropoff_ts as timestamp)                       as dropoff_ts,
    cast(passenger_count as integer)                    as passenger_count,
    cast(trip_distance as double)                       as trip_distance,
    cast(rate_code_id as integer)                       as rate_code_id,
    cast(pickup_location_id as integer)                 as pickup_location_id,
    cast(dropoff_location_id as integer)                as dropoff_location_id,
    cast(payment_type as integer)                       as payment_type,
    cast(fare_amount as double)                         as fare_amount,
    cast(extra as double)                               as extra,
    cast(mta_tax as double)                             as mta_tax,
    cast(tip_amount as double)                          as tip_amount,
    cast(tolls_amount as double)                        as tolls_amount,
    cast(improvement_surcharge as double)               as improvement_surcharge,
    cast(total_amount as double)                        as total_amount,
    nullif(nullif(nullif(pickup_borough, 'Unknown'), 'N/A'), 'NV') as pickup_borough,
    nullif(nullif(pickup_zone, 'N/A'), 'NV')             as pickup_zone,
    nullif(nullif(pickup_service_zone, 'N/A'), 'NV')     as pickup_service_zone,
    nullif(nullif(nullif(dropoff_borough, 'Unknown'), 'N/A'), 'NV') as dropoff_borough,
    nullif(nullif(dropoff_zone, 'N/A'), 'NV')            as dropoff_zone,
    nullif(nullif(dropoff_service_zone, 'N/A'), 'NV')    as dropoff_service_zone,
    cast(pickup_year as integer)                        as pickup_year,
    cast(pickup_month as integer)                       as pickup_month
  from hive.nyc.trips
  where pickup_year >= 2023  -- safety net: drop corrupted rows (2002, 2008, 2009)
)
select * from src