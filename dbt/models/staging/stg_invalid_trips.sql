-- Staging: clean invalid records with their error reasons.
{{ config(materialized='view') }}

with src as (
  select
    cast(vendor_id as integer)                            as vendor_id,
    cast(pickup_ts as timestamp)                          as pickup_ts,
    cast(dropoff_ts as timestamp)                         as dropoff_ts,
    cast(passenger_count as integer)                      as passenger_count,
    cast(trip_distance as double)                        as trip_distance,
    cast(fare_amount as double)                           as fare_amount,
    cast(total_amount as double)                          as total_amount,
    pickup_borough,
    pickup_zone,
    validation_errors,
    cast(quarantine_ts as timestamp)                      as quarantine_ts,
    cast(pickup_year as integer)                          as pickup_year,
    cast(pickup_month as integer)                         as pickup_month
  from hive.nyc.invalid_trips
)
select * from src
