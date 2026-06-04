-- Staging: clean column types and names from the raw silver parquet.


with src as (
  select
    cast(vendor_id as integer)                            as vendor_id,
    cast(pickup_ts as timestamp)                          as pickup_ts,
    cast(dropoff_ts as timestamp)                         as dropoff_ts,
    cast(passenger_count as integer)                      as passenger_count,
    cast(trip_distance as double)                        as trip_distance,
    cast(rate_code_id as integer)                         as rate_code_id,
    cast(pickup_location_id as integer)                   as pickup_location_id,
    cast(dropoff_location_id as integer)                  as dropoff_location_id,
    cast(payment_type as integer)                         as payment_type,
    cast(fare_amount as double)                           as fare_amount,
    cast(extra as double)                                 as extra,
    cast(mta_tax as double)                               as mta_tax,
    cast(tip_amount as double)                            as tip_amount,
    cast(tolls_amount as double)                          as tolls_amount,
    cast(improvement_surcharge as double)                 as improvement_surcharge,
    cast(total_amount as double)                          as total_amount,
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,
    cast(pickup_year as integer)                          as pickup_year,
    cast(pickup_month as integer)                         as pickup_month
  from hive.nyc.trips
)
select * from src