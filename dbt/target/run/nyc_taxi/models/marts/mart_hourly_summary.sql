
  
    

    create table "hive"."mart"."mart_hourly_summary"
      
      
    as (
      -- Mart: hourly trip summary.


select
  pickup_date,
  pickup_hour,
  pickup_borough,
  count(*)                                                    as trip_count,
  avg(fare_amount)                                            as avg_fare,
  avg(total_amount)                                           as avg_total,
  avg(trip_distance)                                          as avg_distance,
  sum(total_amount)                                           as gross_revenue
from "hive"."mart"."fact_trips"
group by 1, 2, 3
    );

  