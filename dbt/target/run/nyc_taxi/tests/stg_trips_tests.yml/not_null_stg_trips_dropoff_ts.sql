
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dropoff_ts
from "hive"."mart"."stg_trips"
where dropoff_ts is null



  
  
      
    ) dbt_internal_test