
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select validation_error
from "hive"."mart"."fact_invalid_trips"
where validation_error is null



  
  
      
    ) dbt_internal_test