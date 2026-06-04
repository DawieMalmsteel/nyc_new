
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Singular test: payment_type must be in 1..6
select payment_type
from "hive"."mart"."stg_trips"
where payment_type is not null
  and (payment_type < 1 or payment_type > 6)
  
  
      
    ) dbt_internal_test