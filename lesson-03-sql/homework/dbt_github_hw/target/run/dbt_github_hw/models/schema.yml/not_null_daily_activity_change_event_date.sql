
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_date
from "warehouse"."main"."daily_activity_change"
where event_date is null



  
  
      
    ) dbt_internal_test