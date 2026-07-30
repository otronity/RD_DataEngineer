
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_count
from "warehouse"."main"."repo_top_events"
where event_count is null



  
  
      
    ) dbt_internal_test