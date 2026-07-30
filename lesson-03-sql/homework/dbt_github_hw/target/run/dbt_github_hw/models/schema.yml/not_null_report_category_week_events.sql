
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select events
from "warehouse"."main"."report_category_week"
where events is null



  
  
      
    ) dbt_internal_test