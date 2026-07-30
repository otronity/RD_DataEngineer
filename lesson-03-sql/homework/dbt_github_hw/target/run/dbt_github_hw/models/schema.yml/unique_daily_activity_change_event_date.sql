
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    event_date as unique_field,
    count(*) as n_records

from "warehouse"."main"."daily_activity_change"
where event_date is not null
group by event_date
having count(*) > 1



  
  
      
    ) dbt_internal_test