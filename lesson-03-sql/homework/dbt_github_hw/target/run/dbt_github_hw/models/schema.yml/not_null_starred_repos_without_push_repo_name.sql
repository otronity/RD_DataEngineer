
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select repo_name
from "warehouse"."main"."starred_repos_without_push"
where repo_name is null



  
  
      
    ) dbt_internal_test