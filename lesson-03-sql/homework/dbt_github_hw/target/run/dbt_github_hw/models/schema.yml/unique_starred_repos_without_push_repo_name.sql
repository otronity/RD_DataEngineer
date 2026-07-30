
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    repo_name as unique_field,
    count(*) as n_records

from "warehouse"."main"."starred_repos_without_push"
where repo_name is not null
group by repo_name
having count(*) > 1



  
  
      
    ) dbt_internal_test