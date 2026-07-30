
    
    

select
    repo_name as unique_field,
    count(*) as n_records

from "warehouse"."main"."starred_repos_without_push"
where repo_name is not null
group by repo_name
having count(*) > 1


