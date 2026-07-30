
    
    

select
    id as unique_field,
    count(*) as n_records

from "warehouse"."main"."stg_events"
where id is not null
group by id
having count(*) > 1


