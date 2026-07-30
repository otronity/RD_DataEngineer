
    
    

select
    event_date as unique_field,
    count(*) as n_records

from "warehouse"."main"."daily_activity"
where event_date is not null
group by event_date
having count(*) > 1


