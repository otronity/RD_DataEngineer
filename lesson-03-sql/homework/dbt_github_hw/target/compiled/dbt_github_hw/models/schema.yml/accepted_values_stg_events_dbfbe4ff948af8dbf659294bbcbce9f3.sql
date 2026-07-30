
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from "warehouse"."main"."stg_events"
    group by event_type

)

select *
from all_values
where value_field not in (
    'PushEvent','IssuesEvent','PullRequestEvent','WatchEvent','IssueCommentEvent'
)


