{{ config(materialized='table') }}


with combined_activity as (
    select
        md5(lower(trim(repo_name))) as repo_id,
        cast(date_format(pushed_at, 'yyyyMMdd') as int) as date_id,
        count(*) as commits,
        count(distinct author_email) as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('commits') }}
    where repo_name is not null and repo_name != '' and pushed_at is not null
    group by 1, 2

    union all

    select
        md5(lower(trim(repo_name))) as repo_id,
        cast(date_format(opened_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        count(*) as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('pull_requests') }}
    where repo_name is not null and repo_name != '' and opened_at is not null
    group by 1, 2

    union all

    select
        md5(lower(trim(repo_name))) as repo_id,
        cast(date_format(merged_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        count(*) as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('pull_requests') }}
    where repo_name is not null and repo_name != '' and merged_at is not null
    group by 1, 2

    union all

    select
        md5(lower(trim(repo_name))) as repo_id,
        cast(date_format(opened_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        count(*) as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('issues') }}
    where repo_name is not null and repo_name != '' and opened_at is not null
    group by 1, 2

    union all

    select
        md5(lower(trim(repo_name))) as repo_id,
        cast(date_format(closed_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        count(*) as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('issues') }}
    where repo_name is not null and repo_name != '' and closed_at is not null
    group by 1, 2

    union all

    select
        md5(lower(trim(repo_name))) as repo_id,
        cast(date_format(created_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        count(case when event_type = 'WatchEvent' then 1 end) as stars,
        count(case when event_type = 'ForkEvent' then 1 end) as forks
    from {{ ref('events') }}
    where repo_name is not null and repo_name != '' and created_at is not null and event_type in ('WatchEvent', 'ForkEvent')
    group by 1, 2
)

select
    md5(concat_ws('|', c.repo_id, cast(c.date_id as string))) as activity_id,
    c.repo_id,
    c.date_id,
    sum(c.commits) as commits,
    max(c.distinct_committers) as distinct_committers,
    sum(c.prs_opened) as prs_opened,
    sum(c.prs_merged) as prs_merged,
    sum(c.issues_opened) as issues_opened,
    sum(c.issues_closed) as issues_closed,
    sum(c.stars) as stars,
    sum(c.forks) as forks
from combined_activity c
where c.repo_id is not null
  AND c.repo_id != ''  
  and c.date_id is not null
  and c.date_id >= 20180313
group by c.repo_id, c.date_id
