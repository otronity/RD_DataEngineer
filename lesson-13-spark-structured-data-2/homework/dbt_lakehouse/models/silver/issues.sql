{{ config(materialized='table') }}

-- Крок 4: silver.issues. Специфікація: ../../SPEC.md → «Крок 4».
-- Джерело: {{ ref('events') }}, типи IssuesEvent ТА IssueCommentEvent (обидва несуть issue{}).
-- from_json(payload, ISSUE_SCHEMA), ISSUE_SCHEMA = var('issue_schema').
-- Грануляція: один рядок на (repo_name, issue_number) — стан з останньої за часом події.
-- Колонки: repo_name, issue_number, title, author_login, state, opened_at, closed_at,
--          comments, label_names, comment_events_seen, last_event_at, hours_to_close

with issue_events as (
    select * from {{ ref('events') }}
    where event_type in ('IssuesEvent', 'IssueCommentEvent')
),

parsed as (
    select
        event_id,
        repo_name,
        created_at as event_at,
        event_type,
        from_json(
            payload,
            'struct<action:string, issue:struct<number:int, title:string, state:string, created_at:string, closed_at:string, comments:int, author_association:string, user:struct<login:string>, labels:array<struct<name:string>>>>'
        ) as iss
    from issue_events
),

aggregated as (
    select
        repo_name,
        iss.issue.number as issue_number,
        iss.issue.title as title,
        iss.issue.user.login as author_login,
        iss.issue.state as state,
        to_timestamp(iss.issue.created_at) as opened_at,
        to_timestamp(iss.issue.closed_at) as closed_at,
        coalesce(iss.issue.comments, 0) as comments,
        transform(iss.issue.labels, x -> x.name) as label_names,
        sum(case when event_type = 'IssueCommentEvent' then 1 else 0 end) over (partition by repo_name, iss.issue.number) as comment_events_seen,
        event_at,
        event_id,
        row_number() over (
            partition by repo_name, iss.issue.number 
            order by event_at desc, event_id desc
        ) as rn
    from parsed
    where iss.issue.number is not null
)

select
    repo_name,
    issue_number,
    title,
    author_login,
    state,
    opened_at,
    closed_at,
    comments,
    label_names,
    comment_events_seen,
    event_at as last_event_at,
    cast((unix_timestamp(closed_at) - unix_timestamp(opened_at)) / 3600.0 as double) as hours_to_close
from aggregated
where rn = 1