{{ config(materialized='table') }}

-- Крок 3: silver.pull_requests. Специфікація: ../../SPEC.md → «Крок 3».
-- Джерело: {{ ref('events') }}, лише PullRequestEvent. from_json(payload, PR_SCHEMA), PR_SCHEMA = var('pr_schema').
-- Грануляція: один рядок на (repo_name, pr_number) — стан з ОСТАННЬОЇ за часом події (row_number desc).
-- Колонки: repo_name, pr_number, title, author_login, state, is_merged, is_draft, opened_at,
--          closed_at, merged_at, additions, deletions, changed_files, commits_count, comments,
--          review_comments, author_association, label_names, last_action, last_event_at, churn, hours_open

with pr_events as (
    select * from {{ ref('events') }}
    where event_type = 'PullRequestEvent'
),

parsed as (
    select
        event_id,
        repo_name,
        created_at as event_at,
        from_json(
            payload,
            'struct<action:string, number:int, pull_request:struct<state:string, title:string, draft:boolean, merged:boolean, created_at:string, closed_at:string, merged_at:string, additions:int, deletions:int, changed_files:int, commits:int, comments:int, review_comments:int, author_association:string, user:struct<login:string>, labels:array<struct<name:string>>>>'
        ) as pr
    from pr_events
),

ranked as (
    select
        repo_name,
        pr.number as pr_number,
        pr.pull_request.title as title,
        pr.pull_request.user.login as author_login,
        pr.pull_request.state as state,
        coalesce(pr.pull_request.merged, false) as is_merged,
        coalesce(pr.pull_request.draft, false) as is_draft,
        to_timestamp(pr.pull_request.created_at) as opened_at,
        to_timestamp(pr.pull_request.closed_at) as closed_at,
        to_timestamp(pr.pull_request.merged_at) as merged_at,
        coalesce(pr.pull_request.additions, 0) as additions,
        coalesce(pr.pull_request.deletions, 0) as deletions,
        coalesce(pr.pull_request.changed_files, 0) as changed_files,
        coalesce(pr.pull_request.commits, 0) as commits_count,
        coalesce(pr.pull_request.comments, 0) as comments,
        coalesce(pr.pull_request.review_comments, 0) as review_comments,
        pr.pull_request.author_association as author_association,
        transform(pr.pull_request.labels, x -> x.name) as label_names,
        pr.action as last_action,
        event_at as last_event_at,
        row_number() over (
            partition by repo_name, pr.number 
            order by event_at desc, event_id desc
        ) as rn
    from parsed
    where pr.number is not null
)

select
    repo_name,
    pr_number,
    title,
    author_login,
    state,
    is_merged,
    is_draft,
    opened_at,
    closed_at,
    merged_at,
    additions,
    deletions,
    changed_files,
    commits_count,
    comments,
    review_comments,
    author_association,
    label_names,
    last_action,
    last_event_at,
    (additions + deletions) as churn,
    cast((unix_timestamp(coalesce(closed_at, last_event_at)) - unix_timestamp(opened_at)) / 3600.0 as double) as hours_open
from ranked
where rn = 1