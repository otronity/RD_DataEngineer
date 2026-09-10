{{ config(materialized='table') }}

-- Крок 2: silver.commits. Специфікація: ../../SPEC.md → «Крок 2».
-- Джерело: {{ ref('events') }}, лише PushEvent.
-- from_json(payload, PUSH_SCHEMA) → explode масиву commits → commit grain. PUSH_SCHEMA = var('push_schema').
-- Дедуп: один рядок на commit_sha, найраніший pushed_at.
-- Колонки: commit_sha, repo_name, pushed_by, branch, author_name, author_email, message,
--          is_distinct, pushed_at, is_merge_commit, message_subject, message_length
-- Пастка: `distinct` — reserved word, у DDL-схемі та доступі до поля потрібні backticks.

with push_events as (
    select * from {{ ref('events') }}
    where event_type = 'PushEvent'
),

parsed as (
    select
        event_id,
        repo_name,
        actor_login as pushed_by,
        created_at as pushed_at,
        from_json(
            payload,
            'struct<size:int, distinct_size:int, ref:string, commits:array<struct<sha:string, message:string, `distinct`:boolean, author:struct<name:string, email:string>>>>'
        ) as parsed_payload
    from push_events
),

exploded as (
    select
        event_id,
        repo_name,
        pushed_by,
        pushed_at,
        parsed_payload.ref as branch_ref,
        c.sha as commit_sha,
        c.message as message,
        c.`distinct` as is_distinct,
        c.author.name as author_name,
        c.author.email as author_email
    from parsed
    lateral view explode(parsed_payload.commits) t as c
),

cleaned as (
    select
        commit_sha,
        repo_name,
        pushed_by,
        regexp_replace(branch_ref, '^refs/heads/', '') as branch,
        author_name,
        author_email,
        message,
        is_distinct,
        pushed_at,
        case when message rlike '^Merge ' then true else false end as is_merge_commit,
        split(message, '\n')[0] as message_subject,
        length(message) as message_length,
        row_number() over (partition by commit_sha order by pushed_at asc, event_id asc) as rn
    from exploded
    where commit_sha is not null
)

select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    is_merge_commit,
    message_subject,
    message_length
from cleaned
where rn = 1