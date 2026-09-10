{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

-- Крок 1: silver.events. Специфікація: ../../SPEC.md → «Крок 1».
-- Джерело: {{ source('bronze', 'raw_events') }}. payload несемо далі сирим рядком — from_json у кроках 2–4.
-- Колонки: event_id, event_type, actor_login, repo_name, repo_owner, created_at,
--          payload, _ingested_at, _source_file
-- Фільтри: 6 типів подій; public = true (NULL відкинути); event_id/repo_name/created_at не null; дедуп по event_id.
-- incremental (append): у is_incremental()-гілці брати лише рядки з _ingested_at > max(_ingested_at) у {{ this }}.

with raw_data as (
    select * from {{ source('bronze', 'raw_events') }}
    {% if is_incremental() %}
    where _ingested_at > (select coalesce(max(_ingested_at), '1970-01-01 00:00:00') from {{ this }})
    {% endif %}
),

cleaned as (
    select
        event_id,
        event_type,
        actor_login,
        repo_name,
        repo_owner,
        created_at,
        payload,
        _ingested_at,
        _source_file
    from (
        select
            cast(id as string) as event_id,
            type as event_type,
            actor['login'] as actor_login,
            repo['name'] as repo_name,
            split(repo['name'], '/')[0] as repo_owner,
            to_timestamp(created_at) as created_at,
            payload,
            _ingested_at,
            _source_file,
            row_number() over (partition by id order by _ingested_at desc) as rn
        from raw_data
        where type in ('PushEvent', 'PullRequestEvent', 'IssuesEvent', 'IssueCommentEvent', 'WatchEvent', 'ForkEvent')
          and public = true
          and id is not null
          and repo['name'] is not null
          and created_at is not null
    )
    where rn = 1
)

select * from cleaned