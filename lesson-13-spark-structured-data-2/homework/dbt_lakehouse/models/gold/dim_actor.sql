{{ config(materialized='table') }}

-- Крок 6: gold.dim_actor. Специфікація: ../../SPEC.md → «Крок 6».
-- Джерело: {{ ref('events') }}, actor_login is not null. Грануляція: один рядок на актора.
-- Колонки: actor_id (md5(actor_login)), actor_login, is_bot (закінчується на [bot]),
--          first_seen_at, last_seen_at, event_count, distinct_repos.

with events as (
    select * from {{ ref('events') }}
    where actor_login is not null
)

select
    md5(actor_login) as actor_id,
    actor_login,
    case when actor_login rlike '\\[bot\\]$' then true else false end as is_bot,
    min(created_at) as first_seen_at,
    max(created_at) as last_seen_at,
    count(*) as event_count,
    count(distinct repo_name) as distinct_repos
from events
group by actor_login