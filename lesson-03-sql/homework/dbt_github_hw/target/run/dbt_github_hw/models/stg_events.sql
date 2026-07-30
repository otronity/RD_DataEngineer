
  
  create view "warehouse"."main"."stg_events__dbt_tmp" as (
    
-- =====================================================================
-- TASK 1 — stg_events (12 балів). Специфікація: ../../MODELS.md → «stg_events».
-- Прочитати партиційований Parquet і застосувати DQ-фільтри (типи, боти, порожні push).
-- Нижче — лише контракт колонок (заглушка повертає 0 рядків). Замініть тіло запиту.
-- =====================================================================

with raw_events as (
    select *
    from read_parquet('../../data/events/**/*.parquet', hive_partitioning = true)
)

select
    id,
    event_type,
    created_at,
    event_date,
    actor_login,
    repo_name,
    payload_commit_count,
    payload_action,
    payload_ref
from raw_events
where
    event_type in (
        'PushEvent',
        'IssuesEvent',
        'PullRequestEvent',
        'WatchEvent',
        'IssueCommentEvent'
    )
    and actor_login not like '%[bot]'
    and not (event_type = 'PushEvent' and payload_commit_count = 0)
  );
