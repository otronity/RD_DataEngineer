-- =====================================================================
-- TASK 5 — starred_repos_without_push (12 балів). Специфікація: ../../MODELS.md → «starred_repos_without_push».
-- Репозиторії зі зіркою (WatchEvent), але без жодного PushEvent: anti-join (NOT EXISTS).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================

with watched_repos as (
    select distinct repo_name
    from "warehouse"."main"."stg_events"
    where event_type = 'WatchEvent'
),
pushed_repos as (
    select distinct repo_name
    from "warehouse"."main"."stg_events"
    where event_type = 'PushEvent'
)
select w.repo_name
from watched_repos w
left join pushed_repos p 
    on w.repo_name = p.repo_name
where p.repo_name is null