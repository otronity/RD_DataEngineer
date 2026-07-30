-- =====================================================================
-- TASK 2 — repo_top_events (12 балів). Специфікація: ../../MODELS.md → «repo_top_events».
-- TOP-5 репозиторіїв за кількістю подій у кожному event_type: ROW_NUMBER() + QUALIFY.
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================

with grouped_events as (
    select
        event_type,
        repo_name,
        count(*) as event_count
    from "warehouse"."main"."stg_events"
    group by event_type, repo_name
)
select
    event_type,
    repo_name,
    event_count,
    row_number() over (
        partition by event_type 
        order by event_count desc, repo_name asc
    ) as type_rank
from grouped_events
qualify type_rank <= 5