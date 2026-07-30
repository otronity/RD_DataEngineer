
  
  create view "warehouse"."main"."daily_activity__dbt_tmp" as (
    -- =====================================================================
-- TASK 3 — daily_activity (12 балів). Специфікація: ../../MODELS.md → «daily_activity».
-- Кількість подій по днях + накопичувальний підсумок: SUM(...) OVER (ORDER BY ...).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================


with daily_summary as (
    select
        event_date,
        count(*) as events
    from "warehouse"."main"."stg_events"
    group by event_date
)
select
    event_date,
    events,
    sum(events) over (
        order by event_date 
        rows between unbounded preceding and current row
    ) as running_events
from daily_summary
order by event_date
  );
