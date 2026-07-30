
  
  create view "warehouse"."main"."daily_activity_change__dbt_tmp" as (
    -- =====================================================================
-- TASK 4 — daily_activity_change (12 балів). Специфікація: ../../MODELS.md → «daily_activity_change».
-- Зміна кількості подій день-до-дня: LAG(...) OVER (ORDER BY ...).
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
    lag(events) over (order by event_date) as prev_day_events,
    events - lag(events) over (order by event_date) as delta_events
from daily_summary
order by event_date
  );
