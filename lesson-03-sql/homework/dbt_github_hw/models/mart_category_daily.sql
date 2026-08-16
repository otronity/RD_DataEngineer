-- =====================================================================
-- TASK 6 — mart_category_daily (20 балів). Специфікація: ../../MODELS.md → «mart_category_daily».
-- Широка вітрина: multi-join stg_events + event_categories + calendar, агрегація по (день × категорія).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================


select
    e.event_date,
    cal.is_weekend,
    c.category,
    count(*) as events,
    count(distinct e.repo_name) as distinct_repos,
    count(distinct e.actor_login) as distinct_actors
from {{ ref('stg_events') }} e
join {{ ref('event_categories') }} c
    on e.event_type = c.event_type
join {{ ref('calendar') }} cal
    on e.event_date = cal.day
group by
    e.event_date,
    cal.is_weekend,
    c.category
order by
    e.event_date,
    c.category