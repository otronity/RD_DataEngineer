{{ config(materialized='table') }}

-- Крок 7: gold.dim_date. Специфікація: ../../SPEC.md → «Крок 7».
-- Згенерований безперервний календар (БЕЗ seed): explode(sequence(min, max, interval 1 day)).
-- Межі min/max — підзапитом по фактичних датах з {{ ref('commits') }}, {{ ref('pull_requests') }},
-- {{ ref('issues') }} (pushed_at / opened_at / merged_at / closed_at). Не хардкодьте.
-- Колонки: date_id (int yyyyMMdd), date_day (date), day_of_week, is_weekend, iso_week, year.

with all_dates as (
    select created_at as dt from {{ ref('events') }}
    union all
    select pushed_at as dt from {{ ref('commits') }}
    union all
    select opened_at as dt from {{ ref('pull_requests') }}
    union all
    select closed_at as dt from {{ ref('pull_requests') }} where closed_at is not null
    union all
    select merged_at as dt from {{ ref('pull_requests') }} where merged_at is not null
    union all
    select opened_at as dt from {{ ref('issues') }}
    union all
    select closed_at as dt from {{ ref('issues') }} where closed_at is not null
),

bounds as (
    select 
        to_date(min(dt)) as min_date,
        to_date(max(dt)) as max_date
    from all_dates
),

calendar as (
    select explode(sequence(min_date, max_date, interval 1 day)) as date_day
    from bounds
)

select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_id,
    date_day,
    dayofweek(date_day) as day_of_week,
    case when dayofweek(date_day) in (1, 7) then true else false end as is_weekend,
    weekofyear(date_day) as iso_week,
    year(date_day) as year
from calendar