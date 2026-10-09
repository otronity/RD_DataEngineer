-- gold.dim_zone — ЕТАП 2. Grain: зона. SPEC.md, розділ 4.4.
--   * table; джерело — ref('seed_taxi_zone'); порожні значення -> 'Unknown'; плюс член zone_key = -1

{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold']
    )
}}

with source_data as (
    select
        coalesce(location_id, -1) as zone_key,
        coalesce(nullif(zone_name, ''), 'Unknown') as zone_name,
        coalesce(nullif(borough, ''), 'Unknown') as borough
    from {{ ref('seed_taxi_zone') }}
),

-- Додаємо спеціальний рядок zone_key = -1 для невідомих/скасованих зон
unknown_zone as (
    select
        -1 as zone_key,
        'Unknown' as zone_name,
        'Unknown' as borough
)

select distinct
    zone_key,
    zone_name,
    borough
from source_data
where zone_key != -1

union all

select
    zone_key,
    zone_name,
    borough
from unknown_zone