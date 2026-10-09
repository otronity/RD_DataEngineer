-- gold.agg_zone_hourly — ЕТАП 2. Grain: (requested_hour, pickup_zone_key). SPEC.md, розділ 4.4.
-- * incremental; вирішуємо проблему зміни зони/часу через зв'язок по ride_id.

{{
    config(
        materialized='incremental',
        unique_key=['requested_hour', 'pickup_zone_key'],
        incremental_strategy='delete+insert',
        tags=['gold'],
        pre_hook="""
            {% if is_incremental() %}
            delete from {{ this }}
            where requested_hour in (
                select distinct requested_hour 
                from {{ ref('fact_ride') }}
                where _ingested_at > (select coalesce(max(_loaded_at), '1970-01-01'::timestamptz) from {{ this }})
            );
            {% endif %}
        """
    )
}}

with source_rides as (
    select * 
    from {{ ref('fact_ride') }}
    where requested_hour is not null
      and pickup_zone_key is not null

    {% if is_incremental() %}
    -- Читаємо з fact_ride тільки зачеплені години
    and requested_hour in (
        select distinct requested_hour 
        from {{ ref('fact_ride') }}
        where _ingested_at > (select coalesce(max(_loaded_at), '1970-01-01'::timestamptz) from {{ this }})
    )
    {% endif %}
)

select
    requested_hour,
    pickup_zone_key,
    count(*) as rides_requested,
    sum(case when status = 'completed' then 1 else 0 end) as rides_completed,
    sum(case when status = 'cancelled' then 1 else 0 end) as rides_cancelled,
    coalesce(sum(case when status = 'completed' then total_amount else 0 end), 0) as gross_revenue,
    coalesce(sum(case when status = 'completed' then tip_amount else 0 end), 0) as tips,
    avg(wait_seconds) as avg_wait_seconds,
    max(_ingested_at) as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from source_rides
where requested_hour is not null
  and pickup_zone_key is not null
group by requested_hour, pickup_zone_key