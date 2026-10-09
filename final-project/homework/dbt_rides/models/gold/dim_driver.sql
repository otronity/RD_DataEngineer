{{
    config(
        materialized='incremental',
        unique_key='driver_key',
        incremental_strategy='delete+insert',
        tags=['gold']
    )
}}

with raw_accepted as (
    select
        -- Дістаємо поля згідно зі структурою вкладеного JSON
        payload #>> '{driver,id}' as driver_id,
        coalesce(payload #>> '{driver,vehicle,type}', 'unknown') as vehicle_type,
        payload #>> '{driver,vehicle,medallion}' as medallion,
        (payload #>> '{driver,rating}')::numeric as latest_rating,
        occurred_at,
        _ingested_at,
        _loaded_at
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
      and payload #>> '{driver,id}' is not null
),

ranked as (
    select
        driver_id as driver_key,
        driver_id,
        vehicle_type,
        medallion,
        latest_rating,
        occurred_at,
        _ingested_at,
        _loaded_at,
        row_number() over (partition by driver_id order by occurred_at desc, _ingested_at desc) as rn,
        min(occurred_at) over (partition by driver_id) as first_seen_at,
        max(occurred_at) over (partition by driver_id) as last_seen_at
    from raw_accepted
),

latest_driver_state as (
    select
        driver_key,
        driver_id,
        vehicle_type,
        medallion,
        latest_rating,
        first_seen_at,
        last_seen_at,
        _ingested_at,
        _loaded_at
    from ranked
    where rn = 1
),

unknown_driver as (
    select
        'unknown'::text as driver_key,
        null::text as driver_id,
        'unknown'::text as vehicle_type,
        cast(null as text) as medallion,
        cast(null as numeric) as latest_rating,
        cast(null as timestamp with time zone) as first_seen_at,
        cast(null as timestamp with time zone) as last_seen_at,
        cast(null as timestamp with time zone) as _ingested_at,
        current_timestamp as _loaded_at
)

select * from latest_driver_state
union all
select * from unknown_driver