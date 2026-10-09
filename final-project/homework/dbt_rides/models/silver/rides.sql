-- silver.rides — ЕТАП 2. Grain: одна поїздка (ride_id). SPEC.md, розділ 4.3.
--   * incremental, unique_key='ride_id', delete+insert
--   * перебудовуйте поїздки, яких торкнулися НОВІ події, з УСІЄЇ їхньої історії в ref('events')
--   * поїздка існує, коли прийшла її ride_requested; порядок прибуття подій байдужий
--   * status, фактична зона (перекриває заявлену), wait_seconds, trip_seconds, суми з ride_completed
--   * _ingested_at = максимум _ingested_at усіх подій поїздки

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='delete+insert'
) }}

with affected_rides as (
    -- 1. Знаходимо ID поїздок, яких торкнулися нові події в поточному батчі
    select distinct ride_id
    from {{ ref('events') }}
    {% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}
),

event_history as (
    -- 2. Беремо ВСЮ історію подій для цих зачеплених поїздок з ref('events')
    select
        ride_id,
        event_type,
        occurred_at,
        source,
        payload,
        _ingested_at
    from {{ ref('events') }}
    
    {% if is_incremental() %}
    where ride_id in (select ride_id from affected_rides)
    {% endif %}
),

aggregated_rides as (
    select
        ride_id,
        max(case when event_type = 'ride_accepted' then payload::jsonb -> 'driver' ->> 'id' end) as driver_id,
        max(case when event_type = 'ride_requested' then payload::jsonb -> 'rider' ->> 'id' end) as rider_id,
        
        -- Зони (фактичні події ride_started / ride_completed перекривають ride_requested)
        coalesce(
            max(case when event_type = 'ride_started' then (payload::jsonb #>> '{pickup,zone_id}')::integer end),
            max(case when event_type = 'ride_requested' then (payload::jsonb #>> '{pickup,zone_id}')::integer end)
        ) as pickup_zone_id,
        
        coalesce(
            max(case when event_type = 'ride_completed' then (payload::jsonb #>> '{dropoff,zone_id}')::integer end),
            max(case when event_type = 'ride_requested' then (payload::jsonb #>> '{dropoff,zone_id}')::integer end)
        ) as dropoff_zone_id,
        
        -- Статус
        case 
            when max(case when event_type = 'ride_cancelled' then 1 else 0 end) = 1 then 'cancelled'
            when max(case when event_type = 'ride_completed' then 1 else 0 end) = 1 then 'completed'
            when max(case when event_type = 'ride_started' then 1 else 0 end) = 1 then 'in_progress'
            when max(case when event_type = 'ride_accepted' then 1 else 0 end) = 1 then 'accepted'
            else 'requested'
        end as status,
        
        -- Часові мітки життєвого циклу
        min(case when event_type = 'ride_requested' then occurred_at end) as requested_at,
        min(case when event_type = 'ride_accepted' then occurred_at end) as accepted_at,
        min(case when event_type = 'ride_started' then occurred_at end) as started_at,
        min(case when event_type = 'ride_completed' then occurred_at end) as completed_at,
        min(case when event_type = 'ride_cancelled' then occurred_at end) as cancelled_at,
        min(case when event_type = 'payment_captured' then occurred_at end) as paid_at,
        
        -- Фінансові поля
        -- Фінансові поля (згідно зі специфікацією: fare.{amount, surge_multiplier, tolls, tip, total, currency})
        max(case when event_type = 'ride_completed' then (payload::jsonb -> 'fare' ->> 'amount')::numeric end) as fare_amount,
        max(case when event_type = 'ride_completed' then (payload::jsonb -> 'fare' ->> 'surge_multiplier')::numeric end) as surge_multiplier,
        max(case when event_type = 'ride_completed' then (payload::jsonb -> 'fare' ->> 'tolls')::numeric end) as tolls_amount,
        max(case when event_type = 'ride_completed' then (payload::jsonb -> 'fare' ->> 'tip')::numeric end) as tip_amount,
        max(case when event_type = 'ride_completed' then (payload::jsonb -> 'fare' ->> 'total')::numeric end) as total_amount,
        max(case when event_type = 'ride_completed' then payload::jsonb -> 'fare' ->> 'currency' end) as currency, -- якщо потрібно
        
        max(case when event_type = 'payment_captured' then payload::jsonb ->> 'payment_method' end) as payment_method,

        max(occurred_at) as updated_at,
        max(_ingested_at) as _ingested_at
    from event_history
    group by ride_id
)

select
    ride_id,
    driver_id,
    rider_id,
    pickup_zone_id,
    dropoff_zone_id,
    status,
    requested_at,
    accepted_at,
    started_at,
    completed_at,
    cancelled_at,
    paid_at,
    fare_amount,
    surge_multiplier,
    tolls_amount,
    tip_amount,
    total_amount,
    payment_method,
    updated_at,
    _ingested_at
from aggregated_rides
where requested_at is not null
