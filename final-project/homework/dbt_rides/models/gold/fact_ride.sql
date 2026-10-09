-- gold.fact_ride — ЕТАП 2. Grain: поїздка (accumulating snapshot). SPEC.md, розділ 4.4.
--   * incremental, unique_key='ride_id', delete+insert; беріть з ref('rides') те, що змінилося
--   * FK до вимірів через LEFT JOIN + COALESCE (-1 / 'unknown'): рядок не губиться
--   * requested_date = utc_date(requested_at); requested_hour = початок години за UTC

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='delete+insert',
    tags=['gold']
) }}

with source_rides as (
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
        tip_amount,
        total_amount,
        total_amount as payment_amount,
        updated_at,
        _ingested_at
    from {{ ref('rides') }}
)

select
    r.ride_id,
    coalesce(d.driver_key, 'unknown') as driver_key,
    coalesce(pz.zone_key, -1) as pickup_zone_key,
    coalesce(dz.zone_key, -1) as dropoff_zone_key,
    r.rider_id,
    r.status,
    r.fare_amount,
    r.tip_amount,
    r.total_amount,
    r.payment_amount,
    r.paid_at,          
    r.requested_at,
    r.accepted_at,
    r.started_at,
    r.completed_at,
    r.cancelled_at,
    (r.requested_at at time zone 'UTC')::date as requested_date,
    date_trunc('hour', r.requested_at at time zone 'UTC') at time zone 'UTC' as requested_hour,
    -- Розрахунок часу очікування, якщо є accepted_at
    case 
        when r.accepted_at is not null and r.requested_at is not null 
        then extract(epoch from (r.accepted_at - r.requested_at))
        else null 
    end as wait_seconds,
    r.updated_at as created_at,
    r._ingested_at
from source_rides r
left join {{ ref('dim_driver') }} d 
    on cast(r.driver_id as varchar) = d.driver_key
left join {{ ref('dim_zone') }} pz 
    on coalesce(r.pickup_zone_id, -1) = pz.zone_key
left join {{ ref('dim_zone') }} dz 
    on coalesce(r.dropoff_zone_id, -1) = dz.zone_key

{% if is_incremental() %}
where r._ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}

