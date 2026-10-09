-- silver.events — ЕТАП 2. Grain: одна подія (event_id). SPEC.md, розділ 4.2.
--   * incremental, unique_key='event_id', delete+insert; межа — high_watermark() за _ingested_at
--   * відкинути source='loadtest' і рядки без occurred_at / ride_id
--   * один рядок на event_id (найраніший _ingested_at, за рівності — _source_file)
--   * payload text -> jsonb; occurred_date = utc_date(occurred_at); _loaded_at = run_started_at

{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert'
) }}

with raw_data as (
    select
        event_id,
        event_type,
        occurred_at,
        ride_id,
        source,
        payload::jsonb as payload,
        _source_file,
        _ingested_at
    FROM {{ source('bronze', 'raw_events') }}
    
    {% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}
),

filtered_and_deduped as (
    select
        event_id,
        event_type,
        occurred_at,
        ride_id,
        source,
        payload,
        _source_file,
        _ingested_at,
        row_number() over (
            partition by event_id 
            order by _ingested_at asc, _source_file asc
        ) as rn
    from raw_data
    where source != 'loadtest'
      and occurred_at is not null
      and ride_id is not null
)

select
    event_id,
    event_type,
    occurred_at,
    (occurred_at at time zone 'UTC')::date as occurred_date,
    ride_id,
    source,
    payload,
    _source_file,
    _ingested_at,
    current_timestamp as _loaded_at
from filtered_and_deduped
where rn = 1