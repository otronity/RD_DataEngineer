-- Налаштування тегу reconcile, щоб уникнути пастки cautious selection
{{ config(tags=['reconcile']) }}

select
    ride_id,
    fare_amount,
    tip_amount,
    total_amount
from {{ ref('fact_ride') }}
where status = 'completed'
  -- Помилка, якщо total_amount чомусь менший за fare_amount (без урахування комісій/чайових)
  and total_amount < fare_amount