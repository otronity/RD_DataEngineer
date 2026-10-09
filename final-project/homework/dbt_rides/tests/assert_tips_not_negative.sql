select *
from {{ ref('fact_ride') }}
where tip_amount < 0