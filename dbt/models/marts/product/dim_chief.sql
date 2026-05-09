{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(chief_id)',
) }}

select
    chief_id,
    first_name,
    last_name,
    phone_number,
    email
from {{ ref('dim_chief') }} FINAL
where is_current = 1
