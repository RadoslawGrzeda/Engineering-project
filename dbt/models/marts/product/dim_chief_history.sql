{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(chief_id, valid_from)',
) }}

select
    chief_id,
    first_name,
    last_name,
    phone_number,
    email,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('dim_chief') }} FINAL

