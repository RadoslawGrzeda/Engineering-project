{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id, indicator_type)',
) }}

select
    person_id,
    indicator_type,
    description,
    rules,
    is_active
from {{ ref('gold_client_fct_indicator') }} FINAL
where is_current = 1
