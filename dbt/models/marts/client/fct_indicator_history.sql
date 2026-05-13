{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id, indicator_type, valid_from)',
) }}

select
    person_id,
    indicator_type,
    description,
    rules,
    is_active,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('gold_client_fct_indicator') }} FINAL
