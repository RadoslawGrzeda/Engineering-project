{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(identifier_id, valid_from)',
) }}

select
    identifier_id,
    person_id,
    status_code,
    status_name,
    status_rules,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('gold_client_fct_loyalty') }} FINAL
