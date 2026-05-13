{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(identifier_id)',
) }}

select
    identifier_id,
    person_id,
    status_code,
    status_name,
    status_rules
from {{ ref('gold_client_fct_loyalty') }} FINAL
where is_current = 1
