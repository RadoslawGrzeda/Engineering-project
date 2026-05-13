{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id, contact_type)',
) }}

select
    person_id,
    contact_type,
    contact_value,
    flag_main_type,
    preferred_channel,
    option_channel,
    flag_valid
from {{ ref('gold_client_dim_contact') }} FINAL
where is_current = 1
