{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id, contact_type, valid_from)',
) }}

select
    person_id,
    contact_type,
    contact_value,
    flag_main_type,
    preferred_channel,
    option_channel,
    flag_valid,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('gold_client_dim_contact') }} FINAL
