{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id, valid_from)',
) }}

select
    person_id,
    username,
    email,
    is_active,
    last_login_at,
    portal_user_confirmation_at,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('gold_client_dim_digital_access') }} FINAL
