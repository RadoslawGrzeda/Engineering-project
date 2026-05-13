{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id)',
) }}

select
    person_id,
    username,
    email,
    is_active,
    last_login_at,
    portal_user_confirmation_at
from {{ ref('gold_client_dim_digital_access') }} FINAL
where is_current = 1
