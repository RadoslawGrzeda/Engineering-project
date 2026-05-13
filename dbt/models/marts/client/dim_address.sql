{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id, address_type)',
    settings={"allow_nullable_key": 1},
) }}

select
    person_id,
    address_type,
    option_channel,
    street,
    zip_code,
    city,
    country,
    latitude,
    longitude
from {{ ref('gold_client_dim_address') }} FINAL
where is_current = 1
