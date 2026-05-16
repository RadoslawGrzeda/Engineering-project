{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id, address_type, valid_from)',
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
    round(latitude, 4) as latitude,
    round(longitude, 4) as longitude,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('gold_client_dim_address') }} FINAL
