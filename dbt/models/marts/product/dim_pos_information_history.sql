{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(art_key, valid_from)',
) }}

select
    pos_information_id,
    art_key,
    ean,
    vat_rate,
    price_net,
    price_gross,
    src_valid_from,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('dim_pos_information_scd') }} FINAL
