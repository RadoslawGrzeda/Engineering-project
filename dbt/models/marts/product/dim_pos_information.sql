{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(art_key, ean)',
) }}

select
    pos_information_id,
    art_key,
    ean,
    vat_rate,
    price_net,
    price_gross,
    src_valid_from
from {{ ref('dim_pos_information') }} FINAL
where is_current = 1
