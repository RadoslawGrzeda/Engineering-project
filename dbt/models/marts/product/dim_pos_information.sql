{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(art_key, ean)',
) }}

select
    pos_information_id,
    assumeNotNull(art_key) as art_key,
    assumeNotNull(ean)     as ean,
    vat_rate,
    price_net,
    price_gross,
    src_valid_from
from {{ ref('dim_pos_information_scd') }} FINAL
where is_current = 1
