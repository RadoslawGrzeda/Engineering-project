with pos as (
    select
        id as pos_information_id,
        art_key,
        ean,
        vat_rate,
        price_net,
        price_gross,
        valid_from,
        valid_to,
        is_current
    from {{ ref('stg_product__pos_information') }}
)
select
    pos_information_id,
    art_key,
    ean,
    price_net,
    price_gross,
    vat_rate,
    is_current,
    valid_from,
    valid_to,
    now() as created_at,
    now() as updated_at
from pos
