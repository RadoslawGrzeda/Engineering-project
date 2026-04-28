with source as (
   select
    id,
    art_key,
    ean,
    vat_rate,
    price_net,
    price_gross,
    valid_from,
    valid_to,
    is_current
    from 
    {{ source('silver', 'stg_pos__information') }}
)
select 
    id,
    art_key,
    ean,
    vat_rate,
    price_net,
    price_gross,
    valid_from,
    valid_to,
    case when is_current = 1 then 1 else 0 end as is_current
from source