with source as (
    select * from
    {{source('bronze', 'product__pos_information')}}
)

,renamed as (
    select
        pos_information_id as id,
        art_key as art_key,
        ean as ean,
        vat_rate as vat_rate,
        price_net as price_net,
        price_gross as price_gross,
        valid_from as valid_from,
        valid_to as valid_to,
        case when is_current = 1 then 1 else 0 end as is_current
    from source
    )
select * from renamed