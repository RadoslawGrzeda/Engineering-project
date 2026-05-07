with source as (
    select * from
    {{source('bronze', 'product__pos_information')}} FINAL
)
,renamed as (
    select
        pos_information_id as id,
        art_key,
        ean,
        vat_rate,
        price_net,
        price_gross,
        valid_from,
        updated_at
    from source
    )
    
select * from renamed