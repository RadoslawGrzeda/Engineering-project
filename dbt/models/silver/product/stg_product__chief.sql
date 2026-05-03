with source as (
    select * from
    {{ source('bronze', 'product__chief') }}
)

,renamed as (
    select
        chief_id         as id,
        chief_first_name as first_name,
        chief_last_name  as last_name,
        phone_number,
        email_address    as email,
        updated_at
    from source
    )
select * from renamed
