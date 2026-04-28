with source as (
    select * from
    {{source('bronze', 'product__contractor')}}
)

,renamed as (
    select
        contractor_id as id,
        contractor_name as name,
        contractor_phone_number as phone_number,
        contractor_email_address as email,
        contractor_address as address
    from source
    )
select * from renamed