with source as (
    select * from
    {{ source('bronze', 'store__site_contact') }}
)

,renamed as (
    select
        site_unique_code as site_unique_code,
        contact_type as type,
        contact_value as value,
        contact_role as role,
        updated_at as updated_at
    from source
    )
select * from renamed