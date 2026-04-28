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
        valid_from as valid_from,
        valid_to as valid_to,
        case when is_primary = 1 then 1 else 0 end as is_primary
    from source
    )
select * from renamed