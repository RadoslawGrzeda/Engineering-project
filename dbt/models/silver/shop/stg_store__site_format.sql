with source as (
    select * from
    {{ source('bronze', 'store__site_format') }}
)

,renamed as (
    select
        site_unique_code as site_unique_code,
        site_format_unique_code as format_code,
        is_current as is_current,
        valid_from as valid_from,
        valid_to as valid_to
    from source
    )
select * from renamed