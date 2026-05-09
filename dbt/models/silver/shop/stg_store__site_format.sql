with source as (
    select * from
    {{ source('bronze', 'store__site_format') }}
)

,renamed as (
    select
        site_unique_code as site_unique_code,
        site_format_unique_code as format_code,
        updated_at as updated_at
    from source
    )
select * from renamed