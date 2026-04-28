with source as (
    select * from
    {{ source('bronze', 'store__site') }}
)

,renamed as (
    select
        site_unique_code as site_unique_code,
        site_code as code,
        site_name as name,
        updated_at as updated_at
    from source
    )
select * from renamed