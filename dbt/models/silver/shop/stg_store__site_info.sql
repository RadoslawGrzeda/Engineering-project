with source as (
    select * from
    {{ source('bronze', 'store__site_info') }}
)

,renamed as (
    select
        site_unique_code as site_unique_code,
        site_status_code as  status_code,
        site_opening_date as opening_date,
        site_closing_date as closing_date,
        is_current as is_current,
        updated_at as updated_at
    from source
    )
select * from renamed