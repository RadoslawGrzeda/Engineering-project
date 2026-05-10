with source as (
    select * from
    {{ source('bronze', 'store__site_address') }}
)

,renamed as (
    select
        site_unique_code as site_unique_code,
        site_address_zip_code as zip_code,
        site_address_city as city,
        site_address_street as street,
        city_code as city_code,
        country_code as country_code,
        site_geo_coordinate_x_value as latitude,
        site_geo_coordinate_y_value as longitude,
        updated_at as updated_at
    from source
    )
select * from renamed