with source_data as (
    select * from {{source ('bronze','client__address')}}
)
,renamed as (
    select 
        person_id,
        address_type,
        option_channel = 1 as option_channel,
        address_street as street,
        address_zip_code as zip_code,
        address_city as city,
        country_code as country,
        nullif(geo_coordinates_x_value, 0) as latitude,
        nullif(geo_coordinates_y_value, 0) as longitude,
        updated_at,
        correlation_id
    from source_data
)
select * from renamed
