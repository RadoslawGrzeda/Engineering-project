with source_data as (
    select * from {{ source('bronze', 'client__dict_country') }}
)
,renamed as (
    select 
        country_code as code,
        country_name as name,
        number_of_neighbors,
        access_to_the_sea,
        population,
        updated_at
    from source_data
)
select * from renamed
