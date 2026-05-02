with source_data as (
    select * from {{ source('bronze', 'client__dict_country') }}
)
,renamed as (
    select 
        country_code as code,
        country_name as name
    from source_data
)
select * from renamed
