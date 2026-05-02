with source_data as (
    select * from {{ source('bronze', 'client__country_language') }}
)
,renamed as (
    select 
        country_code as code,
        language_code as language_code
    from source_data
)
select * from renamed
