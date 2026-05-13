with source_data as (
    select * from {{ source('bronze', 'client__language') }}
)
,renamed as (
    select
        person_id,
        language_code as code,
        language_level as level,
        updated_at,
        correlation_id
    from source_data
)
select * from renamed