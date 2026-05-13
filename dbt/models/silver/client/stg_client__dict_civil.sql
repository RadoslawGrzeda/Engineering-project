with source_data as (
    select * from {{ source('bronze', 'client__dict_civil') }}
)
,renamed as (
    select
        civil_status_type as type,
        civil_status_description as description,
        updated_at
    from source_data
)
select * from renamed