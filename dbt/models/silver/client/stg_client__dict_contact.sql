with source_data as (
    select * from {{ source('bronze', 'client__dict_contact') }}
)
,renamed as (
    select 
        contact_type as code,
        contact_name as name,
        contact_description as description,
        updated_at
    from source_data
)
select * from renamed
