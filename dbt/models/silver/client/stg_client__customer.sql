with source_data as (
    select *
    from {{ source('bronze', 'client__customer') }}
)
,renamed as (
    select 
        person_id,
        first_name,
        middle_name,
        last_name,
        birth_date,
        passport_number,
        gender_code,
        civil_status_code,
        is_deleted = 1 as is_deleted,
        registration_date,
        last_ingested_at as updated_at
    from source_data
)
select * from renamed