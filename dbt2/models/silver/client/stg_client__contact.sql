with source_data as (
    select *
    from {{ source('bronze', 'client__contact') }}
),
renamed as (
    select 
        person_id,
        contact_type,
        value as contact_value,
        flag_main_type as main_type,
        preferred_channel,
        option_channel = 1 as option_channel,
        flag_valid as valid,
        last_ingested_at as updated_at
    from source_data
)
select * from renamed