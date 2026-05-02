with source_data as (
    select * from {{ source('bronze', 'client__contact') }}
),
renamed as (
    select 
        person_id,
        contact_type,
        value as contact_value,
        flag_main_type = 1 as main_type,
        preferred_channel = 1 as preferred_channel,
        option_channel = 1 as option_channel,
        flag_valid = 1 as valid,
        last_ingested_at as updated_at,
        correlation_id as correlation_id
    from source_data
)
select * from renamed