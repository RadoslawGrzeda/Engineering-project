with source_data as ( 
    select * from {{ source('bronze', 'client__digital_access') }}
)
,renamed as (
    select
        person_id,
        username,
        email_user as email, 
        is_active = 1 as is_active,
        last_login_date as last_login_at,
        portal_user_confirmation_date as portal_user_confirmation_at,
        last_ingested_at as updated_at,
        correlation_id as correlation_id
    from source_data
)
select * from renamed
