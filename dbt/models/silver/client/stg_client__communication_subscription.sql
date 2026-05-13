with source_data as (
    select * from {{ source('bronze', 'client__communication_subscription') }}
),
renamed as (
    select 
        person_id,
        communication_code,
        case when value = 'AIV_01' then 1 else 0 end as active,
        date_of_subscription as subscription_date,
        date_of_unsubscription as unsubscription_date,
        reason_of_unsubscription as unsubscription_reason,
        updated_at,
        correlation_id as correlation_id
    from source_data
)
select * from renamed