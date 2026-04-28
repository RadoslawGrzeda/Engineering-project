with source_data as (
    select
         *
    from
        {{ source('bronze', 'client__communication_subscription') }}
),
renamed as (
    select 
        person_id,
        communication_code,
        CASE WHEN value = 'AIV_01' THEN 1 WHEN value='AIV_02' THEN 0 ELSE 3 END as option_channel,
        date_of_subscription as subscription_date,
        date_of_unsubscription as unsubscription_date,
        reason_of_unsubscription as unsubscription_reason,
        last_ingested_at as updated_at
    from source_data
)
select * from renamed