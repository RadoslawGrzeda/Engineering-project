with subscription as (
    select
        person_id,
        communication_code,
        status,
        subscription_date,
        unsubscription_date,
        unsubscription_reason,
        updated_at,
        correlation_id
    from {{ ref('stg_client__communication_subscription') }}
),
dict_subscription as (
    select
        code,
        name as communication_name
    from {{ ref('stg_client__dict_subscription') }}
)
select
    s.person_id,
    s.communication_code,
    ds.communication_name,
    s.status,
    s.subscription_date,
    s.unsubscription_date,
    s.unsubscription_reason,
    now() as created_at,
    s.updated_at,
    s.correlation_id
from subscription s
left join dict_subscription ds on s.communication_code = ds.code
