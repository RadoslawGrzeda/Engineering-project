{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id, communication_code)',
) }}

select
    person_id,
    communication_code,
    communication_name,
    active,
    subscription_date,
    unsubscription_date,
    unsubscription_reason
from {{ ref('gold_client_fct_subscription') }} FINAL
where is_current = 1
