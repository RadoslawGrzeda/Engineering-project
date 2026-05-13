{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id, communication_code, valid_from)',
) }}

select
    person_id,
    communication_code,
    communication_name,
    active,
    subscription_date,
    unsubscription_date,
    unsubscription_reason,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('gold_client_fct_subscription') }} FINAL
