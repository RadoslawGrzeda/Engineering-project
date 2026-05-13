{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(person_id, communication_code, dbt_valid_from)',
) }}

with subscription as (
    select
        person_id,
        communication_code,
        argMax(active,                  updated_at) as active,
        argMax(subscription_date,       updated_at) as subscription_date,
        argMax(unsubscription_date,     updated_at) as unsubscription_date,
        argMax(unsubscription_reason,   updated_at) as unsubscription_reason
    from {{ ref('stg_client__communication_subscription') }}
    group by person_id, communication_code
),
dict_subscription as (
    select
        code,
        name as communication_name
    from {{ ref('stg_client__dict_subscription') }}
),
source as (
    select
        s.person_id,
        s.communication_code,
        ds.communication_name,
        s.active,
        s.subscription_date,
        s.unsubscription_date,
        s.unsubscription_reason,
        MD5(concat(
            coalesce(toString(s.active),                ''), '|',
            coalesce(toString(s.subscription_date),     ''), '|',
            coalesce(toString(s.unsubscription_date),   ''), '|',
            coalesce(toString(s.unsubscription_reason), '')
        )) as _row_hash
    from subscription s
    left join dict_subscription ds on s.communication_code = ds.code
)

{% if is_incremental() %}

, current_in_target as (
    select
        person_id,
        communication_code,
        argMax(_row_hash,               dbt_valid_from) as _row_hash,
        argMax(communication_name,      dbt_valid_from) as communication_name,
        argMax(active,                  dbt_valid_from) as active,
        argMax(subscription_date,       dbt_valid_from) as subscription_date,
        argMax(unsubscription_date,     dbt_valid_from) as unsubscription_date,
        argMax(unsubscription_reason,   dbt_valid_from) as unsubscription_reason,
        max(dbt_valid_from)                             as current_dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('2106-02-07 06:28:15')
    group by person_id, communication_code
),

changed as (
    select s.person_id, s.communication_code
    from source s
    inner join current_in_target t
        on s.person_id = t.person_id and s.communication_code = t.communication_code
    where s._row_hash != t._row_hash
),

new_entries as (
    select s.person_id, s.communication_code
    from source s
    where (s.person_id, s.communication_code) not in (select person_id, communication_code from current_in_target)
),

closed_records as (
    select
        t.person_id,
        t.communication_code,
        t.communication_name,
        t.active,
        t.subscription_date,
        t.unsubscription_date,
        t.unsubscription_reason,
        t._row_hash,
        0               as is_current,
        t.current_dbt_valid_from as dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c
        on t.person_id = c.person_id and t.communication_code = c.communication_code
),

new_records as (
    select
        s.person_id,
        s.communication_code,
        s.communication_name,
        s.active,
        s.subscription_date,
        s.unsubscription_date,
        s.unsubscription_reason,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where (s.person_id, s.communication_code) in (select person_id, communication_code from changed)
       or (s.person_id, s.communication_code) in (select person_id, communication_code from new_entries)
)

select * from closed_records
union all
select * from new_records

{% else %}

select
    person_id,
    communication_code,
    communication_name,
    active,
    subscription_date,
    unsubscription_date,
    unsubscription_reason,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
