{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(person_id, dbt_valid_from)',
) }}

with source_agg as (
    select
        person_id,
        argMax(username,                        updated_at) as username,
        argMax(email,                           updated_at) as email,
        argMax(is_active,                       updated_at) as is_active,
        argMax(last_login_at,                   updated_at) as last_login_at,
        argMax(portal_user_confirmation_at,     updated_at) as portal_user_confirmation_at
    from {{ ref('stg_client__digital_access') }}
    group by person_id
),
source as (
    select
        person_id,
        username,
        email,
        is_active,
        last_login_at,
        portal_user_confirmation_at,
        MD5(concat(
            coalesce(toString(username),                      ''), '|',
            coalesce(toString(email),                         ''), '|',
            coalesce(toString(is_active),                     ''), '|',
            coalesce(toString(last_login_at),                 ''), '|',
            coalesce(toString(portal_user_confirmation_at),   '')
        )) as _row_hash
    from source_agg
)

{% if is_incremental() %}

, current_in_target as (
    select
        person_id,
        argMax(_row_hash,                       dbt_valid_from) as _row_hash,
        argMax(username,                        dbt_valid_from) as username,
        argMax(email,                           dbt_valid_from) as email,
        argMax(is_active,                       dbt_valid_from) as is_active,
        argMax(last_login_at,                   dbt_valid_from) as last_login_at,
        argMax(portal_user_confirmation_at,     dbt_valid_from) as portal_user_confirmation_at,
        max(dbt_valid_from)                                     as current_dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('2106-02-07 06:28:15')
    group by person_id
),

changed as (
    select s.person_id
    from source s
    inner join current_in_target t on s.person_id = t.person_id
    where s._row_hash != t._row_hash
),

new_entries as (
    select s.person_id
    from source s
    left join current_in_target t on s.person_id = t.person_id
    where t.person_id is null
),

closed_records as (
    select
        t.person_id,
        t.username,
        t.email,
        t.is_active,
        t.last_login_at,
        t.portal_user_confirmation_at,
        t._row_hash,
        0               as is_current,
        t.current_dbt_valid_from as dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c on t.person_id = c.person_id
),

new_records as (
    select
        s.person_id,
        s.username,
        s.email,
        s.is_active,
        s.last_login_at,
        s.portal_user_confirmation_at,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where s.person_id in (select person_id from changed)
       or s.person_id in (select person_id from new_entries)
)

select * from closed_records
union all
select * from new_records

{% else %}

select
    person_id,
    username,
    email,
    is_active,
    last_login_at,
    portal_user_confirmation_at,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
