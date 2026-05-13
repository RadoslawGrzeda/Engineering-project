{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(person_id, contact_type, dbt_valid_from)',
) }}

with contact as (
    select
        person_id,
        contact_type,
        argMax(contact_value,       updated_at) as contact_value,
        argMax(main_type,           updated_at) as flag_main_type,
        argMax(preferred_channel,   updated_at) as preferred_channel,
        argMax(option_channel,      updated_at) as option_channel,
        argMax(valid,               updated_at) as flag_valid
    from {{ ref('stg_client__contact') }}
    group by person_id, contact_type
),
source as (
    select
        c.person_id,
        c.contact_type,
        c.contact_value,
        c.flag_main_type,
        c.preferred_channel,
        c.option_channel,
        c.flag_valid,
        MD5(concat(
            coalesce(toString(c.contact_type),         ''), '|',
            coalesce(toString(c.contact_value),        ''), '|',
            coalesce(toString(c.flag_main_type),       ''), '|',
            coalesce(toString(c.preferred_channel),    ''), '|',
            coalesce(toString(c.option_channel),       ''), '|',
            coalesce(toString(c.flag_valid),           '')
        )) as _row_hash
    from contact c
)

{% if is_incremental() %}

, current_in_target as (
    select
        person_id,
        contact_type,
        argMax(contact_value,       dbt_valid_from) as contact_value,
        argMax(_row_hash,           dbt_valid_from) as _row_hash,
        argMax(flag_main_type,      dbt_valid_from) as flag_main_type,
        argMax(preferred_channel,   dbt_valid_from) as preferred_channel,
        argMax(option_channel,      dbt_valid_from) as option_channel,
        argMax(flag_valid,          dbt_valid_from) as flag_valid,
        max(dbt_valid_from)                         as current_dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('2106-02-07 06:28:15')
    group by person_id, contact_type
),

changed as (
    select s.person_id, s.contact_type
    from source s
    inner join current_in_target t
        on  s.person_id     = t.person_id
        and s.contact_type  = t.contact_type
    where s._row_hash != t._row_hash
),

new_entries as (
    select s.person_id, s.contact_type
    from source s
    left join current_in_target t
        on  s.person_id     = t.person_id
        and s.contact_type  = t.contact_type
    where t.person_id is null
),

closed_records as (
    select
        t.person_id,
        t.contact_type,
        t.contact_value,
        t.flag_main_type,
        t.preferred_channel,
        t.option_channel,
        t.flag_valid,
        t._row_hash,
        0               as is_current,
        t.current_dbt_valid_from as dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c
        on  t.person_id     = c.person_id
        and t.contact_type  = c.contact_type
),

new_records as (
    select
        s.person_id,
        s.contact_type,
        s.contact_value,
        s.flag_main_type,
        s.preferred_channel,
        s.option_channel,
        s.flag_valid,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('2106-02-07 06:28:15')  as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where (s.person_id, s.contact_type)
        in (select person_id, contact_type from changed)
       or (s.person_id, s.contact_type)
        in (select person_id, contact_type from new_entries)
)

select * from closed_records
union all
select * from new_records

{% else %}

select
    person_id,
    contact_type,
    contact_value,
    flag_main_type,
    preferred_channel,
    option_channel,
    flag_valid,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
