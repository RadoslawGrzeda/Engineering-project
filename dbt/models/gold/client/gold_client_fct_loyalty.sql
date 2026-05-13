{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(identifier_id, dbt_valid_from)',
) }}

with loyalty as (
    select
        identifier_id,
        argMax(person_id,   updated_at) as person_id,
        argMax(status,      updated_at) as status_code
    from {{ ref('stg_client__loyalty_status') }}
    group by identifier_id
),
dict_loyalty as (
    select
        code,
        name    as status_name,
        rules   as status_rules
    from {{ ref('stg_client__dict_loyalty_status') }}
),
source as (
    select
        l.identifier_id,
        l.person_id,
        l.status_code,
        dl.status_name,
        dl.status_rules,
        MD5(concat(
            coalesce(toString(l.person_id),   ''), '|',
            coalesce(toString(l.status_code), '')
        )) as _row_hash
    from loyalty l
    left join dict_loyalty dl on l.status_code = dl.code
)

{% if is_incremental() %}

, current_in_target as (
    select
        identifier_id,
        argMax(_row_hash,       dbt_valid_from) as _row_hash,
        argMax(person_id,       dbt_valid_from) as person_id,
        argMax(status_code,     dbt_valid_from) as status_code,
        argMax(status_name,     dbt_valid_from) as status_name,
        argMax(status_rules,    dbt_valid_from) as status_rules,
        max(dbt_valid_from)                     as current_dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('2106-02-07 06:28:15')
    group by identifier_id
),

changed as (
    select s.identifier_id
    from source s
    inner join current_in_target t on s.identifier_id = t.identifier_id
    where s._row_hash != t._row_hash
),

new_entries as (
    select s.identifier_id
    from source s
    where s.identifier_id not in (select identifier_id from current_in_target)
),

closed_records as (
    select
        t.identifier_id,
        t.person_id,
        t.status_code,
        t.status_name,
        t.status_rules,
        t._row_hash,
        0               as is_current,
        t.current_dbt_valid_from as dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c on t.identifier_id = c.identifier_id
),

new_records as (
    select
        s.identifier_id,
        s.person_id,
        s.status_code,
        s.status_name,
        s.status_rules,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where s.identifier_id in (select identifier_id from changed)
       or s.identifier_id in (select identifier_id from new_entries)
)

select * from closed_records
union all
select * from new_records

{% else %}

select
    identifier_id,
    person_id,
    status_code,
    status_name,
    status_rules,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
