{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(chief_id, dbt_valid_from)',
) }}

with chief as (
    select
        id              as chief_id,
        argMax(first_name,   updated_at) as first_name,
        argMax(last_name,    updated_at) as last_name,
        argMax(phone_number, updated_at) as phone_number,
        argMax(email,        updated_at) as email
    from {{ ref('stg_product__chief') }}
    group by id
),
source as (
    select
        chief_id,
        first_name,
        last_name,
        phone_number,
        email,
        MD5(concat(
            coalesce(toString(first_name),   ''), '|',
            coalesce(toString(last_name),    ''), '|',
            coalesce(toString(phone_number), ''), '|',
            coalesce(toString(email),        '')
        )) as _row_hash
    from chief
)

{% if is_incremental() %}

, current_in_target as (
    select
        chief_id,
        argMax(_row_hash,       dbt_valid_from) as _row_hash,
        argMax(first_name,      dbt_valid_from) as first_name,
        argMax(last_name,       dbt_valid_from) as last_name,
        argMax(phone_number,    dbt_valid_from) as phone_number,
        argMax(email,           dbt_valid_from) as email,
        max(dbt_valid_from)                     as current_dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('2106-02-07 06:28:15')
    group by chief_id
),

changed as (
    select s.chief_id
    from source s
    inner join current_in_target t on s.chief_id = t.chief_id
    where s._row_hash != t._row_hash
),

new_entries as (
    select s.chief_id
    from source s
    where s.chief_id not in (select chief_id from current_in_target)
),

closed_records as (
    select
        t.chief_id,
        t.first_name,
        t.last_name,
        t.phone_number,
        t.email,
        t._row_hash,
        0               as is_current,
        t.current_dbt_valid_from as dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c on t.chief_id = c.chief_id
),

new_records as (
    select
        s.chief_id,
        s.first_name,
        s.last_name,
        s.phone_number,
        s.email,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where s.chief_id in (select chief_id from changed)
       or s.chief_id in (select chief_id from new_entries)
)

select * from closed_records
union all
select * from new_records

{% else %}

select
    chief_id,
    first_name,
    last_name,
    phone_number,
    email,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
