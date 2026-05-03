{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(segment_chief_id, dbt_valid_from)',
) }}

with segment_chief as (
    select
        id              as segment_chief_id,
        segment_id,
        chief_id,
        valid_from      as src_valid_from,
        valid_to        as src_valid_to
    from {{ ref('stg_product__segment_chief') }}
    where is_current = 1
),
segment as (
    select id, code as segment_code
    from {{ ref('stg_product__segment') }}
),
source as (
    select
        sc.segment_chief_id,
        sc.chief_id,
        sc.segment_id,
        s.segment_code,
        sc.src_valid_from,
        sc.src_valid_to,
        MD5(concat(
            coalesce(toString(sc.chief_id),     ''), '|',
            coalesce(toString(sc.segment_id),   ''), '|',
            coalesce(toString(s.segment_code),  ''), '|',
            coalesce(toString(sc.src_valid_from),''), '|',
            coalesce(toString(sc.src_valid_to), '')
        )) as _row_hash
    from segment_chief sc
    left join segment s on sc.segment_id = s.id
),

{% if is_incremental() %}

current_in_target as (
    select
        segment_chief_id,
        argMax(_row_hash,       dbt_valid_from) as _row_hash,
        argMax(chief_id,        dbt_valid_from) as chief_id,
        argMax(segment_id,      dbt_valid_from) as segment_id,
        argMax(segment_code,    dbt_valid_from) as segment_code,
        argMax(src_valid_from,  dbt_valid_from) as src_valid_from,
        argMax(src_valid_to,    dbt_valid_from) as src_valid_to,
        max(dbt_valid_from)                     as dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('9999-12-31 00:00:00')
    group by segment_chief_id
),

changed as (
    select s.segment_chief_id
    from source s
    inner join current_in_target t on s.segment_chief_id = t.segment_chief_id
    where s._row_hash != t._row_hash
),

new_entries as (
    select s.segment_chief_id
    from source s
    left join current_in_target t on s.segment_chief_id = t.segment_chief_id
    where t.segment_chief_id is null
),

closed_records as (
    select
        t.segment_chief_id,
        t.chief_id,
        t.segment_id,
        t.segment_code,
        t.src_valid_from,
        t.src_valid_to,
        t._row_hash,
        0               as is_current,
        t.dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c on t.segment_chief_id = c.segment_chief_id
),

new_records as (
    select
        s.segment_chief_id,
        s.chief_id,
        s.segment_id,
        s.segment_code,
        s.src_valid_from,
        s.src_valid_to,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('9999-12-31 00:00:00')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where s.segment_chief_id in (select segment_chief_id from changed)
       or s.segment_chief_id in (select segment_chief_id from new_entries)
)

select * from closed_records
union all
select * from new_records

{% else %}

select
    segment_chief_id,
    chief_id,
    segment_id,
    segment_code,
    src_valid_from,
    src_valid_to,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('9999-12-31 00:00:00')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
