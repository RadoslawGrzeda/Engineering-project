{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(person_id, indicator_type, dbt_valid_from)',
) }}

with indicator as (
    select
        person_id,
        indicator       as indicator_type,
        argMax(is_active, updated_at) as is_active
    from {{ ref('stg_client__customer_indicator') }}
    group by person_id, indicator
),
dict_indicator as (
    select
        indicator_type,
        description,
        rules
    from {{ ref('stg_client__dict_indicator') }}
),
source as (
    select
        i.person_id,
        i.indicator_type,
        di.description,
        di.rules,
        i.is_active,
        MD5(concat(
            coalesce(toString(i.is_active), '')
        )) as _row_hash
    from indicator i
    left join dict_indicator di on i.indicator_type = di.indicator_type
)

{% if is_incremental() %}

, current_in_target as (
    select
        person_id,
        indicator_type,
        argMax(_row_hash,       dbt_valid_from) as _row_hash,
        argMax(description,     dbt_valid_from) as description,
        argMax(rules,           dbt_valid_from) as rules,
        argMax(is_active,       dbt_valid_from) as is_active,
        max(dbt_valid_from)                     as current_dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('2106-02-07 06:28:15')
    group by person_id, indicator_type
),

changed as (
    select s.person_id, s.indicator_type
    from source s
    inner join current_in_target t
        on s.person_id = t.person_id and s.indicator_type = t.indicator_type
    where s._row_hash != t._row_hash
),

new_entries as (
    select s.person_id, s.indicator_type
    from source s
    where (s.person_id, s.indicator_type) not in (select person_id, indicator_type from current_in_target)
),

closed_records as (
    select
        t.person_id,
        t.indicator_type,
        t.description,
        t.rules,
        t.is_active,
        t._row_hash,
        0               as is_current,
        t.current_dbt_valid_from as dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c
        on t.person_id = c.person_id and t.indicator_type = c.indicator_type
),

new_records as (
    select
        s.person_id,
        s.indicator_type,
        s.description,
        s.rules,
        s.is_active,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where (s.person_id, s.indicator_type) in (select person_id, indicator_type from changed)
       or (s.person_id, s.indicator_type) in (select person_id, indicator_type from new_entries)
)

select * from closed_records
union all
select * from new_records

{% else %}

select
    person_id,
    indicator_type,
    description,
    rules,
    is_active,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
