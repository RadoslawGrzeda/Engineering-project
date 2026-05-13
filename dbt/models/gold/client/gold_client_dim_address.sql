{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(person_id, address_type, dbt_valid_from)',
    settings={"allow_nullable_key": 1},
) }}

with address as (
    select
        person_id,
        address_type,
        argMax(option_channel,  updated_at) as option_channel,
        argMax(street,          updated_at) as street,
        argMax(zip_code,        updated_at) as zip_code,
        argMax(city,            updated_at) as city,
        argMax(country,         updated_at) as country_code,
        argMax(latitude,        updated_at) as latitude,
        argMax(longitude,       updated_at) as longitude
    from {{ ref('stg_client__address') }}
    group by person_id, address_type
),
dict_country as (
    select code, name
    from {{ ref('stg_client__dict_country') }}
),
source as (
    select
        a.person_id                                 as person_id,
        a.address_type                              as address_type,
        a.option_channel                            as option_channel,
        a.street                                    as street,
        a.zip_code                                  as zip_code,
        a.city                                      as city,
        dc.name                                     as country,
        ifNull(toFloat64(a.latitude),  0.0)         as latitude,
        ifNull(toFloat64(a.longitude), 0.0)         as longitude,
        MD5(concat(
            coalesce(toString(a.option_channel), ''), '|',
            coalesce(toString(a.street),         ''), '|',
            coalesce(toString(a.zip_code),       ''), '|',
            coalesce(toString(a.city),           ''), '|',
            coalesce(toString(a.country_code),   ''), '|',
            coalesce(toString(a.latitude),       ''), '|',
            coalesce(toString(a.longitude),      '')
        )) as _row_hash
    from address a
    left join dict_country dc on a.country_code = dc.code
)

{% if is_incremental() %}

, current_in_target as (
    select
        person_id,
        address_type,
        argMax(_row_hash,       dbt_valid_from) as _row_hash,
        argMax(option_channel,  dbt_valid_from) as option_channel,
        argMax(street,          dbt_valid_from) as street,
        argMax(zip_code,        dbt_valid_from) as zip_code,
        argMax(city,            dbt_valid_from) as city,
        argMax(country,         dbt_valid_from) as country,
        argMax(latitude,        dbt_valid_from) as latitude,
        argMax(longitude,       dbt_valid_from) as longitude,
        max(dbt_valid_from)                     as current_dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('2106-02-07 06:28:15')
    group by person_id, address_type
),

changed as (
    select s.person_id, s.address_type
    from source s
    inner join current_in_target t
        on s.person_id = t.person_id and s.address_type = t.address_type
    where s._row_hash != t._row_hash
),

new_entries as (
    select s.person_id, s.address_type
    from source s
    where (s.person_id, s.address_type) not in (select person_id, address_type from current_in_target)
),

closed_records as (
    select
        t.person_id,
        t.address_type,
        t.option_channel,
        t.street,
        t.zip_code,
        t.city,
        t.country,
        t.latitude,
        t.longitude,
        t._row_hash,
        0               as is_current,
        t.current_dbt_valid_from as dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c
        on t.person_id = c.person_id and t.address_type = c.address_type
),

new_records as (
    select
        s.person_id,
        s.address_type,
        s.option_channel,
        s.street,
        s.zip_code,
        s.city,
        s.country,
        s.latitude,
        s.longitude,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where (s.person_id, s.address_type) in (select person_id, address_type from changed)
       or (s.person_id, s.address_type) in (select person_id, address_type from new_entries)
)

select * from closed_records
union all
select * from new_records

{% else %}

select
    person_id,
    address_type,
    option_channel,
    street,
    zip_code,
    city,
    country,
    latitude,
    longitude,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
