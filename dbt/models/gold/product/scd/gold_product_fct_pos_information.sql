{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(pos_information_id, dbt_valid_from)',
) }}

with source as (
    select
        id as pos_information_id,
        art_key,
        ean,
        vat_rate,
        price_net,
        price_gross,
        valid_from  as src_valid_from,
        valid_to    as src_valid_to,
        MD5(concat(
            coalesce(toString(art_key),     ''), '|',
            coalesce(toString(ean),         ''), '|',
            coalesce(toString(vat_rate),    ''), '|',
            coalesce(toString(price_net),   ''), '|',
            coalesce(toString(price_gross), '')
        )) as _row_hash
    from {{ ref('stg_product__pos_information') }}
    where is_current = 1
),

{% if is_incremental() %}

current_in_target as (
    select
        pos_information_id,
        argMax(_row_hash,           dbt_valid_from) as _row_hash,
        argMax(art_key,             dbt_valid_from) as art_key,
        argMax(ean,                 dbt_valid_from) as ean,
        argMax(vat_rate,            dbt_valid_from) as vat_rate,
        argMax(price_net,           dbt_valid_from) as price_net,
        argMax(price_gross,         dbt_valid_from) as price_gross,
        argMax(src_valid_from,      dbt_valid_from) as src_valid_from,
        argMax(src_valid_to,        dbt_valid_from) as src_valid_to,
        max(dbt_valid_from)                         as dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('9999-12-31 00:00:00')
    group by pos_information_id
),

changed as (
    select s.pos_information_id
    from source s
    inner join current_in_target t on s.pos_information_id = t.pos_information_id
    where s._row_hash != t._row_hash
),

new_entries as (
    select s.pos_information_id
    from source s
    left join current_in_target t on s.pos_information_id = t.pos_information_id
    where t.pos_information_id is null
),

closed_records as (
    select
        t.pos_information_id,
        t.art_key,
        t.ean,
        t.vat_rate,
        t.price_net,
        t.price_gross,
        t.src_valid_from,
        t.src_valid_to,
        t._row_hash,
        0               as is_current,
        t.dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c on t.pos_information_id = c.pos_information_id
),

new_records as (
    select
        s.pos_information_id,
        s.art_key,
        s.ean,
        s.vat_rate,
        s.price_net,
        s.price_gross,
        s.src_valid_from,
        s.src_valid_to,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('9999-12-31 00:00:00')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where s.pos_information_id in (select pos_information_id from changed)
       or s.pos_information_id in (select pos_information_id from new_entries)
)

select * from closed_records
union all
select * from new_records

{% else %}

select
    pos_information_id,
    art_key,
    ean,
    vat_rate,
    price_net,
    price_gross,
    src_valid_from,
    src_valid_to,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('9999-12-31 00:00:00')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
