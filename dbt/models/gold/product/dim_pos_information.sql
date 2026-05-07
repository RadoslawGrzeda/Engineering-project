{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(art_key, dbt_valid_from)',
) }}

with source_raw as (
    select
        id as pos_information_id,
        art_key,
        ean,
        vat_rate,
        price_net,
        price_gross,
        valid_from  as src_valid_from,
        updated_at,
        MD5(concat(
            coalesce(toString(art_key),     ''), '|',
            coalesce(toString(ean),         ''), '|',
            coalesce(toString(vat_rate),    ''), '|',
            coalesce(toString(price_net),   ''), '|',
            coalesce(toString(price_gross), '')
        )) as _row_hash
    from {{ ref('stg_product__pos_information') }}
),
source as (
    select
        pos_information_id,
        art_key,
        ean,
        vat_rate,
        price_net,
        price_gross,
        src_valid_from,
        _row_hash
    from (
        select
            *,
            row_number() over (
                partition by art_key
                order by updated_at desc, pos_information_id desc
            ) as _rn
        from source_raw
    )
    where _rn = 1
)

{% if is_incremental() %}

, current_in_target as (
    select
        art_key,
        argMax(pos_information_id, dbt_valid_from) as pos_information_id,
        argMax(_row_hash,       dbt_valid_from) as _row_hash,
        argMax(ean,             dbt_valid_from) as ean,
        argMax(vat_rate,        dbt_valid_from) as vat_rate,
        argMax(price_net,       dbt_valid_from) as price_net,
        argMax(price_gross,     dbt_valid_from) as price_gross,
        argMax(src_valid_from,  dbt_valid_from) as src_valid_from,
        max(dbt_valid_from)                     as current_dbt_valid_from
    from {{ this }} FINAL
    where dbt_valid_to = toDateTime('2106-02-07 06:28:15')
    group by art_key
),

changed as (
    select s.art_key
    from source s
    inner join current_in_target t on s.art_key = t.art_key
    where s._row_hash != t._row_hash
),

new_entries as (
    select s.art_key
    from source s
    left join current_in_target t on s.art_key = t.art_key
    where t.art_key is null
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
        t._row_hash,
        0               as is_current,
        t.current_dbt_valid_from as dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c on t.art_key = c.art_key
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
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where s.art_key in (select art_key from changed)
       or s.art_key in (select art_key from new_entries)
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
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
