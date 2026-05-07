{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(art_key, dbt_valid_from)',
) }}

with product as (
    select
        assumeNotNull(id) as art_key,
        argMax(art_number,                  updated_at) as art_number,
        argMax(segment_id,                  updated_at) as segment_id,
        argMax(department_id,               updated_at) as department_id,
        argMax(contractor_id,               updated_at) as contractor_id,
        argMax(brand,                       updated_at) as brand,
        argMax(article_codification_date,   updated_at) as article_codification_date
    from {{ ref('stg_product__product') }}
    group by id
),
department as (
    select id, name as department_name
    from {{ ref('stg_product__department') }}
),
segment as (
    select id, code as segment_code, name as segment_name, sector_id
    from {{ ref('stg_product__segment') }}
),
sector as (
    select id, code as sector_code, name as sector_name
    from {{ ref('stg_product__sector') }}
),
contractor as (
    select id, name as contractor_name
    from {{ ref('stg_product__contractor') }}
),
source as (
    select
        p.art_key,
        p.art_number,
        p.brand,
        p.article_codification_date,
        d.department_name,
        s.sector_code,
        s.sector_name,
        seg.segment_code,
        seg.segment_name,
        c.contractor_name,
        MD5(concat(
            coalesce(toString(p.art_number),               ''), '|',
            coalesce(toString(p.brand),                    ''), '|',
            coalesce(toString(p.article_codification_date),''), '|',
            coalesce(toString(d.department_name),          ''), '|',
            coalesce(toString(s.sector_code),              ''), '|',
            coalesce(toString(s.sector_name),              ''), '|',
            coalesce(toString(seg.segment_code),           ''), '|',
            coalesce(toString(seg.segment_name),           ''), '|',
            coalesce(toString(c.contractor_name),          '')
        )) as _row_hash
    from product p
    left join department d   on p.department_id = d.id
    left join segment seg    on p.segment_id = seg.id
    left join sector s       on seg.sector_id = s.id
    left join contractor c   on p.contractor_id = c.id
)

{% if is_incremental() %}

, current_in_target as (
    select
        art_key,
        argMax(_row_hash,                   dbt_valid_from) as _row_hash,
        argMax(art_number,                  dbt_valid_from) as art_number,
        argMax(brand,                       dbt_valid_from) as brand,
        argMax(article_codification_date,   dbt_valid_from) as article_codification_date,
        argMax(department_name,             dbt_valid_from) as department_name,
        argMax(sector_code,                 dbt_valid_from) as sector_code,
        argMax(sector_name,                 dbt_valid_from) as sector_name,
        argMax(segment_code,                dbt_valid_from) as segment_code,
        argMax(segment_name,                dbt_valid_from) as segment_name,
        argMax(contractor_name,             dbt_valid_from) as contractor_name,
        max(dbt_valid_from)                                 as current_dbt_valid_from
    from {{ this }} 
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
        t.art_key,
        t.art_number,
        t.brand,
        t.article_codification_date,
        t.department_name,
        t.sector_code,
        t.sector_name,
        t.segment_code,
        t.segment_name,
        t.contractor_name,
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
        s.art_key,
        s.art_number,
        s.brand,
        s.article_codification_date,
        s.department_name,
        s.sector_code,
        s.sector_name,
        s.segment_code,
        s.segment_name,
        s.contractor_name,
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
    art_key,
    art_number,
    brand,
    article_codification_date,
    department_name,
    sector_code,
    sector_name,
    segment_code,
    segment_name,
    contractor_name,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
