with product as (
    select
        id as art_key,
        art_number,
        segment_id,
        department_id,
        contractor_id,
        brand,
        article_codification_date
    from {{ ref('stg_product__product') }}
),
department as (
    select
        id,
        name as department_name
    from {{ ref('stg_product__department') }}
),
segment as (
    select
        id,
        code as segment_code,
        name as segment_name,
        sector_id
    from {{ ref('stg_product__segment') }}
),
sector as (
    select
        id,
        code as sector_code,
        name as sector_name
    from {{ ref('stg_product__sector') }}
),
contractor as (
    select
        id,
        name as contractor_name
    from {{ ref('stg_product__contractor') }}
)
select
    cityHash64(p.art_key) as product_key,
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
    1 as is_current,
    now() as valid_from,
    cast(null as Nullable(DateTime)) as valid_to,
    now() as created_at,
    now() as updated_at
from product p
left join department d on p.department_id = d.id
left join segment seg on p.segment_id = seg.id
left join sector s on seg.sector_id = s.id
left join contractor c on p.contractor_id = c.id
